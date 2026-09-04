# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Run the TPC-H benchmark against Ballista over Arrow Flight SQL.

Unlike `tpch.py`, this uses no Ballista client library at all -- only the
generic ADBC Flight SQL driver. It therefore measures the same path a BI tool
or JDBC client would take, and doubles as an end-to-end exercise of the
scheduler's Flight SQL frontend.

Prerequisites
-------------

    pip install adbc-driver-flightsql pyarrow

The frontend is a non-default feature and is also off at runtime:

    cargo build --release -p ballista-scheduler --features flight-sql
    cargo build --release -p ballista-executor

    ./target/release/ballista-scheduler --flight-sql &
    ./target/release/ballista-executor -c 8 -p 50051 &

Generate data with `./tpch-gen.sh`, then:

    python tpch_adbc.py --path /mnt/bigdata/tpch/sf1-parquet

Timings
-------

Two numbers are reported per query, because they measure different things:

  plan+exec  GetFlightInfo -- the scheduler plans the query, runs it on the
             cluster, and only then replies. This is where the time goes.
  fetch      DoGet over every endpoint -- pulling the result back through the
             scheduler. TPC-H answers are small, so this is usually noise.

Ballista does not implement PollFlightInfo yet, so GetFlightInfo blocks for the
whole query. On anything but a direct connection you will need --timeout, and
an intervening proxy or load balancer may cut the connection regardless.
"""

import argparse
import json
import os
import statistics
import sys
import time

import adbc_driver_flightsql.dbapi as flight_sql

# Same order as TABLES in src/bin/tpch.rs.
TABLES = [
    "part",
    "supplier",
    "partsupp",
    "customer",
    "orders",
    "lineitem",
    "nation",
    "region",
]

ALL_QUERIES = list(range(1, 23))


def table_location(path, table, ext):
    """Resolve a table's location, mirroring `find_path` in benchmarks/src/lib.rs.

    Object-store URLs cannot be probed on the local filesystem, so the
    per-table directory is used directly; the trailing slash marks it as a
    directory so the listing table enumerates the files inside.
    """
    if "://" in path:
        return f"{path}/{table}/"

    as_file = os.path.join(path, f"{table}.{ext}")
    as_dir = os.path.join(path, table)
    if os.path.exists(as_file):
        return as_file
    if os.path.exists(as_dir):
        return as_dir
    raise SystemExit(f"could not find {ext} files at {as_file} or {as_dir}")


def create_table_sql(table, location, file_format):
    if file_format == "parquet":
        return f"CREATE EXTERNAL TABLE {table} STORED AS PARQUET LOCATION '{location}'"
    if file_format == "csv":
        return (
            f"CREATE EXTERNAL TABLE {table} STORED AS CSV LOCATION '{location}' "
            f"OPTIONS ('format.has_header' 'true')"
        )
    raise SystemExit(
        f"unsupported format '{file_format}'. Use parquet, or csv with a header "
        f"row; TPC-H .tbl files carry no schema, so use the Rust harness "
        f"(src/bin/tpch.rs) for those."
    )


def query_statements(query, queries_dir):
    """Split a query file into statements, as the Rust harness does.

    q15 is the reason this matters: it creates a view, selects from it, and
    drops it again, so the file holds three statements.
    """
    candidates = (
        [os.path.join(queries_dir, f"q{query}.sql")]
        if queries_dir
        else [f"queries/q{query}.sql", f"benchmarks/queries/q{query}.sql"]
    )
    for filename in candidates:
        try:
            with open(filename) as f:
                contents = f.read()
        except OSError:
            continue
        return [s.strip() for s in contents.split(";") if s.strip()]
    raise SystemExit(f"could not find query {query} in {candidates}")


def answer_index(statements):
    """Index of the statement holding the answer.

    Mirrors `answer_statement_index` in benchmarks/src/lib.rs: the last SELECT
    or WITH, or the last statement if neither appears.
    """
    for i in reversed(range(len(statements))):
        head = statements[i].lstrip().lower()
        if head.startswith("select") or head.startswith("with"):
            return i
    return len(statements) - 1


def run_statement(cursor, sql):
    """Execute one statement, returning (plan+exec seconds, fetch seconds, table).

    The result is always fetched, even when it is discarded: an unredeemed
    ticket pins its result on the scheduler until the handle expires.
    """
    started = time.monotonic()
    cursor.execute(sql)
    executed = time.monotonic()
    table = cursor.fetch_arrow_table()
    fetched = time.monotonic()
    return executed - started, fetched - executed, table


def run_query(conn, query, statements, debug):
    answer = answer_index(statements)
    exec_secs = 0.0
    fetch_secs = 0.0
    rows = 0

    with conn.cursor() as cursor:
        for i, sql in enumerate(statements):
            if debug:
                print(f"  executing: {sql.splitlines()[0]}...")
            one_exec, one_fetch, table = run_statement(cursor, sql)
            exec_secs += one_exec
            fetch_secs += one_fetch
            if i == answer:
                rows = table.num_rows

    return exec_secs, fetch_secs, rows


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--scheduler",
        default="grpc://localhost:50050",
        help="Flight SQL endpoint of the Ballista scheduler",
    )
    parser.add_argument("--path", required=True, help="path or URL to the TPC-H data")
    parser.add_argument(
        "--format",
        default="parquet",
        choices=["parquet", "csv"],
        help="file format of the data",
    )
    parser.add_argument(
        "--query",
        default="all",
        help="comma-separated query numbers, or 'all'",
    )
    parser.add_argument(
        "--iterations", type=int, default=1, help="times to run each query"
    )
    parser.add_argument(
        "--queries-dir", help="directory holding q1.sql .. q22.sql"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        help="per-RPC timeout in seconds. GetFlightInfo blocks for the whole "
        "query, so this must exceed your slowest query or it will be cut off",
    )
    parser.add_argument("--username", help="username for the Flight handshake")
    parser.add_argument("--password", help="password for the Flight handshake")
    parser.add_argument("--output", help="write results to this file as JSON")
    parser.add_argument("--debug", action="store_true", help="print each statement")
    args = parser.parse_args()

    if args.query == "all":
        queries = ALL_QUERIES
    else:
        queries = [int(q) for q in args.query.split(",")]

    db_kwargs = {}
    if args.timeout is not None:
        # Applies to GetFlightInfo and to the DoGets that follow it.
        db_kwargs["adbc.flight.sql.rpc.timeout_seconds.query"] = str(args.timeout)
        db_kwargs["adbc.flight.sql.rpc.timeout_seconds.fetch"] = str(args.timeout)
    if args.username is not None:
        db_kwargs["username"] = args.username
    if args.password is not None:
        db_kwargs["password"] = args.password

    results = []

    with flight_sql.connect(args.scheduler, db_kwargs=db_kwargs) as conn:
        # DDL runs on the scheduler and lands in this connection's catalog, so
        # every query below sees these tables. Executors read the files
        # themselves, so the path must be readable from every node.
        for table in TABLES:
            location = table_location(args.path, table, args.format)
            print(f"Registering table {table} at {location}")
            with conn.cursor() as cursor:
                # Unauthenticated clients share one session, so a scheduler that
                # has already served this benchmark still has the tables. Drop
                # them first, or the second run dies on "already exists".
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                cursor.fetch_arrow_table()
                cursor.execute(create_table_sql(table, location, args.format))
                cursor.fetch_arrow_table()

        for query in queries:
            statements = query_statements(query, args.queries_dir)
            elapsed = []

            for iteration in range(args.iterations):
                exec_secs, fetch_secs, rows = run_query(
                    conn, query, statements, args.debug
                )
                total = exec_secs + fetch_secs
                elapsed.append(total)
                print(
                    f"Query {query} iteration {iteration} took {total * 1000:.1f} ms "
                    f"(plan+exec {exec_secs * 1000:.1f} ms, "
                    f"fetch {fetch_secs * 1000:.1f} ms, {rows} rows)"
                )
                results.append(
                    {
                        "query": query,
                        "iteration": iteration,
                        "elapsed_ms": total * 1000,
                        "plan_exec_ms": exec_secs * 1000,
                        "fetch_ms": fetch_secs * 1000,
                        "row_count": rows,
                    }
                )

            if args.iterations > 1:
                print(
                    f"Query {query}: min {min(elapsed) * 1000:.1f} ms, "
                    f"mean {statistics.mean(elapsed) * 1000:.1f} ms"
                )

    total_min = sum(
        min(r["elapsed_ms"] for r in results if r["query"] == q) for q in queries
    )
    print(f"Total (sum of per-query minimums): {total_min:.1f} ms")

    if args.output:
        with open(args.output, "w") as f:
            json.dump({"queries": results, "total_min_ms": total_min}, f, indent=2)
        print(f"Wrote {args.output}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
