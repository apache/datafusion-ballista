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

"""Run distributed queries on Ballista from Python over Arrow Flight SQL.

This uses only the generic ADBC Flight SQL driver -- there is no Ballista
Python package involved. Any Flight SQL client would work the same way.

Prerequisites
-------------

    pip install adbc-driver-flightsql pyarrow

Build a scheduler with the (non-default) Flight SQL frontend compiled in, and
start a cluster:

    cargo build --release -p ballista-scheduler --features flight-sql
    cargo build --release -p ballista-executor

    ./target/release/ballista-scheduler --flight-sql &
    ./target/release/ballista-executor -c 4 -p 50051 &

Then:

    python examples/python/adbc_flight_sql.py

Note that a scheduler started this way performs no authentication: anyone who
can reach port 50050 can run queries. See
docs/source/user-guide/flightsql.md for how to plug in an authenticator.
"""

import argparse
import os
import sys

import adbc_driver_flightsql.dbapi as flight_sql

# The repo's own sample data, so the example runs without any setup.
DEFAULT_DATA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "..",
    "examples",
    "testdata",
    "aggregate_test_100.csv",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scheduler",
        default="grpc://localhost:50050",
        help="Flight SQL endpoint of the Ballista scheduler",
    )
    parser.add_argument(
        "--data",
        default=os.path.normpath(DEFAULT_DATA),
        help="CSV file to register as a table",
    )
    args = parser.parse_args()

    if not os.path.exists(args.data):
        print(f"no such file: {args.data}", file=sys.stderr)
        return 1

    # Executors read the file themselves, so the path must be readable from
    # every node. That is trivially true for a local single-machine cluster;
    # use object storage for a real one.
    data = os.path.abspath(args.data)

    with flight_sql.connect(args.scheduler) as conn:
        with conn.cursor() as cur:
            # DDL is executed on the scheduler and registers the table in this
            # connection's catalog.
            # Unauthenticated clients share one session, so a scheduler that
            # has already run this example still has the table registered.
            cur.execute("DROP TABLE IF EXISTS test")
            cur.fetch_arrow_table()
            cur.execute(
                f"""
                CREATE EXTERNAL TABLE test
                STORED AS CSV
                LOCATION '{data}'
                OPTIONS ('format.has_header' 'true')
                """
            )
            cur.fetch_arrow_table()

            # This query is planned by the scheduler and executed across the
            # cluster; results stream back through the scheduler, one Flight
            # endpoint per output partition.
            cur.execute(
                """
                SELECT c1, MIN(c12) AS min_c12, MAX(c12) AS max_c12
                FROM test
                WHERE c11 > 0.1 AND c11 < 0.9
                GROUP BY c1
                ORDER BY c1
                """
            )
            table = cur.fetch_arrow_table()

        print(table)
        print()

        # Catalog introspection is answered from the scheduler's real
        # DataFusion catalog, which is what BI tools browse.
        print("table types:", conn.adbc_get_table_types())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
