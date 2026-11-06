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

"""Run the TPC-H suite against a DataFusion Distributed cluster and print the timings as JSON.

Runs in the cluster as the runner Job that run.py creates. It talks
to the coordinator, which runs the `worker` binary from datafusion-distributed's
benchmarks/remote-worker crate. That binary answers two HTTP requests:

  GET /?sql=<statements>  runs the statements in order and executes the last one.
                          The answer is JSON with the number of rows, the time it
                          took (physical planning and execution), the number of
                          tasks and the distributed plan with metrics. Not the rows.
  GET /info               the workers it can reach, and the commit it was built from.

It uses only the standard library, so it runs in a stock python image.
Configuration comes from environment variables (see config_from_env).

Steps:
  1. Wait until the coordinator answers and reaches the expected number of workers.
  2. Register the eight tables over the Parquet data (CREATE EXTERNAL TABLE IF NOT
     EXISTS, with the partition columns declared so that queries prune partitions)
     and apply the SET statements. The catalog lives in the coordinator's memory,
     so this happens on every run, and again after a failed query in case the
     coordinator was restarted. Each pod has a memory pool and spills past it, but
     memory outside the pool can still get a pod OOM-killed and restarted.
  3. Check each table: it has rows, a sample has no NULLs (a column that is misnamed
     in tpch_tables.py reads as NULL) and no partition value is still
     percent-encoded (p_brand=Brand%2313 must read as Brand#13).
  4. Run every selected query once, untimed (unless WARMUP=0). The first run reads
     the Parquet footers and fills the caches on the coordinator and the workers.
  5. Run each query ITERATIONS times and record the client's wall-clock time, plus
     the time the coordinator reports. Every execution's row count is checked
     against the expected one (see tpch_expected_rows.py); a difference fails the
     query.

The queries are the files in benchmarks/queries, the ones every engine runs. A query
file may hold several statements: q15 creates a view, selects from it and drops it.
The coordinator executes only the last statement of a request, so each statement is
sent on its own and only the SELECT is timed.

Output: a line "DFD-QUERY-RESULT {json}" as each query finishes, so a run that dies
part way can still be recovered from the log, then "===RESULTS-JSON-FILE===" followed
by the full summary. run.py reads that from the pod log. The exit status is non-zero
if setup or any query failed.
"""

import http.client
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

try:
    from tpch_expected_rows import EXPECTED_ROWS
    from tpch_tables import (
        DEFAULT_MONEY_TYPE,
        MONEY,
        TPCH_TABLES,
        VARCHAR,
        partition_column,
    )
except (
    ImportError
):  # run from the source tree: tpch_tables.py is in ../trino, the row counts in ..
    _here = os.path.dirname(os.path.abspath(__file__))
    sys.path[:0] = [os.path.join(_here, ".."), os.path.join(_here, "..", "trino")]
    from tpch_expected_rows import EXPECTED_ROWS
    from tpch_tables import (
        DEFAULT_MONEY_TYPE,
        MONEY,
        TPCH_TABLES,
        VARCHAR,
        partition_column,
    )

RESULTS_MARKER = "===RESULTS-JSON-FILE==="
QUERY_RESULT_PREFIX = "DFD-QUERY-RESULT "

# Registering a table lists its files and reads every Parquet footer for the
# statistics; lineitem and orders have about 2,500 files each.
SETUP_TIMEOUT_S = 30 * 60

# Rows read from each table to check that its columns are not NULL.
SAMPLE_ROWS = 10000


class QueryFailed(Exception):
    pass


class QueryTimedOut(Exception):
    pass


class WrongResult(Exception):
    """A query returned a different number of rows than it should."""


class SetupError(Exception):
    pass


@dataclass
class QueryResult:
    row_count: int
    elapsed: float
    server_elapsed: float
    tasks: int
    plan: str


class Coordinator:
    """Client for the worker binary's HTTP endpoint."""

    def __init__(self, base_url, opener=None):
        self.base_url = base_url.rstrip("/")
        # The coordinator is a cluster-internal address; never go through a proxy.
        # `opener` is only replaced by the tests.
        self._opener = opener or urllib.request.build_opener(
            urllib.request.ProxyHandler({})
        )

    def _get(self, url, timeout_s):
        try:
            with self._opener.open(
                urllib.request.Request(url), timeout=timeout_s
            ) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", "replace").strip()[:2000]
            e.close()
            raise QueryFailed(f"HTTP {e.code}: {detail}")
        except TimeoutError:
            raise QueryTimedOut(f"no answer within {timeout_s}s")
        except urllib.error.URLError as e:
            if isinstance(e.reason, TimeoutError):
                raise QueryTimedOut(f"no answer within {timeout_s}s")
            raise QueryFailed(f"cannot reach the coordinator: {e.reason}")
        # A coordinator that is OOM-killed mid-query drops the connection.
        except (OSError, http.client.HTTPException, ValueError) as e:
            raise QueryFailed(f"{type(e).__name__}: {e}")

    def info(self, timeout_s=30):
        return self._get(f"{self.base_url}/info", timeout_s)

    def run(self, sql, timeout_s=3600):
        """Send statements; the coordinator executes the last one.

        There is no cancel call: on a timeout the runner only stops waiting and
        closes the connection.
        """
        url = f"{self.base_url}/?{urllib.parse.urlencode({'sql': sql})}"
        start = time.monotonic()
        doc = self._get(url, timeout_s)
        try:
            return QueryResult(
                row_count=doc["count"],
                elapsed=time.monotonic() - start,
                server_elapsed=doc["elapsed_ms"] / 1000,
                tasks=doc.get("tasks", 0),
                plan=doc.get("plan", ""),
            )
        except (KeyError, TypeError, AttributeError):
            raise QueryFailed(f"unexpected answer: {str(doc)[:500]}")


def log(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def config_from_env(env=os.environ):
    settings = {}
    for pair in filter(None, env.get("SETTINGS", "").split(",")):
        key, _, value = pair.partition("=")
        settings[key.strip()] = value.strip()
    scale_factor = int(env.get("SCALE_FACTOR", "1000"))
    check_rows = env.get("CHECK_ROWS", "1") == "1"
    return {
        "coordinator_url": env.get("COORDINATOR_URL", "http://localhost:9000"),
        "data_path": env.get("DATA_PATH", ""),
        "partitioned": env.get("PARTITIONED", "0") == "1",
        "money_type": env.get("MONEY_TYPE", DEFAULT_MONEY_TYPE),
        "expected_workers": int(env.get("EXPECTED_WORKERS", "1")),
        "iterations": int(env.get("ITERATIONS", "2")),
        "queries": [q for q in env.get("QUERIES", "").split(",") if q.strip()],
        "queries_dir": env.get("QUERIES_DIR", "/opt/bench/queries"),
        "warmup": env.get("WARMUP", "1") == "1",
        "query_timeout_s": int(env.get("QUERY_TIMEOUT_S", "3600")),
        "settings": settings,
        # Row counts to check, by query number. Empty when checking is off or
        # the scale factor has no table in tpch_expected_rows.py.
        "scale_factor": scale_factor,
        "expected_rows": EXPECTED_ROWS.get(scale_factor, {}) if check_rows else {},
    }


def table_ddl(table, data_path, partitioned=False, money_type=DEFAULT_MONEY_TYPE):
    """CREATE EXTERNAL TABLE IF NOT EXISTS for one table over the existing Parquet files.

    The columns and types are the ones Trino declares (trino/tpch_tables.py);
    DataFusion takes the same type names. In the partitioned layout, declaring the
    partition column makes its value come from the directory name
    (l_shipdate=1994-01-01/) and lets a filter on it skip whole directories when
    the query is planned, as Spark and Trino do. The files may hold the column too;
    DataFusion then ignores their copy.
    """
    name, _, columns = next(t for t in TPCH_TABLES if t[0] == table)
    partition_col = partition_column(table, partitioned)
    column_sql = ",\n  ".join(
        f"{col} {money_type if typ == MONEY else typ}" for col, typ in columns
    )
    partitioned = f"\nPARTITIONED BY ({partition_col})" if partition_col else ""
    location = f"{data_path.rstrip('/')}/{name}/".replace("'", "''")
    return (
        f"CREATE EXTERNAL TABLE IF NOT EXISTS {name} (\n  {column_sql}\n)\n"
        f"STORED AS PARQUET{partitioned}\nLOCATION '{location}'"
    )


def setting_statements(settings):
    return [f"SET {key} = {value}" for key, value in settings.items()]


def query_number(name):
    """q01, q1, Q1 and 1 all mean query 1."""
    match = re.fullmatch(r"[qQ]?0*(\d+)", name.strip())
    if not match:
        raise ValueError(f"not a query name: {name!r}")
    return int(match.group(1))


def first_keyword(statement):
    """The statement's first word, skipping comments, in lower case."""
    text = re.sub(r"--[^\n]*|/\*.*?\*/", " ", statement, flags=re.DOTALL).split()
    return text[0].lower() if text else ""


def split_statements(sql):
    """(statements to run before, the query to time, statements to run after).

    The coordinator executes only the last statement of a request, so a query file
    like q15 (CREATE VIEW; SELECT; DROP VIEW) is sent as three requests and only the
    SELECT is timed.
    """
    statements = [s.strip() for s in sql.split(";") if s.strip() and first_keyword(s)]
    queries = [
        i
        for i, s in enumerate(statements)
        if first_keyword(s) not in ("create", "drop")
    ]
    if len(queries) != 1:
        raise ValueError(
            f"expected exactly one query among {len(statements)} statements"
        )
    i = queries[0]
    return statements[:i], statements[i], statements[i + 1 :]


def load_queries(queries_dir, selected):
    """Return [(number, sql)] for the selected queries, or all qNN.sql files."""
    available = {}
    for fname in os.listdir(queries_dir):
        match = re.fullmatch(r"q0*(\d+)\.sql", fname)
        if match:
            available[int(match.group(1))] = os.path.join(queries_dir, fname)
    numbers = [query_number(q) for q in selected] if selected else sorted(available)
    missing = [n for n in numbers if n not in available]
    if missing:
        raise FileNotFoundError(f"no query file for q{missing} in {queries_dir}")
    queries = []
    for n in numbers:
        with open(available[n]) as f:
            sql = f.read()
        split_statements(
            sql
        )  # fail at setup, not after the warm-up, on a file it can't run
        queries.append((n, sql))
    return queries


def wait_for_workers(client, expected, timeout_s=900):
    """Wait until the coordinator reaches the expected number of workers; return /info."""
    deadline = time.monotonic() + timeout_s
    found = 0
    while True:
        try:
            info = client.info()
        except (QueryFailed, QueryTimedOut) as e:
            log(f"waiting for coordinator: {e}")
        else:
            found = len(info.get("worker_urls", []))
            errors = info.get("errors") or []
            log(
                f"workers reachable: {found}/{expected}"
                + (f"; {errors[0]}" if errors else "")
            )
            if found >= expected:
                return info
        if time.monotonic() > deadline:
            raise RuntimeError(
                f"only {found}/{expected} workers reachable after {timeout_s}s"
            )
        time.sleep(5)


def check_table_readable(client, table, columns, partition_col):
    """Fail unless the table has rows, no NULLs in a sample and decoded partition values.

    TPC-H has no NULLs. DataFusion matches Parquet columns by name and reads a column
    a file does not have as NULL, so a column misnamed in tpch_tables.py would go
    unnoticed. The coordinator only reports how many rows a query returned, so each
    check is a query whose row count is the answer. `columns` is [(name, type)].
    """
    if client.run(f"SELECT 1 FROM {table} LIMIT 1", SETUP_TIMEOUT_S).row_count == 0:
        raise SetupError(
            f"{table} has no rows: no Parquet files were found under its location"
        )
    names = [name for name, _ in columns]
    sample = f"(SELECT * FROM {table} LIMIT {SAMPLE_ROWS})"
    any_null = " OR ".join(f"{name} IS NULL" for name in names)
    if client.run(
        f"SELECT 1 FROM {sample} WHERE {any_null}", SETUP_TIMEOUT_S
    ).row_count:
        null_columns = [
            name
            for name in names
            if client.run(
                f"SELECT 1 FROM {sample} WHERE {name} IS NULL LIMIT 1", SETUP_TIMEOUT_S
            ).row_count
        ]
        raise SetupError(
            f"{table}: NULLs in a sample of {', '.join(null_columns) or 'some columns'}. TPC-H has no "
            f"NULLs, so the Parquet files probably name these columns differently from tpch_tables.py"
        )
    if partition_col and dict(columns)[partition_col] == VARCHAR:
        sql = f"SELECT 1 FROM {table} WHERE strpos({partition_col}, '%') > 0 LIMIT 1"
        if client.run(sql, SETUP_TIMEOUT_S).row_count:
            raise SetupError(
                f"{table}: {partition_col} values are still percent-encoded "
                f"(Brand%2313 instead of Brand#13); this DataFusion does not decode them"
            )


def register_tables(client, cfg):
    """Register the tables and apply the settings. Idempotent. Returns seconds per table."""
    seconds = {}
    for name, _, _ in TPCH_TABLES:
        t0 = time.monotonic()
        client.run(
            table_ddl(name, cfg["data_path"], cfg["partitioned"], cfg["money_type"]),
            SETUP_TIMEOUT_S,
        )
        seconds[name] = round(time.monotonic() - t0, 1)
    for statement in setting_statements(cfg["settings"]):
        client.run(statement, SETUP_TIMEOUT_S)
    return seconds


def setup(client, cfg):
    started = time.monotonic()
    seconds = register_tables(client, cfg)
    for name, _, columns in TPCH_TABLES:
        log(f"{name}: registered in {seconds[name]}s")
        check_table_readable(
            client, name, columns, partition_column(name, cfg["partitioned"])
        )
    return {"tables": seconds, "seconds": round(time.monotonic() - started, 1)}


def recover(client, cfg):
    """After a failed query, get the cluster back into the state the queries expect.

    The failure may have been a pod that was OOM-killed and restarted. A restarted
    coordinator has lost its tables and settings, and a restarted worker is only
    used again once it is ready. Returns an error message, or None.
    """
    try:
        wait_for_workers(client, cfg["expected_workers"])
        register_tables(client, cfg)
    except (QueryFailed, QueryTimedOut, RuntimeError) as e:
        return f"{type(e).__name__}: {e}"
    return None


def describe_error(e):
    return f"{type(e).__name__}: {e}"[:2000]


def execute_query(client, number, sql, cfg):
    """One execution of a benchmark query, failing if it returns the wrong number of rows.

    Only the query itself is timed; statements around it (a view) are sent separately.
    """
    before, query, after = split_statements(sql)
    for statement in before:
        client.run(statement, cfg["query_timeout_s"])
    result = client.run(query, cfg["query_timeout_s"])
    for statement in after:
        client.run(statement, cfg["query_timeout_s"])
    expected = cfg["expected_rows"].get(number)
    if expected is not None and result.row_count != expected:
        raise WrongResult(
            f"q{number} returned {result.row_count} rows, expected {expected}"
        )
    return result


def iteration_record(result):
    return {
        "elapsed": round(result.elapsed, 3),
        "server_elapsed": round(result.server_elapsed, 3),
        "row_count": result.row_count,
        "tasks": result.tasks,
    }


def warm_up(client, queries, cfg):
    """Run each query once, untimed, before the timed iterations.

    The first run of a query reads Parquet footers and file listings that later
    runs find in the coordinator's and workers' caches. Returns
    {query number: {"elapsed": seconds or None, "error": message or None}}.
    """
    warmups = {}
    for number, sql in queries:
        try:
            result = execute_query(client, number, sql, cfg)
        except (QueryFailed, QueryTimedOut, WrongResult) as e:
            warmups[number] = {"elapsed": None, "error": describe_error(e)}
            log(f"q{number} warm-up FAILED: {warmups[number]['error']}")
            if not isinstance(e, WrongResult):
                recovery_error = recover(client, cfg)
                if recovery_error:
                    log(f"cluster did not recover: {recovery_error}")
        else:
            warmups[number] = {"elapsed": round(result.elapsed, 3), "error": None}
            log(f"q{number} warm-up: {result.elapsed:.2f}s, {result.row_count} rows")
    return warmups


def run_query(client, number, sql, cfg, warmup=None):
    """Run one query cfg['iterations'] times. Stops at the first failed iteration.

    `warmup` is this query's result from warm_up. A query that failed there is not
    run again: it would only fail again, slowly. The record keeps the plan, with
    metrics, of the last iteration.
    """
    record = {"query": number, "iterations": [], "error": None}
    if warmup is not None:
        record["warmup"] = warmup
        record["error"] = warmup["error"]
    if record["error"] is None:
        for i in range(cfg["iterations"]):
            try:
                result = execute_query(client, number, sql, cfg)
            except (QueryFailed, QueryTimedOut, WrongResult) as e:
                record["error"] = describe_error(e)
                log(
                    f"q{number} iteration {i + 1}/{cfg['iterations']} FAILED: {record['error']}"
                )
                if not isinstance(e, WrongResult):
                    recovery_error = recover(client, cfg)
                    if recovery_error:
                        log(f"cluster did not recover: {recovery_error}")
                break
            record["iterations"].append(iteration_record(result))
            record["plan"] = result.plan
            log(
                f"q{number} iteration {i + 1}/{cfg['iterations']}: {result.elapsed:.2f}s, "
                f"{result.row_count} rows, {result.tasks} tasks"
            )
    print(QUERY_RESULT_PREFIX + json.dumps(record), flush=True)
    return record


def run_suite(cfg, opener=None):
    """Returns the summary dict. `error` is set if setup failed."""
    client = Coordinator(cfg["coordinator_url"], opener=opener)
    summary = {
        "warmup": cfg["warmup"],
        "iterations": cfg["iterations"],
        "row_checks": sorted(cfg["expected_rows"]),
        "settings": cfg["settings"],
        "queries": [],
    }
    try:
        queries = load_queries(cfg["queries_dir"], cfg["queries"])
        info = wait_for_workers(client, cfg["expected_workers"])
        summary["workers"] = len(info.get("worker_urls", []))
        summary["git_commit"] = info.get("git_commit_hash") or "unknown"
        summary["build_time_utc"] = info.get("build_time_utc")
        log(
            f"datafusion-distributed {summary['git_commit']}, built {summary['build_time_utc']}"
        )
        summary["setup"] = setup(client, cfg)
    except Exception as e:  # noqa: BLE001 - report any setup failure in the summary
        summary["error"] = f"{type(e).__name__}: {e}"
        log(f"setup failed: {summary['error']}")
        return summary

    warmups = warm_up(client, queries, cfg) if cfg["warmup"] else {}
    for number, sql in queries:
        summary["queries"].append(
            run_query(client, number, sql, cfg, warmups.get(number))
        )
    return summary


def main():
    summary = run_suite(config_from_env())
    print(RESULTS_MARKER, flush=True)
    print(json.dumps(summary), flush=True)
    failed = summary.get("error") or any(q["error"] for q in summary["queries"])
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
