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

"""Run the TPC-H suite against a Trino cluster and print the timings as JSON.

Runs in the cluster as the runner Job that run.py creates. It talks to the
coordinator over Trino's HTTP protocol and uses only the standard library, so it
runs in a stock python image. Configuration comes from environment variables
(see config_from_env).

Steps:
  1. Wait for the expected number of workers to register with the coordinator.
  2. Create the schema and the external Hive tables over the Parquet data, and
     add their partitions if the data is partitioned (PARTITIONED=1). Idempotent: everything is CREATE ... IF NOT EXISTS,
     and partitions already in the metastore are left alone. The metastore lives
     in S3 (hive.metastore.catalog.dir), so a later run with the same Trino
     version finds the tables, partitions and statistics already there.
  3. ANALYZE each table that has no statistics yet (only with ANALYZE=1). Without
     statistics Trino cannot order the joins of TPC-H's comma-join queries.
  4. Run every selected query once, untimed (unless WARMUP=0). Trino compiles
     code for each query and the JVM is cold at the start, so the first run of a
     query is slower than the ones after it.
  5. Run each query ITERATIONS times and record client wall-clock plus the
     server-side stats Trino reports. Every execution's row count is checked
     against the expected one (see tpch_expected_rows.py); a difference fails
     the query.

A query file may hold several statements: q15 creates a view, selects from it
and drops it. They run one after another and are timed together, as the Ballista
runner times them.

Output: a line "TRINO-QUERY-RESULT {json}" as each query finishes, so a run that
dies part way can still be recovered from the log, then "===RESULTS-JSON-FILE==="
followed by the full summary. run.py reads that from the pod log. The exit status is non-zero if setup or any query failed.
"""

import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field

try:
    from tpch_expected_rows import EXPECTED_ROWS
except ImportError:  # run from the source tree, where it is one directory up
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
    from tpch_expected_rows import EXPECTED_ROWS
from tpch_tables import DEFAULT_MONEY_TYPE, TPCH_TABLES, partition_column, table_ddl

RESULTS_MARKER = "===RESULTS-JSON-FILE==="
QUERY_RESULT_PREFIX = "TRINO-QUERY-RESULT "

# Setup statements (sync_partition_metadata, ANALYZE) scan or list a lot of S3.
SETUP_TIMEOUT_S = 2 * 60 * 60

# Rows read from each table to check that its columns are not NULL.
SAMPLE_ROWS = 10000


class QueryFailed(Exception):
    def __init__(self, message, query_id=None, error_name=None):
        super().__init__(message)
        self.query_id = query_id
        self.error_name = error_name


class QueryTimedOut(Exception):
    pass


class WrongResult(Exception):
    """A query returned a different number of rows than it should."""


class SetupError(Exception):
    pass


@dataclass
class QueryResult:
    query_id: str
    columns: list
    rows: list
    row_count: int
    elapsed: float
    stats: dict = field(default_factory=dict)


class TrinoClient:
    """Minimal client for Trino's HTTP protocol (direct result encoding)."""

    def __init__(self, base_url, catalog, schema=None, session_properties=None,
                 user="benchmark", source="ballista-benchmarks", opener=None):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "X-Trino-User": user,
            "X-Trino-Source": source,
            "X-Trino-Catalog": catalog,
            "Content-Type": "text/plain; charset=utf-8",
        }
        if schema:
            self.headers["X-Trino-Schema"] = schema
        if session_properties:
            self.headers["X-Trino-Session"] = ",".join(
                f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in session_properties.items())
        # The coordinator is a cluster-internal address; never go through a proxy.
        # `opener` is only replaced by the tests.
        self._opener = opener or urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def get_json(self, url):
        return self._call("GET", url)

    def _call(self, method, url, body=None, headers=None, retry_for=60.0):
        """One HTTP call, retried while the failure looks transient.

        Trino asks clients to retry 502/503/504 after 50-100 ms and 429 after
        Retry-After. A coordinator that is still starting refuses connections and
        then answers 503, so those count as transient for `retry_for` seconds.
        """
        deadline = time.monotonic() + retry_for
        delay = 0.1
        while True:
            request = urllib.request.Request(url, data=body, method=method, headers=headers or {})
            try:
                with self._opener.open(request, timeout=120) as response:
                    return json.loads(response.read())
            except urllib.error.HTTPError as e:
                detail = e.read()[:500]
                e.close()
                if e.code in (429, 502, 503, 504) and time.monotonic() < deadline:
                    retry_after = e.headers.get("Retry-After")
                    time.sleep(float(retry_after) if e.code == 429 and retry_after else delay)
                    delay = min(delay * 2, 2.0)
                    continue
                raise QueryFailed(f"HTTP {e.code} from {method} {url}: {detail!r}")
            except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
                if time.monotonic() < deadline:
                    time.sleep(delay)
                    delay = min(delay * 2, 2.0)
                    continue
                raise QueryFailed(f"{method} {url}: {e}")

    def _cancel(self, next_uri):
        try:
            request = urllib.request.Request(next_uri, method="DELETE")
            self._opener.open(request, timeout=30).close()
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass

    def execute(self, sql, timeout_s=3600, collect_rows=False):
        """Run one statement to completion.

        Follows nextUri until it is absent, which is how the protocol signals the
        end of a query. Rows are only kept when collect_rows is set; the benchmark
        queries just count them.
        """
        start = time.monotonic()
        deadline = start + timeout_s
        doc = self._call("POST", f"{self.base_url}/v1/statement", sql.encode("utf-8"), self.headers)
        query_id = doc.get("id")
        columns, rows, row_count, stats = None, [], 0, {}
        while True:
            error = doc.get("error")
            if error:
                raise QueryFailed(error.get("message", "unknown error"), query_id, error.get("errorName"))
            columns = doc.get("columns") or columns
            data = doc.get("data") or []
            row_count += len(data)
            if collect_rows:
                rows.extend(data)
            stats = doc.get("stats") or stats
            next_uri = doc.get("nextUri")
            if not next_uri:
                break
            if time.monotonic() > deadline:
                self._cancel(next_uri)
                raise QueryTimedOut(f"query {query_id} did not finish within {timeout_s}s")
            doc = self._call("GET", next_uri)
        return QueryResult(query_id, columns, rows, row_count, time.monotonic() - start, stats)


def log(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def config_from_env(env=os.environ):
    session = {}
    for pair in filter(None, env.get("SESSION_PROPERTIES", "").split(",")):
        key, _, value = pair.partition("=")
        session[key.strip()] = value.strip()
    scale_factor = int(env.get("SCALE_FACTOR", "1000"))
    check_rows = env.get("CHECK_ROWS", "1") == "1"
    return {
        "trino_url": env.get("TRINO_URL", "http://localhost:8080"),
        "catalog": env.get("TRINO_CATALOG", "hive"),
        "schema": env.get("TRINO_SCHEMA", "tpch"),
        "data_path": env.get("DATA_PATH", ""),
        "expected_workers": int(env.get("EXPECTED_WORKERS", "1")),
        "iterations": int(env.get("ITERATIONS", "2")),
        "queries": [q for q in env.get("QUERIES", "").split(",") if q.strip()],
        "queries_dir": env.get("QUERIES_DIR", "/opt/bench/queries"),
        "analyze": env.get("ANALYZE", "0") == "1",
        "partitioned": env.get("PARTITIONED", "0") == "1",
        "money_type": env.get("MONEY_TYPE", DEFAULT_MONEY_TYPE),
        "warmup": env.get("WARMUP", "1") == "1",
        "skip_setup": env.get("SKIP_SETUP", "0") == "1",
        "query_timeout_s": int(env.get("QUERY_TIMEOUT_S", "3600")),
        "session_properties": session,
        # Row counts to check, by query number. Empty when checking is off or
        # the scale factor has no table in tpch_expected_rows.py.
        "scale_factor": scale_factor,
        "expected_rows": EXPECTED_ROWS.get(scale_factor, {}) if check_rows else {},
    }


def query_number(name):
    """q01, q1, Q1 and 1 all mean query 1."""
    match = re.fullmatch(r"[qQ]?0*(\d+)", name.strip())
    if not match:
        raise ValueError(f"not a query name: {name!r}")
    return int(match.group(1))


def split_statements(text):
    """The statements of a query file, without their semicolons.

    The protocol takes a single statement and rejects a trailing semicolon. A
    view is created with CREATE OR REPLACE, so a run that died between creating
    and dropping it does not break the next one. Trino's CREATE VIEW takes no
    column list, so q15's `create view revenue0 (supplier_no, total_revenue) as
    select ...` names the columns on a derived table instead.
    """
    statements = []
    for statement in text.split(";"):
        statement = statement.strip()
        if not statement:
            continue
        view = re.fullmatch(r"(?is)create\s+view\s+(\w+)\s*(?:\(([^)]*)\))?\s+as\s+(.*)", statement)
        if view:
            name, columns, query = view.groups()
            if columns:
                query = f"SELECT * FROM (\n{query}\n) AS {name}_columns ({columns.strip()})"
            statement = f"CREATE OR REPLACE VIEW {name} AS\n{query}"
        statements.append(statement)
    return statements


def load_queries(queries_dir, selected):
    """Return [(number, [statement, ...])] for the selected queries, or all qN.sql files."""
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
            queries.append((n, split_statements(f.read())))
    return queries


def wait_for_workers(client, expected, timeout_s=900):
    sql = "SELECT count(*) FROM system.runtime.nodes WHERE coordinator = false AND state = 'active'"
    deadline = time.monotonic() + timeout_s
    found = 0
    while True:
        try:
            found = int(client.execute(sql, timeout_s=60, collect_rows=True).rows[0][0])
        except (QueryFailed, QueryTimedOut) as e:
            log(f"waiting for coordinator: {e}")
        else:
            log(f"workers registered: {found}/{expected}")
            if found >= expected:
                return found
        if time.monotonic() > deadline:
            raise RuntimeError(f"only {found}/{expected} workers registered after {timeout_s}s")
        time.sleep(5)


def table_has_stats(client, catalog, schema, table):
    """True if ANALYZE has already recorded a row count for the table."""
    result = client.execute(f"SHOW STATS FOR {catalog}.{schema}.{table}", collect_rows=True)
    names = [c["name"] for c in result.columns or []]
    if "column_name" not in names or "row_count" not in names:
        return False
    column, row_count = names.index("column_name"), names.index("row_count")
    # The row with a NULL column_name summarises the whole table.
    return any(row[column] is None and row[row_count] is not None for row in result.rows)


def check_table_readable(client, catalog, schema, table, columns):
    """Fail unless the table has rows and no NULLs in a sample of them.

    TPC-H has no NULLs. The Hive connector matches Parquet columns by name, so a
    column that is misnamed in tpch_tables.py reads as NULL instead of failing,
    and the queries would run on wrong data. This finds that in seconds, before
    ANALYZE spends time on the table.
    """
    counts = ", ".join(f"count({c})" for c in columns)
    sql = (f"SELECT count(*), {counts} "
           f"FROM (SELECT * FROM {catalog}.{schema}.{table} LIMIT {SAMPLE_ROWS})")
    row = client.execute(sql, timeout_s=SETUP_TIMEOUT_S, collect_rows=True).rows[0]
    sampled = int(row[0])
    if sampled == 0:
        raise SetupError(f"{table} has no rows: no Parquet files (or, partitioned, no partitions) "
                         f"were found under its external_location")
    null_columns = [c for c, n in zip(columns, row[1:]) if int(n) < sampled]
    if null_columns:
        raise SetupError(
            f"{table}: NULLs in the first {sampled} rows of {', '.join(null_columns)}. TPC-H has no "
            f"NULLs, so the Parquet files probably name these columns differently from tpch_tables.py")


def setup_tables(client, cfg):
    """Create the schema and tables, sync partitions, check them and collect statistics."""
    catalog, schema = cfg["catalog"], cfg["schema"]
    setup = {"tables": {}}
    started = time.monotonic()
    client.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}", timeout_s=SETUP_TIMEOUT_S)
    for name, _, columns in TPCH_TABLES:
        info = setup["tables"][name] = {}
        client.execute(table_ddl(name, catalog, schema, cfg["data_path"], cfg["partitioned"], cfg["money_type"]),
                       timeout_s=SETUP_TIMEOUT_S)
        if partition_column(name, cfg["partitioned"]):
            t0 = time.monotonic()
            client.execute(
                f"CALL {catalog}.system.sync_partition_metadata('{schema}', '{name}', 'ADD')",
                timeout_s=SETUP_TIMEOUT_S)
            info["sync_s"] = round(time.monotonic() - t0, 1)
            log(f"{name}: partitions synced in {info['sync_s']}s")
        check_table_readable(client, catalog, schema, name, [c for c, _ in columns])
        if cfg["analyze"]:
            info["had_stats"] = table_has_stats(client, catalog, schema, name)
            if info["had_stats"]:
                log(f"{name}: statistics already present, skipping ANALYZE")
            else:
                t0 = time.monotonic()
                client.execute(f"ANALYZE {catalog}.{schema}.{name}", timeout_s=SETUP_TIMEOUT_S)
                info["analyze_s"] = round(time.monotonic() - t0, 1)
                log(f"{name}: ANALYZE took {info['analyze_s']}s")
    setup["seconds"] = round(time.monotonic() - started, 1)
    return setup


def iteration_record(result):
    stats = result.stats
    return {
        "elapsed": round(result.elapsed, 3),
        "row_count": result.row_count,
        "query_id": result.query_id,
        "server_elapsed": stats.get("elapsedTimeMillis", 0) / 1000,
        "queued": stats.get("queuedTimeMillis", 0) / 1000,
        "planning": stats.get("planningTimeMillis", 0) / 1000,
        "cpu": stats.get("cpuTimeMillis", 0) / 1000,
        "processed_rows": stats.get("processedRows", 0),
        "processed_bytes": stats.get("processedBytes", 0),
        "physical_input_bytes": stats.get("physicalInputBytes", 0),
        "peak_memory_bytes": stats.get("peakMemoryBytes", 0),
        "spilled_bytes": stats.get("spilledBytes", 0),
    }


def describe_error(e):
    return f"{getattr(e, 'error_name', None) or type(e).__name__}: {e}"[:1000]


def is_select(statement):
    """True for a query, False for DDL such as q15's CREATE VIEW and DROP VIEW."""
    code = [line for line in statement.splitlines() if line.strip() and not line.strip().startswith("--")]
    return bool(code) and code[0].strip().lstrip("(").split(None, 1)[0].lower() in ("select", "with")


def execute_query(client, number, statements, cfg):
    """One execution of a benchmark query, failing if it returns the wrong number of rows.

    A fast answer is only worth something if it is the right one. The query's
    statements run in order; the result is that of its SELECT, with the time of
    them all.
    """
    start = time.monotonic()
    result = None
    for statement in statements:
        executed = client.execute(statement, timeout_s=cfg["query_timeout_s"])
        if result is None or is_select(statement):
            result = executed
    result.elapsed = time.monotonic() - start
    expected = cfg["expected_rows"].get(number)
    if expected is not None and result.row_count != expected:
        raise WrongResult(f"q{number} returned {result.row_count} rows, expected {expected} ({result.query_id})")
    return result


def warm_up(client, queries, cfg):
    """Run each query once, untimed, before the timed iterations.

    The JVM starts cold and Trino compiles code for each query, so a query's first
    run is slower than the ones after it, which is not what the benchmark is after.
    The run also fills the metadata and file listing caches. Returns
    {query number: {"elapsed": seconds or None, "error": message or None}}.
    """
    warmups = {}
    for number, statements in queries:
        try:
            result = execute_query(client, number, statements, cfg)
        except (QueryFailed, QueryTimedOut, WrongResult) as e:
            warmups[number] = {"elapsed": None, "error": describe_error(e)}
            log(f"q{number} warm-up FAILED: {warmups[number]['error']}")
        else:
            warmups[number] = {"elapsed": round(result.elapsed, 3), "error": None}
            log(f"q{number} warm-up: {result.elapsed:.2f}s, {result.row_count} rows")
    return warmups


def run_query(client, number, statements, cfg, warmup=None):
    """Run one query cfg['iterations'] times. Stops at the first failed iteration.

    `warmup` is this query's result from warm_up. A query that failed there is not
    run again: it would only fail again, slowly.
    """
    record = {"query": number, "iterations": [], "error": None}
    if warmup is not None:
        record["warmup"] = warmup
        record["error"] = warmup["error"]
    if record["error"] is None:
        for i in range(cfg["iterations"]):
            try:
                result = execute_query(client, number, statements, cfg)
            except (QueryFailed, QueryTimedOut, WrongResult) as e:
                record["error"] = describe_error(e)
                log(f"q{number} iteration {i + 1}/{cfg['iterations']} FAILED: {record['error']}")
                break
            record["iterations"].append(iteration_record(result))
            log(f"q{number} iteration {i + 1}/{cfg['iterations']}: {result.elapsed:.2f}s, "
                f"{result.row_count} rows ({result.query_id})")
    print(QUERY_RESULT_PREFIX + json.dumps(record), flush=True)
    return record


def run_suite(cfg, opener=None):
    """Returns the summary dict. `error` is set if setup failed."""
    # The setup statements name everything in full, so their session has no
    # schema: the schema they create need not exist yet. Only the benchmark
    # queries, which use bare table names, get the schema and session properties.
    admin = TrinoClient(cfg["trino_url"], cfg["catalog"], opener=opener)
    client = TrinoClient(cfg["trino_url"], cfg["catalog"], cfg["schema"], cfg["session_properties"],
                         opener=opener)
    summary = {
        "catalog": cfg["catalog"],
        "schema": cfg["schema"],
        "analyze": cfg["analyze"],
        "partitioned": cfg["partitioned"],
        "warmup": cfg["warmup"],
        "iterations": cfg["iterations"],
        "row_checks": sorted(cfg["expected_rows"]),
        "session_properties": cfg["session_properties"],
        "queries": [],
    }
    try:
        summary["trino_version"] = admin.get_json(f"{admin.base_url}/v1/info")["nodeVersion"]["version"]
        log(f"Trino {summary['trino_version']}")
        summary["workers"] = wait_for_workers(admin, cfg["expected_workers"])
        if not cfg["skip_setup"]:
            summary["setup"] = setup_tables(admin, cfg)
        queries = load_queries(cfg["queries_dir"], cfg["queries"])
    except Exception as e:  # noqa: BLE001 - report any setup failure in the summary
        summary["error"] = f"{type(e).__name__}: {e}"
        log(f"setup failed: {summary['error']}")
        return summary

    warmups = warm_up(client, queries, cfg) if cfg["warmup"] else {}
    for number, statements in queries:
        summary["queries"].append(run_query(client, number, statements, cfg, warmups.get(number)))
    return summary


def main():
    summary = run_suite(config_from_env())
    print(RESULTS_MARKER, flush=True)
    print(json.dumps(summary), flush=True)
    failed = summary.get("error") or any(q["error"] for q in summary["queries"])
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
