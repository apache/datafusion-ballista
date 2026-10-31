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

"""Tests for run_queries.py against a fake Trino.

The fake speaks the parts of Trino's HTTP protocol the runner uses: POST
/v1/statement, paging through nextUri, DELETE to cancel, GET /v1/info, and a 503
while "starting". It is injected as the client's URL opener, so no sockets are
involved, but the runner's own request, retry and paging code all run. It is not
a SQL engine: it recognises the few statements the runner issues and answers
everything else as a query.

Run from benchmarks/cluster/trino:  python3 -m unittest -v test_run_queries
"""

import email.message
import io
import itertools
import json
import os
import re
import tempfile
import unittest
import urllib.error

import run_queries as rq
from tpch_tables import TPCH_TABLES

BASE = "http://trino:8080"
COLUMNS = [{"name": "c", "type": "bigint"}]


def stats(state):
    return {"state": state, "elapsedTimeMillis": 1500, "queuedTimeMillis": 100,
            "planningTimeMillis": 200, "cpuTimeMillis": 9000, "processedRows": 1000,
            "processedBytes": 4096, "physicalInputBytes": 2048, "peakMemoryBytes": 1 << 20,
            "spilledBytes": 0}


class Response:
    def __init__(self, doc):
        self._body = json.dumps(doc).encode() if doc is not None else b""

    def read(self):
        return self._body

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass


class FakeTrino:
    """Stands in for urllib's opener. Records what the runner sends."""

    def __init__(self, workers=2):
        self.workers = workers
        self.polls_until_workers = 1  # system.runtime.nodes undercounts this many times
        self.statements = []          # every statement received, in order
        self.cancelled = []           # query ids that got a DELETE
        self.schema_headers = {}      # statement -> the X-Trino-Schema header it was sent with
        self.null_columns = set()     # columns the sample check finds NULL (a misnamed column)
        self.empty_tables = set()     # tables the sample check finds no rows in
        self.analyzed = set()
        self.fail_marker = None       # statements containing this fail
        self.hang_marker = None       # statements containing this never finish
        self.unavailable = 0          # answer this many requests with 503 first
        self.queries = {}

    # urllib opener interface
    def open(self, request, timeout=None):
        if self.unavailable > 0:
            self.unavailable -= 1
            self._raise(request.full_url, 503, {"message": "Trino server is still initializing"})
        method, url = request.get_method(), request.full_url
        path = url[len(BASE):]
        if method == "GET" and path == "/v1/info":
            return Response({"nodeVersion": {"version": "483"}, "starting": False})
        if method == "POST" and path == "/v1/statement":
            statement = request.data.decode()
            self.schema_headers[statement] = request.get_header("X-trino-schema")
            return Response(self._post(statement))
        page = re.fullmatch(r"/v1/statement/(\w+)/(\d+)", path)
        if page and method == "GET":
            return Response(self._page(page.group(1), int(page.group(2))))
        if page and method == "DELETE":
            self.cancelled.append(page.group(1))
            return Response(None)
        self._raise(url, 404, {})

    @staticmethod
    def _raise(url, code, doc):
        raise urllib.error.HTTPError(url, code, "error", email.message.Message(),
                                     io.BytesIO(json.dumps(doc).encode()))

    def _next_uri(self, qid, page):
        return f"{BASE}/v1/statement/{qid}/{page}"

    def _page(self, qid, page):
        statement = self.queries[qid]
        if self.hang_marker and self.hang_marker in statement:
            return {"id": qid, "nextUri": self._next_uri(qid, page + 1), "stats": stats("RUNNING")}
        if page < 3:  # two pages of rows, then the final document
            return {"id": qid, "nextUri": self._next_uri(qid, page + 1), "columns": COLUMNS,
                    "data": [[1], [2]], "stats": stats("RUNNING")}
        return {"id": qid, "columns": COLUMNS, "stats": stats("FINISHED")}

    def _post(self, statement):
        self.statements.append(statement)
        qid = f"q{len(self.statements)}"
        self.queries[qid] = statement
        if statement.rstrip().endswith(";"):
            return {"id": qid, "error": {"message": "mismatched input ';'", "errorName": "SYNTAX_ERROR"}}
        if self.fail_marker and self.fail_marker in statement:
            return {"id": qid, "stats": stats("FAILED"),
                    "error": {"message": "Query exceeded per-node memory limit",
                              "errorName": "EXCEEDED_LOCAL_MEMORY_LIMIT"}}
        if "system.runtime.nodes" in statement:
            count = self.workers if self.polls_until_workers == 0 else 0
            self.polls_until_workers = max(0, self.polls_until_workers - 1)
            return {"id": qid, "columns": COLUMNS, "data": [[count]], "stats": stats("FINISHED")}
        if statement.startswith("SHOW STATS FOR"):
            table = statement.rsplit(".", 1)[-1]
            columns = [{"name": n, "type": "x"} for n in
                       ("column_name", "data_size", "distinct_values_count", "nulls_fraction",
                        "row_count", "low_value", "high_value")]
            row_count = 100.0 if table in self.analyzed else None
            return {"id": qid, "columns": columns, "stats": stats("FINISHED"),
                    "data": [["x", None, 1.0, 0.0, None, None, None],
                             [None, None, None, None, row_count, None, None]]}
        if statement.startswith("ANALYZE"):
            self.analyzed.add(statement.rsplit(".", 1)[-1])
            return {"id": qid, "columns": COLUMNS, "data": [[100]], "stats": stats("FINISHED")}
        if statement.startswith("SELECT count(*), count("):
            # The runner's check that a table's columns are not NULL.
            table = re.search(r"FROM \w+\.\w+\.(\w+) LIMIT", statement).group(1)
            columns = re.findall(r"count\((\w+)\)", statement)
            sampled = 0 if table in self.empty_tables else 10000
            counts = [0 if c in self.null_columns else sampled for c in columns]
            return {"id": qid, "columns": COLUMNS, "data": [[sampled] + counts], "stats": stats("FINISHED")}
        if statement.startswith(("CREATE", "CALL", "DROP")):
            return {"id": qid, "stats": stats("FINISHED")}
        # An ordinary query starts QUEUED; its rows come from the nextUri pages.
        return {"id": qid, "nextUri": self._next_uri(qid, 1), "stats": stats("QUEUED")}


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self.trino = FakeTrino()
        self.tmp = tempfile.TemporaryDirectory()
        for name, sql in {"q01.sql": "-- using default substitutions\nselect 1 from lineitem;\n",
                          "q02.sql": "select 2 from part\n", "s1.sql": "select 'ignored'"}.items():
            with open(os.path.join(self.tmp.name, name), "w") as f:
                f.write(sql)
        self.cfg = {
            "trino_url": BASE, "catalog": "hive", "schema": "tpch", "data_path": "s3://bucket/data/",
            "expected_workers": 2, "iterations": 2, "queries": [], "queries_dir": self.tmp.name,
            "analyze": True, "partitioned": True, "money_type": "decimal(15, 2)",
            "warmup": False, "skip_setup": False, "query_timeout_s": 30,
            "session_properties": {}, "scale_factor": 1000, "expected_rows": {},
        }
        # No real waiting in the retry and polling loops.
        self._sleep = rq.time.sleep
        rq.time.sleep = lambda seconds: None

    def tearDown(self):
        rq.time.sleep = self._sleep
        self.tmp.cleanup()

    def run_suite(self):
        return rq.run_suite(self.cfg, opener=self.trino)

    def statements(self, prefix):
        return [s for s in self.trino.statements if s.startswith(prefix)]

    def test_full_run(self):
        summary = self.run_suite()
        self.assertNotIn("error", summary)
        self.assertEqual(summary["trino_version"], "483")
        self.assertEqual(summary["workers"], 2)
        self.assertEqual([q["query"] for q in summary["queries"]], [1, 2])  # s1.sql ignored
        first = summary["queries"][0]
        self.assertIsNone(first["error"])
        self.assertEqual(len(first["iterations"]), 2)
        it = first["iterations"][0]
        self.assertEqual(it["row_count"], 4)  # two pages of two rows
        self.assertEqual(it["server_elapsed"], 1.5)
        self.assertEqual(it["queued"], 0.1)
        self.assertEqual(it["cpu"], 9.0)
        self.assertEqual(it["peak_memory_bytes"], 1 << 20)

        # A trailing semicolon would make real Trino reject the statement.
        self.assertFalse(any(s.rstrip().endswith(";") for s in self.trino.statements))
        # The leading comment line is kept.
        self.assertTrue(any(s.startswith("-- using default substitutions") for s in self.trino.statements))

        # One external table per TPC-H table, partition column last.
        ddl = self.statements("CREATE TABLE")
        self.assertEqual(len(ddl), len(TPCH_TABLES))
        lineitem = next(s for s in ddl if "hive.tpch.lineitem" in s)
        self.assertIn("external_location = 's3://bucket/data/lineitem'", lineitem)
        self.assertIn("partitioned_by = ARRAY['l_shipdate']", lineitem)
        self.assertLess(lineitem.index("l_comment"), lineitem.index("l_shipdate date"))
        self.assertEqual(len(self.statements("CALL")), 4)   # the four partitioned tables
        self.assertEqual(len(self.statements("ANALYZE")), len(TPCH_TABLES))

    def test_unpartitioned_data_declares_no_partitions(self):
        self.cfg["partitioned"] = False
        self.run_suite()
        ddl = self.statements("CREATE TABLE")
        self.assertEqual(len(ddl), len(TPCH_TABLES))
        self.assertFalse(any("partitioned_by" in s for s in ddl))
        self.assertEqual(self.statements("CALL"), [])

    def test_money_type_is_declared(self):
        self.cfg["money_type"] = "decimal(12, 2)"
        self.run_suite()
        lineitem = next(s for s in self.statements("CREATE TABLE") if "hive.tpch.lineitem" in s)
        self.assertIn("l_extendedprice decimal(12, 2)", lineitem)
        self.assertIn("l_orderkey bigint", lineitem)

    def test_multi_statement_query_runs_every_statement_and_counts_the_select(self):
        with open(os.path.join(self.tmp.name, "q15.sql"), "w") as f:
            f.write("create view revenue0 (supplier_no, total_revenue) as\n"
                    "  select l_suppkey, sum(l_extendedprice) from lineitem group by l_suppkey;\n\n"
                    "select s_suppkey from supplier, revenue0 where s_suppkey = supplier_no;\n\n"
                    "drop view revenue0;")
        self.cfg["queries"] = ["q15"]
        (q15,) = self.run_suite()["queries"]
        self.assertIsNone(q15["error"])
        self.assertEqual(q15["iterations"][0]["row_count"], 4)   # the SELECT's rows
        self.assertEqual(len(self.statements("CREATE OR REPLACE VIEW revenue0 AS")), 2)
        self.assertEqual(len(self.statements("drop view revenue0")), 2)

    def test_create_view_column_list_moves_to_a_derived_table(self):
        create, select = rq.split_statements(
            "create view revenue0 (supplier_no, total_revenue) as\nselect a, b from t;\nselect 1;")
        self.assertTrue(create.startswith("CREATE OR REPLACE VIEW revenue0 AS\nSELECT * FROM (\nselect a, b from t"))
        self.assertTrue(create.endswith(") AS revenue0_columns (supplier_no, total_revenue)"))
        self.assertEqual(select, "select 1")
        self.assertEqual(rq.split_statements("create view v as select 1"),
                         ["CREATE OR REPLACE VIEW v AS\nselect 1"])

    def test_is_select_skips_comments(self):
        self.assertTrue(rq.is_select("-- q1\nselect 1"))
        self.assertTrue(rq.is_select("with t as (select 1) select * from t"))
        self.assertFalse(rq.is_select("-- view\nCREATE OR REPLACE VIEW v AS select 1"))
        self.assertFalse(rq.is_select("drop view v"))

    def test_second_run_skips_analyze_when_stats_exist(self):
        self.run_suite()
        self.trino.statements.clear()
        self.run_suite()
        self.assertEqual(self.statements("ANALYZE"), [])
        self.assertEqual(len(self.statements("CALL")), 4)  # the sync is still issued; it is cheap

    def test_no_analyze(self):
        self.cfg["analyze"] = False
        self.run_suite()
        self.assertEqual(self.statements("ANALYZE"), [])
        self.assertEqual(self.statements("SHOW STATS"), [])

    def test_failed_query_does_not_stop_the_suite(self):
        self.trino.fail_marker = "select 1 from"
        q1, q2 = self.run_suite()["queries"]
        self.assertIn("EXCEEDED_LOCAL_MEMORY_LIMIT", q1["error"])
        self.assertEqual(q1["iterations"], [])            # stops at the first failed iteration
        self.assertIsNone(q2["error"])
        self.assertEqual(len(q2["iterations"]), 2)

    def test_timeout_cancels_the_query(self):
        self.trino.hang_marker = "select 1 from"
        self.cfg["query_timeout_s"] = 10
        ticks = itertools.count()
        orig = rq.time.monotonic
        rq.time.monotonic = lambda: next(ticks)   # the clock advances one "second" per reading
        try:
            q1, q2 = self.run_suite()["queries"]
        finally:
            rq.time.monotonic = orig
        self.assertIn("QueryTimedOut", q1["error"])
        self.assertEqual(len(self.trino.cancelled), 1)   # only the query that hung
        self.assertIsNone(q2["error"])
        self.assertEqual(len(q2["iterations"]), 2)

    def test_retries_503_while_coordinator_starts(self):
        self.trino.unavailable = 3
        summary = self.run_suite()
        self.assertNotIn("error", summary)
        self.assertEqual(summary["trino_version"], "483")

    def test_gives_up_on_persistent_503(self):
        self.trino.unavailable = 10**6
        client = rq.TrinoClient(BASE, "hive", "tpch", opener=self.trino)
        clock = iter(range(0, 1000, 20))     # each call to monotonic() advances 20s
        orig = rq.time.monotonic
        rq.time.monotonic = lambda: next(clock)
        try:
            with self.assertRaises(rq.QueryFailed) as ctx:
                client.get_json(f"{BASE}/v1/info")
        finally:
            rq.time.monotonic = orig
        self.assertIn("HTTP 503", str(ctx.exception))

    def test_non_retryable_http_error_fails_fast(self):
        client = rq.TrinoClient(BASE, "hive", "tpch", opener=self.trino)
        with self.assertRaises(rq.QueryFailed) as ctx:
            client.get_json(f"{BASE}/v1/nope")
        self.assertIn("HTTP 404", str(ctx.exception))

    def test_waits_for_workers(self):
        self.trino.polls_until_workers = 2
        self.assertEqual(self.run_suite()["workers"], 2)
        polls = [s for s in self.trino.statements if "system.runtime.nodes" in s]
        self.assertEqual(len(polls), 3)

    def test_setup_failure_is_reported(self):
        self.cfg["queries"] = ["q99"]
        summary = self.run_suite()
        self.assertIn("FileNotFoundError", summary["error"])
        self.assertEqual(summary["queries"], [])

    def test_skip_setup_only_runs_queries(self):
        self.cfg["skip_setup"] = True
        self.run_suite()
        self.assertEqual(self.statements("CREATE"), [])

    def test_setup_statements_have_no_schema_but_queries_do(self):
        # The schema is created by the setup, so the setup cannot depend on it existing.
        self.run_suite()
        headers = self.trino.schema_headers
        setup = [s for s in headers if s.startswith(("CREATE", "CALL", "ANALYZE", "SHOW STATS", "SELECT count(*), count("))
                 or "system.runtime.nodes" in s]
        self.assertTrue(setup)
        self.assertEqual({headers[s] for s in setup}, {None})
        queries = [s for s in headers if "from lineitem" in s or "from part" in s]
        self.assertEqual(len(queries), 2)
        self.assertEqual({headers[s] for s in queries}, {"tpch"})

    def benchmark_statements(self):
        """The benchmark queries that were sent, in order, as query numbers."""
        return [1 if "from lineitem" in s else 2 for s in self.trino.statements
                if "from lineitem" in s or "from part" in s]

    def test_warmup_runs_each_query_once_before_the_timed_iterations(self):
        self.cfg["warmup"] = True
        summary = self.run_suite()
        # One untimed pass over all the queries, then the timed iterations query by query.
        self.assertEqual(self.benchmark_statements(), [1, 2, 1, 1, 2, 2])
        q1 = summary["queries"][0]
        self.assertEqual(len(q1["iterations"]), 2)              # the warm-up is not one of them
        self.assertEqual(q1["warmup"], {"elapsed": q1["warmup"]["elapsed"], "error": None})
        self.assertIsNotNone(q1["warmup"]["elapsed"])
        self.assertTrue(summary["warmup"])

    def test_no_warmup_runs_only_the_timed_iterations(self):
        summary = self.run_suite()
        self.assertEqual(self.benchmark_statements(), [1, 1, 2, 2])
        self.assertNotIn("warmup", summary["queries"][0])
        self.assertFalse(summary["warmup"])

    def test_failed_warmup_skips_the_timed_iterations(self):
        self.cfg["warmup"] = True
        self.trino.fail_marker = "select 1 from"
        q1, q2 = self.run_suite()["queries"]
        self.assertIn("EXCEEDED_LOCAL_MEMORY_LIMIT", q1["error"])
        self.assertEqual(q1["iterations"], [])
        self.assertEqual(q1["warmup"]["error"], q1["error"])
        self.assertEqual(self.benchmark_statements().count(1), 1)   # once, in the warm-up: it would only fail again
        self.assertIsNone(q2["error"])
        self.assertEqual(len(q2["iterations"]), 2)

    def test_wrong_row_count_fails_the_query(self):
        self.cfg["expected_rows"] = {1: 5, 2: 4}      # the fake returns 4 rows for every query
        q1, q2 = self.run_suite()["queries"]
        self.assertIn("WrongResult", q1["error"])
        self.assertIn("returned 4 rows, expected 5", q1["error"])
        self.assertEqual(q1["iterations"], [])
        self.assertIsNone(q2["error"])

    def test_wrong_row_count_in_the_warmup_fails_the_query(self):
        self.cfg["warmup"] = True
        self.cfg["expected_rows"] = {1: 5}
        q1, q2 = self.run_suite()["queries"]
        self.assertIn("returned 4 rows, expected 5", q1["warmup"]["error"])
        self.assertEqual(q1["iterations"], [])
        self.assertIsNone(q2["error"])

    def test_queries_without_an_expected_count_are_not_checked(self):
        self.cfg["expected_rows"] = {2: 4}
        q1, q2 = self.run_suite()["queries"]
        self.assertIsNone(q1["error"])
        self.assertIsNone(q2["error"])

    def test_sample_check_finds_a_misnamed_column(self):
        self.trino.null_columns = {"l_discount"}
        summary = self.run_suite()
        self.assertIn("lineitem", summary["error"])
        self.assertIn("l_discount", summary["error"])
        self.assertIn("tpch_tables.py", summary["error"])
        self.assertEqual(summary["queries"], [])
        # It fails before ANALYZE spends time on the table, and before the tables after it.
        self.assertNotIn("hive.tpch.lineitem", " ".join(self.statements("ANALYZE")))
        self.assertNotIn("hive.tpch.region", " ".join(self.statements("CREATE TABLE")))

    def test_sample_check_finds_an_empty_table(self):
        self.trino.empty_tables = {"orders"}
        summary = self.run_suite()
        self.assertIn("orders has no rows", summary["error"])

    def test_sample_check_runs_between_the_partition_sync_and_analyze(self):
        self.run_suite()
        statements = self.trino.statements
        sync = next(i for i, s in enumerate(statements) if s.startswith("CALL") and "'lineitem'" in s)
        sample = next(i for i, s in enumerate(statements)
                      if s.startswith("SELECT count(*), count(") and ".lineitem LIMIT" in s)
        analyze = next(i for i, s in enumerate(statements) if s.startswith("ANALYZE") and s.endswith(".lineitem"))
        self.assertLess(sync, sample)
        self.assertLess(sample, analyze)

    def test_sample_check_does_not_run_when_setup_is_skipped(self):
        self.cfg["skip_setup"] = True
        self.run_suite()
        self.assertEqual(self.statements("SELECT count(*), count("), [])

    def test_table_has_stats_tolerates_a_response_without_columns(self):
        class NoColumns:
            def execute(self, sql, timeout_s=0, collect_rows=False):
                return rq.QueryResult("q1", None, [], 0, 0.0)

        self.assertFalse(rq.table_has_stats(NoColumns(), "hive", "tpch", "lineitem"))

    def test_session_properties_are_sent(self):
        client = rq.TrinoClient(BASE, "hive", "tpch",
                                {"join_distribution_type": "BROADCAST", "a": "b c"}, opener=self.trino)
        self.assertEqual(client.headers["X-Trino-Session"], "join_distribution_type=BROADCAST,a=b%20c")

    def test_query_number(self):
        for name in ("q01", "q1", "Q1", "1", "01"):
            self.assertEqual(rq.query_number(name), 1)
        self.assertEqual(rq.query_number("q22"), 22)
        with self.assertRaises(ValueError):
            rq.query_number("tpcds")

    def test_config_from_env(self):
        cfg = rq.config_from_env({"QUERIES": "q1,q06", "SESSION_PROPERTIES": "a=1,b=2", "ANALYZE": "1",
                                  "EXPECTED_WORKERS": "32", "PARTITIONED": "1", "MONEY_TYPE": "decimal(12, 2)"})
        self.assertEqual(cfg["queries"], ["q1", "q06"])
        self.assertEqual(cfg["session_properties"], {"a": "1", "b": "2"})
        self.assertTrue(cfg["analyze"])
        self.assertTrue(cfg["partitioned"])
        self.assertEqual(cfg["money_type"], "decimal(12, 2)")
        self.assertEqual(cfg["expected_workers"], 32)

    def test_config_defaults_warm_up_and_check_the_sf1000_row_counts(self):
        cfg = rq.config_from_env({})
        self.assertTrue(cfg["warmup"])
        self.assertFalse(cfg["analyze"])
        self.assertFalse(cfg["partitioned"])
        self.assertEqual(cfg["scale_factor"], 1000)
        self.assertEqual(cfg["expected_rows"][1], 4)
        self.assertEqual(cfg["expected_rows"][11], 0)
        self.assertFalse(rq.config_from_env({"WARMUP": "0"})["warmup"])

    def test_config_can_turn_row_checks_off_and_unknown_scale_factors_are_unchecked(self):
        self.assertEqual(rq.config_from_env({"CHECK_ROWS": "0"})["expected_rows"], {})
        self.assertEqual(rq.config_from_env({"SCALE_FACTOR": "1"})["expected_rows"], {})

    def test_expected_row_counts_cover_the_queries_that_can_be_checked(self):
        from tpch_expected_rows import SF1000
        # q13 and q20 grow with the scale factor, so nothing is known about them at SF1000.
        self.assertEqual(set(SF1000), set(range(1, 23)) - {13, 20})
        self.assertTrue(all(isinstance(n, int) and n >= 0 for n in SF1000.values()))


if __name__ == "__main__":
    unittest.main()
