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

"""Tests for run_queries.py against a fake coordinator.

The fake answers the two requests of the worker binary's HTTP API, GET /info and
GET /?sql=..., the way benchmarks/remote-worker/src/main.rs does: JSON with the row
count, the time and the plan, or an HTTP 500 whose body is the error. It is injected
as the client's URL opener, so no sockets are involved, but the runner's own
request, error and recovery code all run. It is not a SQL engine: it recognises the
statements the runner issues and answers everything else as a query.

The tables and expected row counts are Trino's (../trino), which the runner shares.
Run from benchmarks/cluster/datafusion-distributed:  python3 -m unittest -v test_run_queries
"""

import email.message
import io
import json
import os
import re
import tempfile
import unittest
import urllib.error
import urllib.parse

# Imported first: it puts ../trino (tpch_tables.py) and .. (tpch_expected_rows.py)
# on the path, behind this directory, since ../trino has a run_queries.py of its own.
import run_queries as rq  # noqa: E402
from tpch_tables import TPCH_TABLES  # noqa: E402

BASE = "http://coordinator:9000"
TABLES = [name for name, _, _ in TPCH_TABLES]


class Response:
    def __init__(self, doc):
        self._body = json.dumps(doc).encode()

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass


class FakeCoordinator:
    """Stands in for urllib's opener. Records what the runner sends."""

    def __init__(self, workers=2):
        self.workers = workers
        self.polls_until_workers = 1  # /info reports no workers this many times
        self.refuse = 0  # refuse this many connections first
        self.statements = []  # every SQL request, in order
        self.null_columns = (
            set()
        )  # columns the sample check finds NULL (a misnamed column)
        self.empty_tables = set()  # tables with no rows
        self.encoded_tables = (
            set()
        )  # tables whose partition values are still percent-encoded
        self.fail_marker = None  # requests containing this fail with HTTP 500
        self.hang_marker = None  # requests containing this time out
        self.row_counts = {}  # marker in a query -> rows it returns

    def open(self, request, timeout=None):
        url = urllib.parse.urlsplit(request.full_url)
        if self.refuse > 0:
            self.refuse -= 1
            raise urllib.error.URLError(
                ConnectionRefusedError(61, "Connection refused")
            )
        if url.path == "/info":
            found = self.workers if self.polls_until_workers == 0 else 0
            self.polls_until_workers = max(0, self.polls_until_workers - 1)
            return Response(
                {
                    "worker_urls": [f"http://10.0.0.{i}:9001/" for i in range(found)],
                    "git_commit_hash": "9e7a5ce",
                    "build_time_utc": "2026-10-06 12:00:00 +00:00",
                    "errors": [],
                }
            )
        sql = urllib.parse.parse_qs(url.query)["sql"][0]
        self.statements.append(sql)
        if self.fail_marker and self.fail_marker in sql:
            raise urllib.error.HTTPError(
                request.full_url,
                500,
                "Internal Server Error",
                email.message.Message(),
                io.BytesIO(b"Resources exhausted: worker connection lost"),
            )
        if self.hang_marker and self.hang_marker in sql:
            raise TimeoutError("timed out")
        return Response(
            {
                "count": self._rows(sql),
                "plan": f"plan of {sql[:20]}",
                "elapsed_ms": 1500.0,
                "tasks": 12,
                "stats_q_error_p50": None,
                "stats_q_error_p95": None,
            }
        )

    def _rows(self, sql):
        if sql.startswith(("CREATE", "SET", "DROP")):
            return 0
        sample = re.fullmatch(
            r"SELECT 1 FROM \(SELECT \* FROM (\w+) LIMIT \d+\) WHERE (.*)",
            sql,
            re.DOTALL,
        )
        if sample:
            return int(
                any(
                    c in self.null_columns
                    for c in re.findall(r"(\w+) IS NULL", sample.group(2))
                )
            )
        first_row = re.fullmatch(r"SELECT 1 FROM (\w+) LIMIT 1", sql)
        if first_row:
            return 0 if first_row.group(1) in self.empty_tables else 1
        encoded = re.fullmatch(
            r"SELECT 1 FROM (\w+) WHERE strpos\(\w+, '%'\) > 0 LIMIT 1", sql
        )
        if encoded:
            return int(encoded.group(1) in self.encoded_tables)
        for marker, rows in self.row_counts.items():
            if marker in sql:
                return rows
        return 7

    def creates(self):
        return [s for s in self.statements if s.startswith("CREATE EXTERNAL TABLE")]

    def queries(self, marker):
        return [s for s in self.statements if marker in s]


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.queries_dir = self.tmp.name
        self.write_query(
            1, "-- using default substitutions\n\nselect 'query one' from lineitem;\n"
        )
        self.write_query(2, "select 'query two' from part")
        self.write_query(
            7,
            "select extract(year from l_shipdate) as l_year, 'query seven' from lineitem",
        )
        self.fake = FakeCoordinator()
        rq.time.sleep = lambda s: None  # no real waiting in the retry loops

    def tearDown(self):
        self.tmp.cleanup()

    def write_query(self, number, sql):
        with open(os.path.join(self.queries_dir, f"q{number:02d}.sql"), "w") as f:
            f.write(sql)

    def cfg(self, **overrides):
        cfg = {
            "coordinator_url": BASE,
            "data_path": "s3://bucket/tpch-parquet/sf1000",
            "partitioned": True,
            "money_type": "decimal(12, 2)",
            "expected_workers": 2,
            "iterations": 2,
            "queries": [],
            "queries_dir": self.queries_dir,
            "warmup": True,
            "query_timeout_s": 60,
            "settings": {},
            "scale_factor": 1000,
            "expected_rows": {},
        }
        cfg.update(overrides)
        return cfg

    def run_suite(self, **overrides):
        return rq.run_suite(self.cfg(**overrides), opener=self.fake)

    def test_full_run(self):
        summary = self.run_suite()
        self.assertNotIn("error", summary)
        self.assertEqual(summary["workers"], 2)
        self.assertEqual(summary["git_commit"], "9e7a5ce")
        self.assertEqual(sorted(summary["setup"]["tables"]), sorted(TABLES))
        self.assertEqual([q["query"] for q in summary["queries"]], [1, 2, 7])
        for q in summary["queries"]:
            self.assertIsNone(q["error"])
            self.assertEqual(len(q["iterations"]), 2)
            self.assertEqual(q["warmup"]["error"], None)
            it = q["iterations"][0]
            self.assertEqual(
                (it["row_count"], it["server_elapsed"], it["tasks"]), (7, 1.5, 12)
            )
            self.assertTrue(q["plan"].startswith("plan of"))
        # The eight tables are registered once, in order, before any query runs.
        self.assertEqual(len(self.fake.creates()), 8)
        self.assertTrue(
            self.fake.statements[0].startswith(
                "CREATE EXTERNAL TABLE IF NOT EXISTS part"
            )
        )
        # Warm-up of every query first, then the timed iterations.
        order = [s for s in self.fake.statements if "'query " in s]
        self.assertEqual(len(order), 9)
        self.assertEqual(
            [("one" in s, "two" in s) for s in order[:3]],
            [(True, False), (False, True), (False, False)],
        )

    def test_table_ddl(self):
        ddl = rq.table_ddl(
            "lineitem", "s3://bucket/tpch-parquet/sf1000/", True, "decimal(12, 2)"
        )
        self.assertTrue(
            ddl.startswith(
                "CREATE EXTERNAL TABLE IF NOT EXISTS lineitem (\n  l_orderkey bigint,"
            )
        )
        self.assertIn("l_shipdate date,", ddl)
        self.assertIn("l_quantity decimal(12, 2),", ddl)
        self.assertIn("STORED AS PARQUET\nPARTITIONED BY (l_shipdate)\n", ddl)
        self.assertTrue(
            ddl.endswith("LOCATION 's3://bucket/tpch-parquet/sf1000/lineitem/'")
        )
        supplier = rq.table_ddl("supplier", "s3://bucket/x", True)
        self.assertNotIn("PARTITIONED BY", supplier)
        self.assertIn("s_acctbal decimal(15, 2),", supplier)  # the default money type
        self.assertTrue(supplier.endswith("LOCATION 's3://bucket/x/supplier/'"))
        self.assertIn("LOCATION 's3://it''s/part/'", rq.table_ddl("part", "s3://it's"))

    def test_unpartitioned_tables_declare_no_partition_columns(self):
        self.assertNotIn(
            "PARTITIONED BY", rq.table_ddl("lineitem", "s3://bucket/x", False)
        )
        self.run_suite(queries=["1"], partitioned=False)
        self.assertFalse([s for s in self.fake.creates() if "PARTITIONED BY" in s])
        self.assertFalse([s for s in self.fake.statements if "strpos" in s])

    def test_split_statements(self):
        self.assertEqual(
            rq.split_statements("-- c\nselect 1;\n"), ([], "-- c\nselect 1", [])
        )
        view = (
            "create view v as select 1;\n\nselect * from v;\n\ndrop view v;\n-- end\n"
        )
        self.assertEqual(
            rq.split_statements(view),
            (["create view v as select 1"], "select * from v", ["drop view v"]),
        )
        with self.assertRaises(ValueError):
            rq.split_statements("select 1; select 2")
        with self.assertRaises(ValueError):
            rq.split_statements("create view v as select 1")

    def test_a_view_query_times_only_the_select(self):
        self.write_query(
            15,
            "create view revenue0 as select 1 as r;\n"
            "select 'query fifteen' from revenue0;\ndrop view revenue0;",
        )
        self.fake.row_counts["query fifteen"] = 1
        summary = self.run_suite(
            queries=["15"], warmup=False, iterations=1, expected_rows={15: 1}
        )
        q = summary["queries"][0]
        self.assertIsNone(q["error"])
        self.assertEqual(q["iterations"][0]["row_count"], 1)
        sent = self.fake.statements[-3:]
        self.assertEqual([s.split()[0] for s in sent], ["create", "select", "drop"])

    def test_a_query_file_with_two_queries_fails_setup(self):
        self.write_query(3, "select 1; select 2")
        summary = self.run_suite(queries=["3"])
        self.assertIn("exactly one query", summary["error"])
        self.assertEqual(self.fake.statements, [])

    def test_settings_are_applied_after_the_tables(self):
        self.run_suite(
            settings={
                "distributed.file_scan_config_bytes_per_partition": "33554432",
                "datafusion.execution.target_partitions": "16",
            }
        )
        sets = [s for s in self.fake.statements if s.startswith("SET ")]
        self.assertEqual(
            sets,
            [
                "SET distributed.file_scan_config_bytes_per_partition = 33554432",
                "SET datafusion.execution.target_partitions = 16",
            ],
        )
        first_set = self.fake.statements.index(sets[0])
        self.assertTrue(
            all(self.fake.statements.index(c) < first_set for c in self.fake.creates())
        )

    def test_waits_for_workers(self):
        self.fake.polls_until_workers = 3
        summary = self.run_suite()
        self.assertEqual(summary["workers"], 2)
        self.assertNotIn("error", summary)

    def test_waits_while_the_coordinator_refuses_connections(self):
        self.fake.refuse = 4
        summary = self.run_suite()
        self.assertNotIn("error", summary)
        self.assertEqual(summary["workers"], 2)

    def test_failed_query_does_not_stop_the_suite_and_the_tables_are_registered_again(
        self,
    ):
        self.fake.fail_marker = "query two"
        summary = self.run_suite(warmup=False)
        errors = {q["query"]: q["error"] for q in summary["queries"]}
        self.assertIsNone(errors[1])
        self.assertIn(
            "HTTP 500: Resources exhausted: worker connection lost", errors[2]
        )
        self.assertIsNone(errors[7])
        self.assertEqual(len(summary["queries"][1]["iterations"]), 0)
        # A failure may have been a restarted coordinator, which has lost its tables.
        failed_at = self.fake.statements.index(self.fake.queries("query two")[0])
        after = self.fake.statements[failed_at + 1 :]
        self.assertEqual(
            len([s for s in after if s.startswith("CREATE EXTERNAL TABLE")]), 8
        )

    def test_settings_are_applied_again_after_a_failure(self):
        self.fake.fail_marker = "query one"
        self.run_suite(warmup=False, settings={"distributed.compression": "zstd"})
        self.assertEqual(
            len([s for s in self.fake.statements if s.startswith("SET ")]), 2
        )

    def test_timeout_fails_the_query(self):
        self.fake.hang_marker = "query one"
        summary = self.run_suite(warmup=False)
        self.assertIn(
            "QueryTimedOut: no answer within 60s", summary["queries"][0]["error"]
        )
        self.assertIsNone(summary["queries"][1]["error"])

    def test_wrong_row_count_fails_the_query(self):
        summary = self.run_suite(warmup=False, expected_rows={1: 7, 2: 100})
        errors = {q["query"]: q["error"] for q in summary["queries"]}
        self.assertIsNone(errors[1])
        self.assertIn("WrongResult: q2 returned 7 rows, expected 100", errors[2])
        self.assertIsNone(errors[7])  # no expected count: not checked
        # A wrong answer is not a crashed cluster, so the tables are not registered again.
        self.assertEqual(len(self.fake.creates()), 8)

    def test_wrong_row_count_in_the_warmup_fails_the_query(self):
        summary = self.run_suite(expected_rows={2: 100})
        q2 = summary["queries"][1]
        self.assertIn("expected 100", q2["error"])
        self.assertEqual(q2["iterations"], [])
        self.assertEqual(len(self.fake.queries("query two")), 1)

    def test_failed_warmup_skips_the_timed_iterations(self):
        self.fake.fail_marker = "query two"
        summary = self.run_suite()
        q2 = summary["queries"][1]
        self.assertIn("HTTP 500", q2["warmup"]["error"])
        self.assertEqual(q2["error"], q2["warmup"]["error"])
        self.assertEqual(len(self.fake.queries("query two")), 1)

    def test_no_warmup_runs_only_the_timed_iterations(self):
        summary = self.run_suite(warmup=False, queries=["1"])
        self.assertNotIn("warmup", summary["queries"][0])
        self.assertEqual(len(self.fake.queries("query one")), 2)

    def test_sample_check_finds_a_misnamed_column(self):
        self.fake.null_columns = {"l_comment"}
        summary = self.run_suite()
        self.assertIn("lineitem: NULLs in a sample of l_comment", summary["error"])
        self.assertEqual(summary["queries"], [])

    def test_sample_check_finds_an_empty_table(self):
        self.fake.empty_tables = {"orders"}
        summary = self.run_suite()
        self.assertIn("orders has no rows", summary["error"])

    def test_percent_encoded_partition_values_fail_setup(self):
        self.fake.encoded_tables = {"part"}
        summary = self.run_suite()
        self.assertIn("p_brand values are still percent-encoded", summary["error"])
        # Only string partition columns can carry an escaped character.
        checks = [s for s in self.fake.statements if "strpos" in s]
        self.assertEqual(len(checks), 1)

    def test_partition_checks_cover_the_string_partition_columns(self):
        self.run_suite(queries=["1"])
        checks = [s for s in self.fake.statements if "strpos" in s]
        self.assertEqual(
            checks,
            [
                "SELECT 1 FROM part WHERE strpos(p_brand, '%') > 0 LIMIT 1",
                "SELECT 1 FROM customer WHERE strpos(c_mktsegment, '%') > 0 LIMIT 1",
            ],
        )

    def test_setup_failure_is_reported(self):
        self.fake.fail_marker = "CREATE EXTERNAL TABLE IF NOT EXISTS orders"
        summary = self.run_suite()
        self.assertIn("QueryFailed: HTTP 500", summary["error"])
        self.assertEqual(summary["queries"], [])

    def test_unexpected_answer_fails_cleanly(self):
        class Garbage(FakeCoordinator):
            def open(self, request, timeout=None):
                if "query one" in request.full_url or "query+one" in request.full_url:
                    return Response({"something": "else"})
                return super().open(request, timeout)

        self.fake = Garbage()
        summary = self.run_suite(warmup=False)
        self.assertIn("unexpected answer", summary["queries"][0]["error"])
        self.assertIsNone(summary["queries"][1]["error"])

    def test_query_number(self):
        for name in ("q01", "q1", "Q1", "1", " q001 "):
            self.assertEqual(rq.query_number(name), 1)
        with self.assertRaises(ValueError):
            rq.query_number("s1")

    def test_config_from_env(self):
        cfg = rq.config_from_env(
            {
                "COORDINATOR_URL": "http://c:9000",
                "EXPECTED_WORKERS": "32",
                "ITERATIONS": "3",
                "QUERIES": "q01,q06",
                "WARMUP": "0",
                "SETTINGS": "distributed.compression=zstd,datafusion.execution.batch_size=16384",
            }
        )
        self.assertEqual(cfg["coordinator_url"], "http://c:9000")
        self.assertEqual((cfg["expected_workers"], cfg["iterations"]), (32, 3))
        self.assertEqual(cfg["queries"], ["q01", "q06"])
        self.assertFalse(cfg["warmup"])
        self.assertEqual(
            (cfg["partitioned"], cfg["money_type"]), (False, "decimal(15, 2)")
        )
        self.assertEqual(
            cfg["settings"],
            {
                "distributed.compression": "zstd",
                "datafusion.execution.batch_size": "16384",
            },
        )

    def test_config_takes_the_layout_from_the_environment(self):
        cfg = rq.config_from_env({"PARTITIONED": "1", "MONEY_TYPE": "decimal(12, 2)"})
        self.assertEqual(
            (cfg["partitioned"], cfg["money_type"]), (True, "decimal(12, 2)")
        )

    def test_config_defaults_warm_up_and_check_the_sf1000_row_counts(self):
        cfg = rq.config_from_env({})
        self.assertTrue(cfg["warmup"])
        self.assertEqual(cfg["scale_factor"], 1000)
        self.assertEqual(cfg["expected_rows"][16], 27840)

    def test_config_can_turn_row_checks_off_and_unknown_scale_factors_are_unchecked(
        self,
    ):
        self.assertEqual(rq.config_from_env({"CHECK_ROWS": "0"})["expected_rows"], {})
        self.assertEqual(
            rq.config_from_env({"SCALE_FACTOR": "10"})["expected_rows"], {}
        )


if __name__ == "__main__":
    unittest.main()
