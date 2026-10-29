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

"""Tests for tpcbench.py's statement handling and bookkeeping, against a stand-in for PySpark.

The stand-in records the SQL it is given and returns a fixed number of rows, so
these run without Spark or Java. Running the queries on Spark itself is what
run.py --engine spark does.

Run from benchmarks/cluster/spark:  python3 -m unittest -v test_tpcbench
"""

import argparse
import contextlib
import io
import os
import sys
import tempfile
import types
import unittest

# tpcbench imports pyspark.sql.SparkSession at module level.
pyspark = types.ModuleType("pyspark")
pyspark.sql = types.ModuleType("pyspark.sql")
pyspark.sql.SparkSession = None
sys.modules.setdefault("pyspark", pyspark)
sys.modules.setdefault("pyspark.sql", pyspark.sql)

import tpcbench  # noqa: E402

QUERIES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "queries")


class FakeDataFrame:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return self.rows


class FakeSpark:
    def __init__(self, rows=4, fail_marker=None):
        self.statements = []
        self.rows = rows
        self.fail_marker = fail_marker
        self.sparkContext = types.SimpleNamespace(setJobDescription=lambda d: None)

    def sql(self, statement):
        self.statements.append(statement)
        if self.fail_marker and self.fail_marker in statement:
            raise RuntimeError("SparkOutOfMemoryError")
        is_ddl = not tpcbench.is_select(statement)
        return FakeDataFrame([] if is_ddl else [(i,) for i in range(self.rows)])


def bench_args(**kw):
    return argparse.Namespace(**{"benchmark": "tpch", "iterations": 2, **kw})


class TpcbenchTest(unittest.TestCase):
    def setUp(self):
        self._log = tpcbench.log
        tpcbench.log = lambda message: None

    def tearDown(self):
        tpcbench.log = self._log

    def run_query(self, spark, statements, expected_rows=None, **kw):
        with contextlib.redirect_stdout(io.StringIO()) as out:
            record = tpcbench.run_query(spark, bench_args(**kw), 15, statements, expected_rows or {})
        self.assertIn(tpcbench.QUERY_RESULT_PREFIX, out.getvalue())
        return record

    def test_q15_view_becomes_a_temp_view_and_all_statements_run(self):
        statements = tpcbench.load_statements(QUERIES_DIR, 15, [])
        self.assertEqual(len(statements), 3)
        self.assertTrue(statements[0].startswith("CREATE OR REPLACE TEMP VIEW revenue0 (supplier_no, total_revenue)"))
        self.assertTrue(statements[2].lower().startswith("drop view revenue0"))
        spark = FakeSpark(rows=1)
        record = self.run_query(spark, statements)
        self.assertIsNone(record["error"])
        self.assertEqual(len(spark.statements), 6)  # three statements, two iterations
        self.assertEqual([it["row_count"] for it in record["iterations"]], [1, 1])  # the SELECT's rows

    def test_every_tpch_query_has_one_select(self):
        for n in range(1, 23):
            statements = tpcbench.load_statements(QUERIES_DIR, n, [])
            self.assertEqual(sum(tpcbench.is_select(s) for s in statements), 1, f"q{n}")
            self.assertFalse(any(s.endswith(";") for s in statements))

    def test_wrong_row_count_fails_the_query(self):
        record = self.run_query(FakeSpark(rows=4), ["select 1"], expected_rows={15: 1})
        self.assertIn("returned 4 rows, expected 1", record["error"])
        self.assertEqual(record["iterations"], [])

    def test_failure_stops_the_query_and_is_recorded(self):
        record = self.run_query(FakeSpark(fail_marker="lineitem"), ["select * from lineitem"])
        self.assertIn("SparkOutOfMemoryError", record["error"])
        self.assertEqual(record["iterations"], [])

    def test_tpcds_renames_apply_only_when_the_data_uses_the_generated_names(self):
        generated = {"ib_income_band_id", "r_reason_description", "cr_return_amt_inc_tax"}
        renames = tpcbench.column_renames("tpcds", generated)
        self.assertEqual(renames, [("ib_income_band_sk", "ib_income_band_id"),
                                   ("r_reason_desc", "r_reason_description")])
        self.assertEqual(tpcbench.column_renames("tpch", generated), [])
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "q1.sql"), "w") as f:
                f.write("select ib_income_band_sk, ib_income_band_sk_x from income_band;")
            (statement,) = tpcbench.load_statements(d, 1, renames)
        self.assertEqual(statement, "select ib_income_band_id, ib_income_band_sk_x from income_band")

    def test_selected_queries(self):
        self.assertEqual(tpcbench.selected_queries("tpch", None), list(range(1, 23)))
        self.assertEqual(tpcbench.selected_queries("tpcds", "q1, 99"), [1, 99])
        with self.assertRaises(SystemExit):
            tpcbench.selected_queries("tpch", "23")


if __name__ == "__main__":
    unittest.main()
