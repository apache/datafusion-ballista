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

"""Run the TPC-H or TPC-DS queries on Spark (with or without Comet) and print the timings as JSON.

Runs as the Spark driver, in client mode, inside the driver Job that run.py
creates; it can also be passed to any spark-submit. It runs the same query files
as the Ballista runners (benchmarks/queries and benchmarks/queries-tpcds) the
same way:

- Each table is a directory of Parquet files under --data. Spark finds Hive-style
  partition directories (lineitem/l_shipdate=1994-01-01/) itself and prunes them.
- A query file may hold several statements: q15 creates a view, selects from it
  and drops it, and some TPC-DS files hold two queries. They run in order and are
  timed together, as the Ballista runners time them.
- Each query runs --iterations times before the next one starts. The first
  iteration is cold and counts. Results are collected to the driver.
- With --scale-factor 1000, every TPC-H result's row count is checked against
  tpch_expected_rows.py; a difference fails the query.

Output: a line "SPARK-QUERY-RESULT {json}" as each query finishes, so a run that
dies part way can still be recovered from the log, then "===RESULTS-JSON-FILE==="
followed by the full summary. The exit status is non-zero if any query failed.
"""

import argparse
import json
import os
import re
import sys
import time

from pyspark.sql import SparkSession

try:
    from tpch_expected_rows import EXPECTED_ROWS
except ImportError:  # run from the source tree, where it is one directory up
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
    from tpch_expected_rows import EXPECTED_ROWS

RESULTS_MARKER = "===RESULTS-JSON-FILE==="
QUERY_RESULT_PREFIX = "SPARK-QUERY-RESULT "

TABLES = {
    "tpch": ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"],
    "tpcds": [
        "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
        "customer_address", "customer_demographics", "date_dim", "household_demographics",
        "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store",
        "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
        "web_sales", "web_site",
    ],
}
NUM_QUERIES = {"tpch": 22, "tpcds": 99}

# TPC-DS columns that tpcgen-cli names differently from the specification, as
# (specification name, generated name). The Ballista tpcds runner rewrites the
# queries the same way when the data uses the generated names.
TPCDS_COLUMN_RENAMES = [
    ("ib_income_band_sk", "ib_income_band_id"),
    ("r_reason_desc", "r_reason_description"),
    ("cr_return_amt_inc_tax", "cr_return_amount_inc_tax"),
]


def log(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def register_tables(spark, benchmark, data_path):
    """Register each table as a temp view. Returns the column names of all of them."""
    columns = set()
    for table in TABLES[benchmark]:
        df = spark.read.parquet(f"{data_path.rstrip('/')}/{table}")
        df.createOrReplaceTempView(table)
        columns.update(df.columns)
        log(f"registered {table} ({len(df.columns)} columns)")
    return columns


def column_renames(benchmark, columns):
    if benchmark != "tpcds":
        return []
    return [(spec, generated) for spec, generated in TPCDS_COLUMN_RENAMES
            if generated in columns and spec not in columns]


def is_select(statement):
    """True for a query, False for DDL such as q15's CREATE VIEW and DROP VIEW."""
    code = [line for line in statement.splitlines() if line.strip() and not line.strip().startswith("--")]
    return bool(code) and code[0].strip().lstrip("(").split(None, 1)[0].lower() in ("select", "with")


def load_statements(queries_dir, number, renames):
    """The statements of qN.sql. Views become temp views, replaced if a failed run left one."""
    with open(os.path.join(queries_dir, f"q{number}.sql")) as f:
        text = f.read()
    for spec, generated in renames:
        text = re.sub(rf"\b{spec}\b", generated, text)
    statements = []
    for statement in text.split(";"):
        statement = statement.strip()
        if statement:
            statements.append(re.sub(r"(?i)^create\s+view\b", "CREATE OR REPLACE TEMP VIEW", statement))
    return statements


def execute(spark, benchmark, number, statements):
    """Run a query's statements once. Returns (seconds, rows returned by its last SELECT)."""
    spark.sparkContext.setJobDescription(f"{benchmark} q{number}")
    start = time.monotonic()
    row_count = None
    for statement in statements:
        rows = spark.sql(statement).collect()
        if is_select(statement):
            row_count = len(rows)
    return time.monotonic() - start, row_count


def describe_error(e):
    """The error's first line and its root cause.

    A Spark failure is a long Java stack trace whose cause, such as an S3 403,
    is in its last "Caused by:" line.
    """
    lines = str(e).splitlines()
    first = f"{type(e).__name__}: {lines[0] if lines else ''}"
    causes = [line.strip() for line in lines if line.strip().startswith("Caused by:")]
    return (f"{first} ... {causes[-1]}" if causes else first)[:2000]


def run_query(spark, args, number, statements, expected_rows):
    """Run one query args.iterations times. Stops at the first failed iteration."""
    record = {"query": number, "iterations": [], "error": None}
    for i in range(args.iterations):
        try:
            elapsed, row_count = execute(spark, args.benchmark, number, statements)
            expected = expected_rows.get(number)
            if expected is not None and row_count != expected:
                raise RuntimeError(f"q{number} returned {row_count} rows, expected {expected}")
        except Exception as e:  # noqa: BLE001 - a failed query must not stop the suite
            record["error"] = describe_error(e)
            log(f"q{number} iteration {i + 1}/{args.iterations} FAILED: {record['error']}")
            break
        record["iterations"].append({"elapsed": round(elapsed, 3), "row_count": row_count})
        log(f"q{number} iteration {i + 1}/{args.iterations}: {elapsed:.2f}s, {row_count} rows")
    print(QUERY_RESULT_PREFIX + json.dumps(record), flush=True)
    return record


def selected_queries(benchmark, queries):
    if not queries:
        return list(range(1, NUM_QUERIES[benchmark] + 1))
    numbers = [int(q.strip().lstrip("qQ")) for q in queries.split(",") if q.strip()]
    for n in numbers:
        if not 1 <= n <= NUM_QUERIES[benchmark]:
            raise SystemExit(f"{benchmark} has queries 1 to {NUM_QUERIES[benchmark]}, not {n}")
    return numbers


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--benchmark", choices=sorted(TABLES), required=True)
    parser.add_argument("--data", required=True, help="Directory with one subdirectory of Parquet files per table")
    parser.add_argument("--queries", required=True, help="Directory with the q1.sql ... qN.sql files")
    parser.add_argument("--query", help="Comma-separated query numbers to run (default: all)")
    parser.add_argument("--iterations", type=int, default=2)
    parser.add_argument("--scale-factor", type=int, default=1000,
                        help="Selects the expected TPC-H row counts (default: 1000)")
    parser.add_argument("--no-check-rows", action="store_true", help="Do not check the row counts")
    args = parser.parse_args()

    numbers = selected_queries(args.benchmark, args.query)
    expected_rows = {}
    if args.benchmark == "tpch" and not args.no_check_rows:
        expected_rows = EXPECTED_ROWS.get(args.scale_factor, {})

    spark = SparkSession.builder.appName(f"{args.benchmark} benchmark").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    conf = dict(spark.sparkContext.getConf().getAll())
    summary = {
        "spark_version": spark.version,
        "benchmark": args.benchmark,
        "iterations": args.iterations,
        "row_checks": sorted(expected_rows),
        "spark_conf": {k: v for k, v in sorted(conf.items()) if k.startswith(("spark.sql.", "spark.comet.",
                       "spark.executor.", "spark.memory.", "spark.shuffle.", "spark.plugins",
                       "spark.serializer", "spark.io.", "spark.kubernetes.executor."))},
        "queries": [],
    }
    log(f"Spark {spark.version}, {args.benchmark} queries {numbers[0]}..{numbers[-1]} x {args.iterations}")
    try:
        renames = column_renames(args.benchmark, register_tables(spark, args.benchmark, args.data))
        if renames:
            summary["column_renames"] = renames
        for number in numbers:
            statements = load_statements(args.queries, number, renames)
            summary["queries"].append(run_query(spark, args, number, statements, expected_rows))
    except Exception as e:  # noqa: BLE001 - report a setup failure in the summary
        summary["error"] = f"{type(e).__name__}: {e}"[:1000]
        log(f"setup failed: {summary['error']}")
    finally:
        print(RESULTS_MARKER, flush=True)
        print(json.dumps(summary), flush=True)
        spark.stop()
    failed = summary.get("error") or any(q["error"] for q in summary["queries"])
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
