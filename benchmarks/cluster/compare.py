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

"""Print a Markdown table comparing the per-query mean times of run.py result files.

    python3 compare.py results/tpch-ballista-*.json results/tpch-spark-*.json

One column per file, in the order given, with the mean of each query's timed
iterations in seconds, and a total row. A query that failed in a run shows FAIL,
or OOM if that is its error, and leaves that run's total out (shown as a dash).
Column headings are the engine and run name unless given with --label, once per
file.
"""

import argparse
import json
import sys


def load(path):
    with open(path) as f:
        return json.load(f)


def total(run):
    """The run's total: its recorded total if it has one, else the sum of its means."""
    return run.get("total", sum(q["mean"] for q in run["queries"]))


def failure_cell(q):
    """The cell for a query without a time: blank if it never ran."""
    if q is None:
        return ""
    return "OOM" if q.get("error") == "OOM" else "FAIL"


def rows(runs, labels):
    """The cells of the table: the headings, one row per query, and the totals."""
    queries = sorted({q["query"] for run in runs for q in run["queries"]})
    by_query = [{q["query"]: q for q in run["queries"]} for run in runs]
    result = [["Query"] + [f"{label} (s)" for label in labels]]
    complete = [True] * len(runs)
    for number in queries:
        cells = []
        for i, results in enumerate(by_query):
            q = results.get(number)
            if q is None or q.get("error") or "mean" not in q:
                cells.append(failure_cell(q))
                complete[i] = False
            else:
                cells.append(f"{q['mean']:.2f}")
        result.append([str(number)] + cells)
    result.append(
        ["**Total**"]
        + [f"**{total(run):.2f}**" if ok else "-" for run, ok in zip(runs, complete)]
    )
    return result


def table(runs, labels):
    header, *body = rows(runs, labels)
    rule = ["----:"] + ["-" * max(len(label) + 4, 5) + ":" for label in labels]
    return "\n".join("| " + " | ".join(row) + " |" for row in [header, rule, *body])


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("results", nargs="+", help="Result files written by run.py")
    parser.add_argument(
        "--label",
        action="append",
        default=[],
        help="Column heading, once per file in order",
    )
    args = parser.parse_args()
    if args.label and len(args.label) != len(args.results):
        sys.exit("give --label once per result file, or not at all")
    runs = [load(p) for p in args.results]
    labels = args.label or [f"{run['engine']} {run['run']}" for run in runs]
    print(table(runs, labels))


if __name__ == "__main__":
    main()
