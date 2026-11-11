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

"""Expected row counts of the TPC-H queries, to catch an engine that is fast because it is wrong.

The Spark and Trino runners compare every execution's row count with these and
treat a difference as a failed query. A table definition that names a column
wrongly is the case this is for: Trino's Hive connector matches Parquet columns
by name, so the column reads as NULL without an error, and the query returns the
wrong number of rows.

The queries are the ones in `benchmarks/queries/`. Only queries whose row count
does not depend on the data are listed for SF1000:

- Fixed by the query or the data domain: q01, q04, q05, q06, q07, q08, q09, q12,
  q14, q17, q19 and q22 (group-by keys from small fixed sets, or a single
  aggregate row), q02, q03, q10, q18 and q21 (a LIMIT that the data exceeds at
  this scale), q15 (the one top supplier) and q16 (every brand, type and size
  combination that can appear: 24 brands * 145 types * 8 sizes = 27,840).
- q11 returns no rows. The query keeps the specification's fraction 0.0001
  instead of dividing it by the scale factor, so no part reaches it once the
  scale factor is above a few. Every engine gets an empty result; the
  aggregation underneath is still done.
- q13 and q20 are left out: their row counts grow with the scale factor without
  settling.

How these were established: the queries were run with DuckDB on `tpchgen-cli`
data at SF1 and SF10. The SF1 row counts match the specification's official
answer set for all 22 queries, and every count below already holds at SF10. To
benchmark another scale factor, add its counts here or turn the checks off
(`--no-check-rows`).
"""

# query number -> number of rows
SF1000 = {
    1: 4,
    2: 100,
    3: 10,
    4: 5,
    5: 5,
    6: 1,
    7: 4,
    8: 2,
    9: 175,
    10: 20,
    11: 0,
    12: 2,
    14: 1,
    15: 1,
    16: 27840,
    17: 1,
    18: 100,
    19: 1,
    21: 100,
    22: 7,
}

# scale factor -> expected row counts. A scale factor that is not here is not checked.
EXPECTED_ROWS = {1000: SF1000}
