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

"""Helpers and constants shared by the python tests."""

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.json as pa_json
import pyarrow.parquet as pq
import pytest

TEST_CSV = "testdata/test.csv"
TEST_PARQUET = "testdata/test.parquet"

# Known gaps in distributed execution, kept as strict xfails so they are
# noticed once fixed.
XFAIL_COMPRESSED_READ = pytest.mark.xfail(
    strict=True,
    reason="file_compression_type is not applied on executors, "
    "compressed files are parsed as plain text",
)
XFAIL_IN_MEMORY = pytest.mark.xfail(
    strict=True,
    reason="in-memory table providers cannot be serialized "
    "(no extension codec handles them)",
)

# contents of TEST_CSV
EXPECTED_A = [1, 2, 3, 4, 5]
EXPECTED_B = [-2, -3, -4, -5, -6]


def to_table(df) -> pa.Table:
    return pa.Table.from_batches(df.collect(), schema=df.schema())


def sorted_pydict(table: pa.Table, by: str = "a") -> dict:
    return table.sort_by(by).to_pydict()


def data_files(path, suffix):
    return sorted(p for p in path.rglob(f"*{suffix}") if p.is_file())


def read_back(fmt, path) -> pa.Table:
    """Reads written output with pyarrow, independently of Ballista."""
    files = data_files(path, f".{fmt}") if path.is_dir() else [path]
    assert len(files) > 0
    if fmt == "csv":
        tables = [pa_csv.read_csv(f) for f in files]
    elif fmt == "json":
        tables = [pa_json.read_json(f) for f in files]
    else:
        tables = [pq.read_table(f) for f in files]
    return pa.concat_tables(tables)


def write_df(df, fmt, path, write_options=None):
    if fmt == "csv":
        df.write_csv(str(path), with_header=True, write_options=write_options)
    elif fmt == "json":
        df.write_json(str(path), write_options=write_options)
    else:
        df.write_parquet(str(path), write_options=write_options)
