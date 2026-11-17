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

import gzip
import shutil

from ballista import (
    BallistaSessionConfig,
    BallistaSessionContext,
)
from datafusion import (
    CsvReadOptions,
    col,
    lit,
)
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ._util import (
    EXPECTED_A,
    EXPECTED_B,
    TEST_CSV,
    TEST_PARQUET,
    XFAIL_COMPRESSED_READ,
    XFAIL_IN_MEMORY,
    sorted_pydict,
    to_table,
)


def test_read_csv(ctx):
    df = ctx.read_csv(TEST_CSV, has_header=True)
    result = to_table(df)
    assert result.num_rows == 5
    assert sorted_pydict(result) == {"a": EXPECTED_A, "b": EXPECTED_B}


def test_read_csv_with_schema(ctx):
    schema = pa.schema([("a", pa.int32()), ("b", pa.float64())])
    result = to_table(ctx.read_csv(TEST_CSV, schema=schema, has_header=True))
    assert result.schema == schema
    assert sorted_pydict(result) == {
        "a": EXPECTED_A,
        "b": [float(b) for b in EXPECTED_B],
    }


def test_read_csv_without_header(ctx):
    schema = pa.schema([("x", pa.string()), ("y", pa.string())])
    result = to_table(ctx.read_csv(TEST_CSV, schema=schema, has_header=False))
    # header line is read as data
    assert result.num_rows == 6
    assert "a" in result.column("x").to_pylist()


def test_read_csv_delimiter(ctx, tmp_path):
    path = tmp_path / "pipe.csv"
    path.write_text(open(TEST_CSV).read().replace(",", "|"))
    result = to_table(ctx.read_csv(str(path), has_header=True, delimiter="|"))
    assert sorted_pydict(result) == {"a": EXPECTED_A, "b": EXPECTED_B}


@XFAIL_COMPRESSED_READ
def test_read_csv_gzip(ctx, tmp_path):
    path = tmp_path / "test.csv.gz"
    with open(TEST_CSV, "rb") as src, gzip.open(path, "wb") as dst:
        shutil.copyfileobj(src, dst)
    result = to_table(
        ctx.read_csv(
            str(path),
            has_header=True,
            file_extension=".csv.gz",
            file_compression_type="gzip",
        )
    )
    assert sorted_pydict(result) == {"a": EXPECTED_A, "b": EXPECTED_B}


def test_read_csv_multiple_paths(ctx, tmp_path):
    paths = []
    for i in range(2):
        path = tmp_path / f"part{i}.csv"
        shutil.copy(TEST_CSV, path)
        paths.append(str(path))
    result = to_table(ctx.read_csv(paths, has_header=True))
    assert result.num_rows == 10


def test_read_csv_with_options(ctx, tmp_path):
    path = tmp_path / "options.csv"
    path.write_text("a;b\n1;'x;y'\n# comment\n2;'z'\n")
    options = CsvReadOptions(has_header=True, delimiter=";", quote="'", comment="#")
    result = to_table(ctx.read_csv(str(path), options=options))
    assert sorted_pydict(result) == {"a": [1, 2], "b": ["x;y", "z"]}


def test_register_csv(ctx):
    ctx.register_csv("test", TEST_CSV, has_header=True)
    result = to_table(ctx.sql("SELECT * FROM test"))
    assert sorted_pydict(result) == {"a": EXPECTED_A, "b": EXPECTED_B}


def test_read_parquet(ctx):
    df = ctx.read_parquet(TEST_PARQUET)
    result = to_table(df)
    assert result.num_rows == 8
    assert result.equals(pq.read_table(TEST_PARQUET))


def test_read_parquet_with_schema(ctx):
    schema = pa.schema([("id", pa.int32()), ("int_col", pa.int32())])
    result = to_table(ctx.read_parquet(TEST_PARQUET, schema=schema))
    assert result.schema == schema
    assert result.num_rows == 8


def test_read_parquet_options(ctx):
    df = ctx.read_parquet(
        TEST_PARQUET,
        parquet_pruning=False,
        skip_metadata=False,
        file_sort_order=[[col("id").sort(ascending=True)]],
    )
    result = to_table(df.filter(col("id") > lit(3)).select("id"))
    expected = [
        i for i in pq.read_table(TEST_PARQUET).column("id").to_pylist() if i > 3
    ]
    assert sorted(result.column("id").to_pylist()) == sorted(expected)


def test_read_parquet_partitioned(ctx, tmp_path):
    source = pq.read_table(TEST_PARQUET)
    for year in ("2023", "2024"):
        (tmp_path / f"year={year}").mkdir()
        pq.write_table(source, tmp_path / f"year={year}" / "data.parquet")

    df = ctx.read_parquet(str(tmp_path), table_partition_cols=[("year", pa.string())])
    assert to_table(df).num_rows == 16

    result = to_table(df.filter(col("year") == lit("2024")).select("id", "year"))
    assert result.num_rows == 8
    assert set(result.column("year").to_pylist()) == {"2024"}


def test_register_parquet(ctx):
    ctx.register_parquet("test", TEST_PARQUET)
    result = to_table(ctx.sql("SELECT * FROM test"))
    assert result.num_rows == 8


@pytest.mark.parametrize("with_schema", [False, True])
def test_read_json(ctx, ndjson_file, with_schema):
    schema = pa.schema([("a", pa.int64()), ("b", pa.int64())]) if with_schema else None
    result = to_table(ctx.read_json(str(ndjson_file), schema=schema))
    assert sorted_pydict(result) == {"a": EXPECTED_A, "b": EXPECTED_B}


@XFAIL_COMPRESSED_READ
def test_read_json_gzip(ctx, ndjson_file, tmp_path):
    path = tmp_path / "test.json.gz"
    with open(ndjson_file, "rb") as src, gzip.open(path, "wb") as dst:
        shutil.copyfileobj(src, dst)
    result = to_table(
        ctx.read_json(
            str(path), file_extension=".json.gz", file_compression_type="gzip"
        )
    )
    assert sorted_pydict(result) == {"a": EXPECTED_A, "b": EXPECTED_B}


def test_register_json(ctx, ndjson_file):
    ctx.register_json("test", str(ndjson_file))
    result = to_table(ctx.sql("SELECT a, b FROM test WHERE a > 3"))
    assert sorted_pydict(result) == {"a": [4, 5], "b": [-5, -6]}


@pytest.mark.parametrize(
    "stored_as,location,options",
    [
        ("CSV", TEST_CSV, "OPTIONS ('format.has_header' 'true')"),
        ("PARQUET", TEST_PARQUET, ""),
    ],
)
def test_create_external_table(ctx, stored_as, location, options):
    ctx.sql(
        f"CREATE EXTERNAL TABLE ext STORED AS {stored_as} LOCATION '{location}' {options}"
    ).collect()
    assert ctx.table_exist("ext")
    result = to_table(ctx.sql("SELECT count(*) AS c FROM ext"))
    assert result.column("c").to_pylist()[0] > 0


def test_table_registration(ctx):
    ctx.register_csv("test", TEST_CSV, has_header=True)
    assert ctx.table_exist("test")

    ctx.register_view("big_a", ctx.table("test").filter(col("a") > lit(3)))
    result = to_table(ctx.sql("SELECT a FROM big_a"))
    assert sorted(result.column("a").to_pylist()) == [4, 5]

    ctx.deregister_table("test")
    assert not ctx.table_exist("test")


#
# in-memory sources
#


@XFAIL_IN_MEMORY
def test_from_pydict(ctx):
    df = ctx.from_pydict({"a": EXPECTED_A, "b": EXPECTED_B})
    assert sorted_pydict(to_table(df)) == {"a": EXPECTED_A, "b": EXPECTED_B}


@XFAIL_IN_MEMORY
def test_from_arrow(ctx):
    table = pa.table({"a": EXPECTED_A, "b": EXPECTED_B})
    assert sorted_pydict(to_table(ctx.from_arrow(table))) == table.to_pydict()


@XFAIL_IN_MEMORY
def test_register_record_batches(ctx):
    batch = pa.RecordBatch.from_pydict({"a": EXPECTED_A, "b": EXPECTED_B})
    ctx.register_record_batches("mem", [[batch]])
    result = to_table(ctx.sql("SELECT sum(a) AS s FROM mem"))
    assert result.to_pydict() == {"s": [15]}


@pytest.mark.xfail(
    strict=True,
    reason="BallistaCacheFactory is not installed on python sessions, cache() "
    "falls back to an in-memory table which cannot be serialized",
)
@pytest.mark.parametrize("cache_noop", [True, False])
def test_dataframe_cache(cluster, cache_noop):
    (address, port) = cluster
    ctx = BallistaSessionContext(
        address=f"df://{address}:{port}",
        config=BallistaSessionConfig().with_cache_noop(cache_noop),
    )
    cached = ctx.read_csv(TEST_CSV, has_header=True).cache()
    assert sorted_pydict(to_table(cached)) == {"a": EXPECTED_A, "b": EXPECTED_B}

    result = to_table(cached.filter(col("a") > lit(3)))
    assert sorted_pydict(result) == {"a": [4, 5], "b": [-5, -6]}
