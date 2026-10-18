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
import json
import shutil

from ballista import BallistaConfigExtension, BallistaSessionContext, setup_test_cluster
from datafusion import CsvReadOptions, SessionConfig, SessionContext, col, lit, udf
from datafusion.dataframe import (
    DataFrameWriteOptions,
    InsertOp,
    ParquetColumnOptions,
    ParquetWriterOptions,
)
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.dataset as ds
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
XFAIL_UDF = pytest.mark.xfail(
    strict=True,
    reason="python udfs cannot be serialized "
    "(no logical extension codec handles them)",
)

# contents of TEST_CSV
EXPECTED_A = [1, 2, 3, 4, 5]
EXPECTED_B = [-2, -3, -4, -5, -6]


@pytest.fixture(scope="module")
def cluster():
    return setup_test_cluster()


@pytest.fixture
def ctx(cluster):
    (address, port) = cluster
    return BallistaSessionContext(address=f"df://{address}:{port}")


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


@pytest.fixture
def ndjson_file(tmp_path):
    path = tmp_path / "test.json"
    path.write_text(
        "\n".join(json.dumps({"a": a, "b": b}) for a, b in zip(EXPECTED_A, EXPECTED_B))
        + "\n"
    )
    return path


#
# basic queries
#


def test_select_one(ctx):
    df = ctx.sql("SELECT 1")
    batches = df.collect()
    assert len(batches) == 1


def test_sql_aggregate(ctx):
    ctx.register_csv("test", TEST_CSV, has_header=True)
    result = to_table(
        ctx.sql("SELECT count(*) AS c, sum(a) AS s, min(b) AS m FROM test")
    )
    assert result.to_pydict() == {"c": [5], "s": [15], "m": [-6]}


@pytest.mark.parametrize(
    "query",
    [
        "SELECT bool_col, count(*) AS c, sum(int_col) AS s FROM t GROUP BY bool_col",
        "SELECT l.id, r.int_col FROM t l JOIN t r ON l.id = r.id",
        "SELECT id, int_col FROM t ORDER BY id DESC LIMIT 3",
        "SELECT id FROM t UNION ALL SELECT id FROM t",
        "SELECT id, sum(int_col) OVER (PARTITION BY bool_col ORDER BY id) AS w FROM t",
        "SELECT DISTINCT tinyint_col FROM t",
    ],
)
def test_sql_matches_local(ctx, query):
    """Distributed result must match plain DataFusion on the same query."""
    local = SessionContext()
    for c in (ctx, local):
        c.register_parquet("t", TEST_PARQUET)

    result = to_table(ctx.sql(query))
    expected = to_table(local.sql(query))

    sort_keys = [(name, "ascending") for name in expected.column_names]
    assert result.sort_by(sort_keys).equals(expected.sort_by(sort_keys))


def test_read_dataframe_api(ctx):
    df = (
        ctx.read_csv(TEST_CSV, has_header=True)
        .select("a", "b")
        .filter(col("a") > lit(2))
    )
    result = df.collect()[0]

    assert result.column(0) == pa.array([3, 4, 5])
    assert result.column(1) == pa.array([-4, -5, -6])


def test_custom_session_config(cluster):
    (address, port) = cluster
    config = SessionConfig().with_target_partitions(3)
    ctx = BallistaSessionContext(address=f"df://{address}:{port}", config=config)
    ctx.register_parquet("t", TEST_PARQUET)
    result = to_table(ctx.sql("SELECT count(*) AS c FROM t"))
    assert result.to_pydict() == {"c": [8]}


def test_cluster_config(cluster):
    (address, port) = cluster
    ctx = BallistaSessionContext(
        address=f"df://{address}:{port}",
        cluster_config={"datafusion.execution.batch_size": 2},
    )
    ctx.register_parquet("t", TEST_PARQUET)
    result = to_table(ctx.sql("SELECT count(*) AS c FROM t"))
    assert result.to_pydict() == {"c": [8]}


def test_sql_explain_analyze(ctx):
    ctx.register_parquet("t", TEST_PARQUET)
    result = to_table(
        ctx.sql("EXPLAIN ANALYZE SELECT bool_col, count(*) FROM t GROUP BY bool_col")
    )

    assert result.column_names == ["plan_type", "plan"]
    plan = "\n".join(result.column("plan").to_pylist())
    assert "output_rows" in plan


def test_dataframe_explain_analyze(ctx, capsys):
    ctx.register_parquet("t", TEST_PARQUET)
    ctx.sql("SELECT count(*) FROM t").explain(analyze=True)

    assert "output_rows" in capsys.readouterr().out


def test_invalid_sql_raises(ctx):
    with pytest.raises(Exception):
        ctx.sql("SELECT * FROM table_does_not_exist").collect()


def test_missing_file_raises(ctx, tmp_path):
    with pytest.raises(Exception):
        ctx.read_parquet(str(tmp_path / "missing.parquet")).collect()


#
# reads
#


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


#
# udfs
#

# note: looks like exception is coming from schedule's 
#       logical planner

def is_null(array: pa.Array) -> pa.Array:
    return array.is_null()


is_null_udf = udf(is_null, [pa.int64()], pa.bool_(), "stable", name="is_null_udf")

@XFAIL_UDF
def test_udf_dataframe(ctx):
    df = ctx.read_csv(TEST_CSV, has_header=True).select(
        col("a"), is_null_udf(col("a")).alias("n")
    )
    assert sorted_pydict(to_table(df)) == {"a": EXPECTED_A, "n": [False] * 5}


@XFAIL_UDF
def test_udf_sql(ctx):
    ctx.register_csv("test", TEST_CSV, has_header=True)
    ctx.register_udf(is_null_udf)
    result = to_table(ctx.sql("SELECT a, is_null_udf(a) AS n FROM test"))
    assert sorted_pydict(result) == {"a": EXPECTED_A, "n": [False] * 5}


#
# writes
#


@pytest.mark.parametrize("fmt", ["csv", "json", "parquet"])
def test_write_roundtrip(ctx, tmp_path, fmt):
    df = ctx.read_csv(TEST_CSV, has_header=True)
    out = tmp_path / "out"
    write_df(df, fmt, out)
    assert sorted_pydict(read_back(fmt, out)) == {"a": EXPECTED_A, "b": EXPECTED_B}


@pytest.mark.parametrize(
    "with_header",
    [
        pytest.param(
            True,
            marks=pytest.mark.xfail(
                strict=True, reason="executor CSV writer quotes every field"
            ),
        ),
        False,
    ],
)
def test_write_csv_header(ctx, tmp_path, with_header):
    df = ctx.read_csv(TEST_CSV, has_header=True)
    out = tmp_path / "out"
    df.write_csv(str(out), with_header=with_header)

    lines = []
    for f in data_files(out, ".csv"):
        lines.extend(f.read_text().splitlines())
    assert ("a,b" in lines) == with_header
    assert len(lines) == 5 + (1 if with_header else 0) * len(data_files(out, ".csv"))


@pytest.mark.parametrize(
    "compression,compression_level,expected",
    [
        ("zstd", None, "ZSTD"),
        ("zstd", 10, "ZSTD"),
        ("snappy", None, "SNAPPY"),
        ("gzip", 6, "GZIP"),
        # pyarrow reports LZ4_RAW as UNKNOWN, only check the readback
        ("lz4", None, None),
        ("uncompressed", None, "UNCOMPRESSED"),
    ],
)
def test_write_parquet_compression(
    ctx, tmp_path, compression, compression_level, expected
):
    df = ctx.read_csv(TEST_CSV, has_header=True)
    out = tmp_path / "out"
    df.write_parquet(
        str(out), compression=compression, compression_level=compression_level
    )

    files = data_files(out, ".parquet")
    assert len(files) > 0
    for f in files if expected is not None else []:
        metadata = pq.ParquetFile(f).metadata
        for rg in range(metadata.num_row_groups):
            assert metadata.row_group(rg).column(0).compression.startswith(expected)
    assert sorted_pydict(read_back("parquet", out)) == {
        "a": EXPECTED_A,
        "b": EXPECTED_B,
    }


def test_write_parquet_with_options(ctx, tmp_path):
    df = ctx.read_csv(TEST_CSV, has_header=True)
    out = tmp_path / "out.parquet"
    options = ParquetWriterOptions(
        compression="gzip(4)",
        max_row_group_size=2,
        statistics_enabled="none",
        created_by="ballista-test",
        column_specific_options={"b": ParquetColumnOptions(compression="snappy")},
    )
    df.write_parquet_with_options(
        str(out), options, write_options=DataFrameWriteOptions(single_file_output=True)
    )

    metadata = pq.ParquetFile(out).metadata
    assert metadata.created_by == "ballista-test"
    assert metadata.num_rows == 5
    # 5 rows with max_row_group_size=2 -> 3 row groups
    assert metadata.num_row_groups == 3
    rg = metadata.row_group(0)
    assert rg.column(0).compression == "GZIP"
    assert rg.column(1).compression == "SNAPPY"
    assert rg.column(0).statistics is None
    assert sorted_pydict(pq.read_table(out)) == {"a": EXPECTED_A, "b": EXPECTED_B}


@pytest.mark.parametrize("fmt", ["csv", "json", "parquet"])
def test_write_single_file_output(ctx, tmp_path, fmt):
    df = ctx.read_csv(TEST_CSV, has_header=True)
    out = tmp_path / f"single.{fmt}"
    write_df(df, fmt, out, DataFrameWriteOptions(single_file_output=True))

    assert out.is_file()
    assert sorted_pydict(read_back(fmt, out)) == {"a": EXPECTED_A, "b": EXPECTED_B}


@pytest.mark.parametrize("fmt", ["csv", "json", "parquet"])
def test_write_sort_by(ctx, tmp_path, fmt):
    df = ctx.read_csv(TEST_CSV, has_header=True)
    out = tmp_path / f"sorted.{fmt}"
    write_df(
        df,
        fmt,
        out,
        DataFrameWriteOptions(
            single_file_output=True, sort_by=col("a").sort(ascending=False)
        ),
    )
    assert read_back(fmt, out).column("a").to_pylist() == sorted(
        EXPECTED_A, reverse=True
    )


@pytest.mark.parametrize("fmt", ["csv", "json", "parquet"])
def test_write_partition_by(ctx, tmp_path, fmt):
    df = ctx.read_csv(TEST_CSV, has_header=True).with_column(
        "even", (col("a") % lit(2)) == lit(0)
    )
    out = tmp_path / "out"
    write_df(df, fmt, out, DataFrameWriteOptions(partition_by="even"))

    assert sorted(p.name for p in out.iterdir() if p.is_dir()) == [
        "even=false",
        "even=true",
    ]
    # partition column is not stored in the files, read it back from the path
    result = ds.dataset(
        str(out), format=fmt if fmt != "csv" else "csv", partitioning="hive"
    ).to_table()
    assert sorted(
        zip(result.column("a").to_pylist(), map(str, result.column("even").to_pylist()))
    ) == [(a, str(a % 2 == 0).lower()) for a in EXPECTED_A]


def test_write_table(ctx, tmp_path):
    out = tmp_path / "table"
    out.mkdir()
    ctx.sql(
        f"CREATE EXTERNAL TABLE sink (a BIGINT, b BIGINT) STORED AS PARQUET LOCATION '{out}/'"
    ).collect()

    df = ctx.read_csv(TEST_CSV, has_header=True)
    df.write_table("sink")
    df.write_table("sink", DataFrameWriteOptions(insert_operation=InsertOp.APPEND))
    assert to_table(ctx.sql("SELECT count(*) AS c FROM sink")).to_pydict() == {
        "c": [10]
    }


def test_insert_into(ctx, tmp_path):
    out = tmp_path / "table"
    out.mkdir()
    ctx.sql(
        f"CREATE EXTERNAL TABLE sink (a BIGINT, b BIGINT) STORED AS CSV LOCATION '{out}/' "
        "OPTIONS ('format.has_header' 'true')"
    ).collect()
    ctx.sql("INSERT INTO sink VALUES (1, 2), (3, 4)").collect()

    result = to_table(ctx.sql("SELECT a, b FROM sink"))
    assert sorted_pydict(result) == {"a": [1, 3], "b": [2, 4]}


@pytest.mark.xfail(
    strict=True,
    reason="datafusion-proto drops CopyTo options, COPY ... OPTIONS are ignored",
)
def test_copy_to(ctx, tmp_path):
    ctx.register_csv("test", TEST_CSV, has_header=True)
    out = tmp_path / "out"
    ctx.sql(
        f"COPY (SELECT * FROM test WHERE a > 2) TO '{out}/' STORED AS PARQUET "
        "OPTIONS ('format.compression' 'snappy')"
    ).collect()

    files = data_files(out, ".parquet")
    assert len(files) > 0
    assert (
        pq.ParquetFile(files[0]).metadata.row_group(0).column(0).compression == "SNAPPY"
    )
    assert sorted_pydict(read_back("parquet", out)) == {
        "a": [3, 4, 5],
        "b": [-4, -5, -6],
    }


@pytest.mark.xfail(
    strict=True,
    reason="information_schema scans cannot be serialized across the FFI "
    "planner boundary",
)
def test_ballista_config_in_information_schema(ctx):
    result = to_table(
        ctx.sql(
            "SELECT name, value FROM information_schema.df_settings "
            "WHERE name = 'ballista.planner.adaptive.enabled'"
        )
    )
    assert result.num_rows == 1
    assert result.column("name").to_pylist() == ["ballista.planner.adaptive.enabled"]
    assert result.column("value").to_pylist()[0] in ("true", "false")


def adaptive_enabled_setting(ctx) -> list:
    return to_table(
        ctx.sql(
            "SELECT name, value FROM information_schema.df_settings "
            "WHERE name = 'ballista.planner.adaptive.enabled'"
        )
    ).to_pylist()


def test_ballista_config_extension_local():
    config = SessionConfig().with_information_schema(True)
    ctx = SessionContext(config.with_extension(BallistaConfigExtension()))
    rows = adaptive_enabled_setting(ctx)
    assert len(rows) == 1
    assert rows[0]["value"] in ("true", "false")


def test_ballista_config_extension_overrides():
    extension = BallistaConfigExtension({"ballista.planner.adaptive.enabled": "false"})
    config = SessionConfig().with_information_schema(True).with_extension(extension)
    assert adaptive_enabled_setting(SessionContext(config)) == [
        {"name": "ballista.planner.adaptive.enabled", "value": "false"}
    ]


@pytest.mark.parametrize(
    "settings",
    [
        {"ballista.no_such_key": "1"},
        {"datafusion.execution.batch_size": "1"},
    ],
)
def test_ballista_config_extension_invalid_key(settings):
    with pytest.raises(ValueError):
        BallistaConfigExtension(settings)


def test_set_ballista_config(ctx):
    ctx.sql("SET ballista.planner.adaptive.enabled = false").collect()
