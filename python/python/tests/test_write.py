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


from datafusion import (
    col,
    lit,
)
from datafusion.dataframe import (
    DataFrameWriteOptions,
    InsertOp,
    ParquetColumnOptions,
    ParquetWriterOptions,
)
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from ._util import (
    EXPECTED_A,
    EXPECTED_B,
    TEST_CSV,
    data_files,
    read_back,
    sorted_pydict,
    to_table,
    write_df,
)


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
