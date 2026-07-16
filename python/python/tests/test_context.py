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

from ballista import setup_test_cluster
from ballista._internal_ballista import (
    BallistaQueryPlanner,
    ballista_datafusion_config_defaults,
)
from datafusion import DataFrame, SessionConfig, SessionContext, col, lit
import pytest
import pyarrow as pa


def make_ffi_context(address, overrides=None):
    overrides = overrides or {}
    local_options = ballista_datafusion_config_defaults()
    local_options.update(
        {
            key: str(value)
            for key, value in overrides.items()
            if not key.startswith("ballista.")
        }
    )

    source_ctx = SessionContext(SessionConfig(local_options))
    planner = BallistaQueryPlanner(address, source_ctx, overrides)
    return source_ctx.with_query_planner(planner)


@pytest.fixture
def ctx():
    address, port = setup_test_cluster()
    return make_ffi_context(f"df://{address}:{port}")


def assert_uses_ballista(df):
    assert "DistributedQueryExec" in str(df.execution_plan())


def test_ballista_datafusion_defaults_are_applied(ctx):
    config_text = str(ctx)

    assert "datafusion.execution.parquet.schema_force_view_types = false" in config_text
    assert "datafusion.optimizer.enable_dynamic_filter_pushdown = false" in config_text


def test_select_one(ctx):
    df = ctx.sql("SELECT 1")
    batches = df.collect()
    assert len(batches) == 1


def test_read_csv(ctx):
    df = ctx.read_csv("testdata/test.csv", has_header=True)
    assert_uses_ballista(df)
    batches = df.collect()
    assert len(batches) == 1
    assert len(batches[0]) == 5


def test_register_csv(ctx):
    ctx.register_csv("test", "testdata/test.csv", has_header=True)
    df = ctx.sql("SELECT * FROM test")
    assert_uses_ballista(df)
    batches = df.collect()
    assert len(batches) == 1
    assert len(batches[0]) == 5


def test_read_parquet(ctx):
    df = ctx.read_parquet("testdata/test.parquet")
    assert_uses_ballista(df)
    batches = df.collect()
    assert len(batches) == 1
    assert len(batches[0]) == 8


def test_register_parquet(ctx):
    ctx.register_parquet("test", "testdata/test.parquet")
    df = ctx.sql("SELECT * FROM test")
    assert_uses_ballista(df)
    batches = df.collect()
    assert len(batches) == 1
    assert len(batches[0]) == 8


def test_read_dataframe_api(ctx):
    df = (
        ctx.read_csv("testdata/test.csv", has_header=True)
        .select("a", "b")
        .filter(col("a") > lit(2))
    )
    assert_uses_ballista(df)
    result = df.collect()[0]

    assert result.column(0) == pa.array([3, 4, 5])
    assert result.column(1) == pa.array([-4, -5, -6])


def test_config_overrides_work_with_ffi_planner():
    address, port = setup_test_cluster()
    ctx = make_ffi_context(
        f"df://{address}:{port}",
        {"datafusion.execution.target_partitions": "256"},
    )

    df = ctx.read_csv("testdata/test.csv", has_header=True)
    assert_uses_ballista(df)
    assert len(df.collect()[0]) == 5


def test_cluster_config_accepts_ballista_namespaced_keys():
    """Ballista-namespaced keys are passed to the FFI planner, not the local
    DataFusion SessionConfig, and must not crash context construction.
    """
    address, port = setup_test_cluster()
    overrides = {
        "datafusion.execution.target_partitions": "8",
        "ballista.shuffle.sort_based.enabled": "true",
    }
    ctx = make_ffi_context(f"df://{address}:{port}", overrides)

    df = ctx.read_csv("testdata/test.csv", has_header=True)
    assert_uses_ballista(df)
    assert len(df.collect()[0]) == 5


@pytest.mark.xfail(
    reason="DataFusion logical codec FFI does not transport COPY file formats"
)
def test_write_csv(ctx, tmp_path):
    df = ctx.read_csv("testdata/test.csv", has_header=True)
    out_dir = str(tmp_path / "out")
    df.write_csv(out_dir, with_header=True)
    csv_files = list((tmp_path / "out").glob("*.csv"))
    assert len(csv_files) > 0


@pytest.mark.xfail(
    reason="DataFusion logical codec FFI does not transport COPY file formats"
)
def test_write_parquet(ctx, tmp_path):
    df = ctx.read_csv("testdata/test.csv", has_header=True)
    out_dir = str(tmp_path / "out")
    df.write_parquet(out_dir)
    parquet_files = list((tmp_path / "out").glob("*.parquet"))
    assert len(parquet_files) > 0


@pytest.mark.xfail(
    reason="DataFusion logical codec FFI does not transport COPY file formats"
)
def test_write_json(ctx, tmp_path):
    df = ctx.read_csv("testdata/test.csv", has_header=True)
    out_dir = str(tmp_path / "out")
    df.write_json(out_dir)
    json_files = list((tmp_path / "out").glob("*.json"))
    assert len(json_files) > 0


def test_ffi_query_planner_returns_standard_dataframe(ctx):
    df = ctx.read_csv("testdata/test.csv", has_header=True).select("a")

    assert type(df) is DataFrame
    assert_uses_ballista(df)
    assert len(df.collect()[0]) == 5


def test_malformed_scheduler_url_fails_lazily():
    ctx = make_ffi_context("not a scheduler URL")
    ctx.register_csv("test", "testdata/test.csv", has_header=True)

    with pytest.raises(Exception, match="relative URL without a base"):
        ctx.sql("SELECT * FROM test").collect()
