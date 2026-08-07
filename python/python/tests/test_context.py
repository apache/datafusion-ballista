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

from ballista import (
    BallistaLogicalExtensionCodec,
    BallistaPhysicalExtensionCodec,
    BallistaQueryPlanner,
    BallistaSessionContext,
    setup_test_cluster,
)
from ballista._internal_ballista import (
    ballista_datafusion_config_defaults,
    with_ballista_query_planner,
)
from ballista.extension import (
    DataFrame,
    DistributedDataFrame,
    SessionConfig,
)
from datafusion import SessionContext, col, lit
import ctypes
import pytest
import pyarrow as pa


@pytest.fixture
def ctx():
    (address, port) = setup_test_cluster()
    return BallistaSessionContext(address=f"df://{address}:{port}")


def assert_uses_ballista(df):
    assert "DistributedQueryExec" in str(df.execution_plan())


def test_ballista_ffi_capsules_use_current_datafusion_abi():
    source_ctx = SessionContext()
    logical_codec = BallistaLogicalExtensionCodec(source_ctx)
    physical_codec = BallistaPhysicalExtensionCodec(source_ctx, logical_codec)
    planner = BallistaQueryPlanner("df://localhost:50050", source_ctx)
    is_valid = ctypes.pythonapi.PyCapsule_IsValid
    is_valid.argtypes = [ctypes.py_object, ctypes.c_char_p]
    is_valid.restype = ctypes.c_int

    capsules = {
        b"datafusion_logical_extension_codec": (
            logical_codec.__datafusion_logical_extension_codec__()
        ),
        b"datafusion_physical_extension_codec": (
            physical_codec.__datafusion_physical_extension_codec__()
        ),
        b"datafusion_query_planner": planner.__datafusion_query_planner__(),
    }
    for name, capsule in capsules.items():
        assert is_valid(capsule, name) == 1

    assert not hasattr(logical_codec, "__datafusion_physical_extension_codec__")
    assert not hasattr(physical_codec, "__datafusion_logical_extension_codec__")
    assert not hasattr(planner, "__datafusion_logical_extension_codec__")
    assert not hasattr(planner, "__datafusion_physical_extension_codec__")


def test_manual_query_planner_and_codec_composition():
    address, port = setup_test_cluster()
    source_ctx = SessionContext()
    source_ctx.register_csv("registered", "testdata/test.csv", has_header=True)
    logical_codec = BallistaLogicalExtensionCodec(source_ctx)
    physical_codec = BallistaPhysicalExtensionCodec(source_ctx, logical_codec)
    planner = BallistaQueryPlanner(f"df://{address}:{port}", source_ctx)

    configured_ctx = source_ctx.with_logical_extension_codec(logical_codec)
    configured_ctx = configured_ctx.with_physical_extension_codec(physical_codec)
    configured_ctx = configured_ctx.with_query_planner(planner)

    df = configured_ctx.sql("SELECT COUNT(*) FROM registered")
    assert type(df) is DataFrame
    assert_uses_ballista(df)
    assert df.collect()[0].column(0).to_pylist() == [5]


def test_low_level_helper_returns_standard_distributed_dataframe():
    address, port = setup_test_cluster()
    source_ctx = SessionContext()
    source_ctx.register_csv("registered", "testdata/test.csv", has_header=True)
    configured_ctx = with_ballista_query_planner(
        source_ctx,
        f"df://{address}:{port}",
        {"datafusion.execution.target_partitions": "7"},
    )

    assert type(configured_ctx) is SessionContext
    assert "datafusion.execution.target_partitions = 7" in str(source_ctx)
    assert "datafusion.execution.target_partitions = 7" in str(configured_ctx)
    df = configured_ctx.sql("SELECT COUNT(*) FROM registered")
    assert type(df) is DataFrame
    assert_uses_ballista(df)
    assert df.collect()[0].column(0).to_pylist() == [5]


def test_ballista_datafusion_defaults_are_applied(ctx):
    config_text = str(ctx)

    for key, value in ballista_datafusion_config_defaults().items():
        assert f"{key} = {value}" in config_text


def test_caller_session_config_is_not_overwritten():
    key = "datafusion.execution.target_partitions"
    config = SessionConfig().set(key, "7")
    ctx = BallistaSessionContext("df://localhost:50050", config=config)

    assert f"{key} = 7" in str(ctx)

    override_config = SessionConfig().set(key, "7")
    override_ctx = BallistaSessionContext(
        "df://localhost:50050",
        config=override_config,
        cluster_config={key: "8"},
    )
    assert f"{key} = 8" in str(override_ctx)


def test_select_one(ctx):
    df = ctx.sql("SELECT 1")
    assert_uses_ballista(df)
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


def test_cluster_config_propagates_to_distributed_dataframe():
    """The cluster_config dict should be passed to DistributedDataFrame
    instances created through the BallistaSessionContext, so it can be
    forwarded to the scheduler-side session.
    """
    (address, port) = setup_test_cluster()
    overrides = {"datafusion.execution.target_partitions": "256"}
    ctx = BallistaSessionContext(
        address=f"df://{address}:{port}",
        cluster_config=overrides,
    )

    assert ctx.cluster_config == overrides

    df = ctx.sql("SELECT 1")
    assert df.cluster_config == overrides
    assert_uses_ballista(df)
    assert len(df.collect()) == 1


def test_cluster_config_accepts_ballista_namespaced_keys():
    """Ballista-namespaced keys (e.g. ``ballista.shuffle.sort_based.batch_size``)
    are not understood by the local DataFusion ``SessionConfig`` and used to
    panic when applied to it. They are forwarded to the scheduler only and
    must be ignored locally rather than crashing context construction.
    """
    (address, port) = setup_test_cluster()
    overrides = {
        "datafusion.execution.target_partitions": "8",
        "ballista.shuffle.sort_based.batch_size": "8192",
    }
    ctx = BallistaSessionContext(
        address=f"df://{address}:{port}",
        cluster_config=overrides,
    )

    assert ctx.cluster_config == overrides

    df = ctx.sql("SELECT 1")
    assert df.cluster_config == overrides
    assert_uses_ballista(df)
    assert len(df.collect()) == 1


def test_malformed_scheduler_url_fails_lazily():
    ctx = BallistaSessionContext(address="not a scheduler URL")
    ctx.register_csv("test", "testdata/test.csv", has_header=True)

    with pytest.raises(Exception, match="relative URL without a base"):
        ctx.sql("SELECT * FROM test").collect()


def test_write_csv(ctx, tmp_path):
    df = ctx.read_csv("testdata/test.csv", has_header=True)
    out_dir = str(tmp_path / "out")
    df.write_csv(out_dir, with_header=True)
    csv_files = list((tmp_path / "out").glob("*.csv"))
    assert len(csv_files) > 0


def test_write_parquet(ctx, tmp_path):
    df = ctx.read_csv("testdata/test.csv", has_header=True)
    out_dir = str(tmp_path / "out")
    df.write_parquet(out_dir)
    parquet_files = list((tmp_path / "out").glob("*.parquet"))
    assert len(parquet_files) > 0


def test_write_json(ctx, tmp_path):
    df = ctx.read_csv("testdata/test.csv", has_header=True)
    out_dir = str(tmp_path / "out")
    df.write_json(out_dir)
    json_files = list((tmp_path / "out").glob("*.json"))
    assert len(json_files) > 0


def _assert_dataframe_returning_methods_wrapped(base_cls, sub_cls):
    should_be_wrapped = {
        name
        for name, val in base_cls.__dict__.items()
        if callable(val)
        and not name.startswith("__")
        and val.__annotations__.get("return") == DataFrame.__name__
    }

    assert should_be_wrapped
    for name in should_be_wrapped:
        assert name in sub_cls.__dict__, f"{name} not found in {sub_cls.__name__}"
        assert callable(sub_cls.__dict__[name]), (
            f"{name} is not callable in {sub_cls.__name__}"
        )
        assert sub_cls.__dict__[name] is not base_cls.__dict__[name], (
            f"{name} was not replaced in {sub_cls.__name__}"
        )


def test_distributed_dataframe_wraps_dataframe_returning_methods():
    _assert_dataframe_returning_methods_wrapped(DataFrame, DistributedDataFrame)


def test_ballista_session_context_wraps_dataframe_returning_methods():
    _assert_dataframe_returning_methods_wrapped(SessionContext, BallistaSessionContext)
