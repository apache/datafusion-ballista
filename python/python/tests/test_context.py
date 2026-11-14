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

import warnings

from ballista import (
    BallistaSessionConfig,
    BallistaSessionContext,
)
from datafusion import (
    SessionConfig,
    SessionContext,
    col,
    lit,
)
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ._util import (
    TEST_CSV,
    TEST_PARQUET,
    to_table,
)


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


def test_dict_config(cluster):
    (address, port) = cluster
    ctx = BallistaSessionContext(
        address=f"df://{address}:{port}",
        config={
            "datafusion.execution.batch_size": 2,
            "ballista.job.name": "test_job",
        },
    )
    ctx.register_parquet("t", TEST_PARQUET)
    result = to_table(ctx.sql("SELECT count(*) AS c FROM t"))
    assert result.to_pydict() == {"c": [8]}


def test_ballista_session_config(cluster):
    (address, port) = cluster
    config = (
        BallistaSessionConfig()
        .with_job_name("test_job")
        .set("datafusion.execution.batch_size", 2)
    )
    ctx = BallistaSessionContext(address=f"df://{address}:{port}", config=config)
    ctx.register_parquet("t", TEST_PARQUET)
    result = to_table(ctx.sql("SELECT count(*) AS c FROM t"))
    assert result.to_pydict() == {"c": [8]}


def test_target_partitions_applied_to_stage(cluster):
    (address, port) = cluster
    config = BallistaSessionConfig().with_target_partitions(3)
    ctx = BallistaSessionContext(address=f"df://{address}:{port}", config=config)
    ctx.register_parquet("t", TEST_PARQUET)
    result = to_table(
        ctx.sql("EXPLAIN ANALYZE SELECT bool_col, count(*) FROM t GROUP BY bool_col")
    )

    plan = "\n".join(result.column("plan").to_pylist())
    # final aggregation stage runs with configured number of partitions
    assert "SuccessfulStage[stage_id=1, partitions=3]" in plan


# TODO: remove in 57.0.0, with `cluster_config` argument


def test_cluster_config(cluster):
    (address, port) = cluster
    with pytest.warns(DeprecationWarning, match="cluster_config") as record:
        ctx = BallistaSessionContext(
            address=f"df://{address}:{port}",
            cluster_config={"datafusion.execution.batch_size": 2},
        )
    # warning points at the caller, otherwise python hides it in scripts
    assert record[0].filename == __file__

    ctx.register_parquet("t", TEST_PARQUET)
    result = to_table(ctx.sql("SELECT count(*) AS c FROM t"))
    assert result.to_pydict() == {"c": [8]}


def test_cluster_config_reaches_cluster(cluster):
    (address, port) = cluster
    with pytest.warns(DeprecationWarning):
        ctx = BallistaSessionContext(
            address=f"df://{address}:{port}",
            cluster_config={"datafusion.execution.target_partitions": 3},
        )
    ctx.register_parquet("t", TEST_PARQUET)
    result = to_table(
        ctx.sql("EXPLAIN ANALYZE SELECT bool_col, count(*) FROM t GROUP BY bool_col")
    )

    plan = "\n".join(result.column("plan").to_pylist())
    assert "SuccessfulStage[stage_id=1, partitions=3]" in plan


def test_cluster_config_with_session_config(cluster):
    (address, port) = cluster
    with pytest.warns(DeprecationWarning):
        ctx = BallistaSessionContext(
            address=f"df://{address}:{port}",
            config=SessionConfig().with_batch_size(2),
            cluster_config={"ballista.job.name": "test_job"},
        )
    ctx.register_parquet("t", TEST_PARQUET)
    result = to_table(ctx.sql("SELECT count(*) AS c FROM t"))
    assert result.to_pydict() == {"c": [8]}


def test_no_deprecation_warning_without_cluster_config(cluster):
    (address, port) = cluster
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        BallistaSessionContext(
            address=f"df://{address}:{port}",
            config={"ballista.job.name": "test_job"},
        )


def test_sql_explain_analyze(ctx):
    ctx.register_parquet("t", TEST_PARQUET)
    result = to_table(
        ctx.sql("EXPLAIN ANALYZE SELECT bool_col, count(*) FROM t GROUP BY bool_col")
    )

    assert result.column_names == ["plan_type", "plan"]
    plan = "\n".join(result.column("plan").to_pylist())
    assert "output_rows" in plan


NOT_IN_QUERY = "SELECT a FROM t WHERE a NOT IN (SELECT b FROM s)"


def register_not_in_tables(ctx, tmp_path):
    pq.write_table(pa.table({"a": [1, 2, 3, 4, None]}), tmp_path / "t.parquet")
    pq.write_table(pa.table({"b": [2, 4]}), tmp_path / "s.parquet")
    ctx.register_parquet("t", str(tmp_path / "t.parquet"))
    ctx.register_parquet("s", str(tmp_path / "s.parquet"))
    ctx.sql("SET datafusion.optimizer.prefer_hash_join = true").collect()


def test_not_in_subquery(ctx, tmp_path):
    register_not_in_tables(ctx, tmp_path)
    result = to_table(ctx.sql(NOT_IN_QUERY))
    assert sorted(result.column("a").to_pylist()) == [1, 3]


@pytest.mark.xfail(
    strict=True,
    reason="The Python client optimizes plans with datafusion-python, which "
    "can not install Ballista's NOT IN rewrite (a logical optimizer rule), so "
    "NOT IN reaches the scheduler as a null-aware anti join and runs as a "
    "single task.",
)
def test_not_in_subquery_avoids_null_aware_join(ctx, tmp_path):
    register_not_in_tables(ctx, tmp_path)
    result = to_table(ctx.sql(f"EXPLAIN ANALYZE {NOT_IN_QUERY}"))
    plan = "\n".join(result.column("plan").to_pylist())
    assert "null_aware" not in plan, plan


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
