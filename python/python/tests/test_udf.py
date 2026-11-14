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
    BallistaSessionContext,
)
from datafusion import (
    Accumulator,
    col,
    lit,
    udaf,
    udf,
    udwf,
)
from datafusion.functions import count
from datafusion.user_defined import WindowEvaluator
import pyarrow as pa
import pyarrow.compute as pc

from ._util import (
    EXPECTED_A,
    EXPECTED_B,
    TEST_CSV,
    read_back,
    sorted_pydict,
    to_table,
)


def is_null(array: pa.Array) -> pa.Array:
    return array.is_null()


is_null_udf = udf(is_null, [pa.int64()], pa.bool_(), "stable", name="is_null_udf")


def test_udf_dataframe(ctx):
    df = ctx.read_csv(TEST_CSV, has_header=True).select(
        col("a"), is_null_udf(col("a")).alias("n")
    )
    assert sorted_pydict(to_table(df)) == {"a": EXPECTED_A, "n": [False] * 5}


def test_udf_sql(ctx):
    ctx.register_csv("test", TEST_CSV, has_header=True)
    ctx.register_udf(is_null_udf)
    result = to_table(ctx.sql("SELECT a, is_null_udf(a) AS n FROM test"))
    assert sorted_pydict(result) == {"a": EXPECTED_A, "n": [False] * 5}


def test_udf_filter(ctx):
    df = ctx.read_csv(TEST_CSV, has_header=True).filter(
        is_null_udf(col("a")) == lit(False)
    )
    assert sorted_pydict(to_table(df)) == {"a": EXPECTED_A, "b": EXPECTED_B}


def test_udf_closure(ctx):
    # never registered, the captured offset has to travel with the plan
    offset = 10
    add_offset = udf(
        lambda array: pc.add(array, offset), [pa.int64()], pa.int64(), "stable"
    )
    df = ctx.read_csv(TEST_CSV, has_header=True).select(
        col("a"), add_offset(col("a")).alias("c")
    )
    assert sorted_pydict(to_table(df)) == {
        "a": EXPECTED_A,
        "c": [a + offset for a in EXPECTED_A],
    }


parity_udf = udf(
    lambda array: pc.bit_wise_and(array, 1), [pa.int64()], pa.int64(), "stable"
)


def test_udf_group_key(ctx):
    # aggregation shuffles, so the udf is evaluated in a non-final stage
    df = (
        ctx.read_csv(TEST_CSV, has_header=True)
        .aggregate([parity_udf(col("a")).alias("p")], [count(col("a")).alias("n")])
        .sort(col("p"))
    )
    assert to_table(df).to_pydict() == {"p": [0, 1], "n": [2, 3]}


def test_udf_join_key(cluster):
    (address, port) = cluster
    # force a partitioned hash join, so both sides shuffle on the udf key
    ctx = BallistaSessionContext(
        address=f"df://{address}:{port}",
        config={
            "datafusion.optimizer.hash_join_single_partition_threshold": 0,
            "datafusion.optimizer.hash_join_single_partition_threshold_rows": 0,
        },
    )
    left = ctx.read_csv(TEST_CSV, has_header=True).select(
        col("a"), parity_udf(col("a")).alias("p")
    )
    right = ctx.read_csv(TEST_CSV, has_header=True).select(
        col("a").alias("r"), parity_udf(col("a")).alias("q")
    )
    df = left.join(right, left_on="p", right_on="q").aggregate(
        [col("p")], [count(col("r")).alias("n")]
    )
    # 2 even and 3 odd values match each other
    assert sorted_pydict(to_table(df), by="p") == {"p": [0, 1], "n": [4, 9]}


def test_udf_write_roundtrip(ctx, tmp_path):
    df = ctx.read_csv(TEST_CSV, has_header=True).select(
        col("a"), is_null_udf(col("a")).alias("n")
    )
    out = tmp_path / "out"
    df.write_parquet(str(out))
    assert sorted_pydict(read_back("parquet", out)) == {
        "a": EXPECTED_A,
        "n": [False] * 5,
    }


class SumAccumulator(Accumulator):
    def __init__(self):
        self._sum = 0

    def state(self) -> list[pa.Scalar]:
        return [pa.scalar(self._sum)]

    def update(self, values: pa.Array) -> None:
        self._sum += pc.sum(values).as_py() or 0

    def merge(self, states: list[pa.Array]) -> None:
        self._sum += pc.sum(states[0]).as_py() or 0

    def evaluate(self) -> pa.Scalar:
        return pa.scalar(self._sum)


sum_udaf = udaf(
    SumAccumulator, pa.int64(), pa.int64(), [pa.int64()], "stable", name="sum_udaf"
)


def test_udaf_dataframe(ctx):
    df = ctx.read_csv(TEST_CSV, has_header=True)
    total = df.aggregate([], [sum_udaf(col("a")).alias("s")])
    assert to_table(total).to_pydict() == {"s": [sum(EXPECTED_A)]}

    grouped = df.aggregate(
        [parity_udf(col("a")).alias("p")], [sum_udaf(col("a")).alias("s")]
    ).sort(col("p"))
    assert to_table(grouped).to_pydict() == {"p": [0, 1], "s": [6, 9]}


def test_udaf_sql(ctx):
    ctx.register_csv("test", TEST_CSV, has_header=True)
    ctx.register_udaf(sum_udaf)
    result = to_table(ctx.sql("SELECT sum_udaf(b) AS s FROM test"))
    assert result.to_pydict() == {"s": [sum(EXPECTED_B)]}


class RunningSum(WindowEvaluator):
    def evaluate_all(self, values: list[pa.Array], num_rows: int) -> pa.Array:
        return pc.cumulative_sum(values[0])


running_sum_udwf = udwf(
    RunningSum, pa.int64(), pa.int64(), "stable", name="running_sum"
)


def test_udwf_dataframe(ctx):
    df = ctx.read_csv(TEST_CSV, has_header=True).select(
        col("a"),
        running_sum_udwf(col("a")).order_by(col("a")).build().alias("r"),
    )
    assert sorted_pydict(to_table(df)) == {"a": EXPECTED_A, "r": [1, 3, 6, 10, 15]}


def test_udwf_sql(ctx):
    ctx.register_csv("test", TEST_CSV, has_header=True)
    ctx.register_udwf(running_sum_udwf)
    result = to_table(
        ctx.sql("SELECT a, running_sum(a) OVER (ORDER BY a) AS r FROM test")
    )
    assert sorted_pydict(result) == {"a": EXPECTED_A, "r": [1, 3, 6, 10, 15]}
