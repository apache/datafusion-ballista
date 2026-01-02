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

from ballista import BallistaSessionContext, setup_test_cluster
from datafusion import col, lit
import pytest
import pyarrow as pa


@pytest.fixture
def ctx():
    (address, port) = setup_test_cluster()
    return BallistaSessionContext(address=f"df://{address}:{port}")


def test_select_one(ctx):
    df = ctx.sql("SELECT 1")
    batches = df.collect()
    assert len(batches) == 1


def test_read_csv(ctx):
    df = ctx.read_csv("testdata/test.csv", has_header=True)
    batches = df.collect()
    assert len(batches) == 1
    assert len(batches[0]) == 5


def test_register_csv(ctx):
    ctx.register_csv("test", "testdata/test.csv", has_header=True)
    df = ctx.sql("SELECT * FROM test")
    batches = df.collect()
    assert len(batches) == 1
    assert len(batches[0]) == 5


def test_read_parquet(ctx):
    df = ctx.read_parquet("testdata/test.parquet")
    batches = df.collect()
    assert len(batches) == 1
    assert len(batches[0]) == 8


def test_register_parquet(ctx):
    ctx.register_parquet("test", "testdata/test.parquet")
    df = ctx.sql("SELECT * FROM test")
    batches = df.collect()
    assert len(batches) == 1
    assert len(batches[0]) == 8


def test_read_dataframe_api(ctx):
    df = (
        ctx.read_csv("testdata/test.csv", has_header=True)
        .select("a", "b")
        .filter(col("a") > lit(2))
    )
    result = df.collect()[0]

    assert result.column(0) == pa.array([3, 4, 5])
    assert result.column(1) == pa.array([-4, -5, -6])
