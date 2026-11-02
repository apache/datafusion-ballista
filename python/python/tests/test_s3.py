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

"""
S3 tests against a RustFS container, skipped when docker is not available.

RustFS replaces MinIO, which withdrew its images, same as the Rust
integration tests in `examples/tests/common/mod.rs`.
"""

from ballista import BallistaSessionContext, setup_test_cluster
from datafusion import col, udf
from datafusion.object_store import AmazonS3
import pyarrow as pa
import pytest

pytest.importorskip("testcontainers")

from testcontainers.core.container import DockerContainer  # noqa: E402
from testcontainers.core.wait_strategies import HttpWaitStrategy  # noqa: E402

TEST_PARQUET = "testdata/test.parquet"

RUSTFS_IMAGE = "rustfs/rustfs:1.0.0"
S3_PORT = 9000
REGION = "eu-west-1"
BUCKET = "ballista"
ACCESS_KEY_ID = "BALLISTA"
SECRET_KEY = "BALLISTASECRET"


def _docker_available() -> bool:
    try:
        import docker

        return docker.from_env().ping()
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _docker_available(), reason="docker is required for S3 tests"
)


@pytest.fixture(scope="module")
def s3_endpoint():
    # RustFS logs only warnings by default, wait on its health endpoint
    # rather than a startup message
    container = (
        DockerContainer(RUSTFS_IMAGE)
        .with_exposed_ports(S3_PORT)
        .with_env("RUSTFS_ACCESS_KEY", ACCESS_KEY_ID)
        .with_env("RUSTFS_SECRET_KEY", SECRET_KEY)
        .waiting_for(
            HttpWaitStrategy(S3_PORT, "/health")
            .for_status_code(200)
            .with_startup_timeout(60)
        )
    )
    with container:
        # a top-level directory under the data volume is a bucket
        exit_code, output = container.exec(["mkdir", f"/data/{BUCKET}"])
        assert exit_code == 0, output

        host = container.get_container_host_ip()
        port = container.get_exposed_port(S3_PORT)
        yield f"http://{host}:{port}"


@pytest.fixture(scope="module")
def ctx(s3_endpoint):
    host, port = setup_test_cluster()
    ctx = BallistaSessionContext(f"df://{host}:{port}")

    # used by the client for schema inference
    ctx.register_object_store(
        "s3://",
        AmazonS3(
            bucket_name=BUCKET,
            region=REGION,
            access_key_id=ACCESS_KEY_ID,
            secret_access_key=SECRET_KEY,
            endpoint=s3_endpoint,
            allow_http=True,
        ),
        BUCKET,
    )

    # used by the scheduler and executors
    ctx.sql(f"SET s3.region = '{REGION}'")
    ctx.sql(f"SET s3.access_key_id = '{ACCESS_KEY_ID}'")
    ctx.sql(f"SET s3.secret_access_key = '{SECRET_KEY}'")
    ctx.sql(f"SET s3.endpoint = '{s3_endpoint}'")
    ctx.sql("SET s3.allow_http = true")

    # executors write the data, so the cluster has to have S3 support
    ctx.read_parquet(TEST_PARQUET).write_parquet(f"s3://{BUCKET}/test/")
    ctx.sql(f"CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION 's3://{BUCKET}/test/'")
    return ctx


def test_read_s3(ctx):
    expected = ctx.read_parquet(TEST_PARQUET).sort(col("id")).to_arrow_table()

    result = ctx.sql("SELECT * FROM t ORDER BY id").to_arrow_table()

    assert result == expected


def test_python_udf_on_s3_data(ctx):
    def is_null(array: pa.Array) -> pa.Array:
        return array.is_null()

    is_null_udf = udf(is_null, [pa.int64()], pa.bool_(), "stable", name="is_null")

    result = (
        ctx.table("t")
        .select(is_null_udf(col("bigint_col")).alias("n"))
        .to_arrow_table()
    )

    assert result.num_rows == 8
    assert result.column("n").to_pylist() == [False] * 8
