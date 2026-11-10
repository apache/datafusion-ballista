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

"""Tests of the Python ``BallistaClient``, see also ``test_context.py``."""

import pyarrow as pa
import pytest

from ballista import (
    BallistaClient,
    BallistaJob,
    BallistaJobError,
    BallistaSessionConfig,
    BallistaSessionContext,
    setup_test_cluster,
)
from ballista import client as client_module
from ballista.client import check_version_compatibility

TEST_CSV = "testdata/test.csv"


@pytest.fixture(scope="module")
def address():
    (host, port) = setup_test_cluster()
    return f"df://{host}:{port}"


class RecordingClient(BallistaClient):
    """Records the jobs it runs and the metadata it sends."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.jobs = []
        self.metadata_calls = 0

    def call_metadata(self):
        self.metadata_calls += 1
        return super().call_metadata() + [("x-test-header", "test")]

    def execute(self, params, schema):
        job = super().execute(params, schema)
        self.jobs.append(job)
        return job


def test_custom_client_runs_queries(address):
    client = RecordingClient(address)
    ctx = BallistaSessionContext(client=client)
    ctx.register_csv("t", TEST_CSV)

    result = ctx.sql("SELECT sum(a) AS s FROM t").to_pydict()

    assert result == {"s": [15]}
    assert len(client.jobs) == 1
    assert client.jobs[0].job_id
    assert client.metadata_calls > 0


@pytest.mark.parametrize("pull", [True, False])
def test_job_tracking_follows_client_pull(address, pull):
    client = RecordingClient(
        address, config={"ballista.client.pull": str(pull).lower()}
    )
    ctx = BallistaSessionContext(client=client)
    ctx.register_csv("t", TEST_CSV)

    result = ctx.sql("SELECT a FROM t ORDER BY a").to_pydict()

    assert result == {"a": [1, 2, 3, 4, 5]}
    # push mode follows the job on the status stream of the submission
    assert (client.jobs[0]._statuses is None) == pull


@pytest.mark.parametrize("pull", [True, False])
def test_set_ballista_config_reaches_scheduler(address, pull):
    # The plan crosses the datafusion-ffi boundary without its settings and
    # reads them from the TaskContext at execution, which datafusion-ffi
    # rebuilds from strings, dropping the native BallistaConfig extension.
    client = RecordingClient(
        address, config={"ballista.client.pull": str(pull).lower()}
    )
    ctx = BallistaSessionContext(client=client)
    ctx.register_csv("t", TEST_CSV)
    ctx.sql("SET ballista.job.name = 'set_reaches_scheduler'").collect()

    assert ctx.sql("SELECT count(*) AS c FROM t").to_pydict() == {"c": [5]}

    status = client.job_status(client.jobs[-1].job_id).status
    assert status.job_name == "set_reaches_scheduler"


def test_session_context_without_client_uses_native_client(address):
    client = RecordingClient(address)
    ctx = BallistaSessionContext(address)
    ctx.register_csv("t", TEST_CSV)

    assert ctx.sql("SELECT count(*) AS c FROM t").to_pydict() == {"c": [5]}
    assert client.jobs == []


def test_client_ignores_address(address):
    client = RecordingClient(address)
    # the client carries its own address, `address` is not used
    ctx = BallistaSessionContext("df://unreachable:1", client=client)
    ctx.register_csv("t", TEST_CSV)

    assert ctx.sql("SELECT count(*) AS c FROM t").to_pydict() == {"c": [5]}
    assert len(client.jobs) == 1


def test_cluster_config_with_client(address):
    client = RecordingClient(address)
    with pytest.warns(DeprecationWarning, match="cluster_config"):
        ctx = BallistaSessionContext(
            client=client, cluster_config={"ballista.job.name": "cluster_config_job"}
        )
    ctx.register_csv("t", TEST_CSV)

    assert ctx.sql("SELECT count(*) AS c FROM t").to_pydict() == {"c": [5]}
    status = client.job_status(client.jobs[-1].job_id).status
    assert status.job_name == "cluster_config_job"


def test_job_failure_is_reported(address):
    # the test cluster runs pull-staged, which can not run pipelined jobs
    ctx = BallistaSessionContext(
        config={"ballista.execution.mode": "pipelined"},
        client=BallistaClient(address),
    )
    ctx.register_csv("t", TEST_CSV)

    with pytest.raises(Exception, match="requires push-staged scheduling"):
        ctx.sql("SELECT a FROM t").collect()


class FakeClient:
    def __init__(self):
        self.cancelled = []

    def cancel_job(self, job_id):
        self.cancelled.append(job_id)


def test_closing_unfinished_job_cancels_it():
    client = FakeClient()
    job = BallistaJob(client, "job-1", pa.schema([]), {})

    job.close()
    job.close()

    assert client.cancelled == ["job-1"]


def test_closing_finished_job_does_not_cancel_it():
    client = FakeClient()
    job = BallistaJob(client, "job-1", pa.schema([]), {})
    job._finished = True

    job.close()

    assert client.cancelled == []


def test_realigns_batches_without_aligned_reads(monkeypatch):
    monkeypatch.setattr(client_module, "_ALIGNED_READ_OPTIONS", None)
    batch = pa.record_batch({"a": [1, 2, None], "b": ["x", None, "zz"]})

    assert client_module._aligned(batch).equals(batch)


@pytest.mark.parametrize(
    "client, scheduler",
    [("55.0.0", "55.0.0"), ("55.0.0", "55.1.2"), ("54.0.0", None), (None, None)],
)
def test_compatible_versions(client, scheduler):
    check_version_compatibility(client, scheduler)


@pytest.mark.parametrize(
    "client, scheduler",
    [("55.0.0", "56.0.0"), ("55.0.0", None), (None, "55.0.0"), ("x", "x")],
)
def test_incompatible_versions(client, scheduler):
    with pytest.raises(BallistaJobError, match="must have the same major version"):
        check_version_compatibility(client, scheduler)


def test_invalid_address():
    with pytest.raises(ValueError, match="hostname"):
        BallistaClient("localhost")


@pytest.fixture
def python_jobs(monkeypatch):
    """Jobs run by any ``BallistaClient``, with the client configs."""
    jobs = []
    execute = client_module.BallistaClient.execute

    def recording_execute(self, params, schema):
        job = execute(self, params, schema)
        jobs.append((self.config, job))
        return job

    monkeypatch.setattr(client_module.BallistaClient, "execute", recording_execute)
    return jobs


@pytest.mark.parametrize("pull", [True, False])
def test_python_client_from_config(address, python_jobs, pull):
    ctx = BallistaSessionContext(
        address,
        config={"ballista.python.client": "python", "ballista.client.pull": pull},
    )
    ctx.register_csv("t", TEST_CSV)

    assert ctx.sql("SELECT count(*) AS c FROM t").to_pydict() == {"c": [5]}
    assert len(python_jobs) == 1
    config, job = python_jobs[0]
    # ballista.* options of the config reach the client
    assert config["ballista.client.pull"] == str(pull).lower()
    assert (job._statuses is None) == pull
    # used only to pick the client
    assert "ballista.python.client" not in config


def test_python_client_from_session_config(address, python_jobs):
    config = BallistaSessionConfig().with_python_client("python").with_job_name("py")
    ctx = BallistaSessionContext(address, config=config)
    ctx.register_csv("t", TEST_CSV)

    assert ctx.sql("SELECT count(*) AS c FROM t").to_pydict() == {"c": [5]}
    assert len(python_jobs) == 1
    assert python_jobs[0][0]["ballista.job.name"] == "py"


@pytest.mark.parametrize("config", [None, {"ballista.python.client": "rust"}])
def test_rust_client_from_config(address, python_jobs, config):
    ctx = BallistaSessionContext(address, config=config)
    ctx.register_csv("t", TEST_CSV)

    assert ctx.sql("SELECT count(*) AS c FROM t").to_pydict() == {"c": [5]}
    assert python_jobs == []


def test_client_with_rust_client_config_raises(address):
    with pytest.raises(ValueError, match="ballista.python.client"):
        BallistaSessionContext(
            client=BallistaClient(address),
            config={"ballista.python.client": "rust"},
        )
