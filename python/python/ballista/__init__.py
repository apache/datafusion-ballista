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

import os

try:
    import importlib.metadata as importlib_metadata
except ImportError:
    import importlib_metadata

from ._internal_ballista import (
    BallistaScheduler,
    BallistaExecutor,
    setup_test_cluster as _setup_in_process_test_cluster,
)
from .extension import (
    BallistaSessionContext,
    DistributedDataFrame,
    ExecutionPlanVisualization,
)

__version__ = importlib_metadata.version(__name__)

#: Environment variable naming an already-running scheduler to test against,
#: as ``host:port`` (the port defaults to 50050 if omitted).
TEST_SCHEDULER_ENV_VAR = "BALLISTA_TEST_SCHEDULER"


def setup_test_cluster():
    """Return the ``(host, port)`` of a scheduler to run tests against.

    By default this starts an in-process standalone scheduler and executor,
    which is convenient but only ever exercises the Rust code the bindings
    were compiled against.

    If ``BALLISTA_TEST_SCHEDULER`` is set, no cluster is started and its
    value is used instead. That lets CI point the Python client at a
    scheduler and executor built from the working tree, so changes to the
    Rust crates are actually covered by the Python tests.
    """
    external = os.environ.get(TEST_SCHEDULER_ENV_VAR)
    if not external:
        return _setup_in_process_test_cluster()

    host, sep, port = external.rpartition(":")
    if not sep:
        return (external, 50050)
    return (host, int(port))


__all__ = [
    "setup_test_cluster",
    "TEST_SCHEDULER_ENV_VAR",
    "BallistaScheduler",
    "BallistaExecutor",
    "BallistaSessionContext",
    "DistributedDataFrame",
    "ExecutionPlanVisualization",
]
