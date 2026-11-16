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

import json

from ballista import (
    BallistaClient,
    BallistaSessionContext,
    setup_test_cluster,
)
import pytest

from ._util import EXPECTED_A, EXPECTED_B


@pytest.fixture(scope="module")
def cluster():
    return setup_test_cluster()


@pytest.fixture(params=["native", "python"])
def ctx(cluster, request):
    """Context using the native Rust client or the Python ``BallistaClient``."""
    (address, port) = cluster
    address = f"df://{address}:{port}"
    if request.param == "native":
        return BallistaSessionContext(address=address)
    return BallistaSessionContext(client=BallistaClient(address))


@pytest.fixture
def ndjson_file(tmp_path):
    path = tmp_path / "test.json"
    path.write_text(
        "\n".join(json.dumps({"a": a, "b": b}) for a, b in zip(EXPECTED_A, EXPECTED_B))
        + "\n"
    )
    return path
