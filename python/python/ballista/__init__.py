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


import importlib
import importlib.metadata as importlib_metadata
from typing import TYPE_CHECKING

from ._internal_ballista import (
    BallistaConfigExtension,
    BallistaScheduler,
    BallistaExecutor,
    setup_test_cluster,
)
from .extension import (
    BallistaSessionConfig,
    BallistaSessionContext,
    ExecutionPlanVisualization,
)

if TYPE_CHECKING:
    from .client import (
        BallistaClient,
        BallistaJob,
        BallistaJobError,
    )

__version__ = importlib_metadata.version(__name__)

# The Python cluster client needs grpcio, protobuf and pyarrow.flight, which
# come with the `python-client` extra. It is imported on first use, so
# `import ballista` works without them.
# top level modules the `python-client` extra provides
_CLIENT_DEPENDENCIES = frozenset(["grpc", "google", "pyarrow"])
_CLIENT_EXPORTS = frozenset(
    [
        "BallistaClient",
        "BallistaJob",
        "BallistaJobError",
    ]
)


def __getattr__(name: str):
    if name in _CLIENT_EXPORTS:
        try:
            client = importlib.import_module(".client", __name__)
        except ModuleNotFoundError as e:
            if (e.name or "").split(".")[0] not in _CLIENT_DEPENDENCIES:
                raise
            raise ImportError(
                f"ballista.{name} requires the Python client dependencies, "
                "install them with `pip install 'ballista[python-client]'`"
            ) from e
        return getattr(client, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(set(globals()) | _CLIENT_EXPORTS)


__all__ = [
    "BallistaConfigExtension",
    "setup_test_cluster",
    "BallistaScheduler",
    "BallistaExecutor",
    "BallistaSessionConfig",
    "BallistaSessionContext",
    "BallistaClient",
    "BallistaJob",
    "BallistaJobError",
    "ExecutionPlanVisualization",
]
