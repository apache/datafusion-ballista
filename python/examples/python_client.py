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

# Customizing how the Python Ballista client reaches the cluster.
#
# `BallistaPythonSessionContext` runs queries through a `BallistaClient`
# implemented in Python. Subclass it and override its hooks to change how the
# scheduler and executors are reached:
#
# - `call_metadata`: gRPC metadata sent with every scheduler call
# - `create_channel`: the gRPC channel to the scheduler
# - `create_flight_client`: the Arrow Flight client fetching results
#
# Requires the `python-client` extra: pip install 'ballista[python-client]'

# %%
import time
from typing import List, Mapping, Tuple

import grpc
import pyarrow.flight as flight

from ballista import BallistaClient, BallistaPythonSessionContext, setup_test_cluster

# helper starting a local test cluster
host, port = setup_test_cluster()


# %%
# A gRPC interceptor logging every scheduler call. Most calls are unary, but
# `ExecuteQueryPush`, used when `ballista.client.pull` is false (the default),
# streams job status updates, so both kinds are intercepted.
class LoggingInterceptor(
    grpc.UnaryUnaryClientInterceptor, grpc.UnaryStreamClientInterceptor
):
    def intercept_unary_unary(self, continuation, client_call_details, request):
        start = time.perf_counter()
        response = continuation(client_call_details, request)
        # `result()` waits for the call, so the time covers the round trip
        response.result()
        elapsed = (time.perf_counter() - start) * 1000
        print(f"scheduler call {client_call_details.method} took {elapsed:.1f} ms")
        return response

    def intercept_unary_stream(self, continuation, client_call_details, request):
        print(f"scheduler stream {client_call_details.method} started")
        return continuation(client_call_details, request)


# %%
# Flight middleware adding a header to every result fetch from an executor
class HeaderMiddleware(flight.ClientMiddleware):
    def __init__(self, headers: Mapping[str, str]):
        self._headers = headers

    def sending_headers(self):
        return self._headers


class HeaderMiddlewareFactory(flight.ClientMiddlewareFactory):
    def __init__(self, headers: Mapping[str, str]):
        self._headers = headers

    def start_call(self, info):
        return HeaderMiddleware(self._headers)


# %%
class CustomClient(BallistaClient):
    def __init__(self, address: str, token: str):
        super().__init__(address)
        self._token = token

    def call_metadata(self) -> List[Tuple[str, str]]:
        # keep the default metadata, it carries the Ballista version header
        return super().call_metadata() + [("authorization", f"Bearer {self._token}")]

    def create_channel(self, target: str, options: List[Tuple[str, object]]):
        # the default channel honours `ballista.client.use_tls`, wrap it
        channel = super().create_channel(target, options)
        return grpc.intercept_channel(channel, LoggingInterceptor())

    def create_flight_client(
        self, host: str, port: int, settings: Mapping[str, str]
    ) -> flight.FlightClient:
        # this replaces the default Flight client, so TLS and the message
        # size limit are taken from `settings` here, as the default does
        tls = settings.get("ballista.client.use_tls", "false").lower() == "true"
        size = int(settings.get("ballista.client.grpc_max_message_size", "16777216"))
        print(f"fetching results from {host}:{port}")
        return flight.FlightClient(
            f"{'grpc+tls' if tls else 'grpc+tcp'}://{host}:{port}",
            middleware=[
                HeaderMiddlewareFactory({"authorization": f"Bearer {self._token}"})
            ],
            generic_options=[
                ("grpc.max_send_message_length", size),
                ("grpc.max_receive_message_length", size),
            ],
        )


# %%
client = CustomClient(f"df://{host}:{port}", token="secret")
ctx = BallistaPythonSessionContext(client=client)

ctx.sql("create external table t stored as parquet location '../testdata/test.parquet'")
ctx.sql("select id, bool_col from t where id > 4").show()

# %%
client.close()
# %%
