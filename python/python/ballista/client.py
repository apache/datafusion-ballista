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
Ballista cluster client implemented in Python.

``BallistaClient`` submits jobs to a Ballista scheduler over gRPC and fetches
their results from executors over Arrow Flight. It is the Python counterpart
of the native Rust client used by ``BallistaSessionContext``, and is used by
``BallistaPythonSessionContext``.

Plans are still built and encoded in Rust. The planner hands every query to
``BallistaClient.execute`` as an ``ExecuteQueryParams`` message, and reads the
results from the returned ``BallistaJob`` through the Arrow C stream interface.
Subclass ``BallistaClient`` to customize how the cluster is reached, e.g.
``create_channel``, ``call_metadata`` or ``create_flight_client``.
"""

import logging
import queue
import threading
import time
from typing import Dict, Iterator, List, Mapping, Optional, Sequence, Tuple, Union
from urllib.parse import urlparse

import grpc
import pyarrow as pa
import pyarrow.flight as flight
from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext

from ._internal_ballista import BALLISTA_VERSION
from ._internal_ballista import BallistaPythonClientExtension
from ._internal_ballista import ballista_datafusion_config_defaults
from .extension import BallistaSessionConfig, ConfigValue
from .proto import ballista_pb2, ballista_pb2_grpc

logger = logging.getLogger(__name__)

DEFAULT_SCHEDULER_PORT = 50050

# gRPC metadata key carrying the Ballista version, see `ballista_core::version`
BALLISTA_VERSION_HEADER = "ballista-version"
# first major version whose clients and schedulers send the version header
FIRST_MAJOR_WITH_VERSION_HEADER = 55

# how often the job status is polled in pull mode
STATUS_POLL_INTERVAL = 0.05
# how often a pipelined job's status is polled for a failure while its
# results stream
PIPELINED_STATUS_POLL_INTERVAL = 0.5
# how long a pipelined result stream whose fetch failed waits for the job's
# own failure, which says better what went wrong
JOB_FAILURE_GRACE = 5.0


# Batches are handed to Rust, which requires buffers aligned to their data
# type. Flight reads are zero-copy from gRPC messages, so they may not be.
# pyarrow 21 and later realign while reading, older versions copy each batch.
_ALIGNED_READ_OPTIONS = (
    flight.FlightCallOptions(
        read_options=pa.ipc.IpcReadOptions(
            ensure_alignment=pa.ipc.Alignment.DataTypeSpecific
        )
    )
    if hasattr(pa.ipc, "Alignment")
    else None
)


def _aligned(batch: pa.RecordBatch) -> pa.RecordBatch:
    if _ALIGNED_READ_OPTIONS is not None:
        return batch
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return pa.ipc.open_stream(sink.getvalue()).read_next_batch()


class BallistaJobError(RuntimeError):
    """A Ballista job failed or could not be submitted."""


def _major_version(version: Optional[str]) -> Optional[int]:
    if version is None:
        return None
    major = version.split(".")[0]
    return int(major) if major.isdigit() else None


def check_version_compatibility(
    client: Optional[str], scheduler: Optional[str]
) -> None:
    """
    Raise ``BallistaJobError`` if a client and a scheduler can not work together.

    Mirrors ``ballista_core::version::check_compatibility``: major versions must
    match, and ``None`` means the side is older than the version header.
    """
    if client is not None and scheduler is not None:
        compatible = _major_version(client) is not None and _major_version(
            client
        ) == _major_version(scheduler)
    elif client is not None or scheduler is not None:
        major = _major_version(client if client is not None else scheduler)
        compatible = major is not None and major < FIRST_MAJOR_WITH_VERSION_HEADER
    else:
        compatible = True

    if not compatible:

        def describe(role: str, version: Optional[str]) -> str:
            if version is not None:
                return f"{role} {version}"
            return f"{role} older than {FIRST_MAJOR_WITH_VERSION_HEADER}.0.0"

        raise BallistaJobError(
            f"Ballista {describe('client', client)} cannot be used with "
            f"{describe('scheduler', scheduler)}. The client and the scheduler "
            "must have the same major version."
        )
    if scheduler is not None and scheduler != client:
        logger.warning(
            "Scheduler version %s differs from client version %s", scheduler, client
        )


def _parse_address(address: str) -> Tuple[str, int]:
    url = urlparse(address)
    if not url.hostname:
        raise ValueError(f"hostname should be provided in {address!r}")
    return url.hostname, url.port or DEFAULT_SCHEDULER_PORT


def _as_bool(value: str) -> bool:
    return value.strip().lower() == "true"


class BallistaClient:
    """
    Client of a Ballista cluster.

    Args:
        address: Ballista scheduler URL, e.g. ``"df://localhost:50050"``.
        config: ``ballista.*`` options of the client. They take precedence
            over a query's settings, which take precedence over Ballista
            defaults.
        metadata: Additional gRPC metadata sent with every scheduler call,
            e.g. authentication headers.

    The gRPC channel options (TLS, message size, keep alive) are taken from
    ``config`` and Ballista defaults when the channel is created. Per query
    options (pull or push job tracking, retries, Flight TLS) are resolved for
    every query, so ``SET ballista.client.pull = false`` applies unless
    ``config`` sets it.

    Examples:
        >>> from ballista import BallistaClient, BallistaPythonSessionContext
        >>> class AuthClient(BallistaClient):
        ...     def call_metadata(self):
        ...         return super().call_metadata() + [("authorization", "token")]
        >>> ctx = BallistaPythonSessionContext(
        ...     client=AuthClient("df://localhost:50050")
        ... )
    """

    def __init__(
        self,
        address: str = "df://localhost:50050",
        config: Optional[Mapping[str, str]] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ):
        self._host, self._port = _parse_address(address)
        self._defaults: Dict[str, str] = {
            key: value
            for key, value in ballista_datafusion_config_defaults().items()
            if key.startswith("ballista.")
        }
        self._overrides: Dict[str, str] = dict(config or {})
        self._config: Dict[str, str] = {**self._defaults, **self._overrides}
        self._metadata = list(metadata)
        self._lock = threading.Lock()
        self._channel: Optional[grpc.Channel] = None
        self._scheduler: Optional[ballista_pb2_grpc.SchedulerGrpcStub] = None

    @property
    def scheduler_address(self) -> Tuple[str, int]:
        """Host and port of the scheduler."""
        return self._host, self._port

    @property
    def config(self) -> Dict[str, str]:
        """``ballista.*`` options of the client."""
        return dict(self._config)

    # hooks

    def create_channel(self, target: str, options: List[Tuple[str, object]]):
        """Create the gRPC channel to the scheduler, override to customize it."""
        if _as_bool(self._config.get("ballista.client.use_tls", "false")):
            return grpc.secure_channel(
                target, grpc.ssl_channel_credentials(), options=options
            )
        return grpc.insecure_channel(target, options=options)

    def call_metadata(self) -> List[Tuple[str, str]]:
        """gRPC metadata sent with every scheduler call."""
        return [(BALLISTA_VERSION_HEADER, BALLISTA_VERSION)] + self._metadata

    def create_flight_client(
        self, host: str, port: int, settings: Mapping[str, str]
    ) -> flight.FlightClient:
        """Create the Flight client fetching results from ``host:port``."""
        scheme = (
            "grpc+tls"
            if _as_bool(settings.get("ballista.client.use_tls", "false"))
            else "grpc+tcp"
        )
        return flight.FlightClient(
            f"{scheme}://{host}:{port}",
            generic_options=self._message_size_options(settings),
        )

    # scheduler calls

    @property
    def scheduler(self) -> ballista_pb2_grpc.SchedulerGrpcStub:
        """Scheduler gRPC stub, connected lazily."""
        with self._lock:
            if self._scheduler is None:
                self._channel = self.create_channel(
                    f"{self._host}:{self._port}", self._channel_options()
                )
                self._scheduler = ballista_pb2_grpc.SchedulerGrpcStub(self._channel)
            return self._scheduler

    def execute(
        self, params: ballista_pb2.ExecuteQueryParams, schema: pa.Schema
    ) -> "BallistaJob":
        """
        Submit a query and return its job, whose results have ``schema``.

        Blocks until the scheduler has accepted the job. Waiting for the job
        and fetching its results happen while its results are read.
        """
        settings = self._settings(params)
        if _as_bool(settings.get("ballista.client.pull", "true")):
            response, call = self.scheduler.ExecuteQuery.with_call(
                params, metadata=self.call_metadata(), timeout=self._timeout(settings)
            )
            version_error = self._version_error(call.initial_metadata())
            if response.WhichOneof("result") == "failure":
                # a version mismatch is the likelier cause, so report it first
                if version_error is not None:
                    raise version_error
                raise BallistaJobError(
                    f"Fail to execute query due to {response.failure}"
                )
            job_id = response.success.job_id
            if version_error is not None:
                # a scheduler older than 55.0.0 doesn't check versions, so it
                # has already queued the job
                self.cancel_job(job_id)
                raise version_error
            return BallistaJob(self, job_id, schema, settings)

        statuses = self.scheduler.ExecuteQueryPush(
            params, metadata=self.call_metadata()
        )
        version_error = self._version_error(statuses.initial_metadata())
        if version_error is not None:
            statuses.cancel()
            raise version_error
        # the job id arrives with the first status update
        first = next(statuses, None)
        if first is None or not first.status.job_id:
            raise BallistaJobError("Stream closed without job completing")
        return BallistaJob(
            self, first.status.job_id, schema, settings, statuses=statuses, first=first
        )

    def job_status(self, job_id: str) -> ballista_pb2.GetJobStatusResult:
        """Current status of a job."""
        return self.scheduler.GetJobStatus(
            ballista_pb2.GetJobStatusParams(job_id=job_id),
            metadata=self.call_metadata(),
        )

    def cancel_job(self, job_id: str) -> None:
        """Cancel a job, a failure is only logged."""
        try:
            self.scheduler.CancelJob(
                ballista_pb2.CancelJobParams(job_id=job_id),
                metadata=self.call_metadata(),
            )
        except grpc.RpcError as e:
            logger.warning("Failed to cancel job %s: %s", job_id, e)

    def job_metrics(self, job_id: str) -> ballista_pb2.GetJobMetricsResult:
        """Per stage metrics of a finished job, used by ``EXPLAIN ANALYZE``."""
        return self.scheduler.GetJobMetrics(
            ballista_pb2.GetJobMetricsParams(job_id=job_id),
            metadata=self.call_metadata(),
        )

    def close(self) -> None:
        """Close the scheduler channel."""
        with self._lock:
            if self._channel is not None:
                self._channel.close()
            self._channel = None
            self._scheduler = None

    # internals

    def _settings(self, params: ballista_pb2.ExecuteQueryParams) -> Dict[str, str]:
        settings = dict(self._defaults)
        for kv in params.settings:
            if kv.key.startswith("ballista.") and kv.HasField("value"):
                settings[kv.key] = kv.value
        settings.update(self._overrides)
        return settings

    def _message_size_options(
        self, settings: Mapping[str, str]
    ) -> List[Tuple[str, object]]:
        size = int(settings.get("ballista.client.grpc_max_message_size", "16777216"))
        return [
            ("grpc.max_send_message_length", size),
            ("grpc.max_receive_message_length", size),
        ]

    def _channel_options(self) -> List[Tuple[str, object]]:
        options = self._message_size_options(self._config)
        keepalive = self._config.get(
            "ballista.client.grpc_http2_keepalive_interval_seconds"
        )
        if keepalive:
            options.append(("grpc.keepalive_time_ms", int(keepalive) * 1000))
        return options

    def _timeout(self, settings: Mapping[str, str]) -> Optional[float]:
        timeout = settings.get("ballista.client.grpc_timeout_seconds")
        return float(timeout) if timeout else None

    def _version_error(self, metadata) -> Optional[BallistaJobError]:
        scheduler_version = None
        for key, value in metadata or ():
            if key == BALLISTA_VERSION_HEADER:
                scheduler_version = value
        try:
            check_version_compatibility(BALLISTA_VERSION, scheduler_version)
        except BallistaJobError as e:
            return e
        return None


class BallistaJob:
    """
    A job submitted by ``BallistaClient``.

    Its results are read once, through ``__arrow_c_stream__``, ``to_reader``
    or by iterating it. ``close`` cancels the job if it has not finished.
    """

    def __init__(
        self,
        client: BallistaClient,
        job_id: str,
        schema: pa.Schema,
        settings: Mapping[str, str],
        statuses=None,
        first: Optional[ballista_pb2.GetJobStatusResult] = None,
    ):
        self.client = client
        self.job_id = job_id
        self.schema = schema
        self._settings = settings
        self._statuses = statuses
        self._first = first
        self._finished = False
        self._closed = False
        self._flight_clients: Dict[Tuple[str, int], flight.FlightClient] = {}

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        try:
            for batch in self._batches():
                if batch.schema != self.schema:
                    # nullability and metadata may differ from the plan's schema
                    batch = pa.RecordBatch.from_arrays(
                        batch.columns, schema=self.schema
                    )
                yield batch
        finally:
            self.close()

    def to_reader(self) -> pa.RecordBatchReader:
        """Reader of the job's results."""
        return pa.RecordBatchReader.from_batches(self.schema, iter(self))

    def __arrow_c_stream__(self, requested_schema=None):
        return self.to_reader().__arrow_c_stream__(requested_schema)

    def close(self) -> None:
        """Cancel the job if it has not finished, and release its connections."""
        if self._closed:
            return
        self._closed = True
        if not self._finished:
            logger.info(
                "Result stream of job %s closed before its end; cancelling it",
                self.job_id,
            )
            self.client.cancel_job(self.job_id)
        if self._statuses is not None:
            self._statuses.cancel()
        for flight_client in self._flight_clients.values():
            flight_client.close()
        self._flight_clients.clear()

    # status tracking

    def _status_updates(self) -> Iterator[ballista_pb2.GetJobStatusResult]:
        if self._statuses is not None:
            if self._first is not None:
                yield self._first
            try:
                yield from self._statuses
            except grpc.RpcError as e:
                raise BallistaJobError(str(e)) from e
            raise BallistaJobError("Stream closed without job completing")
        while True:
            yield self.client.job_status(self.job_id)
            time.sleep(STATUS_POLL_INTERVAL)

    def _batches(self) -> Iterator[pa.RecordBatch]:
        previous = None
        for update in self._status_updates():
            state = update.status.WhichOneof("status")
            if state != previous:
                logger.info("Job %s is %s", self.job_id, state or "initializing")
                previous = state
            proxy = self._flight_proxy(update)
            if state in ("running", "successful"):
                job = getattr(update.status, state)
                if job.pipelined_output_locations:
                    if state == "successful":
                        self._finished = True
                    yield from self._pipelined(
                        job.pipelined_output_locations, proxy, watch=state == "running"
                    )
                    return
            if state == "failed":
                self._finished = True
                raise BallistaJobError(
                    f"Job {self.job_id} failed: {update.status.failed.error}"
                )
            if state == "successful":
                self._finished = True
                for location in update.status.successful.partition_location:
                    yield from self._fetch_partition(location, proxy)
                return

    def _watch_failure(self) -> Optional[str]:
        """Follow the job until it ends, returning its error if it fails."""
        if self._statuses is not None:
            try:
                for update in self._statuses:
                    state = update.status.WhichOneof("status")
                    if state == "failed":
                        return update.status.failed.error
                    if state == "successful":
                        return None
            except grpc.RpcError:
                pass
            return None
        while not self._closed:
            try:
                update = self.client.job_status(self.job_id)
            except grpc.RpcError:
                return None
            state = update.status.WhichOneof("status")
            if state == "failed":
                return update.status.failed.error
            if state == "successful":
                return None
            time.sleep(PIPELINED_STATUS_POLL_INTERVAL)
        return None

    # result fetching

    def _flight_proxy(self, update: ballista_pb2.GetJobStatusResult):
        kind = update.WhichOneof("flight_proxy")
        if kind == "external":
            return _parse_address(f"http://{update.external}")
        if kind == "local" and update.local:
            return self.client.scheduler_address
        return None

    def _flight_client(
        self, executor: ballista_pb2.ExecutorMetadata, proxy
    ) -> flight.FlightClient:
        host, port = proxy if proxy is not None else (executor.host, executor.port)
        key = (host, port)
        if key not in self._flight_clients:
            self._flight_clients[key] = self.client.create_flight_client(
                host, port, self._settings
            )
        return self._flight_clients[key]

    def _do_get(self, flight_client, action: ballista_pb2.Action, retry: bool):
        ticket = flight.Ticket(action.SerializeToString())
        retries = (
            int(self._settings.get("ballista.client.io_retries_times", "3"))
            if retry
            else 0
        )
        wait = int(self._settings.get("ballista.client.io_retry_wait_time_ms", "50"))
        for attempt in range(retries + 1):
            try:
                return flight_client.do_get(ticket, _ALIGNED_READ_OPTIONS)
            except (flight.FlightUnavailableError, flight.FlightServerError):
                if attempt == retries:
                    raise
                time.sleep(wait / 1000)

    def _fetch_partition(
        self, location: ballista_pb2.PartitionLocation, proxy
    ) -> Iterator[pa.RecordBatch]:
        executor = location.executor_meta
        fetch = ballista_pb2.FetchPartition(
            job_id=location.partition_id.job_id,
            stage_id=location.partition_id.stage_id,
            partition_id=location.partition_id.partition_id,
            host=executor.host,
            port=executor.port,
            layout=(
                ballista_pb2.SHUFFLE_LAYOUT_SORT
                if location.is_sort_shuffle
                else ballista_pb2.SHUFFLE_LAYOUT_PASSTHROUGH
            ),
            file_kind=ballista_pb2.SHUFFLE_FILE_KIND_DATA,
        )
        if location.HasField("file_id"):
            fetch.file_id = location.file_id
        reader = self._do_get(
            self._flight_client(executor, proxy),
            ballista_pb2.Action(fetch_partition=fetch),
            retry=True,
        )
        for chunk in reader:
            yield _aligned(chunk.data)

    def _fetch_pipelined_partition(
        self, location: ballista_pb2.PipelinedPartitionLocation, proxy
    ):
        executor = location.executor_meta
        timeout = int(
            self._settings.get("ballista.pipelined.registration_timeout_seconds", "0")
        )
        fetch = ballista_pb2.FetchPipelinedPartition(
            job_id=location.partition_id.job_id,
            stage_id=location.partition_id.stage_id,
            task_id=location.task_id,
            partition_id=location.partition_id.partition_id,
            host=executor.host,
            port=executor.port,
            registration_timeout_ms=timeout * 1000,
        )
        # a pipelined producer hands its partition to a single reader, so it
        # can not be retried
        return self._do_get(
            self._flight_client(executor, proxy),
            ballista_pb2.Action(fetch_pipelined_partition=fetch),
            retry=False,
        )

    def _pipelined(self, locations, proxy, watch: bool) -> Iterator[pa.RecordBatch]:
        """
        Stream every output partition of a pipelined job's final stage at once.

        The final stage can not finish until it has been read, so partitions
        are read concurrently. If the job fails, the stream ends with its
        error. A fetch failure waits a moment for the job's own failure.
        """
        results: "queue.Queue[Tuple[str, object]]" = queue.Queue(maxsize=64)
        stop = threading.Event()
        readers = []
        readers_lock = threading.Lock()

        def put(item) -> bool:
            while not stop.is_set():
                try:
                    results.put(item, timeout=0.1)
                    return True
                except queue.Full:
                    pass
            return False

        def read(location):
            try:
                reader = self._fetch_pipelined_partition(location, proxy)
                with readers_lock:
                    readers.append(reader)
                for chunk in reader:
                    if not put(("batch", _aligned(chunk.data))):
                        return
            except Exception as e:
                put(("error", e))
            finally:
                put(("done", None))

        def watch_failure():
            failure = self._watch_failure()
            if failure is not None:
                put(("failed", failure))

        threads = [
            threading.Thread(target=read, args=(location,), daemon=True)
            for location in locations
        ]
        if watch:
            threads.append(threading.Thread(target=watch_failure, daemon=True))
        for thread in threads:
            thread.start()

        remaining = len(locations)
        try:
            while remaining > 0:
                kind, value = results.get()
                if kind == "batch":
                    yield value
                elif kind == "done":
                    remaining -= 1
                elif kind == "failed":
                    self._finished = True
                    raise BallistaJobError(f"Job {self.job_id} failed: {value}")
                elif kind == "error":
                    if watch:
                        # the job's error says better what went wrong
                        deadline = time.monotonic() + JOB_FAILURE_GRACE
                        while (left := deadline - time.monotonic()) > 0:
                            try:
                                kind, failure = results.get(timeout=left)
                            except queue.Empty:
                                break
                            if kind == "failed":
                                self._finished = True
                                raise BallistaJobError(
                                    f"Job {self.job_id} failed: {failure}"
                                ) from value
                    raise value
            self._finished = True
        finally:
            stop.set()
            with readers_lock:
                for reader in readers:
                    try:
                        reader.cancel()
                    except Exception:
                        pass


def BallistaPythonSessionContext(
    address: str = "df://localhost:50050",
    config: Optional[
        Union[BallistaSessionConfig, SessionConfig, Dict[str, ConfigValue]]
    ] = None,
    runtime: Optional[RuntimeEnvBuilder] = None,
    client: Optional[BallistaClient] = None,
) -> SessionContext:
    """
    Create a DataFusion ``SessionContext`` that executes queries on a Ballista
    cluster through a Python ``BallistaClient``.

    Works like ``BallistaSessionContext``, but the communication with the
    cluster (job submission, status tracking and fetching results over Arrow
    Flight) is done in Python, by ``client``, instead of the native Rust
    client. Planning is the same for both.

    Args:
        address: Ballista scheduler URL, used when ``client`` is not given.
            Defaults to ``"df://localhost:50050"``.
        config: Session configuration, see ``BallistaSessionContext``. Its
            ``ballista.*`` options also configure the default client.
        runtime: Optional runtime environment used by the local session.
        client: Client used to talk to the cluster. Defaults to a
            ``BallistaClient`` for ``address`` and ``config``.

    Returns:
        A ``SessionContext`` configured with the Ballista query planner.

    Examples:
        >>> from ballista import BallistaPythonSessionContext
        >>> ctx = BallistaPythonSessionContext("df://localhost:50050")
        >>> ctx.sql("SELECT 1").show()
    """
    if not isinstance(config, BallistaSessionConfig):
        config = BallistaSessionConfig(config)

    if client is None:
        client = BallistaClient(address, config=dict(config._ballista_settings))

    source_ctx = SessionContext(config.build(), runtime)
    return source_ctx.with_extensions(BallistaPythonClientExtension(client))
