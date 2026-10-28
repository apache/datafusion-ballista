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

from datafusion import (
    RuntimeEnvBuilder,
    SessionConfig,
    SessionContext,
)
from typing import Dict, Optional, Union
import warnings

from ._internal_ballista import BallistaConfigExtension
from ._internal_ballista import ballista_datafusion_config_defaults
from ._internal_ballista import with_ballista_query_planner

ConfigValue = Union[str, int, float, bool]


class ExecutionPlanVisualization:
    """
    A wrapper for execution plan visualizations that can render as SVG in Jupyter.

    This class takes the text representation of an execution plan and converts
    it to a Graphviz DOT format, which is then rendered as SVG.
    """

    def __init__(self, plan_str: str, analyze: bool = False):
        self.plan_str = plan_str
        self.analyze = analyze
        self._svg_cache: Optional[str] = None

    def _parse_plan_to_dot(self) -> str:
        """Convert the plan string to DOT format for Graphviz."""
        lines = self.plan_str.strip().split("\n")

        dot_lines = [
            "digraph ExecutionPlan {",
            "    rankdir=TB;",
            '    node [shape=box, style="rounded,filled", fontname="Helvetica"];',
            '    edge [fontname="Helvetica"];',
            "",
        ]

        nodes = []
        edges = []
        node_id = 0
        stack = []  # (indent_level, node_id)

        for line in lines:
            if not line.strip():
                continue

            # Calculate indent level
            indent = len(line) - len(line.lstrip())
            content = line.strip()

            # Skip non-plan lines
            if content.startswith("physical_plan") or content.startswith(
                "logical_plan"
            ):
                continue

            # Create a node for this plan element
            current_id = node_id
            node_id += 1

            # Determine node color based on operation type
            color = "#E3F2FD"  # Default light blue
            if "Scan" in content or "TableScan" in content:
                color = "#E8F5E9"  # Light green for scans
            elif "Filter" in content:
                color = "#FFF3E0"  # Light orange for filters
            elif "Aggregate" in content or "HashAggregate" in content:
                color = "#F3E5F5"  # Light purple for aggregations
            elif "Join" in content:
                color = "#FFEBEE"  # Light red for joins
            elif "Sort" in content:
                color = "#E0F7FA"  # Light cyan for sorts
            elif "Projection" in content:
                color = "#FFF8E1"  # Light amber for projections

            # Escape special characters for DOT format
            label = content.replace('"', '\\"').replace("\n", "\\n")
            if len(label) > 60:
                # Wrap long labels
                label = label[:57] + "..."

            nodes.append(
                f'    node{current_id} [label="{label}", fillcolor="{color}"];'
            )

            # Connect to parent based on indentation
            while stack and stack[-1][0] >= indent:
                stack.pop()

            if stack:
                parent_id = stack[-1][1]
                edges.append(f"    node{parent_id} -> node{current_id};")

            stack.append((indent, current_id))

        dot_lines.extend(nodes)
        dot_lines.append("")
        dot_lines.extend(edges)
        dot_lines.append("}")

        return "\n".join(dot_lines)

    def to_dot(self) -> str:
        """Get the DOT representation of the execution plan."""
        return self._parse_plan_to_dot()

    def to_svg(self) -> str:
        """
        Convert the execution plan to SVG format.

        Requires graphviz to be installed. If graphviz is not available,
        returns a simple HTML representation instead.
        """
        if self._svg_cache is not None:
            return self._svg_cache

        dot_source = self._parse_plan_to_dot()

        try:
            import subprocess

            # Try to use graphviz's dot command
            process = subprocess.run(
                ["dot", "-Tsvg"],
                input=dot_source.encode(),
                capture_output=True,
                timeout=30,
            )

            if process.returncode == 0:
                self._svg_cache = process.stdout.decode()
                return self._svg_cache
        except (
            subprocess.SubprocessError,
            FileNotFoundError,
            subprocess.TimeoutExpired,
        ) as e:
            warnings.warn(f"Could not convert the execution plan to SVG format: {e}")
            pass

        # Fallback: return a pre-formatted HTML representation
        escaped_plan = (
            self.plan_str.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        self._svg_cache = f"""
        <div style="font-family: monospace; background: #f5f5f5; padding: 10px; 
                    border-radius: 5px; overflow-x: auto;">
            <div style="color: #666; margin-bottom: 5px;">
                Execution Plan {"(with statistics)" if self.analyze else ""}
                <br><small>Install graphviz for visual diagram: brew install graphviz</small>
            </div>
            <pre style="margin: 0;">{escaped_plan}</pre>
        </div>
        """
        return self._svg_cache

    def save(self, path: str) -> None:
        """Save the visualization to a file (SVG or DOT format)."""
        if path.endswith(".dot"):
            content = self.to_dot()
        else:
            content = self.to_svg()

        with open(path, "w") as f:
            f.write(content)

    def _repr_html_(self) -> str:
        """HTML representation for Jupyter notebooks."""
        return self.to_svg()

    def _repr_svg_(self) -> str:
        """SVG representation for Jupyter notebooks."""
        svg = self.to_svg()
        # Only return if it's actual SVG content
        if svg.strip().startswith("<svg") or svg.strip().startswith("<?xml"):
            return svg
        return ""

    def __repr__(self) -> str:
        """String representation."""
        return f"ExecutionPlanVisualization(analyze={self.analyze})\n{self.plan_str}"


def BallistaSessionContext(
    address: str = "df://localhost:50050",
    config: Optional[
        Union["BallistaSessionConfig", SessionConfig, Dict[str, ConfigValue]]
    ] = None,
    runtime: Optional[RuntimeEnvBuilder] = None,
) -> SessionContext:
    """
    Create a DataFusion ``SessionContext`` that executes queries on a Ballista cluster.

    The returned object is a regular ``datafusion.SessionContext``, so the whole
    DataFusion Python API (``sql``, ``read_parquet``, ``register_*``, DataFrame
    operations, ...) is available. Planning happens locally; physical plans are
    sent to the Ballista scheduler at ``address`` for distributed execution.

    Args:
        address: Ballista scheduler URL. Defaults to ``"df://localhost:50050"``.
        config: Session configuration, either a ``BallistaSessionConfig``, a
            ``SessionConfig`` or a dictionary of ``datafusion.*`` and
            ``ballista.*`` options with ``str``, ``int``, ``float`` or ``bool``
            values. It is used for local planning and propagated to the
            scheduler-side session. DataFusion options which Ballista does not
            support are overridden with Ballista defaults, see
            ``BallistaSessionConfig``.
        runtime: Optional runtime environment used by the local session.

    Returns:
        A ``SessionContext`` configured with the Ballista query planner.

    Raises:
        ValueError: If a ``config`` key does not start with ``datafusion.`` or
            ``ballista.``, or is an unknown ``ballista.*`` key.

    Examples:
        Connect to a scheduler and run SQL:

        >>> from ballista import BallistaSessionContext
        >>> ctx = BallistaSessionContext("df://localhost:50050")
        >>> ctx.register_parquet("t", "./testdata/test.parquet")
        >>> ctx.sql("SELECT count(*) FROM t").show()

        Use the DataFrame API:

        >>> from datafusion import col, lit
        >>> df = ctx.read_parquet("./testdata/test.parquet")
        >>> batches = df.filter(col("id") > lit(4)).limit(5).collect()

        Configure session settings with a dictionary:

        >>> ctx = BallistaSessionContext(
        ...     "df://localhost:50050",
        ...     config={
        ...         "datafusion.execution.target_partitions": 16,
        ...         "ballista.job.name": "daily report",
        ...     },
        ... )

        Configure Ballista specific settings, such as job name shown in the
        scheduler UI, or adaptive query planning, with ``BallistaSessionConfig``:

        >>> from ballista import BallistaSessionConfig
        >>> ctx = BallistaSessionContext(
        ...     "df://localhost:50050",
        ...     config=BallistaSessionConfig()
        ...     .with_job_name("daily report")
        ...     .with_adaptive_query_planner(True)
        ...     .with_broadcast_join_threshold_bytes(10 * 1024 * 1024)
        ...     .set("datafusion.execution.batch_size", 8192),
        ... )

        Set the number of partitions to match cluster capacity
        (``executors * vcores_per_executor``). Ballista defaults
        ``datafusion.execution.target_partitions`` to ``16``, a value set in a
        passed ``SessionConfig`` is overridden, so use
        ``with_target_partitions`` or a dict instead:

        >>> ctx = BallistaSessionContext(
        ...     "df://localhost:50050",
        ...     config=BallistaSessionConfig().with_target_partitions(32),
        ... )
    """
    if not isinstance(config, BallistaSessionConfig):
        config = BallistaSessionConfig(config)

    source_ctx = SessionContext(config.build(), runtime)
    return with_ballista_query_planner(source_ctx, address)


# DataFusion options overridden by Ballista, mirrors
# `SessionConfigExt::ballista_restricted_configuration` in ballista-core
#
# value is `True` if option is restricted and cant be overridden,
# if value is `False` if users can override it using dict parameter
# in dictionary.
#
# TODO: At the moment values are duplicated,
_BALLISTA_RESTRICTED_DATAFUSION_OPTIONS: Dict[str, bool] = {
    "datafusion.optimizer.enable_round_robin_repartition": True,
    "datafusion.execution.parquet.schema_force_view_types": False,
    "datafusion.execution.target_partitions": False,
    "datafusion.sql_parser.map_string_types_to_utf8view": False,
    "datafusion.optimizer.hash_join_single_partition_threshold": False,
    "datafusion.optimizer.hash_join_single_partition_threshold_rows": False,
    "datafusion.optimizer.prefer_hash_join": True,
    "datafusion.optimizer.enable_physical_uncorrelated_scalar_subquery": True,
    "datafusion.optimizer.enable_dynamic_filter_pushdown": True,
}


def _config_value_to_str(value: ConfigValue) -> str:
    # `str(True)` is "True", config parsers expect lowercase
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


class BallistaSessionConfig:
    """
    Builder for a Ballista-ready DataFusion ``SessionConfig``.

    Mirrors Rust ``BallistaConfig``: typed ``with_*`` setters for Ballista
    options, plus ``set`` for any ``datafusion.*`` or ``ballista.*`` option.
    ``build`` produces a ``SessionConfig`` with Ballista defaults applied and
    ``BallistaConfigExtension`` registered.

    DataFusion options which Ballista overrides are set to Ballista defaults:
    restricted options are always overridden, non-restricted options are kept
    only when set with ``set`` or a dict.
    Options inside a passed ``SessionConfig`` can not be inspected, so all of
    them are overridden with Ballista defaults.

    For example ``datafusion.execution.target_partitions`` defaults to ``16``,
    change it with ``with_target_partitions``, ``set`` or a dict, not with
    ``SessionConfig.with_target_partitions``.

    Args:
        config: Initial configuration, either a ``SessionConfig`` or a dictionary
            of ``datafusion.*`` and ``ballista.*`` options.

    Examples:
        >>> from ballista import BallistaSessionConfig
        >>> config = (
        ...     BallistaSessionConfig({"datafusion.execution.batch_size": 8192})
        ...     .with_target_partitions(32)
        ...     .with_job_name("daily report")
        ...     .with_adaptive_query_planner(True)
        ...     .set("ballista.shuffle.compression.codec", "zstd")
        ...     .build()
        ... )
    """

    def __init__(
        self,
        config: Optional[Union[SessionConfig, Dict[str, ConfigValue]]] = None,
    ):
        self._base: Optional[SessionConfig] = None
        self._datafusion_options: Dict[str, str] = {}
        self._ballista_settings: Dict[str, str] = {}

        if isinstance(config, SessionConfig):
            self._base = config
        elif config is not None:
            for key, value in config.items():
                self.set(key, value)

    def set(self, key: str, value: ConfigValue) -> "BallistaSessionConfig":
        """
        Set a ``datafusion.*`` or ``ballista.*`` option.

        Raises:
            ValueError: If the key has an unsupported prefix, or is an unknown
                ``ballista.*`` key.
        """
        value = _config_value_to_str(value)
        if key.startswith("ballista."):
            # validates the key, values are not validated by ballista
            BallistaConfigExtension({key: value})
            self._ballista_settings[key] = value
        elif key.startswith("datafusion."):
            self._datafusion_options[key] = value
        else:
            raise ValueError(
                f"unsupported configuration key: {key}, "
                "expected `datafusion.` or `ballista.` prefix"
            )
        return self

    def build(self) -> SessionConfig:
        """Produce a ``SessionConfig`` with ``BallistaConfigExtension`` registered."""
        config = self._base if self._base is not None else SessionConfig()
        for key, value in self._datafusion_options.items():
            config = config.set(key, value)

        ballista_settings = dict(self._ballista_settings)
        for key, value in ballista_datafusion_config_defaults().items():
            if (
                key.startswith("datafusion.")
                and key in _BALLISTA_RESTRICTED_DATAFUSION_OPTIONS
            ):
                restricted = _BALLISTA_RESTRICTED_DATAFUSION_OPTIONS[key]
                # keep user value for options which are not restricted
                if not restricted and key in self._datafusion_options:
                    continue
                config = config.set(key, value)
            elif key.startswith("ballista."):
                ballista_settings.setdefault(key, value)

        return config.with_extension(BallistaConfigExtension(ballista_settings))

    # datafusion

    def with_target_partitions(self, target_partitions: int) -> "BallistaSessionConfig":
        """Number of partitions for query execution, overrides Ballista default."""
        return self.set("datafusion.execution.target_partitions", target_partitions)

    # job

    def with_job_name(self, job_name: str) -> "BallistaSessionConfig":
        """Job name displayed in the scheduler UI."""
        return self.set("ballista.job.name", job_name)

    def with_standalone_parallelism(self, parallelism: int) -> "BallistaSessionConfig":
        """Parallelism of the standalone cluster."""
        return self.set("ballista.standalone.parallelism", parallelism)

    def with_cache_noop(self, enabled: bool) -> "BallistaSessionConfig":
        """Disable the default cache extension node."""
        return self.set("ballista.cache.noop", enabled)

    # shuffle reader

    def with_shuffle_reader_maximum_concurrent_requests(
        self, max_requests: int
    ) -> "BallistaSessionConfig":
        """Maximum concurrent shuffle read requests."""
        return self.set("ballista.shuffle.max_concurrent_read_requests", max_requests)

    def with_shuffle_reader_force_remote_read(
        self, enabled: bool
    ) -> "BallistaSessionConfig":
        """Force remote reads even for local partitions."""
        return self.set("ballista.shuffle.force_remote_read", enabled)

    def with_shuffle_reader_remote_prefer_flight(
        self, enabled: bool
    ) -> "BallistaSessionConfig":
        """Prefer Flight protocol for remote shuffle reads."""
        return self.set("ballista.shuffle.remote_read_prefer_flight", enabled)

    def with_shuffle_reader_max_bytes_in_flight(
        self, max_bytes: int
    ) -> "BallistaSessionConfig":
        """Reduce-side in-flight bytes budget."""
        return self.set("ballista.shuffle.reader.max_bytes_in_flight", max_bytes)

    def with_shuffle_reader_max_blocks_in_flight_per_address(
        self, max_blocks: int
    ) -> "BallistaSessionConfig":
        """Per-address in-flight block cap."""
        return self.set(
            "ballista.shuffle.reader.max_blocks_in_flight_per_address", max_blocks
        )

    def with_shuffle_reader_default_block_size_bytes(
        self, block_size: int
    ) -> "BallistaSessionConfig":
        """Assumed block size when partition statistics lack a byte count."""
        return self.set("ballista.shuffle.reader.default_block_size_bytes", block_size)

    # shuffle writer

    def with_shuffle_sort_based_batch_size(
        self, batch_size: int
    ) -> "BallistaSessionConfig":
        """Sort shuffle target batch size in rows."""
        return self.set("ballista.shuffle.sort_based.batch_size", batch_size)

    def with_shuffle_writer_channel_capacity(
        self, capacity: int
    ) -> "BallistaSessionConfig":
        """Shuffle writer bounded-channel capacity."""
        return self.set("ballista.shuffle.writer_channel_capacity", capacity)

    def with_shuffle_sort_based_memory_limit_per_task_bytes(
        self, limit_bytes: int
    ) -> "BallistaSessionConfig":
        """Per-task buffered bytes at which the sort shuffle writer spills, ``0`` disables."""
        return self.set(
            "ballista.shuffle.sort_based.memory_limit_per_task_bytes", limit_bytes
        )

    def with_shuffle_compression_codec(self, codec: str) -> "BallistaSessionConfig":
        """Shuffle compression codec: ``none``, ``lz4`` or ``zstd``."""
        return self.set("ballista.shuffle.compression.codec", codec)

    # grpc client

    def with_grpc_client_initial_connection_window_size(
        self, size: int
    ) -> "BallistaSessionConfig":
        """HTTP/2 initial connection-level flow-control window."""
        return self.set("ballista.client.initial_connection_window_size", size)

    def with_grpc_client_initial_stream_window_size(
        self, size: int
    ) -> "BallistaSessionConfig":
        """HTTP/2 initial stream-level flow-control window."""
        return self.set("ballista.client.initial_stream_window_size", size)

    def with_grpc_client_max_message_size(self, size: int) -> "BallistaSessionConfig":
        """Maximum gRPC message size."""
        return self.set("ballista.client.grpc_max_message_size", size)

    def with_grpc_client_connect_timeout_seconds(
        self, seconds: int
    ) -> "BallistaSessionConfig":
        """gRPC client connection timeout in seconds."""
        return self.set("ballista.client.grpc_connect_timeout_seconds", seconds)

    def with_grpc_client_timeout_seconds(self, seconds: int) -> "BallistaSessionConfig":
        """gRPC client request timeout in seconds."""
        return self.set("ballista.client.grpc_timeout_seconds", seconds)

    def with_grpc_client_tcp_keepalive_seconds(
        self, seconds: int
    ) -> "BallistaSessionConfig":
        """TCP keep-alive interval for gRPC clients in seconds."""
        return self.set("ballista.client.grpc_tcp_keepalive_seconds", seconds)

    def with_grpc_client_http2_keepalive_interval_seconds(
        self, seconds: int
    ) -> "BallistaSessionConfig":
        """HTTP/2 keep-alive interval for gRPC clients in seconds."""
        return self.set(
            "ballista.client.grpc_http2_keepalive_interval_seconds", seconds
        )

    # client

    def with_client_pull(self, enabled: bool) -> "BallistaSessionConfig":
        """Use pull (``True``) or push job tracking strategy."""
        return self.set("ballista.client.pull", enabled)

    def with_use_tls(self, enabled: bool) -> "BallistaSessionConfig":
        """Use TLS connection to the cluster."""
        return self.set("ballista.client.use_tls", enabled)

    def with_io_retries_times(self, retries: int) -> "BallistaSessionConfig":
        """Number of retries for client IO operations."""
        return self.set("ballista.client.io_retries_times", retries)

    def with_io_retry_wait_time_ms(self, wait_ms: int) -> "BallistaSessionConfig":
        """Wait time in milliseconds between client IO retries."""
        return self.set("ballista.client.io_retry_wait_time_ms", wait_ms)

    # planner

    def with_adaptive_query_planner(self, enabled: bool) -> "BallistaSessionConfig":
        """Enable adaptive query planning."""
        return self.set("ballista.planner.adaptive.enabled", enabled)

    def with_adaptive_join_enabled(self, enabled: bool) -> "BallistaSessionConfig":
        """Enable the AQE dynamic join selection rule."""
        return self.set("ballista.planner.adaptive_join.enabled", enabled)

    def with_coalesce_enabled(self, enabled: bool) -> "BallistaSessionConfig":
        """Enable the AQE coalesce shuffle partitions rule."""
        return self.set("ballista.planner.coalesce.enabled", enabled)

    def with_coalesce_target_partition_bytes(
        self, target_bytes: int
    ) -> "BallistaSessionConfig":
        """Target post-coalesce partition size in bytes."""
        return self.set(
            "ballista.planner.coalesce.target_partition_bytes", target_bytes
        )

    def with_coalesce_small_partition_factor(
        self, factor: float
    ) -> "BallistaSessionConfig":
        """Small partition merge factor."""
        return self.set("ballista.planner.coalesce.small_partition_factor", factor)

    def with_coalesce_merged_partition_factor(
        self, factor: float
    ) -> "BallistaSessionConfig":
        """Merged partition early-flush factor."""
        return self.set("ballista.planner.coalesce.merged_partition_factor", factor)

    def with_propagate_empty_enabled(self, enabled: bool) -> "BallistaSessionConfig":
        """Enable the AQE propagate empty exec rule."""
        return self.set("ballista.planner.propagate_empty.enabled", enabled)

    def with_parallel_window_enabled(self, enabled: bool) -> "BallistaSessionConfig":
        """Enable the AQE parallel window rule."""
        return self.set("ballista.planner.parallel_window.enabled", enabled)

    # optimizer

    def with_broadcast_join_threshold_bytes(
        self, threshold_bytes: int
    ) -> "BallistaSessionConfig":
        """Byte threshold below which a join side is broadcast, ``0`` disables."""
        return self.set(
            "ballista.optimizer.broadcast_join_threshold_bytes", threshold_bytes
        )

    def with_broadcast_join_threshold_rows(
        self, threshold_rows: int
    ) -> "BallistaSessionConfig":
        """Row threshold below which a join side is broadcast, ``0`` disables."""
        return self.set(
            "ballista.optimizer.broadcast_join_threshold_rows", threshold_rows
        )

    def with_hash_join_max_build_partition_bytes(
        self, max_bytes: int
    ) -> "BallistaSessionConfig":
        """Maximum hash join build partition bytes under AQE, ``0`` disables."""
        return self.set(
            "ballista.optimizer.hash_join_max_build_partition_bytes", max_bytes
        )

    def with_stage_build_side(self, enabled: bool) -> "BallistaSessionConfig":
        """Stage a join's build side as its own shuffle under AQE."""
        return self.set("ballista.optimizer.stage_build_side", enabled)

    def with_stage_build_side_min_probe_ratio(
        self, ratio: int
    ) -> "BallistaSessionConfig":
        """Minimal probe to build side ratio for staging the build side."""
        return self.set("ballista.optimizer.stage_build_side_min_probe_ratio", ratio)

    def with_stage_build_side_max_estimate_multiple(
        self, multiple: int
    ) -> "BallistaSessionConfig":
        """Maximal estimated build side multiple of broadcast threshold to stage."""
        return self.set(
            "ballista.optimizer.stage_build_side_max_estimate_multiple", multiple
        )

    def with_not_in_subquery_rewrite(self, enabled: bool) -> "BallistaSessionConfig":
        """Rewrite uncorrelated ``NOT IN (subquery)`` into an anti join."""
        return self.set("ballista.optimizer.not_in_subquery_rewrite", enabled)

    def with_balance_scan_file_groups(self, enabled: bool) -> "BallistaSessionConfig":
        """Prune scan files by statistics and balance scan file groups."""
        return self.set("ballista.optimizer.balance_scan_file_groups", enabled)

    # scheduler

    def with_max_partitions_per_task(
        self, max_partitions: int
    ) -> "BallistaSessionConfig":
        """Scheduler per-task partition slice cap."""
        return self.set("ballista.scheduler.max_partitions_per_task", max_partitions)

    def with_share_file_statistics_cache(
        self, enabled: bool
    ) -> "BallistaSessionConfig":
        """Plan jobs with the scheduler-wide file statistics cache."""
        return self.set("ballista.scheduler.share_file_statistics_cache", enabled)
