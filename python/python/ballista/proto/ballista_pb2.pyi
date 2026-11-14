from . import datafusion_pb2 as _datafusion_pb2
from . import datafusion_common_pb2 as _datafusion_common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BufferMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    BUFFER_MODE_DAM: _ClassVar[BufferMode]

class ScalarOpNode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SCALAR_OP_NODE_ADD: _ClassVar[ScalarOpNode]
    SCALAR_OP_NODE_MIN: _ClassVar[ScalarOpNode]
    SCALAR_OP_NODE_MAX: _ClassVar[ScalarOpNode]
    SCALAR_OP_NODE_OVERWRITE: _ClassVar[ScalarOpNode]

class ShuffleLayout(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SHUFFLE_LAYOUT_PASSTHROUGH: _ClassVar[ShuffleLayout]
    SHUFFLE_LAYOUT_SORT: _ClassVar[ShuffleLayout]

class ShuffleFileKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SHUFFLE_FILE_KIND_DATA: _ClassVar[ShuffleFileKind]
    SHUFFLE_FILE_KIND_INDEX: _ClassVar[ShuffleFileKind]

class SketchType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SKETCH_TYPE_UNSPECIFIED: _ClassVar[SketchType]
    SKETCH_TYPE_HLL: _ClassVar[SketchType]
BUFFER_MODE_DAM: BufferMode
SCALAR_OP_NODE_ADD: ScalarOpNode
SCALAR_OP_NODE_MIN: ScalarOpNode
SCALAR_OP_NODE_MAX: ScalarOpNode
SCALAR_OP_NODE_OVERWRITE: ScalarOpNode
SHUFFLE_LAYOUT_PASSTHROUGH: ShuffleLayout
SHUFFLE_LAYOUT_SORT: ShuffleLayout
SHUFFLE_FILE_KIND_DATA: ShuffleFileKind
SHUFFLE_FILE_KIND_INDEX: ShuffleFileKind
SKETCH_TYPE_UNSPECIFIED: SketchType
SKETCH_TYPE_HLL: SketchType

class BallistaLogicalPlanNode(_message.Message):
    __slots__ = ("cache_node",)
    CACHE_NODE_FIELD_NUMBER: _ClassVar[int]
    cache_node: LogicalPlanCacheNode
    def __init__(self, cache_node: _Optional[_Union[LogicalPlanCacheNode, _Mapping]] = ...) -> None: ...

class LogicalPlanCacheNode(_message.Message):
    __slots__ = ("cache_id", "session_id")
    CACHE_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    cache_id: str
    session_id: str
    def __init__(self, cache_id: _Optional[str] = ..., session_id: _Optional[str] = ...) -> None: ...

class BallistaPhysicalPlanNode(_message.Message):
    __slots__ = ("shuffle_writer", "shuffle_reader", "unresolved_shuffle", "sort_shuffle_writer", "chaos_exec", "runtime_stats", "buffer", "unordered_range_repartition", "ordered_range_repartition", "per_partition_filter", "partitioned_bounded_window_agg", "range_shuffle_reader", "range_filter", "prefix_merge", "range_shuffle_writer", "distributed_query", "distributed_explain_analyze", "pipelined_shuffle_writer", "pipelined_shuffle_reader", "runtime_filter", "partition_id_repartition")
    SHUFFLE_WRITER_FIELD_NUMBER: _ClassVar[int]
    SHUFFLE_READER_FIELD_NUMBER: _ClassVar[int]
    UNRESOLVED_SHUFFLE_FIELD_NUMBER: _ClassVar[int]
    SORT_SHUFFLE_WRITER_FIELD_NUMBER: _ClassVar[int]
    CHAOS_EXEC_FIELD_NUMBER: _ClassVar[int]
    RUNTIME_STATS_FIELD_NUMBER: _ClassVar[int]
    BUFFER_FIELD_NUMBER: _ClassVar[int]
    UNORDERED_RANGE_REPARTITION_FIELD_NUMBER: _ClassVar[int]
    ORDERED_RANGE_REPARTITION_FIELD_NUMBER: _ClassVar[int]
    PER_PARTITION_FILTER_FIELD_NUMBER: _ClassVar[int]
    PARTITIONED_BOUNDED_WINDOW_AGG_FIELD_NUMBER: _ClassVar[int]
    RANGE_SHUFFLE_READER_FIELD_NUMBER: _ClassVar[int]
    RANGE_FILTER_FIELD_NUMBER: _ClassVar[int]
    PREFIX_MERGE_FIELD_NUMBER: _ClassVar[int]
    RANGE_SHUFFLE_WRITER_FIELD_NUMBER: _ClassVar[int]
    DISTRIBUTED_QUERY_FIELD_NUMBER: _ClassVar[int]
    DISTRIBUTED_EXPLAIN_ANALYZE_FIELD_NUMBER: _ClassVar[int]
    PIPELINED_SHUFFLE_WRITER_FIELD_NUMBER: _ClassVar[int]
    PIPELINED_SHUFFLE_READER_FIELD_NUMBER: _ClassVar[int]
    RUNTIME_FILTER_FIELD_NUMBER: _ClassVar[int]
    PARTITION_ID_REPARTITION_FIELD_NUMBER: _ClassVar[int]
    shuffle_writer: ShuffleWriterExecNode
    shuffle_reader: ShuffleReaderExecNode
    unresolved_shuffle: UnresolvedShuffleExecNode
    sort_shuffle_writer: SortShuffleWriterExecNode
    chaos_exec: ChaosExecNode
    runtime_stats: RuntimeStatsExecNode
    buffer: BufferExecNode
    unordered_range_repartition: UnorderedRangeRepartitionExecNode
    ordered_range_repartition: OrderedRangeRepartitionExecNode
    per_partition_filter: PerPartitionFilterExecNode
    partitioned_bounded_window_agg: PartitionedBoundedWindowAggExecNode
    range_shuffle_reader: RangeShuffleReaderExecNode
    range_filter: RangeFilterExecNode
    prefix_merge: PrefixMergeExecNode
    range_shuffle_writer: RangeShuffleWriterExecNode
    distributed_query: DistributedQueryExecNode
    distributed_explain_analyze: DistributedExplainAnalyzeExecNode
    pipelined_shuffle_writer: PipelinedShuffleWriterExecNode
    pipelined_shuffle_reader: PipelinedShuffleReaderExecNode
    runtime_filter: RuntimeFilterExecNode
    partition_id_repartition: PartitionIdRepartitionExecNode
    def __init__(self, shuffle_writer: _Optional[_Union[ShuffleWriterExecNode, _Mapping]] = ..., shuffle_reader: _Optional[_Union[ShuffleReaderExecNode, _Mapping]] = ..., unresolved_shuffle: _Optional[_Union[UnresolvedShuffleExecNode, _Mapping]] = ..., sort_shuffle_writer: _Optional[_Union[SortShuffleWriterExecNode, _Mapping]] = ..., chaos_exec: _Optional[_Union[ChaosExecNode, _Mapping]] = ..., runtime_stats: _Optional[_Union[RuntimeStatsExecNode, _Mapping]] = ..., buffer: _Optional[_Union[BufferExecNode, _Mapping]] = ..., unordered_range_repartition: _Optional[_Union[UnorderedRangeRepartitionExecNode, _Mapping]] = ..., ordered_range_repartition: _Optional[_Union[OrderedRangeRepartitionExecNode, _Mapping]] = ..., per_partition_filter: _Optional[_Union[PerPartitionFilterExecNode, _Mapping]] = ..., partitioned_bounded_window_agg: _Optional[_Union[PartitionedBoundedWindowAggExecNode, _Mapping]] = ..., range_shuffle_reader: _Optional[_Union[RangeShuffleReaderExecNode, _Mapping]] = ..., range_filter: _Optional[_Union[RangeFilterExecNode, _Mapping]] = ..., prefix_merge: _Optional[_Union[PrefixMergeExecNode, _Mapping]] = ..., range_shuffle_writer: _Optional[_Union[RangeShuffleWriterExecNode, _Mapping]] = ..., distributed_query: _Optional[_Union[DistributedQueryExecNode, _Mapping]] = ..., distributed_explain_analyze: _Optional[_Union[DistributedExplainAnalyzeExecNode, _Mapping]] = ..., pipelined_shuffle_writer: _Optional[_Union[PipelinedShuffleWriterExecNode, _Mapping]] = ..., pipelined_shuffle_reader: _Optional[_Union[PipelinedShuffleReaderExecNode, _Mapping]] = ..., runtime_filter: _Optional[_Union[RuntimeFilterExecNode, _Mapping]] = ..., partition_id_repartition: _Optional[_Union[PartitionIdRepartitionExecNode, _Mapping]] = ...) -> None: ...

class RuntimeFilterExecNode(_message.Message):
    __slots__ = ("filter_id", "key", "build", "payload")
    FILTER_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    BUILD_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    filter_id: int
    key: _datafusion_pb2.PhysicalExprNode
    build: bool
    payload: RuntimeFilterPayload
    def __init__(self, filter_id: _Optional[int] = ..., key: _Optional[_Union[_datafusion_pb2.PhysicalExprNode, _Mapping]] = ..., build: _Optional[bool] = ..., payload: _Optional[_Union[RuntimeFilterPayload, _Mapping]] = ...) -> None: ...

class RuntimeFilterPayload(_message.Message):
    __slots__ = ("filter_id", "version", "bloom", "has_null", "has_in_set", "in_hashes", "min", "max")
    FILTER_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    BLOOM_FIELD_NUMBER: _ClassVar[int]
    HAS_NULL_FIELD_NUMBER: _ClassVar[int]
    HAS_IN_SET_FIELD_NUMBER: _ClassVar[int]
    IN_HASHES_FIELD_NUMBER: _ClassVar[int]
    MIN_FIELD_NUMBER: _ClassVar[int]
    MAX_FIELD_NUMBER: _ClassVar[int]
    filter_id: int
    version: int
    bloom: bytes
    has_null: bool
    has_in_set: bool
    in_hashes: _containers.RepeatedScalarFieldContainer[int]
    min: _datafusion_common_pb2.ScalarValue
    max: _datafusion_common_pb2.ScalarValue
    def __init__(self, filter_id: _Optional[int] = ..., version: _Optional[int] = ..., bloom: _Optional[bytes] = ..., has_null: _Optional[bool] = ..., has_in_set: _Optional[bool] = ..., in_hashes: _Optional[_Iterable[int]] = ..., min: _Optional[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]] = ..., max: _Optional[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]] = ...) -> None: ...

class PipelinedShuffleWriterExecNode(_message.Message):
    __slots__ = ("job_id", "stage_id", "output_partitioning", "broadcast")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_PARTITIONING_FIELD_NUMBER: _ClassVar[int]
    BROADCAST_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    stage_id: int
    output_partitioning: _datafusion_pb2.PhysicalHashRepartition
    broadcast: bool
    def __init__(self, job_id: _Optional[str] = ..., stage_id: _Optional[int] = ..., output_partitioning: _Optional[_Union[_datafusion_pb2.PhysicalHashRepartition, _Mapping]] = ..., broadcast: _Optional[bool] = ...) -> None: ...

class PipelinedShuffleReaderExecNode(_message.Message):
    __slots__ = ("partition", "schema", "stage_id", "partitioning", "broadcast", "executors")
    PARTITION_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITIONING_FIELD_NUMBER: _ClassVar[int]
    BROADCAST_FIELD_NUMBER: _ClassVar[int]
    EXECUTORS_FIELD_NUMBER: _ClassVar[int]
    partition: _containers.RepeatedCompositeFieldContainer[PipelinedReaderPartition]
    schema: _datafusion_common_pb2.Schema
    stage_id: int
    partitioning: _datafusion_pb2.Partitioning
    broadcast: bool
    executors: _containers.RepeatedCompositeFieldContainer[ExecutorEndpoint]
    def __init__(self, partition: _Optional[_Iterable[_Union[PipelinedReaderPartition, _Mapping]]] = ..., schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., stage_id: _Optional[int] = ..., partitioning: _Optional[_Union[_datafusion_pb2.Partitioning, _Mapping]] = ..., broadcast: _Optional[bool] = ..., executors: _Optional[_Iterable[_Union[ExecutorEndpoint, _Mapping]]] = ...) -> None: ...

class PipelinedReaderPartition(_message.Message):
    __slots__ = ("location",)
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    location: _containers.RepeatedCompositeFieldContainer[PipelinedPartitionLocation]
    def __init__(self, location: _Optional[_Iterable[_Union[PipelinedPartitionLocation, _Mapping]]] = ...) -> None: ...

class PipelinedPartitionLocation(_message.Message):
    __slots__ = ("partition_id", "task_id", "executor_meta", "executor_index")
    PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_META_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_INDEX_FIELD_NUMBER: _ClassVar[int]
    partition_id: PartitionId
    task_id: int
    executor_meta: ExecutorEndpoint
    executor_index: int
    def __init__(self, partition_id: _Optional[_Union[PartitionId, _Mapping]] = ..., task_id: _Optional[int] = ..., executor_meta: _Optional[_Union[ExecutorEndpoint, _Mapping]] = ..., executor_index: _Optional[int] = ...) -> None: ...

class PartitionIdRouting(_message.Message):
    __slots__ = ("column", "partition_count")
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    PARTITION_COUNT_FIELD_NUMBER: _ClassVar[int]
    column: int
    partition_count: int
    def __init__(self, column: _Optional[int] = ..., partition_count: _Optional[int] = ...) -> None: ...

class PartitionIdRepartitionExecNode(_message.Message):
    __slots__ = ("partition_id_column", "partition_count")
    PARTITION_ID_COLUMN_FIELD_NUMBER: _ClassVar[int]
    PARTITION_COUNT_FIELD_NUMBER: _ClassVar[int]
    partition_id_column: int
    partition_count: int
    def __init__(self, partition_id_column: _Optional[int] = ..., partition_count: _Optional[int] = ...) -> None: ...

class DistributedQueryExecNode(_message.Message):
    __slots__ = ("scheduler_url", "settings", "logical_plan", "session_id")
    SCHEDULER_URL_FIELD_NUMBER: _ClassVar[int]
    SETTINGS_FIELD_NUMBER: _ClassVar[int]
    LOGICAL_PLAN_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    scheduler_url: str
    settings: _containers.RepeatedCompositeFieldContainer[KeyValuePair]
    logical_plan: bytes
    session_id: str
    def __init__(self, scheduler_url: _Optional[str] = ..., settings: _Optional[_Iterable[_Union[KeyValuePair, _Mapping]]] = ..., logical_plan: _Optional[bytes] = ..., session_id: _Optional[str] = ...) -> None: ...

class DistributedExplainAnalyzeExecNode(_message.Message):
    __slots__ = ("scheduler_url", "schema", "verbose")
    SCHEDULER_URL_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    VERBOSE_FIELD_NUMBER: _ClassVar[int]
    scheduler_url: str
    schema: _datafusion_common_pb2.Schema
    verbose: bool
    def __init__(self, scheduler_url: _Optional[str] = ..., schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., verbose: _Optional[bool] = ...) -> None: ...

class OrderedRangeRepartitionExecNode(_message.Message):
    __slots__ = ("order_by", "output_partitions")
    ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    order_by: _containers.RepeatedCompositeFieldContainer[_datafusion_pb2.PhysicalSortExprNode]
    output_partitions: int
    def __init__(self, order_by: _Optional[_Iterable[_Union[_datafusion_pb2.PhysicalSortExprNode, _Mapping]]] = ..., output_partitions: _Optional[int] = ...) -> None: ...

class RuntimeStatsExecNode(_message.Message):
    __slots__ = ("order_by",)
    ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    order_by: _containers.RepeatedCompositeFieldContainer[_datafusion_pb2.PhysicalSortExprNode]
    def __init__(self, order_by: _Optional[_Iterable[_Union[_datafusion_pb2.PhysicalSortExprNode, _Mapping]]] = ...) -> None: ...

class SortKeySketchState(_message.Message):
    __slots__ = ("k", "null_count", "key_min", "key_max", "levels")
    K_FIELD_NUMBER: _ClassVar[int]
    NULL_COUNT_FIELD_NUMBER: _ClassVar[int]
    KEY_MIN_FIELD_NUMBER: _ClassVar[int]
    KEY_MAX_FIELD_NUMBER: _ClassVar[int]
    LEVELS_FIELD_NUMBER: _ClassVar[int]
    k: int
    null_count: int
    key_min: _containers.RepeatedCompositeFieldContainer[_datafusion_common_pb2.ScalarValue]
    key_max: _containers.RepeatedCompositeFieldContainer[_datafusion_common_pb2.ScalarValue]
    levels: bytes
    def __init__(self, k: _Optional[int] = ..., null_count: _Optional[int] = ..., key_min: _Optional[_Iterable[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]]] = ..., key_max: _Optional[_Iterable[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]]] = ..., levels: _Optional[bytes] = ...) -> None: ...

class BufferExecNode(_message.Message):
    __slots__ = ("mode",)
    MODE_FIELD_NUMBER: _ClassVar[int]
    mode: BufferMode
    def __init__(self, mode: _Optional[_Union[BufferMode, str]] = ...) -> None: ...

class UnorderedRangeRepartitionExecNode(_message.Message):
    __slots__ = ("order_by", "output_partitions")
    ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    order_by: _containers.RepeatedCompositeFieldContainer[_datafusion_pb2.PhysicalSortExprNode]
    output_partitions: int
    def __init__(self, order_by: _Optional[_Iterable[_Union[_datafusion_pb2.PhysicalSortExprNode, _Mapping]]] = ..., output_partitions: _Optional[int] = ...) -> None: ...

class PerPartitionFilterExecNode(_message.Message):
    __slots__ = ("predicates",)
    PREDICATES_FIELD_NUMBER: _ClassVar[int]
    predicates: _containers.RepeatedCompositeFieldContainer[_datafusion_pb2.PhysicalExprNode]
    def __init__(self, predicates: _Optional[_Iterable[_Union[_datafusion_pb2.PhysicalExprNode, _Mapping]]] = ...) -> None: ...

class RangeFilterExecNode(_message.Message):
    __slots__ = ("filter_expr", "halo_lo", "halo_hi", "raw_bounds", "unordered_nulls_first", "ordered")
    FILTER_EXPR_FIELD_NUMBER: _ClassVar[int]
    HALO_LO_FIELD_NUMBER: _ClassVar[int]
    HALO_HI_FIELD_NUMBER: _ClassVar[int]
    RAW_BOUNDS_FIELD_NUMBER: _ClassVar[int]
    UNORDERED_NULLS_FIRST_FIELD_NUMBER: _ClassVar[int]
    ORDERED_FIELD_NUMBER: _ClassVar[int]
    filter_expr: _datafusion_pb2.PhysicalExprNode
    halo_lo: _datafusion_common_pb2.ScalarValue
    halo_hi: _datafusion_common_pb2.ScalarValue
    raw_bounds: _containers.RepeatedCompositeFieldContainer[RangeBound]
    unordered_nulls_first: bool
    ordered: SortOptions
    def __init__(self, filter_expr: _Optional[_Union[_datafusion_pb2.PhysicalExprNode, _Mapping]] = ..., halo_lo: _Optional[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]] = ..., halo_hi: _Optional[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]] = ..., raw_bounds: _Optional[_Iterable[_Union[RangeBound, _Mapping]]] = ..., unordered_nulls_first: _Optional[bool] = ..., ordered: _Optional[_Union[SortOptions, _Mapping]] = ...) -> None: ...

class SortOptions(_message.Message):
    __slots__ = ("descending", "nulls_first")
    DESCENDING_FIELD_NUMBER: _ClassVar[int]
    NULLS_FIRST_FIELD_NUMBER: _ClassVar[int]
    descending: bool
    nulls_first: bool
    def __init__(self, descending: _Optional[bool] = ..., nulls_first: _Optional[bool] = ...) -> None: ...

class RangeBound(_message.Message):
    __slots__ = ("lo", "hi")
    LO_FIELD_NUMBER: _ClassVar[int]
    HI_FIELD_NUMBER: _ClassVar[int]
    lo: _datafusion_common_pb2.ScalarValue
    hi: _datafusion_common_pb2.ScalarValue
    def __init__(self, lo: _Optional[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]] = ..., hi: _Optional[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]] = ...) -> None: ...

class PrefixMergeExecNode(_message.Message):
    __slots__ = ("applies", "per_partition_state")
    APPLIES_FIELD_NUMBER: _ClassVar[int]
    PER_PARTITION_STATE_FIELD_NUMBER: _ClassVar[int]
    applies: _containers.RepeatedCompositeFieldContainer[WindowApplyNode]
    per_partition_state: _containers.RepeatedCompositeFieldContainer[FinalizedPartitionStateNode]
    def __init__(self, applies: _Optional[_Iterable[_Union[WindowApplyNode, _Mapping]]] = ..., per_partition_state: _Optional[_Iterable[_Union[FinalizedPartitionStateNode, _Mapping]]] = ...) -> None: ...

class WindowApplyNode(_message.Message):
    __slots__ = ("scalar", "aggregate")
    SCALAR_FIELD_NUMBER: _ClassVar[int]
    AGGREGATE_FIELD_NUMBER: _ClassVar[int]
    scalar: ScalarWindowApplyNode
    aggregate: AggregateWindowApplyNode
    def __init__(self, scalar: _Optional[_Union[ScalarWindowApplyNode, _Mapping]] = ..., aggregate: _Optional[_Union[AggregateWindowApplyNode, _Mapping]] = ...) -> None: ...

class ScalarWindowApplyNode(_message.Message):
    __slots__ = ("op", "offset", "output_column")
    OP_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    op: ScalarOpNode
    offset: _containers.RepeatedCompositeFieldContainer[_datafusion_common_pb2.ScalarValue]
    output_column: int
    def __init__(self, op: _Optional[_Union[ScalarOpNode, str]] = ..., offset: _Optional[_Iterable[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]]] = ..., output_column: _Optional[int] = ...) -> None: ...

class AggregateWindowApplyNode(_message.Message):
    __slots__ = ("udf_name", "args", "output_column", "window_expr_index")
    UDF_NAME_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    WINDOW_EXPR_INDEX_FIELD_NUMBER: _ClassVar[int]
    udf_name: str
    args: _containers.RepeatedCompositeFieldContainer[_datafusion_pb2.PhysicalExprNode]
    output_column: int
    window_expr_index: int
    def __init__(self, udf_name: _Optional[str] = ..., args: _Optional[_Iterable[_Union[_datafusion_pb2.PhysicalExprNode, _Mapping]]] = ..., output_column: _Optional[int] = ..., window_expr_index: _Optional[int] = ...) -> None: ...

class FinalizedPartitionStateNode(_message.Message):
    __slots__ = ("slots",)
    SLOTS_FIELD_NUMBER: _ClassVar[int]
    slots: _containers.RepeatedCompositeFieldContainer[AggregateStateSlotNode]
    def __init__(self, slots: _Optional[_Iterable[_Union[AggregateStateSlotNode, _Mapping]]] = ...) -> None: ...

class AggregateStateSlotNode(_message.Message):
    __slots__ = ("state",)
    STATE_FIELD_NUMBER: _ClassVar[int]
    state: AggregateStateNode
    def __init__(self, state: _Optional[_Union[AggregateStateNode, _Mapping]] = ...) -> None: ...

class AggregateStateNode(_message.Message):
    __slots__ = ("values",)
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedCompositeFieldContainer[_datafusion_common_pb2.ScalarValue]
    def __init__(self, values: _Optional[_Iterable[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]]] = ...) -> None: ...

class PartitionedBoundedWindowAggExecNode(_message.Message):
    __slots__ = ("window_expr",)
    WINDOW_EXPR_FIELD_NUMBER: _ClassVar[int]
    window_expr: _containers.RepeatedCompositeFieldContainer[_datafusion_pb2.PhysicalWindowExprNode]
    def __init__(self, window_expr: _Optional[_Iterable[_Union[_datafusion_pb2.PhysicalWindowExprNode, _Mapping]]] = ...) -> None: ...

class ChaosExecNode(_message.Message):
    __slots__ = ("failure_probability", "fault_type", "seed")
    FAILURE_PROBABILITY_FIELD_NUMBER: _ClassVar[int]
    FAULT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SEED_FIELD_NUMBER: _ClassVar[int]
    failure_probability: float
    fault_type: str
    seed: int
    def __init__(self, failure_probability: _Optional[float] = ..., fault_type: _Optional[str] = ..., seed: _Optional[int] = ...) -> None: ...

class ShuffleWriterExecNode(_message.Message):
    __slots__ = ("job_id", "stage_id", "input", "output_partitioning")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_PARTITIONING_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    stage_id: int
    input: _datafusion_pb2.PhysicalPlanNode
    output_partitioning: _datafusion_pb2.PhysicalHashRepartition
    def __init__(self, job_id: _Optional[str] = ..., stage_id: _Optional[int] = ..., input: _Optional[_Union[_datafusion_pb2.PhysicalPlanNode, _Mapping]] = ..., output_partitioning: _Optional[_Union[_datafusion_pb2.PhysicalHashRepartition, _Mapping]] = ...) -> None: ...

class SortShuffleWriterExecNode(_message.Message):
    __slots__ = ("job_id", "stage_id", "input", "output_partitioning", "partition_id_routing", "partitioning", "batch_size", "memory_limit_per_task_bytes")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_PARTITIONING_FIELD_NUMBER: _ClassVar[int]
    PARTITION_ID_ROUTING_FIELD_NUMBER: _ClassVar[int]
    PARTITIONING_FIELD_NUMBER: _ClassVar[int]
    BATCH_SIZE_FIELD_NUMBER: _ClassVar[int]
    MEMORY_LIMIT_PER_TASK_BYTES_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    stage_id: int
    input: _datafusion_pb2.PhysicalPlanNode
    output_partitioning: _datafusion_pb2.PhysicalHashRepartition
    partition_id_routing: PartitionIdRouting
    partitioning: _datafusion_pb2.Partitioning
    batch_size: int
    memory_limit_per_task_bytes: int
    def __init__(self, job_id: _Optional[str] = ..., stage_id: _Optional[int] = ..., input: _Optional[_Union[_datafusion_pb2.PhysicalPlanNode, _Mapping]] = ..., output_partitioning: _Optional[_Union[_datafusion_pb2.PhysicalHashRepartition, _Mapping]] = ..., partition_id_routing: _Optional[_Union[PartitionIdRouting, _Mapping]] = ..., partitioning: _Optional[_Union[_datafusion_pb2.Partitioning, _Mapping]] = ..., batch_size: _Optional[int] = ..., memory_limit_per_task_bytes: _Optional[int] = ...) -> None: ...

class RangeShuffleWriterExecNode(_message.Message):
    __slots__ = ("job_id", "stage_id", "input")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    stage_id: int
    input: _datafusion_pb2.PhysicalPlanNode
    def __init__(self, job_id: _Optional[str] = ..., stage_id: _Optional[int] = ..., input: _Optional[_Union[_datafusion_pb2.PhysicalPlanNode, _Mapping]] = ...) -> None: ...

class UnresolvedShuffleExecNode(_message.Message):
    __slots__ = ("stage_id", "schema", "partitioning", "broadcast", "upstream_partition_count", "coalesce")
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    PARTITIONING_FIELD_NUMBER: _ClassVar[int]
    BROADCAST_FIELD_NUMBER: _ClassVar[int]
    UPSTREAM_PARTITION_COUNT_FIELD_NUMBER: _ClassVar[int]
    COALESCE_FIELD_NUMBER: _ClassVar[int]
    stage_id: int
    schema: _datafusion_common_pb2.Schema
    partitioning: _datafusion_pb2.Partitioning
    broadcast: bool
    upstream_partition_count: int
    coalesce: CoalescePlan
    def __init__(self, stage_id: _Optional[int] = ..., schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., partitioning: _Optional[_Union[_datafusion_pb2.Partitioning, _Mapping]] = ..., broadcast: _Optional[bool] = ..., upstream_partition_count: _Optional[int] = ..., coalesce: _Optional[_Union[CoalescePlan, _Mapping]] = ...) -> None: ...

class ShuffleReaderExecNode(_message.Message):
    __slots__ = ("partition", "schema", "stage_id", "partitioning", "broadcast", "upstream_partition_count", "coalesce", "executors")
    PARTITION_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITIONING_FIELD_NUMBER: _ClassVar[int]
    BROADCAST_FIELD_NUMBER: _ClassVar[int]
    UPSTREAM_PARTITION_COUNT_FIELD_NUMBER: _ClassVar[int]
    COALESCE_FIELD_NUMBER: _ClassVar[int]
    EXECUTORS_FIELD_NUMBER: _ClassVar[int]
    partition: _containers.RepeatedCompositeFieldContainer[ShuffleReaderPartition]
    schema: _datafusion_common_pb2.Schema
    stage_id: int
    partitioning: _datafusion_pb2.Partitioning
    broadcast: bool
    upstream_partition_count: int
    coalesce: CoalescePlan
    executors: _containers.RepeatedCompositeFieldContainer[ExecutorEndpoint]
    def __init__(self, partition: _Optional[_Iterable[_Union[ShuffleReaderPartition, _Mapping]]] = ..., schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., stage_id: _Optional[int] = ..., partitioning: _Optional[_Union[_datafusion_pb2.Partitioning, _Mapping]] = ..., broadcast: _Optional[bool] = ..., upstream_partition_count: _Optional[int] = ..., coalesce: _Optional[_Union[CoalescePlan, _Mapping]] = ..., executors: _Optional[_Iterable[_Union[ExecutorEndpoint, _Mapping]]] = ...) -> None: ...

class ShuffleReaderPartition(_message.Message):
    __slots__ = ("location",)
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    location: _containers.RepeatedCompositeFieldContainer[PartitionLocation]
    def __init__(self, location: _Optional[_Iterable[_Union[PartitionLocation, _Mapping]]] = ...) -> None: ...

class RangeShuffleReaderExecNode(_message.Message):
    __slots__ = ("partition", "schema", "stage_id", "merge_ordering", "fetch", "bounds", "executors")
    PARTITION_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    MERGE_ORDERING_FIELD_NUMBER: _ClassVar[int]
    FETCH_FIELD_NUMBER: _ClassVar[int]
    BOUNDS_FIELD_NUMBER: _ClassVar[int]
    EXECUTORS_FIELD_NUMBER: _ClassVar[int]
    partition: _containers.RepeatedCompositeFieldContainer[ShuffleReaderPartition]
    schema: _datafusion_common_pb2.Schema
    stage_id: int
    merge_ordering: _containers.RepeatedCompositeFieldContainer[_datafusion_pb2.PhysicalSortExprNode]
    fetch: int
    bounds: _containers.RepeatedCompositeFieldContainer[RangeBound]
    executors: _containers.RepeatedCompositeFieldContainer[ExecutorEndpoint]
    def __init__(self, partition: _Optional[_Iterable[_Union[ShuffleReaderPartition, _Mapping]]] = ..., schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., stage_id: _Optional[int] = ..., merge_ordering: _Optional[_Iterable[_Union[_datafusion_pb2.PhysicalSortExprNode, _Mapping]]] = ..., fetch: _Optional[int] = ..., bounds: _Optional[_Iterable[_Union[RangeBound, _Mapping]]] = ..., executors: _Optional[_Iterable[_Union[ExecutorEndpoint, _Mapping]]] = ...) -> None: ...

class CoalescePlan(_message.Message):
    __slots__ = ("upstream_partition_count", "groups")
    UPSTREAM_PARTITION_COUNT_FIELD_NUMBER: _ClassVar[int]
    GROUPS_FIELD_NUMBER: _ClassVar[int]
    upstream_partition_count: int
    groups: _containers.RepeatedCompositeFieldContainer[PartitionGroup]
    def __init__(self, upstream_partition_count: _Optional[int] = ..., groups: _Optional[_Iterable[_Union[PartitionGroup, _Mapping]]] = ...) -> None: ...

class PartitionGroup(_message.Message):
    __slots__ = ("upstream_indices",)
    UPSTREAM_INDICES_FIELD_NUMBER: _ClassVar[int]
    upstream_indices: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, upstream_indices: _Optional[_Iterable[int]] = ...) -> None: ...

class ExecutionGraph(_message.Message):
    __slots__ = ("job_id", "session_id", "status", "stages", "output_partitions", "output_locations", "scheduler_id", "failed_attempts", "job_name", "start_time", "end_time", "queued_at")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    STAGES_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_LOCATIONS_FIELD_NUMBER: _ClassVar[int]
    SCHEDULER_ID_FIELD_NUMBER: _ClassVar[int]
    FAILED_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    JOB_NAME_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    END_TIME_FIELD_NUMBER: _ClassVar[int]
    QUEUED_AT_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    session_id: str
    status: JobStatus
    stages: _containers.RepeatedCompositeFieldContainer[ExecutionGraphStage]
    output_partitions: int
    output_locations: _containers.RepeatedCompositeFieldContainer[PartitionLocation]
    scheduler_id: str
    failed_attempts: _containers.RepeatedCompositeFieldContainer[StageAttempts]
    job_name: str
    start_time: int
    end_time: int
    queued_at: int
    def __init__(self, job_id: _Optional[str] = ..., session_id: _Optional[str] = ..., status: _Optional[_Union[JobStatus, _Mapping]] = ..., stages: _Optional[_Iterable[_Union[ExecutionGraphStage, _Mapping]]] = ..., output_partitions: _Optional[int] = ..., output_locations: _Optional[_Iterable[_Union[PartitionLocation, _Mapping]]] = ..., scheduler_id: _Optional[str] = ..., failed_attempts: _Optional[_Iterable[_Union[StageAttempts, _Mapping]]] = ..., job_name: _Optional[str] = ..., start_time: _Optional[int] = ..., end_time: _Optional[int] = ..., queued_at: _Optional[int] = ...) -> None: ...

class StageAttempts(_message.Message):
    __slots__ = ("stage_id", "stage_attempt_num")
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_ATTEMPT_NUM_FIELD_NUMBER: _ClassVar[int]
    stage_id: int
    stage_attempt_num: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, stage_id: _Optional[int] = ..., stage_attempt_num: _Optional[_Iterable[int]] = ...) -> None: ...

class ExecutionGraphStage(_message.Message):
    __slots__ = ("unresolved_stage", "resolved_stage", "successful_stage", "failed_stage")
    UNRESOLVED_STAGE_FIELD_NUMBER: _ClassVar[int]
    RESOLVED_STAGE_FIELD_NUMBER: _ClassVar[int]
    SUCCESSFUL_STAGE_FIELD_NUMBER: _ClassVar[int]
    FAILED_STAGE_FIELD_NUMBER: _ClassVar[int]
    unresolved_stage: UnResolvedStage
    resolved_stage: ResolvedStage
    successful_stage: SuccessfulStage
    failed_stage: FailedStage
    def __init__(self, unresolved_stage: _Optional[_Union[UnResolvedStage, _Mapping]] = ..., resolved_stage: _Optional[_Union[ResolvedStage, _Mapping]] = ..., successful_stage: _Optional[_Union[SuccessfulStage, _Mapping]] = ..., failed_stage: _Optional[_Union[FailedStage, _Mapping]] = ...) -> None: ...

class UnResolvedStage(_message.Message):
    __slots__ = ("stage_id", "output_links", "inputs", "plan", "stage_attempt_num", "last_attempt_failure_reasons")
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_LINKS_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    PLAN_FIELD_NUMBER: _ClassVar[int]
    STAGE_ATTEMPT_NUM_FIELD_NUMBER: _ClassVar[int]
    LAST_ATTEMPT_FAILURE_REASONS_FIELD_NUMBER: _ClassVar[int]
    stage_id: int
    output_links: _containers.RepeatedScalarFieldContainer[int]
    inputs: _containers.RepeatedCompositeFieldContainer[GraphStageInput]
    plan: bytes
    stage_attempt_num: int
    last_attempt_failure_reasons: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, stage_id: _Optional[int] = ..., output_links: _Optional[_Iterable[int]] = ..., inputs: _Optional[_Iterable[_Union[GraphStageInput, _Mapping]]] = ..., plan: _Optional[bytes] = ..., stage_attempt_num: _Optional[int] = ..., last_attempt_failure_reasons: _Optional[_Iterable[str]] = ...) -> None: ...

class ResolvedStage(_message.Message):
    __slots__ = ("stage_id", "partitions", "output_links", "inputs", "plan", "stage_attempt_num", "last_attempt_failure_reasons")
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_LINKS_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    PLAN_FIELD_NUMBER: _ClassVar[int]
    STAGE_ATTEMPT_NUM_FIELD_NUMBER: _ClassVar[int]
    LAST_ATTEMPT_FAILURE_REASONS_FIELD_NUMBER: _ClassVar[int]
    stage_id: int
    partitions: int
    output_links: _containers.RepeatedScalarFieldContainer[int]
    inputs: _containers.RepeatedCompositeFieldContainer[GraphStageInput]
    plan: bytes
    stage_attempt_num: int
    last_attempt_failure_reasons: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, stage_id: _Optional[int] = ..., partitions: _Optional[int] = ..., output_links: _Optional[_Iterable[int]] = ..., inputs: _Optional[_Iterable[_Union[GraphStageInput, _Mapping]]] = ..., plan: _Optional[bytes] = ..., stage_attempt_num: _Optional[int] = ..., last_attempt_failure_reasons: _Optional[_Iterable[str]] = ...) -> None: ...

class SuccessfulStage(_message.Message):
    __slots__ = ("stage_id", "partitions", "output_links", "inputs", "plan", "task_infos", "stage_metrics", "stage_attempt_num")
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_LINKS_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    PLAN_FIELD_NUMBER: _ClassVar[int]
    TASK_INFOS_FIELD_NUMBER: _ClassVar[int]
    STAGE_METRICS_FIELD_NUMBER: _ClassVar[int]
    STAGE_ATTEMPT_NUM_FIELD_NUMBER: _ClassVar[int]
    stage_id: int
    partitions: int
    output_links: _containers.RepeatedScalarFieldContainer[int]
    inputs: _containers.RepeatedCompositeFieldContainer[GraphStageInput]
    plan: bytes
    task_infos: _containers.RepeatedCompositeFieldContainer[TaskInfo]
    stage_metrics: _containers.RepeatedCompositeFieldContainer[OperatorMetricsSet]
    stage_attempt_num: int
    def __init__(self, stage_id: _Optional[int] = ..., partitions: _Optional[int] = ..., output_links: _Optional[_Iterable[int]] = ..., inputs: _Optional[_Iterable[_Union[GraphStageInput, _Mapping]]] = ..., plan: _Optional[bytes] = ..., task_infos: _Optional[_Iterable[_Union[TaskInfo, _Mapping]]] = ..., stage_metrics: _Optional[_Iterable[_Union[OperatorMetricsSet, _Mapping]]] = ..., stage_attempt_num: _Optional[int] = ...) -> None: ...

class FailedStage(_message.Message):
    __slots__ = ("stage_id", "partitions", "output_links", "plan", "task_infos", "stage_metrics", "error_message", "stage_attempt_num")
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_LINKS_FIELD_NUMBER: _ClassVar[int]
    PLAN_FIELD_NUMBER: _ClassVar[int]
    TASK_INFOS_FIELD_NUMBER: _ClassVar[int]
    STAGE_METRICS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    STAGE_ATTEMPT_NUM_FIELD_NUMBER: _ClassVar[int]
    stage_id: int
    partitions: int
    output_links: _containers.RepeatedScalarFieldContainer[int]
    plan: bytes
    task_infos: _containers.RepeatedCompositeFieldContainer[TaskInfo]
    stage_metrics: _containers.RepeatedCompositeFieldContainer[OperatorMetricsSet]
    error_message: str
    stage_attempt_num: int
    def __init__(self, stage_id: _Optional[int] = ..., partitions: _Optional[int] = ..., output_links: _Optional[_Iterable[int]] = ..., plan: _Optional[bytes] = ..., task_infos: _Optional[_Iterable[_Union[TaskInfo, _Mapping]]] = ..., stage_metrics: _Optional[_Iterable[_Union[OperatorMetricsSet, _Mapping]]] = ..., error_message: _Optional[str] = ..., stage_attempt_num: _Optional[int] = ...) -> None: ...

class TaskInfo(_message.Message):
    __slots__ = ("task_id", "partition_id", "scheduled_time", "launch_time", "start_exec_time", "end_exec_time", "finish_time", "running", "failed", "successful")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
    SCHEDULED_TIME_FIELD_NUMBER: _ClassVar[int]
    LAUNCH_TIME_FIELD_NUMBER: _ClassVar[int]
    START_EXEC_TIME_FIELD_NUMBER: _ClassVar[int]
    END_EXEC_TIME_FIELD_NUMBER: _ClassVar[int]
    FINISH_TIME_FIELD_NUMBER: _ClassVar[int]
    RUNNING_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    SUCCESSFUL_FIELD_NUMBER: _ClassVar[int]
    task_id: int
    partition_id: int
    scheduled_time: int
    launch_time: int
    start_exec_time: int
    end_exec_time: int
    finish_time: int
    running: RunningTask
    failed: FailedTask
    successful: SuccessfulTask
    def __init__(self, task_id: _Optional[int] = ..., partition_id: _Optional[int] = ..., scheduled_time: _Optional[int] = ..., launch_time: _Optional[int] = ..., start_exec_time: _Optional[int] = ..., end_exec_time: _Optional[int] = ..., finish_time: _Optional[int] = ..., running: _Optional[_Union[RunningTask, _Mapping]] = ..., failed: _Optional[_Union[FailedTask, _Mapping]] = ..., successful: _Optional[_Union[SuccessfulTask, _Mapping]] = ...) -> None: ...

class GraphStageInput(_message.Message):
    __slots__ = ("stage_id", "partition_locations", "complete")
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITION_LOCATIONS_FIELD_NUMBER: _ClassVar[int]
    COMPLETE_FIELD_NUMBER: _ClassVar[int]
    stage_id: int
    partition_locations: _containers.RepeatedCompositeFieldContainer[TaskInputPartitions]
    complete: bool
    def __init__(self, stage_id: _Optional[int] = ..., partition_locations: _Optional[_Iterable[_Union[TaskInputPartitions, _Mapping]]] = ..., complete: _Optional[bool] = ...) -> None: ...

class TaskInputPartitions(_message.Message):
    __slots__ = ("partition", "partition_location")
    PARTITION_FIELD_NUMBER: _ClassVar[int]
    PARTITION_LOCATION_FIELD_NUMBER: _ClassVar[int]
    partition: int
    partition_location: _containers.RepeatedCompositeFieldContainer[PartitionLocation]
    def __init__(self, partition: _Optional[int] = ..., partition_location: _Optional[_Iterable[_Union[PartitionLocation, _Mapping]]] = ...) -> None: ...

class KeyValuePair(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: str
    def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

class Action(_message.Message):
    __slots__ = ("fetch_partition", "fetch_pipelined_partition", "settings")
    FETCH_PARTITION_FIELD_NUMBER: _ClassVar[int]
    FETCH_PIPELINED_PARTITION_FIELD_NUMBER: _ClassVar[int]
    SETTINGS_FIELD_NUMBER: _ClassVar[int]
    fetch_partition: FetchPartition
    fetch_pipelined_partition: FetchPipelinedPartition
    settings: _containers.RepeatedCompositeFieldContainer[KeyValuePair]
    def __init__(self, fetch_partition: _Optional[_Union[FetchPartition, _Mapping]] = ..., fetch_pipelined_partition: _Optional[_Union[FetchPipelinedPartition, _Mapping]] = ..., settings: _Optional[_Iterable[_Union[KeyValuePair, _Mapping]]] = ...) -> None: ...

class ExecutePartition(_message.Message):
    __slots__ = ("job_id", "stage_id", "partition_id", "plan", "partition_location", "output_partitioning")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
    PLAN_FIELD_NUMBER: _ClassVar[int]
    PARTITION_LOCATION_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_PARTITIONING_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    stage_id: int
    partition_id: _containers.RepeatedScalarFieldContainer[int]
    plan: _datafusion_pb2.PhysicalPlanNode
    partition_location: _containers.RepeatedCompositeFieldContainer[PartitionLocation]
    output_partitioning: _datafusion_pb2.PhysicalHashRepartition
    def __init__(self, job_id: _Optional[str] = ..., stage_id: _Optional[int] = ..., partition_id: _Optional[_Iterable[int]] = ..., plan: _Optional[_Union[_datafusion_pb2.PhysicalPlanNode, _Mapping]] = ..., partition_location: _Optional[_Iterable[_Union[PartitionLocation, _Mapping]]] = ..., output_partitioning: _Optional[_Union[_datafusion_pb2.PhysicalHashRepartition, _Mapping]] = ...) -> None: ...

class ByteRange(_message.Message):
    __slots__ = ("offset", "length")
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    LENGTH_FIELD_NUMBER: _ClassVar[int]
    offset: int
    length: int
    def __init__(self, offset: _Optional[int] = ..., length: _Optional[int] = ...) -> None: ...

class FetchPartition(_message.Message):
    __slots__ = ("job_id", "stage_id", "partition_id", "host", "port", "file_id", "layout", "file_kind", "byte_ranges")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
    HOST_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    LAYOUT_FIELD_NUMBER: _ClassVar[int]
    FILE_KIND_FIELD_NUMBER: _ClassVar[int]
    BYTE_RANGES_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    stage_id: int
    partition_id: int
    host: str
    port: int
    file_id: int
    layout: ShuffleLayout
    file_kind: ShuffleFileKind
    byte_ranges: _containers.RepeatedCompositeFieldContainer[ByteRange]
    def __init__(self, job_id: _Optional[str] = ..., stage_id: _Optional[int] = ..., partition_id: _Optional[int] = ..., host: _Optional[str] = ..., port: _Optional[int] = ..., file_id: _Optional[int] = ..., layout: _Optional[_Union[ShuffleLayout, str]] = ..., file_kind: _Optional[_Union[ShuffleFileKind, str]] = ..., byte_ranges: _Optional[_Iterable[_Union[ByteRange, _Mapping]]] = ...) -> None: ...

class FetchPipelinedPartition(_message.Message):
    __slots__ = ("job_id", "stage_id", "task_id", "partition_id", "host", "port", "registration_timeout_ms")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
    HOST_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    REGISTRATION_TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    stage_id: int
    task_id: int
    partition_id: int
    host: str
    port: int
    registration_timeout_ms: int
    def __init__(self, job_id: _Optional[str] = ..., stage_id: _Optional[int] = ..., task_id: _Optional[int] = ..., partition_id: _Optional[int] = ..., host: _Optional[str] = ..., port: _Optional[int] = ..., registration_timeout_ms: _Optional[int] = ...) -> None: ...

class PartitionLocation(_message.Message):
    __slots__ = ("map_partition_id", "partition_id", "executor_meta", "partition_stats", "file_id", "is_sort_shuffle", "executor_index")
    MAP_PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_META_FIELD_NUMBER: _ClassVar[int]
    PARTITION_STATS_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    IS_SORT_SHUFFLE_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_INDEX_FIELD_NUMBER: _ClassVar[int]
    map_partition_id: int
    partition_id: PartitionId
    executor_meta: ExecutorEndpoint
    partition_stats: PartitionStats
    file_id: int
    is_sort_shuffle: bool
    executor_index: int
    def __init__(self, map_partition_id: _Optional[int] = ..., partition_id: _Optional[_Union[PartitionId, _Mapping]] = ..., executor_meta: _Optional[_Union[ExecutorEndpoint, _Mapping]] = ..., partition_stats: _Optional[_Union[PartitionStats, _Mapping]] = ..., file_id: _Optional[int] = ..., is_sort_shuffle: _Optional[bool] = ..., executor_index: _Optional[int] = ...) -> None: ...

class PartitionId(_message.Message):
    __slots__ = ("job_id", "stage_id", "partition_id")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    stage_id: int
    partition_id: int
    def __init__(self, job_id: _Optional[str] = ..., stage_id: _Optional[int] = ..., partition_id: _Optional[int] = ...) -> None: ...

class TaskId(_message.Message):
    __slots__ = ("task_id", "task_attempt_num", "global_output_partition_ids", "vcores_consumed")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ATTEMPT_NUM_FIELD_NUMBER: _ClassVar[int]
    GLOBAL_OUTPUT_PARTITION_IDS_FIELD_NUMBER: _ClassVar[int]
    VCORES_CONSUMED_FIELD_NUMBER: _ClassVar[int]
    task_id: int
    task_attempt_num: int
    global_output_partition_ids: _containers.RepeatedScalarFieldContainer[int]
    vcores_consumed: int
    def __init__(self, task_id: _Optional[int] = ..., task_attempt_num: _Optional[int] = ..., global_output_partition_ids: _Optional[_Iterable[int]] = ..., vcores_consumed: _Optional[int] = ...) -> None: ...

class PartitionStats(_message.Message):
    __slots__ = ("num_rows", "num_batches", "num_bytes", "column_stats")
    NUM_ROWS_FIELD_NUMBER: _ClassVar[int]
    NUM_BATCHES_FIELD_NUMBER: _ClassVar[int]
    NUM_BYTES_FIELD_NUMBER: _ClassVar[int]
    COLUMN_STATS_FIELD_NUMBER: _ClassVar[int]
    num_rows: int
    num_batches: int
    num_bytes: int
    column_stats: _containers.RepeatedCompositeFieldContainer[ColumnStats]
    def __init__(self, num_rows: _Optional[int] = ..., num_batches: _Optional[int] = ..., num_bytes: _Optional[int] = ..., column_stats: _Optional[_Iterable[_Union[ColumnStats, _Mapping]]] = ...) -> None: ...

class ColumnStats(_message.Message):
    __slots__ = ("min_value", "max_value", "null_count", "distinct_count")
    MIN_VALUE_FIELD_NUMBER: _ClassVar[int]
    MAX_VALUE_FIELD_NUMBER: _ClassVar[int]
    NULL_COUNT_FIELD_NUMBER: _ClassVar[int]
    DISTINCT_COUNT_FIELD_NUMBER: _ClassVar[int]
    min_value: _datafusion_common_pb2.ScalarValue
    max_value: _datafusion_common_pb2.ScalarValue
    null_count: int
    distinct_count: int
    def __init__(self, min_value: _Optional[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]] = ..., max_value: _Optional[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]] = ..., null_count: _Optional[int] = ..., distinct_count: _Optional[int] = ...) -> None: ...

class OperatorMetricsSet(_message.Message):
    __slots__ = ("metrics",)
    METRICS_FIELD_NUMBER: _ClassVar[int]
    metrics: _containers.RepeatedCompositeFieldContainer[OperatorMetric]
    def __init__(self, metrics: _Optional[_Iterable[_Union[OperatorMetric, _Mapping]]] = ...) -> None: ...

class NamedCount(_message.Message):
    __slots__ = ("name", "value")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    name: str
    value: int
    def __init__(self, name: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...

class NamedGauge(_message.Message):
    __slots__ = ("name", "value")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    name: str
    value: int
    def __init__(self, name: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...

class NamedTime(_message.Message):
    __slots__ = ("name", "value")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    name: str
    value: int
    def __init__(self, name: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...

class NamedPruningMetrics(_message.Message):
    __slots__ = ("name", "pruned", "matched")
    NAME_FIELD_NUMBER: _ClassVar[int]
    PRUNED_FIELD_NUMBER: _ClassVar[int]
    MATCHED_FIELD_NUMBER: _ClassVar[int]
    name: str
    pruned: int
    matched: int
    def __init__(self, name: _Optional[str] = ..., pruned: _Optional[int] = ..., matched: _Optional[int] = ...) -> None: ...

class NamedRatio(_message.Message):
    __slots__ = ("name", "part", "total")
    NAME_FIELD_NUMBER: _ClassVar[int]
    PART_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    name: str
    part: int
    total: int
    def __init__(self, name: _Optional[str] = ..., part: _Optional[int] = ..., total: _Optional[int] = ...) -> None: ...

class OperatorMetric(_message.Message):
    __slots__ = ("output_rows", "elapse_time", "spill_count", "spilled_bytes", "current_memory_usage", "count", "gauge", "time", "start_timestamp", "end_timestamp", "spilled_rows", "output_bytes", "pruning_metrics", "ratio", "output_batches", "partition")
    OUTPUT_ROWS_FIELD_NUMBER: _ClassVar[int]
    ELAPSE_TIME_FIELD_NUMBER: _ClassVar[int]
    SPILL_COUNT_FIELD_NUMBER: _ClassVar[int]
    SPILLED_BYTES_FIELD_NUMBER: _ClassVar[int]
    CURRENT_MEMORY_USAGE_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    GAUGE_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    START_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    END_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    SPILLED_ROWS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_BYTES_FIELD_NUMBER: _ClassVar[int]
    PRUNING_METRICS_FIELD_NUMBER: _ClassVar[int]
    RATIO_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_BATCHES_FIELD_NUMBER: _ClassVar[int]
    PARTITION_FIELD_NUMBER: _ClassVar[int]
    output_rows: int
    elapse_time: int
    spill_count: int
    spilled_bytes: int
    current_memory_usage: int
    count: NamedCount
    gauge: NamedGauge
    time: NamedTime
    start_timestamp: int
    end_timestamp: int
    spilled_rows: int
    output_bytes: int
    pruning_metrics: NamedPruningMetrics
    ratio: NamedRatio
    output_batches: int
    partition: int
    def __init__(self, output_rows: _Optional[int] = ..., elapse_time: _Optional[int] = ..., spill_count: _Optional[int] = ..., spilled_bytes: _Optional[int] = ..., current_memory_usage: _Optional[int] = ..., count: _Optional[_Union[NamedCount, _Mapping]] = ..., gauge: _Optional[_Union[NamedGauge, _Mapping]] = ..., time: _Optional[_Union[NamedTime, _Mapping]] = ..., start_timestamp: _Optional[int] = ..., end_timestamp: _Optional[int] = ..., spilled_rows: _Optional[int] = ..., output_bytes: _Optional[int] = ..., pruning_metrics: _Optional[_Union[NamedPruningMetrics, _Mapping]] = ..., ratio: _Optional[_Union[NamedRatio, _Mapping]] = ..., output_batches: _Optional[int] = ..., partition: _Optional[int] = ...) -> None: ...

class ExecutorEndpoint(_message.Message):
    __slots__ = ("id", "host", "port")
    ID_FIELD_NUMBER: _ClassVar[int]
    HOST_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    id: str
    host: str
    port: int
    def __init__(self, id: _Optional[str] = ..., host: _Optional[str] = ..., port: _Optional[int] = ...) -> None: ...

class ExecutorRegistration(_message.Message):
    __slots__ = ("id", "host", "port", "grpc_port", "specification", "os_info", "ballista_protocol_version")
    ID_FIELD_NUMBER: _ClassVar[int]
    HOST_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    GRPC_PORT_FIELD_NUMBER: _ClassVar[int]
    SPECIFICATION_FIELD_NUMBER: _ClassVar[int]
    OS_INFO_FIELD_NUMBER: _ClassVar[int]
    BALLISTA_PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    id: str
    host: str
    port: int
    grpc_port: int
    specification: ExecutorSpecification
    os_info: ExecutorOperatingSystemSpecification
    ballista_protocol_version: int
    def __init__(self, id: _Optional[str] = ..., host: _Optional[str] = ..., port: _Optional[int] = ..., grpc_port: _Optional[int] = ..., specification: _Optional[_Union[ExecutorSpecification, _Mapping]] = ..., os_info: _Optional[_Union[ExecutorOperatingSystemSpecification, _Mapping]] = ..., ballista_protocol_version: _Optional[int] = ...) -> None: ...

class ExecutorHeartbeat(_message.Message):
    __slots__ = ("executor_id", "timestamp", "metrics", "status", "peak_proc_physical_memory", "peak_proc_virtual_memory")
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    PEAK_PROC_PHYSICAL_MEMORY_FIELD_NUMBER: _ClassVar[int]
    PEAK_PROC_VIRTUAL_MEMORY_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    timestamp: int
    metrics: _containers.RepeatedCompositeFieldContainer[ExecutorMetric]
    status: ExecutorStatus
    peak_proc_physical_memory: int
    peak_proc_virtual_memory: int
    def __init__(self, executor_id: _Optional[str] = ..., timestamp: _Optional[int] = ..., metrics: _Optional[_Iterable[_Union[ExecutorMetric, _Mapping]]] = ..., status: _Optional[_Union[ExecutorStatus, _Mapping]] = ..., peak_proc_physical_memory: _Optional[int] = ..., peak_proc_virtual_memory: _Optional[int] = ...) -> None: ...

class ExecutorMetric(_message.Message):
    __slots__ = ("available_memory", "total_memory", "used_memory", "proc_physical_memory", "proc_virtual_memory", "peak_physical_memory", "peak_virtual_memory", "memory_pool_reserved", "memory_pool_size", "allocated_memory")
    AVAILABLE_MEMORY_FIELD_NUMBER: _ClassVar[int]
    TOTAL_MEMORY_FIELD_NUMBER: _ClassVar[int]
    USED_MEMORY_FIELD_NUMBER: _ClassVar[int]
    PROC_PHYSICAL_MEMORY_FIELD_NUMBER: _ClassVar[int]
    PROC_VIRTUAL_MEMORY_FIELD_NUMBER: _ClassVar[int]
    PEAK_PHYSICAL_MEMORY_FIELD_NUMBER: _ClassVar[int]
    PEAK_VIRTUAL_MEMORY_FIELD_NUMBER: _ClassVar[int]
    MEMORY_POOL_RESERVED_FIELD_NUMBER: _ClassVar[int]
    MEMORY_POOL_SIZE_FIELD_NUMBER: _ClassVar[int]
    ALLOCATED_MEMORY_FIELD_NUMBER: _ClassVar[int]
    available_memory: int
    total_memory: int
    used_memory: int
    proc_physical_memory: int
    proc_virtual_memory: int
    peak_physical_memory: int
    peak_virtual_memory: int
    memory_pool_reserved: int
    memory_pool_size: int
    allocated_memory: int
    def __init__(self, available_memory: _Optional[int] = ..., total_memory: _Optional[int] = ..., used_memory: _Optional[int] = ..., proc_physical_memory: _Optional[int] = ..., proc_virtual_memory: _Optional[int] = ..., peak_physical_memory: _Optional[int] = ..., peak_virtual_memory: _Optional[int] = ..., memory_pool_reserved: _Optional[int] = ..., memory_pool_size: _Optional[int] = ..., allocated_memory: _Optional[int] = ...) -> None: ...

class ExecutorStatus(_message.Message):
    __slots__ = ("active", "dead", "unknown", "terminating")
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    DEAD_FIELD_NUMBER: _ClassVar[int]
    UNKNOWN_FIELD_NUMBER: _ClassVar[int]
    TERMINATING_FIELD_NUMBER: _ClassVar[int]
    active: str
    dead: str
    unknown: str
    terminating: str
    def __init__(self, active: _Optional[str] = ..., dead: _Optional[str] = ..., unknown: _Optional[str] = ..., terminating: _Optional[str] = ...) -> None: ...

class ExecutorSpecification(_message.Message):
    __slots__ = ("resources",)
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    resources: _containers.RepeatedCompositeFieldContainer[ExecutorResource]
    def __init__(self, resources: _Optional[_Iterable[_Union[ExecutorResource, _Mapping]]] = ...) -> None: ...

class ExecutorResource(_message.Message):
    __slots__ = ("vcores",)
    VCORES_FIELD_NUMBER: _ClassVar[int]
    vcores: int
    def __init__(self, vcores: _Optional[int] = ...) -> None: ...

class ExecutorOperatingSystemSpecification(_message.Message):
    __slots__ = ("system_name", "kernel_ver", "os_ver", "os_ver_long", "physical_cores", "num_disks", "total_disk_space", "total_available_disk_space", "open_files_limit")
    SYSTEM_NAME_FIELD_NUMBER: _ClassVar[int]
    KERNEL_VER_FIELD_NUMBER: _ClassVar[int]
    OS_VER_FIELD_NUMBER: _ClassVar[int]
    OS_VER_LONG_FIELD_NUMBER: _ClassVar[int]
    PHYSICAL_CORES_FIELD_NUMBER: _ClassVar[int]
    NUM_DISKS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_DISK_SPACE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_AVAILABLE_DISK_SPACE_FIELD_NUMBER: _ClassVar[int]
    OPEN_FILES_LIMIT_FIELD_NUMBER: _ClassVar[int]
    system_name: str
    kernel_ver: str
    os_ver: str
    os_ver_long: str
    physical_cores: int
    num_disks: int
    total_disk_space: int
    total_available_disk_space: int
    open_files_limit: int
    def __init__(self, system_name: _Optional[str] = ..., kernel_ver: _Optional[str] = ..., os_ver: _Optional[str] = ..., os_ver_long: _Optional[str] = ..., physical_cores: _Optional[int] = ..., num_disks: _Optional[int] = ..., total_disk_space: _Optional[int] = ..., total_available_disk_space: _Optional[int] = ..., open_files_limit: _Optional[int] = ...) -> None: ...

class AvailableVcores(_message.Message):
    __slots__ = ("executor_id", "vcores")
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    VCORES_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    vcores: int
    def __init__(self, executor_id: _Optional[str] = ..., vcores: _Optional[int] = ...) -> None: ...

class ExecutorVcores(_message.Message):
    __slots__ = ("vcores",)
    VCORES_FIELD_NUMBER: _ClassVar[int]
    vcores: _containers.RepeatedCompositeFieldContainer[AvailableVcores]
    def __init__(self, vcores: _Optional[_Iterable[_Union[AvailableVcores, _Mapping]]] = ...) -> None: ...

class ExecutorData(_message.Message):
    __slots__ = ("executor_id", "resources")
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    resources: _containers.RepeatedCompositeFieldContainer[ExecutorResourcePair]
    def __init__(self, executor_id: _Optional[str] = ..., resources: _Optional[_Iterable[_Union[ExecutorResourcePair, _Mapping]]] = ...) -> None: ...

class ExecutorResourcePair(_message.Message):
    __slots__ = ("total", "available")
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    AVAILABLE_FIELD_NUMBER: _ClassVar[int]
    total: ExecutorResource
    available: ExecutorResource
    def __init__(self, total: _Optional[_Union[ExecutorResource, _Mapping]] = ..., available: _Optional[_Union[ExecutorResource, _Mapping]] = ...) -> None: ...

class RunningTask(_message.Message):
    __slots__ = ("executor_id",)
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    def __init__(self, executor_id: _Optional[str] = ...) -> None: ...

class FailedTask(_message.Message):
    __slots__ = ("error", "retryable", "count_to_failures", "execution_error", "fetch_partition_error", "io_error", "executor_lost", "result_lost", "task_killed")
    ERROR_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    COUNT_TO_FAILURES_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_ERROR_FIELD_NUMBER: _ClassVar[int]
    FETCH_PARTITION_ERROR_FIELD_NUMBER: _ClassVar[int]
    IO_ERROR_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_LOST_FIELD_NUMBER: _ClassVar[int]
    RESULT_LOST_FIELD_NUMBER: _ClassVar[int]
    TASK_KILLED_FIELD_NUMBER: _ClassVar[int]
    error: str
    retryable: bool
    count_to_failures: bool
    execution_error: ExecutionError
    fetch_partition_error: FetchPartitionError
    io_error: IOError
    executor_lost: ExecutorLost
    result_lost: ResultLost
    task_killed: TaskKilled
    def __init__(self, error: _Optional[str] = ..., retryable: _Optional[bool] = ..., count_to_failures: _Optional[bool] = ..., execution_error: _Optional[_Union[ExecutionError, _Mapping]] = ..., fetch_partition_error: _Optional[_Union[FetchPartitionError, _Mapping]] = ..., io_error: _Optional[_Union[IOError, _Mapping]] = ..., executor_lost: _Optional[_Union[ExecutorLost, _Mapping]] = ..., result_lost: _Optional[_Union[ResultLost, _Mapping]] = ..., task_killed: _Optional[_Union[TaskKilled, _Mapping]] = ...) -> None: ...

class TaskColumnStats(_message.Message):
    __slots__ = ("column", "null_count", "sketches")
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    NULL_COUNT_FIELD_NUMBER: _ClassVar[int]
    SKETCHES_FIELD_NUMBER: _ClassVar[int]
    column: int
    null_count: int
    sketches: _containers.RepeatedCompositeFieldContainer[ColumnSketch]
    def __init__(self, column: _Optional[int] = ..., null_count: _Optional[int] = ..., sketches: _Optional[_Iterable[_Union[ColumnSketch, _Mapping]]] = ...) -> None: ...

class ColumnSketch(_message.Message):
    __slots__ = ("type", "data")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    type: SketchType
    data: bytes
    def __init__(self, type: _Optional[_Union[SketchType, str]] = ..., data: _Optional[bytes] = ...) -> None: ...

class SuccessfulTask(_message.Message):
    __slots__ = ("executor_id", "partitions", "runtime_stats", "window_state", "task_column_stats", "runtime_filters")
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    RUNTIME_STATS_FIELD_NUMBER: _ClassVar[int]
    WINDOW_STATE_FIELD_NUMBER: _ClassVar[int]
    TASK_COLUMN_STATS_FIELD_NUMBER: _ClassVar[int]
    RUNTIME_FILTERS_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    partitions: _containers.RepeatedCompositeFieldContainer[ShuffleWritePartition]
    runtime_stats: _containers.RepeatedCompositeFieldContainer[RuntimeStatsReport]
    window_state: _containers.RepeatedCompositeFieldContainer[WindowStateReport]
    task_column_stats: _containers.RepeatedCompositeFieldContainer[TaskColumnStats]
    runtime_filters: _containers.RepeatedCompositeFieldContainer[RuntimeFilterPayload]
    def __init__(self, executor_id: _Optional[str] = ..., partitions: _Optional[_Iterable[_Union[ShuffleWritePartition, _Mapping]]] = ..., runtime_stats: _Optional[_Iterable[_Union[RuntimeStatsReport, _Mapping]]] = ..., window_state: _Optional[_Iterable[_Union[WindowStateReport, _Mapping]]] = ..., task_column_stats: _Optional[_Iterable[_Union[TaskColumnStats, _Mapping]]] = ..., runtime_filters: _Optional[_Iterable[_Union[RuntimeFilterPayload, _Mapping]]] = ...) -> None: ...

class WindowStateReport(_message.Message):
    __slots__ = ("global_partition_id", "window_expr_index", "partition_key", "state")
    GLOBAL_PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
    WINDOW_EXPR_INDEX_FIELD_NUMBER: _ClassVar[int]
    PARTITION_KEY_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    global_partition_id: int
    window_expr_index: int
    partition_key: _containers.RepeatedCompositeFieldContainer[_datafusion_common_pb2.ScalarValue]
    state: _containers.RepeatedCompositeFieldContainer[_datafusion_common_pb2.ScalarValue]
    def __init__(self, global_partition_id: _Optional[int] = ..., window_expr_index: _Optional[int] = ..., partition_key: _Optional[_Iterable[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]]] = ..., state: _Optional[_Iterable[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]]] = ...) -> None: ...

class RuntimeStatsReport(_message.Message):
    __slots__ = ("order_by", "partitions", "sketch")
    ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    SKETCH_FIELD_NUMBER: _ClassVar[int]
    order_by: _containers.RepeatedCompositeFieldContainer[_datafusion_pb2.PhysicalSortExprNode]
    partitions: _containers.RepeatedCompositeFieldContainer[RuntimeStatsPartitionEntry]
    sketch: SortKeySketchState
    def __init__(self, order_by: _Optional[_Iterable[_Union[_datafusion_pb2.PhysicalSortExprNode, _Mapping]]] = ..., partitions: _Optional[_Iterable[_Union[RuntimeStatsPartitionEntry, _Mapping]]] = ..., sketch: _Optional[_Union[SortKeySketchState, _Mapping]] = ...) -> None: ...

class RuntimeStatsPartitionEntry(_message.Message):
    __slots__ = ("partition_id", "row_count", "key_min", "key_max", "null_count")
    PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
    ROW_COUNT_FIELD_NUMBER: _ClassVar[int]
    KEY_MIN_FIELD_NUMBER: _ClassVar[int]
    KEY_MAX_FIELD_NUMBER: _ClassVar[int]
    NULL_COUNT_FIELD_NUMBER: _ClassVar[int]
    partition_id: int
    row_count: int
    key_min: _containers.RepeatedCompositeFieldContainer[_datafusion_common_pb2.ScalarValue]
    key_max: _containers.RepeatedCompositeFieldContainer[_datafusion_common_pb2.ScalarValue]
    null_count: int
    def __init__(self, partition_id: _Optional[int] = ..., row_count: _Optional[int] = ..., key_min: _Optional[_Iterable[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]]] = ..., key_max: _Optional[_Iterable[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]]] = ..., null_count: _Optional[int] = ...) -> None: ...

class ExecutionError(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class FetchPartitionError(_message.Message):
    __slots__ = ("executor_id", "map_stage_id", "map_partition_id")
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    MAP_STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    MAP_PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    map_stage_id: int
    map_partition_id: int
    def __init__(self, executor_id: _Optional[str] = ..., map_stage_id: _Optional[int] = ..., map_partition_id: _Optional[int] = ...) -> None: ...

class IOError(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ExecutorLost(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ResultLost(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class TaskKilled(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ShuffleWritePartition(_message.Message):
    __slots__ = ("partition_id", "num_batches", "num_rows", "num_bytes", "file_id", "is_sort_shuffle")
    PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
    NUM_BATCHES_FIELD_NUMBER: _ClassVar[int]
    NUM_ROWS_FIELD_NUMBER: _ClassVar[int]
    NUM_BYTES_FIELD_NUMBER: _ClassVar[int]
    FILE_ID_FIELD_NUMBER: _ClassVar[int]
    IS_SORT_SHUFFLE_FIELD_NUMBER: _ClassVar[int]
    partition_id: int
    num_batches: int
    num_rows: int
    num_bytes: int
    file_id: int
    is_sort_shuffle: bool
    def __init__(self, partition_id: _Optional[int] = ..., num_batches: _Optional[int] = ..., num_rows: _Optional[int] = ..., num_bytes: _Optional[int] = ..., file_id: _Optional[int] = ..., is_sort_shuffle: _Optional[bool] = ...) -> None: ...

class TaskStatus(_message.Message):
    __slots__ = ("task_id", "job_id", "stage_id", "stage_attempt_num", "launch_time", "start_exec_time", "end_exec_time", "running", "failed", "successful", "metrics")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_ATTEMPT_NUM_FIELD_NUMBER: _ClassVar[int]
    LAUNCH_TIME_FIELD_NUMBER: _ClassVar[int]
    START_EXEC_TIME_FIELD_NUMBER: _ClassVar[int]
    END_EXEC_TIME_FIELD_NUMBER: _ClassVar[int]
    RUNNING_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    SUCCESSFUL_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    task_id: int
    job_id: str
    stage_id: int
    stage_attempt_num: int
    launch_time: int
    start_exec_time: int
    end_exec_time: int
    running: RunningTask
    failed: FailedTask
    successful: SuccessfulTask
    metrics: _containers.RepeatedCompositeFieldContainer[OperatorMetricsSet]
    def __init__(self, task_id: _Optional[int] = ..., job_id: _Optional[str] = ..., stage_id: _Optional[int] = ..., stage_attempt_num: _Optional[int] = ..., launch_time: _Optional[int] = ..., start_exec_time: _Optional[int] = ..., end_exec_time: _Optional[int] = ..., running: _Optional[_Union[RunningTask, _Mapping]] = ..., failed: _Optional[_Union[FailedTask, _Mapping]] = ..., successful: _Optional[_Union[SuccessfulTask, _Mapping]] = ..., metrics: _Optional[_Iterable[_Union[OperatorMetricsSet, _Mapping]]] = ...) -> None: ...

class PollWorkParams(_message.Message):
    __slots__ = ("metadata", "num_free_vcores", "task_status")
    METADATA_FIELD_NUMBER: _ClassVar[int]
    NUM_FREE_VCORES_FIELD_NUMBER: _ClassVar[int]
    TASK_STATUS_FIELD_NUMBER: _ClassVar[int]
    metadata: ExecutorRegistration
    num_free_vcores: int
    task_status: _containers.RepeatedCompositeFieldContainer[TaskStatus]
    def __init__(self, metadata: _Optional[_Union[ExecutorRegistration, _Mapping]] = ..., num_free_vcores: _Optional[int] = ..., task_status: _Optional[_Iterable[_Union[TaskStatus, _Mapping]]] = ...) -> None: ...

class TaskDefinition(_message.Message):
    __slots__ = ("task_id", "task_attempt_num", "job_id", "stage_id", "stage_attempt_num", "plan", "session_id", "launch_time", "props", "global_output_partition_ids", "vcores_consumed")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ATTEMPT_NUM_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_ATTEMPT_NUM_FIELD_NUMBER: _ClassVar[int]
    PLAN_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    LAUNCH_TIME_FIELD_NUMBER: _ClassVar[int]
    PROPS_FIELD_NUMBER: _ClassVar[int]
    GLOBAL_OUTPUT_PARTITION_IDS_FIELD_NUMBER: _ClassVar[int]
    VCORES_CONSUMED_FIELD_NUMBER: _ClassVar[int]
    task_id: int
    task_attempt_num: int
    job_id: str
    stage_id: int
    stage_attempt_num: int
    plan: bytes
    session_id: str
    launch_time: int
    props: _containers.RepeatedCompositeFieldContainer[KeyValuePair]
    global_output_partition_ids: _containers.RepeatedScalarFieldContainer[int]
    vcores_consumed: int
    def __init__(self, task_id: _Optional[int] = ..., task_attempt_num: _Optional[int] = ..., job_id: _Optional[str] = ..., stage_id: _Optional[int] = ..., stage_attempt_num: _Optional[int] = ..., plan: _Optional[bytes] = ..., session_id: _Optional[str] = ..., launch_time: _Optional[int] = ..., props: _Optional[_Iterable[_Union[KeyValuePair, _Mapping]]] = ..., global_output_partition_ids: _Optional[_Iterable[int]] = ..., vcores_consumed: _Optional[int] = ...) -> None: ...

class MultiTaskDefinition(_message.Message):
    __slots__ = ("task_ids", "job_id", "stage_id", "stage_attempt_num", "plan", "session_id", "launch_time", "props")
    TASK_IDS_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_ATTEMPT_NUM_FIELD_NUMBER: _ClassVar[int]
    PLAN_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    LAUNCH_TIME_FIELD_NUMBER: _ClassVar[int]
    PROPS_FIELD_NUMBER: _ClassVar[int]
    task_ids: _containers.RepeatedCompositeFieldContainer[TaskId]
    job_id: str
    stage_id: int
    stage_attempt_num: int
    plan: bytes
    session_id: str
    launch_time: int
    props: _containers.RepeatedCompositeFieldContainer[KeyValuePair]
    def __init__(self, task_ids: _Optional[_Iterable[_Union[TaskId, _Mapping]]] = ..., job_id: _Optional[str] = ..., stage_id: _Optional[int] = ..., stage_attempt_num: _Optional[int] = ..., plan: _Optional[bytes] = ..., session_id: _Optional[str] = ..., launch_time: _Optional[int] = ..., props: _Optional[_Iterable[_Union[KeyValuePair, _Mapping]]] = ...) -> None: ...

class JobSessionConfig(_message.Message):
    __slots__ = ("session_id", "configs")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    CONFIGS_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    configs: _containers.RepeatedCompositeFieldContainer[KeyValuePair]
    def __init__(self, session_id: _Optional[str] = ..., configs: _Optional[_Iterable[_Union[KeyValuePair, _Mapping]]] = ...) -> None: ...

class PollWorkResult(_message.Message):
    __slots__ = ("tasks", "jobs_to_clean")
    TASKS_FIELD_NUMBER: _ClassVar[int]
    JOBS_TO_CLEAN_FIELD_NUMBER: _ClassVar[int]
    tasks: _containers.RepeatedCompositeFieldContainer[TaskDefinition]
    jobs_to_clean: _containers.RepeatedCompositeFieldContainer[CleanJobDataParams]
    def __init__(self, tasks: _Optional[_Iterable[_Union[TaskDefinition, _Mapping]]] = ..., jobs_to_clean: _Optional[_Iterable[_Union[CleanJobDataParams, _Mapping]]] = ...) -> None: ...

class RegisterExecutorParams(_message.Message):
    __slots__ = ("metadata",)
    METADATA_FIELD_NUMBER: _ClassVar[int]
    metadata: ExecutorRegistration
    def __init__(self, metadata: _Optional[_Union[ExecutorRegistration, _Mapping]] = ...) -> None: ...

class RegisterExecutorResult(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: _Optional[bool] = ...) -> None: ...

class HeartBeatParams(_message.Message):
    __slots__ = ("executor_id", "metrics", "status", "metadata")
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    metrics: _containers.RepeatedCompositeFieldContainer[ExecutorMetric]
    status: ExecutorStatus
    metadata: ExecutorRegistration
    def __init__(self, executor_id: _Optional[str] = ..., metrics: _Optional[_Iterable[_Union[ExecutorMetric, _Mapping]]] = ..., status: _Optional[_Union[ExecutorStatus, _Mapping]] = ..., metadata: _Optional[_Union[ExecutorRegistration, _Mapping]] = ...) -> None: ...

class HeartBeatResult(_message.Message):
    __slots__ = ("reregister",)
    REREGISTER_FIELD_NUMBER: _ClassVar[int]
    reregister: bool
    def __init__(self, reregister: _Optional[bool] = ...) -> None: ...

class StopExecutorParams(_message.Message):
    __slots__ = ("executor_id", "reason", "force")
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    FORCE_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    reason: str
    force: bool
    def __init__(self, executor_id: _Optional[str] = ..., reason: _Optional[str] = ..., force: _Optional[bool] = ...) -> None: ...

class StopExecutorResult(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ExecutorStoppedParams(_message.Message):
    __slots__ = ("executor_id", "reason")
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    reason: str
    def __init__(self, executor_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class ExecutorStoppedResult(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class UpdateTaskStatusParams(_message.Message):
    __slots__ = ("executor_id", "task_status")
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_STATUS_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    task_status: _containers.RepeatedCompositeFieldContainer[TaskStatus]
    def __init__(self, executor_id: _Optional[str] = ..., task_status: _Optional[_Iterable[_Union[TaskStatus, _Mapping]]] = ...) -> None: ...

class UpdateTaskStatusResult(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: _Optional[bool] = ...) -> None: ...

class ExecuteQueryParams(_message.Message):
    __slots__ = ("logical_plan", "substrait_plan", "physical_plan", "session_id", "settings", "operation_id")
    LOGICAL_PLAN_FIELD_NUMBER: _ClassVar[int]
    SUBSTRAIT_PLAN_FIELD_NUMBER: _ClassVar[int]
    PHYSICAL_PLAN_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    SETTINGS_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    logical_plan: bytes
    substrait_plan: bytes
    physical_plan: bytes
    session_id: str
    settings: _containers.RepeatedCompositeFieldContainer[KeyValuePair]
    operation_id: str
    def __init__(self, logical_plan: _Optional[bytes] = ..., substrait_plan: _Optional[bytes] = ..., physical_plan: _Optional[bytes] = ..., session_id: _Optional[str] = ..., settings: _Optional[_Iterable[_Union[KeyValuePair, _Mapping]]] = ..., operation_id: _Optional[str] = ...) -> None: ...

class CreateUpdateSessionParams(_message.Message):
    __slots__ = ("session_id", "settings")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    SETTINGS_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    settings: _containers.RepeatedCompositeFieldContainer[KeyValuePair]
    def __init__(self, session_id: _Optional[str] = ..., settings: _Optional[_Iterable[_Union[KeyValuePair, _Mapping]]] = ...) -> None: ...

class CreateUpdateSessionResult(_message.Message):
    __slots__ = ("session_id",)
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    def __init__(self, session_id: _Optional[str] = ...) -> None: ...

class UpdateSessionParams(_message.Message):
    __slots__ = ("session_id", "settings")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    SETTINGS_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    settings: _containers.RepeatedCompositeFieldContainer[KeyValuePair]
    def __init__(self, session_id: _Optional[str] = ..., settings: _Optional[_Iterable[_Union[KeyValuePair, _Mapping]]] = ...) -> None: ...

class UpdateSessionResult(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: _Optional[bool] = ...) -> None: ...

class RemoveSessionParams(_message.Message):
    __slots__ = ("session_id",)
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    def __init__(self, session_id: _Optional[str] = ...) -> None: ...

class RemoveSessionResult(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: _Optional[bool] = ...) -> None: ...

class ExecuteSqlParams(_message.Message):
    __slots__ = ("sql",)
    SQL_FIELD_NUMBER: _ClassVar[int]
    sql: str
    def __init__(self, sql: _Optional[str] = ...) -> None: ...

class ExecuteQueryResult(_message.Message):
    __slots__ = ("success", "failure", "operation_id")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    success: ExecuteQuerySuccessResult
    failure: ExecuteQueryFailureResult
    operation_id: str
    def __init__(self, success: _Optional[_Union[ExecuteQuerySuccessResult, _Mapping]] = ..., failure: _Optional[_Union[ExecuteQueryFailureResult, _Mapping]] = ..., operation_id: _Optional[str] = ...) -> None: ...

class ExecuteQuerySuccessResult(_message.Message):
    __slots__ = ("job_id", "session_id")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    session_id: str
    def __init__(self, job_id: _Optional[str] = ..., session_id: _Optional[str] = ...) -> None: ...

class ExecuteQueryFailureResult(_message.Message):
    __slots__ = ("session_not_found", "plan_parsing_failure", "sql_parsing_failure")
    SESSION_NOT_FOUND_FIELD_NUMBER: _ClassVar[int]
    PLAN_PARSING_FAILURE_FIELD_NUMBER: _ClassVar[int]
    SQL_PARSING_FAILURE_FIELD_NUMBER: _ClassVar[int]
    session_not_found: str
    plan_parsing_failure: str
    sql_parsing_failure: str
    def __init__(self, session_not_found: _Optional[str] = ..., plan_parsing_failure: _Optional[str] = ..., sql_parsing_failure: _Optional[str] = ...) -> None: ...

class GetJobStatusParams(_message.Message):
    __slots__ = ("job_id",)
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    def __init__(self, job_id: _Optional[str] = ...) -> None: ...

class GetJobMetricsParams(_message.Message):
    __slots__ = ("job_id",)
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    def __init__(self, job_id: _Optional[str] = ...) -> None: ...

class JobStageMetrics(_message.Message):
    __slots__ = ("stage_id", "partitions", "operators")
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    OPERATORS_FIELD_NUMBER: _ClassVar[int]
    stage_id: int
    partitions: int
    operators: _containers.RepeatedCompositeFieldContainer[OperatorWithMetrics]
    def __init__(self, stage_id: _Optional[int] = ..., partitions: _Optional[int] = ..., operators: _Optional[_Iterable[_Union[OperatorWithMetrics, _Mapping]]] = ...) -> None: ...

class OperatorWithMetrics(_message.Message):
    __slots__ = ("depth", "operator_type", "operator_desc", "metrics")
    DEPTH_FIELD_NUMBER: _ClassVar[int]
    OPERATOR_TYPE_FIELD_NUMBER: _ClassVar[int]
    OPERATOR_DESC_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    depth: int
    operator_type: str
    operator_desc: str
    metrics: _containers.RepeatedCompositeFieldContainer[OperatorMetric]
    def __init__(self, depth: _Optional[int] = ..., operator_type: _Optional[str] = ..., operator_desc: _Optional[str] = ..., metrics: _Optional[_Iterable[_Union[OperatorMetric, _Mapping]]] = ...) -> None: ...

class GetJobMetricsResult(_message.Message):
    __slots__ = ("stages",)
    STAGES_FIELD_NUMBER: _ClassVar[int]
    stages: _containers.RepeatedCompositeFieldContainer[JobStageMetrics]
    def __init__(self, stages: _Optional[_Iterable[_Union[JobStageMetrics, _Mapping]]] = ...) -> None: ...

class SuccessfulJob(_message.Message):
    __slots__ = ("partition_location", "queued_at", "started_at", "ended_at", "pipelined_output_locations")
    PARTITION_LOCATION_FIELD_NUMBER: _ClassVar[int]
    QUEUED_AT_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    ENDED_AT_FIELD_NUMBER: _ClassVar[int]
    PIPELINED_OUTPUT_LOCATIONS_FIELD_NUMBER: _ClassVar[int]
    partition_location: _containers.RepeatedCompositeFieldContainer[PartitionLocation]
    queued_at: int
    started_at: int
    ended_at: int
    pipelined_output_locations: _containers.RepeatedCompositeFieldContainer[PipelinedPartitionLocation]
    def __init__(self, partition_location: _Optional[_Iterable[_Union[PartitionLocation, _Mapping]]] = ..., queued_at: _Optional[int] = ..., started_at: _Optional[int] = ..., ended_at: _Optional[int] = ..., pipelined_output_locations: _Optional[_Iterable[_Union[PipelinedPartitionLocation, _Mapping]]] = ...) -> None: ...

class QueuedJob(_message.Message):
    __slots__ = ("queued_at",)
    QUEUED_AT_FIELD_NUMBER: _ClassVar[int]
    queued_at: int
    def __init__(self, queued_at: _Optional[int] = ...) -> None: ...

class RunningJob(_message.Message):
    __slots__ = ("queued_at", "started_at", "scheduler", "pipelined_output_locations")
    QUEUED_AT_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    SCHEDULER_FIELD_NUMBER: _ClassVar[int]
    PIPELINED_OUTPUT_LOCATIONS_FIELD_NUMBER: _ClassVar[int]
    queued_at: int
    started_at: int
    scheduler: str
    pipelined_output_locations: _containers.RepeatedCompositeFieldContainer[PipelinedPartitionLocation]
    def __init__(self, queued_at: _Optional[int] = ..., started_at: _Optional[int] = ..., scheduler: _Optional[str] = ..., pipelined_output_locations: _Optional[_Iterable[_Union[PipelinedPartitionLocation, _Mapping]]] = ...) -> None: ...

class FailedJob(_message.Message):
    __slots__ = ("error", "queued_at", "started_at", "ended_at")
    ERROR_FIELD_NUMBER: _ClassVar[int]
    QUEUED_AT_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    ENDED_AT_FIELD_NUMBER: _ClassVar[int]
    error: str
    queued_at: int
    started_at: int
    ended_at: int
    def __init__(self, error: _Optional[str] = ..., queued_at: _Optional[int] = ..., started_at: _Optional[int] = ..., ended_at: _Optional[int] = ...) -> None: ...

class JobStatus(_message.Message):
    __slots__ = ("job_id", "job_name", "queued", "running", "failed", "successful")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    JOB_NAME_FIELD_NUMBER: _ClassVar[int]
    QUEUED_FIELD_NUMBER: _ClassVar[int]
    RUNNING_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    SUCCESSFUL_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    job_name: str
    queued: QueuedJob
    running: RunningJob
    failed: FailedJob
    successful: SuccessfulJob
    def __init__(self, job_id: _Optional[str] = ..., job_name: _Optional[str] = ..., queued: _Optional[_Union[QueuedJob, _Mapping]] = ..., running: _Optional[_Union[RunningJob, _Mapping]] = ..., failed: _Optional[_Union[FailedJob, _Mapping]] = ..., successful: _Optional[_Union[SuccessfulJob, _Mapping]] = ...) -> None: ...

class GetJobStatusResult(_message.Message):
    __slots__ = ("status", "local", "external")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    LOCAL_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_FIELD_NUMBER: _ClassVar[int]
    status: JobStatus
    local: bool
    external: str
    def __init__(self, status: _Optional[_Union[JobStatus, _Mapping]] = ..., local: _Optional[bool] = ..., external: _Optional[str] = ...) -> None: ...

class FilePartitionMetadata(_message.Message):
    __slots__ = ("filename",)
    FILENAME_FIELD_NUMBER: _ClassVar[int]
    filename: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, filename: _Optional[_Iterable[str]] = ...) -> None: ...

class CancelJobParams(_message.Message):
    __slots__ = ("job_id",)
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    def __init__(self, job_id: _Optional[str] = ...) -> None: ...

class CancelJobResult(_message.Message):
    __slots__ = ("cancelled",)
    CANCELLED_FIELD_NUMBER: _ClassVar[int]
    cancelled: bool
    def __init__(self, cancelled: _Optional[bool] = ...) -> None: ...

class CleanJobDataParams(_message.Message):
    __slots__ = ("job_id", "remove_stage_ids")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    REMOVE_STAGE_IDS_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    remove_stage_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, job_id: _Optional[str] = ..., remove_stage_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class CleanJobDataResult(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class LaunchTaskParams(_message.Message):
    __slots__ = ("tasks", "scheduler_endpoint")
    TASKS_FIELD_NUMBER: _ClassVar[int]
    SCHEDULER_ENDPOINT_FIELD_NUMBER: _ClassVar[int]
    tasks: _containers.RepeatedCompositeFieldContainer[TaskDefinition]
    scheduler_endpoint: str
    def __init__(self, tasks: _Optional[_Iterable[_Union[TaskDefinition, _Mapping]]] = ..., scheduler_endpoint: _Optional[str] = ...) -> None: ...

class LaunchMultiTaskParams(_message.Message):
    __slots__ = ("multi_tasks", "scheduler_endpoint")
    MULTI_TASKS_FIELD_NUMBER: _ClassVar[int]
    SCHEDULER_ENDPOINT_FIELD_NUMBER: _ClassVar[int]
    multi_tasks: _containers.RepeatedCompositeFieldContainer[MultiTaskDefinition]
    scheduler_endpoint: str
    def __init__(self, multi_tasks: _Optional[_Iterable[_Union[MultiTaskDefinition, _Mapping]]] = ..., scheduler_endpoint: _Optional[str] = ...) -> None: ...

class LaunchTaskResult(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    def __init__(self, success: _Optional[bool] = ...) -> None: ...

class LaunchMultiTaskResult(_message.Message):
    __slots__ = ("failed_jobs",)
    FAILED_JOBS_FIELD_NUMBER: _ClassVar[int]
    failed_jobs: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, failed_jobs: _Optional[_Iterable[str]] = ...) -> None: ...

class CancelTasksParams(_message.Message):
    __slots__ = ("task_infos",)
    TASK_INFOS_FIELD_NUMBER: _ClassVar[int]
    task_infos: _containers.RepeatedCompositeFieldContainer[RunningTaskInfo]
    def __init__(self, task_infos: _Optional[_Iterable[_Union[RunningTaskInfo, _Mapping]]] = ...) -> None: ...

class CancelTasksResult(_message.Message):
    __slots__ = ("cancelled",)
    CANCELLED_FIELD_NUMBER: _ClassVar[int]
    cancelled: bool
    def __init__(self, cancelled: _Optional[bool] = ...) -> None: ...

class RemoveJobDataParams(_message.Message):
    __slots__ = ("job_id", "remove_stage_ids")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    REMOVE_STAGE_IDS_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    remove_stage_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, job_id: _Optional[str] = ..., remove_stage_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class RemoveJobDataResult(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RunningTaskInfo(_message.Message):
    __slots__ = ("task_id", "job_id", "stage_id")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_ID_FIELD_NUMBER: _ClassVar[int]
    task_id: int
    job_id: str
    stage_id: int
    def __init__(self, task_id: _Optional[int] = ..., job_id: _Optional[str] = ..., stage_id: _Optional[int] = ...) -> None: ...
