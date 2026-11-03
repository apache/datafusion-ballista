from . import datafusion_common_pb2 as _datafusion_common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class FileFormatKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FILE_FORMAT_KIND_UNSPECIFIED: _ClassVar[FileFormatKind]
    FILE_FORMAT_KIND_CSV: _ClassVar[FileFormatKind]
    FILE_FORMAT_KIND_JSON: _ClassVar[FileFormatKind]
    FILE_FORMAT_KIND_PARQUET: _ClassVar[FileFormatKind]
    FILE_FORMAT_KIND_ARROW: _ClassVar[FileFormatKind]
    FILE_FORMAT_KIND_AVRO: _ClassVar[FileFormatKind]

class WindowFrameUnits(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ROWS: _ClassVar[WindowFrameUnits]
    RANGE: _ClassVar[WindowFrameUnits]
    GROUPS: _ClassVar[WindowFrameUnits]

class WindowFrameBoundType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CURRENT_ROW: _ClassVar[WindowFrameBoundType]
    PRECEDING: _ClassVar[WindowFrameBoundType]
    FOLLOWING: _ClassVar[WindowFrameBoundType]

class NullTreatment(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RESPECT_NULLS: _ClassVar[NullTreatment]
    IGNORE_NULLS: _ClassVar[NullTreatment]

class DateUnit(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    Day: _ClassVar[DateUnit]
    DateMillisecond: _ClassVar[DateUnit]

class FileOutputMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FILE_OUTPUT_MODE_AUTOMATIC: _ClassVar[FileOutputMode]
    FILE_OUTPUT_MODE_SINGLE_FILE: _ClassVar[FileOutputMode]
    FILE_OUTPUT_MODE_DIRECTORY: _ClassVar[FileOutputMode]

class InsertOp(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    Append: _ClassVar[InsertOp]
    Overwrite: _ClassVar[InsertOp]
    Replace: _ClassVar[InsertOp]

class PartitionMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COLLECT_LEFT: _ClassVar[PartitionMode]
    PARTITIONED: _ClassVar[PartitionMode]
    AUTO: _ClassVar[PartitionMode]

class StreamPartitionMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SINGLE_PARTITION: _ClassVar[StreamPartitionMode]
    PARTITIONED_EXEC: _ClassVar[StreamPartitionMode]

class AggregateMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PARTIAL: _ClassVar[AggregateMode]
    FINAL: _ClassVar[AggregateMode]
    FINAL_PARTITIONED: _ClassVar[AggregateMode]
    SINGLE: _ClassVar[AggregateMode]
    SINGLE_PARTITIONED: _ClassVar[AggregateMode]
    PARTIAL_REDUCE: _ClassVar[AggregateMode]

class GenerateSeriesName(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    GS_GENERATE_SERIES: _ClassVar[GenerateSeriesName]
    GS_RANGE: _ClassVar[GenerateSeriesName]
FILE_FORMAT_KIND_UNSPECIFIED: FileFormatKind
FILE_FORMAT_KIND_CSV: FileFormatKind
FILE_FORMAT_KIND_JSON: FileFormatKind
FILE_FORMAT_KIND_PARQUET: FileFormatKind
FILE_FORMAT_KIND_ARROW: FileFormatKind
FILE_FORMAT_KIND_AVRO: FileFormatKind
ROWS: WindowFrameUnits
RANGE: WindowFrameUnits
GROUPS: WindowFrameUnits
CURRENT_ROW: WindowFrameBoundType
PRECEDING: WindowFrameBoundType
FOLLOWING: WindowFrameBoundType
RESPECT_NULLS: NullTreatment
IGNORE_NULLS: NullTreatment
Day: DateUnit
DateMillisecond: DateUnit
FILE_OUTPUT_MODE_AUTOMATIC: FileOutputMode
FILE_OUTPUT_MODE_SINGLE_FILE: FileOutputMode
FILE_OUTPUT_MODE_DIRECTORY: FileOutputMode
Append: InsertOp
Overwrite: InsertOp
Replace: InsertOp
COLLECT_LEFT: PartitionMode
PARTITIONED: PartitionMode
AUTO: PartitionMode
SINGLE_PARTITION: StreamPartitionMode
PARTITIONED_EXEC: StreamPartitionMode
PARTIAL: AggregateMode
FINAL: AggregateMode
FINAL_PARTITIONED: AggregateMode
SINGLE: AggregateMode
SINGLE_PARTITIONED: AggregateMode
PARTIAL_REDUCE: AggregateMode
GS_GENERATE_SERIES: GenerateSeriesName
GS_RANGE: GenerateSeriesName

class LogicalPlanNode(_message.Message):
    __slots__ = ("listing_scan", "projection", "selection", "limit", "aggregate", "join", "sort", "repartition", "empty_relation", "create_external_table", "explain", "window", "analyze", "cross_join", "values", "extension", "create_catalog_schema", "union", "create_catalog", "subquery_alias", "create_view", "distinct", "view_scan", "custom_scan", "prepare", "drop_view", "distinct_on", "copy_to", "unnest", "recursive_query", "cte_work_table_scan", "dml", "empty_table_scan")
    LISTING_SCAN_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_FIELD_NUMBER: _ClassVar[int]
    SELECTION_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    AGGREGATE_FIELD_NUMBER: _ClassVar[int]
    JOIN_FIELD_NUMBER: _ClassVar[int]
    SORT_FIELD_NUMBER: _ClassVar[int]
    REPARTITION_FIELD_NUMBER: _ClassVar[int]
    EMPTY_RELATION_FIELD_NUMBER: _ClassVar[int]
    CREATE_EXTERNAL_TABLE_FIELD_NUMBER: _ClassVar[int]
    EXPLAIN_FIELD_NUMBER: _ClassVar[int]
    WINDOW_FIELD_NUMBER: _ClassVar[int]
    ANALYZE_FIELD_NUMBER: _ClassVar[int]
    CROSS_JOIN_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    EXTENSION_FIELD_NUMBER: _ClassVar[int]
    CREATE_CATALOG_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    UNION_FIELD_NUMBER: _ClassVar[int]
    CREATE_CATALOG_FIELD_NUMBER: _ClassVar[int]
    SUBQUERY_ALIAS_FIELD_NUMBER: _ClassVar[int]
    CREATE_VIEW_FIELD_NUMBER: _ClassVar[int]
    DISTINCT_FIELD_NUMBER: _ClassVar[int]
    VIEW_SCAN_FIELD_NUMBER: _ClassVar[int]
    CUSTOM_SCAN_FIELD_NUMBER: _ClassVar[int]
    PREPARE_FIELD_NUMBER: _ClassVar[int]
    DROP_VIEW_FIELD_NUMBER: _ClassVar[int]
    DISTINCT_ON_FIELD_NUMBER: _ClassVar[int]
    COPY_TO_FIELD_NUMBER: _ClassVar[int]
    UNNEST_FIELD_NUMBER: _ClassVar[int]
    RECURSIVE_QUERY_FIELD_NUMBER: _ClassVar[int]
    CTE_WORK_TABLE_SCAN_FIELD_NUMBER: _ClassVar[int]
    DML_FIELD_NUMBER: _ClassVar[int]
    EMPTY_TABLE_SCAN_FIELD_NUMBER: _ClassVar[int]
    listing_scan: ListingTableScanNode
    projection: ProjectionNode
    selection: SelectionNode
    limit: LimitNode
    aggregate: AggregateNode
    join: JoinNode
    sort: SortNode
    repartition: RepartitionNode
    empty_relation: EmptyRelationNode
    create_external_table: CreateExternalTableNode
    explain: ExplainNode
    window: WindowNode
    analyze: AnalyzeNode
    cross_join: CrossJoinNode
    values: ValuesNode
    extension: LogicalExtensionNode
    create_catalog_schema: CreateCatalogSchemaNode
    union: UnionNode
    create_catalog: CreateCatalogNode
    subquery_alias: SubqueryAliasNode
    create_view: CreateViewNode
    distinct: DistinctNode
    view_scan: ViewTableScanNode
    custom_scan: CustomTableScanNode
    prepare: PrepareNode
    drop_view: DropViewNode
    distinct_on: DistinctOnNode
    copy_to: CopyToNode
    unnest: UnnestNode
    recursive_query: RecursiveQueryNode
    cte_work_table_scan: CteWorkTableScanNode
    dml: DmlNode
    empty_table_scan: EmptyTableScanNode
    def __init__(self, listing_scan: _Optional[_Union[ListingTableScanNode, _Mapping]] = ..., projection: _Optional[_Union[ProjectionNode, _Mapping]] = ..., selection: _Optional[_Union[SelectionNode, _Mapping]] = ..., limit: _Optional[_Union[LimitNode, _Mapping]] = ..., aggregate: _Optional[_Union[AggregateNode, _Mapping]] = ..., join: _Optional[_Union[JoinNode, _Mapping]] = ..., sort: _Optional[_Union[SortNode, _Mapping]] = ..., repartition: _Optional[_Union[RepartitionNode, _Mapping]] = ..., empty_relation: _Optional[_Union[EmptyRelationNode, _Mapping]] = ..., create_external_table: _Optional[_Union[CreateExternalTableNode, _Mapping]] = ..., explain: _Optional[_Union[ExplainNode, _Mapping]] = ..., window: _Optional[_Union[WindowNode, _Mapping]] = ..., analyze: _Optional[_Union[AnalyzeNode, _Mapping]] = ..., cross_join: _Optional[_Union[CrossJoinNode, _Mapping]] = ..., values: _Optional[_Union[ValuesNode, _Mapping]] = ..., extension: _Optional[_Union[LogicalExtensionNode, _Mapping]] = ..., create_catalog_schema: _Optional[_Union[CreateCatalogSchemaNode, _Mapping]] = ..., union: _Optional[_Union[UnionNode, _Mapping]] = ..., create_catalog: _Optional[_Union[CreateCatalogNode, _Mapping]] = ..., subquery_alias: _Optional[_Union[SubqueryAliasNode, _Mapping]] = ..., create_view: _Optional[_Union[CreateViewNode, _Mapping]] = ..., distinct: _Optional[_Union[DistinctNode, _Mapping]] = ..., view_scan: _Optional[_Union[ViewTableScanNode, _Mapping]] = ..., custom_scan: _Optional[_Union[CustomTableScanNode, _Mapping]] = ..., prepare: _Optional[_Union[PrepareNode, _Mapping]] = ..., drop_view: _Optional[_Union[DropViewNode, _Mapping]] = ..., distinct_on: _Optional[_Union[DistinctOnNode, _Mapping]] = ..., copy_to: _Optional[_Union[CopyToNode, _Mapping]] = ..., unnest: _Optional[_Union[UnnestNode, _Mapping]] = ..., recursive_query: _Optional[_Union[RecursiveQueryNode, _Mapping]] = ..., cte_work_table_scan: _Optional[_Union[CteWorkTableScanNode, _Mapping]] = ..., dml: _Optional[_Union[DmlNode, _Mapping]] = ..., empty_table_scan: _Optional[_Union[EmptyTableScanNode, _Mapping]] = ...) -> None: ...

class LogicalExtensionNode(_message.Message):
    __slots__ = ("node", "inputs")
    NODE_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    node: bytes
    inputs: _containers.RepeatedCompositeFieldContainer[LogicalPlanNode]
    def __init__(self, node: _Optional[bytes] = ..., inputs: _Optional[_Iterable[_Union[LogicalPlanNode, _Mapping]]] = ...) -> None: ...

class ProjectionColumns(_message.Message):
    __slots__ = ("columns",)
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, columns: _Optional[_Iterable[str]] = ...) -> None: ...

class LogicalExprNodeCollection(_message.Message):
    __slots__ = ("logical_expr_nodes",)
    LOGICAL_EXPR_NODES_FIELD_NUMBER: _ClassVar[int]
    logical_expr_nodes: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    def __init__(self, logical_expr_nodes: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ...) -> None: ...

class SortExprNodeCollection(_message.Message):
    __slots__ = ("sort_expr_nodes",)
    SORT_EXPR_NODES_FIELD_NUMBER: _ClassVar[int]
    sort_expr_nodes: _containers.RepeatedCompositeFieldContainer[SortExprNode]
    def __init__(self, sort_expr_nodes: _Optional[_Iterable[_Union[SortExprNode, _Mapping]]] = ...) -> None: ...

class ListingTableScanNode(_message.Message):
    __slots__ = ("table_name", "paths", "file_extension", "projection", "schema", "filters", "table_partition_cols", "csv", "parquet", "avro", "json", "arrow", "file_sort_order")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    PATHS_FIELD_NUMBER: _ClassVar[int]
    FILE_EXTENSION_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    TABLE_PARTITION_COLS_FIELD_NUMBER: _ClassVar[int]
    CSV_FIELD_NUMBER: _ClassVar[int]
    PARQUET_FIELD_NUMBER: _ClassVar[int]
    AVRO_FIELD_NUMBER: _ClassVar[int]
    JSON_FIELD_NUMBER: _ClassVar[int]
    ARROW_FIELD_NUMBER: _ClassVar[int]
    FILE_SORT_ORDER_FIELD_NUMBER: _ClassVar[int]
    table_name: TableReference
    paths: _containers.RepeatedScalarFieldContainer[str]
    file_extension: str
    projection: ProjectionColumns
    schema: _datafusion_common_pb2.Schema
    filters: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    table_partition_cols: _containers.RepeatedCompositeFieldContainer[PartitionColumn]
    csv: _datafusion_common_pb2.CsvFormat
    parquet: _datafusion_common_pb2.ParquetFormat
    avro: _datafusion_common_pb2.AvroFormat
    json: _datafusion_common_pb2.NdJsonFormat
    arrow: _datafusion_common_pb2.ArrowFormat
    file_sort_order: _containers.RepeatedCompositeFieldContainer[SortExprNodeCollection]
    def __init__(self, table_name: _Optional[_Union[TableReference, _Mapping]] = ..., paths: _Optional[_Iterable[str]] = ..., file_extension: _Optional[str] = ..., projection: _Optional[_Union[ProjectionColumns, _Mapping]] = ..., schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., filters: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ..., table_partition_cols: _Optional[_Iterable[_Union[PartitionColumn, _Mapping]]] = ..., csv: _Optional[_Union[_datafusion_common_pb2.CsvFormat, _Mapping]] = ..., parquet: _Optional[_Union[_datafusion_common_pb2.ParquetFormat, _Mapping]] = ..., avro: _Optional[_Union[_datafusion_common_pb2.AvroFormat, _Mapping]] = ..., json: _Optional[_Union[_datafusion_common_pb2.NdJsonFormat, _Mapping]] = ..., arrow: _Optional[_Union[_datafusion_common_pb2.ArrowFormat, _Mapping]] = ..., file_sort_order: _Optional[_Iterable[_Union[SortExprNodeCollection, _Mapping]]] = ...) -> None: ...

class ViewTableScanNode(_message.Message):
    __slots__ = ("table_name", "input", "schema", "projection", "definition")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_FIELD_NUMBER: _ClassVar[int]
    DEFINITION_FIELD_NUMBER: _ClassVar[int]
    table_name: TableReference
    input: LogicalPlanNode
    schema: _datafusion_common_pb2.Schema
    projection: ProjectionColumns
    definition: str
    def __init__(self, table_name: _Optional[_Union[TableReference, _Mapping]] = ..., input: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., projection: _Optional[_Union[ProjectionColumns, _Mapping]] = ..., definition: _Optional[str] = ...) -> None: ...

class CustomTableScanNode(_message.Message):
    __slots__ = ("table_name", "projection", "schema", "filters", "custom_table_data")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    CUSTOM_TABLE_DATA_FIELD_NUMBER: _ClassVar[int]
    table_name: TableReference
    projection: ProjectionColumns
    schema: _datafusion_common_pb2.Schema
    filters: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    custom_table_data: bytes
    def __init__(self, table_name: _Optional[_Union[TableReference, _Mapping]] = ..., projection: _Optional[_Union[ProjectionColumns, _Mapping]] = ..., schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., filters: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ..., custom_table_data: _Optional[bytes] = ...) -> None: ...

class ProjectionNode(_message.Message):
    __slots__ = ("input", "expr", "alias")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    ALIAS_FIELD_NUMBER: _ClassVar[int]
    input: LogicalPlanNode
    expr: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    alias: str
    def __init__(self, input: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., expr: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ..., alias: _Optional[str] = ...) -> None: ...

class SelectionNode(_message.Message):
    __slots__ = ("input", "expr")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    input: LogicalPlanNode
    expr: LogicalExprNode
    def __init__(self, input: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class SortNode(_message.Message):
    __slots__ = ("input", "expr", "fetch")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    FETCH_FIELD_NUMBER: _ClassVar[int]
    input: LogicalPlanNode
    expr: _containers.RepeatedCompositeFieldContainer[SortExprNode]
    fetch: int
    def __init__(self, input: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., expr: _Optional[_Iterable[_Union[SortExprNode, _Mapping]]] = ..., fetch: _Optional[int] = ...) -> None: ...

class RepartitionNode(_message.Message):
    __slots__ = ("input", "round_robin", "hash", "range")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    ROUND_ROBIN_FIELD_NUMBER: _ClassVar[int]
    HASH_FIELD_NUMBER: _ClassVar[int]
    RANGE_FIELD_NUMBER: _ClassVar[int]
    input: LogicalPlanNode
    round_robin: int
    hash: HashRepartition
    range: RangeRepartition
    def __init__(self, input: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., round_robin: _Optional[int] = ..., hash: _Optional[_Union[HashRepartition, _Mapping]] = ..., range: _Optional[_Union[RangeRepartition, _Mapping]] = ...) -> None: ...

class RangeSplitPoint(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: _containers.RepeatedCompositeFieldContainer[_datafusion_common_pb2.ScalarValue]
    def __init__(self, value: _Optional[_Iterable[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]]] = ...) -> None: ...

class RangeRepartition(_message.Message):
    __slots__ = ("sort_expr", "split_point")
    SORT_EXPR_FIELD_NUMBER: _ClassVar[int]
    SPLIT_POINT_FIELD_NUMBER: _ClassVar[int]
    sort_expr: _containers.RepeatedCompositeFieldContainer[SortExprNode]
    split_point: _containers.RepeatedCompositeFieldContainer[RangeSplitPoint]
    def __init__(self, sort_expr: _Optional[_Iterable[_Union[SortExprNode, _Mapping]]] = ..., split_point: _Optional[_Iterable[_Union[RangeSplitPoint, _Mapping]]] = ...) -> None: ...

class HashRepartition(_message.Message):
    __slots__ = ("hash_expr", "partition_count")
    HASH_EXPR_FIELD_NUMBER: _ClassVar[int]
    PARTITION_COUNT_FIELD_NUMBER: _ClassVar[int]
    hash_expr: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    partition_count: int
    def __init__(self, hash_expr: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ..., partition_count: _Optional[int] = ...) -> None: ...

class EmptyRelationNode(_message.Message):
    __slots__ = ("produce_one_row",)
    PRODUCE_ONE_ROW_FIELD_NUMBER: _ClassVar[int]
    produce_one_row: bool
    def __init__(self, produce_one_row: _Optional[bool] = ...) -> None: ...

class CreateExternalTableNode(_message.Message):
    __slots__ = ("name", "location", "locations", "file_type", "schema", "table_partition_cols", "if_not_exists", "or_replace", "temporary", "definition", "order_exprs", "unbounded", "options", "constraints", "column_defaults")
    class OptionsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class ColumnDefaultsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: LogicalExprNode
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...
    NAME_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    LOCATIONS_FIELD_NUMBER: _ClassVar[int]
    FILE_TYPE_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    TABLE_PARTITION_COLS_FIELD_NUMBER: _ClassVar[int]
    IF_NOT_EXISTS_FIELD_NUMBER: _ClassVar[int]
    OR_REPLACE_FIELD_NUMBER: _ClassVar[int]
    TEMPORARY_FIELD_NUMBER: _ClassVar[int]
    DEFINITION_FIELD_NUMBER: _ClassVar[int]
    ORDER_EXPRS_FIELD_NUMBER: _ClassVar[int]
    UNBOUNDED_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    CONSTRAINTS_FIELD_NUMBER: _ClassVar[int]
    COLUMN_DEFAULTS_FIELD_NUMBER: _ClassVar[int]
    name: TableReference
    location: str
    locations: _containers.RepeatedScalarFieldContainer[str]
    file_type: str
    schema: _datafusion_common_pb2.DfSchema
    table_partition_cols: _containers.RepeatedScalarFieldContainer[str]
    if_not_exists: bool
    or_replace: bool
    temporary: bool
    definition: str
    order_exprs: _containers.RepeatedCompositeFieldContainer[SortExprNodeCollection]
    unbounded: bool
    options: _containers.ScalarMap[str, str]
    constraints: _datafusion_common_pb2.Constraints
    column_defaults: _containers.MessageMap[str, LogicalExprNode]
    def __init__(self, name: _Optional[_Union[TableReference, _Mapping]] = ..., location: _Optional[str] = ..., locations: _Optional[_Iterable[str]] = ..., file_type: _Optional[str] = ..., schema: _Optional[_Union[_datafusion_common_pb2.DfSchema, _Mapping]] = ..., table_partition_cols: _Optional[_Iterable[str]] = ..., if_not_exists: _Optional[bool] = ..., or_replace: _Optional[bool] = ..., temporary: _Optional[bool] = ..., definition: _Optional[str] = ..., order_exprs: _Optional[_Iterable[_Union[SortExprNodeCollection, _Mapping]]] = ..., unbounded: _Optional[bool] = ..., options: _Optional[_Mapping[str, str]] = ..., constraints: _Optional[_Union[_datafusion_common_pb2.Constraints, _Mapping]] = ..., column_defaults: _Optional[_Mapping[str, LogicalExprNode]] = ...) -> None: ...

class PrepareNode(_message.Message):
    __slots__ = ("name", "data_types", "input", "fields")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DATA_TYPES_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    name: str
    data_types: _containers.RepeatedCompositeFieldContainer[_datafusion_common_pb2.ArrowType]
    input: LogicalPlanNode
    fields: _containers.RepeatedCompositeFieldContainer[_datafusion_common_pb2.Field]
    def __init__(self, name: _Optional[str] = ..., data_types: _Optional[_Iterable[_Union[_datafusion_common_pb2.ArrowType, _Mapping]]] = ..., input: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., fields: _Optional[_Iterable[_Union[_datafusion_common_pb2.Field, _Mapping]]] = ...) -> None: ...

class CreateCatalogSchemaNode(_message.Message):
    __slots__ = ("schema_name", "if_not_exists", "schema")
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    IF_NOT_EXISTS_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    schema_name: str
    if_not_exists: bool
    schema: _datafusion_common_pb2.DfSchema
    def __init__(self, schema_name: _Optional[str] = ..., if_not_exists: _Optional[bool] = ..., schema: _Optional[_Union[_datafusion_common_pb2.DfSchema, _Mapping]] = ...) -> None: ...

class CreateCatalogNode(_message.Message):
    __slots__ = ("catalog_name", "if_not_exists", "schema")
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    IF_NOT_EXISTS_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    catalog_name: str
    if_not_exists: bool
    schema: _datafusion_common_pb2.DfSchema
    def __init__(self, catalog_name: _Optional[str] = ..., if_not_exists: _Optional[bool] = ..., schema: _Optional[_Union[_datafusion_common_pb2.DfSchema, _Mapping]] = ...) -> None: ...

class DropViewNode(_message.Message):
    __slots__ = ("name", "if_exists", "schema")
    NAME_FIELD_NUMBER: _ClassVar[int]
    IF_EXISTS_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    name: TableReference
    if_exists: bool
    schema: _datafusion_common_pb2.DfSchema
    def __init__(self, name: _Optional[_Union[TableReference, _Mapping]] = ..., if_exists: _Optional[bool] = ..., schema: _Optional[_Union[_datafusion_common_pb2.DfSchema, _Mapping]] = ...) -> None: ...

class CreateViewNode(_message.Message):
    __slots__ = ("name", "input", "or_replace", "temporary", "definition")
    NAME_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    OR_REPLACE_FIELD_NUMBER: _ClassVar[int]
    TEMPORARY_FIELD_NUMBER: _ClassVar[int]
    DEFINITION_FIELD_NUMBER: _ClassVar[int]
    name: TableReference
    input: LogicalPlanNode
    or_replace: bool
    temporary: bool
    definition: str
    def __init__(self, name: _Optional[_Union[TableReference, _Mapping]] = ..., input: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., or_replace: _Optional[bool] = ..., temporary: _Optional[bool] = ..., definition: _Optional[str] = ...) -> None: ...

class ValuesNode(_message.Message):
    __slots__ = ("n_cols", "values_list")
    N_COLS_FIELD_NUMBER: _ClassVar[int]
    VALUES_LIST_FIELD_NUMBER: _ClassVar[int]
    n_cols: int
    values_list: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    def __init__(self, n_cols: _Optional[int] = ..., values_list: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ...) -> None: ...

class AnalyzeNode(_message.Message):
    __slots__ = ("input", "verbose", "analyze_level", "analyze_categories", "format")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    VERBOSE_FIELD_NUMBER: _ClassVar[int]
    ANALYZE_LEVEL_FIELD_NUMBER: _ClassVar[int]
    ANALYZE_CATEGORIES_FIELD_NUMBER: _ClassVar[int]
    FORMAT_FIELD_NUMBER: _ClassVar[int]
    input: LogicalPlanNode
    verbose: bool
    analyze_level: _datafusion_common_pb2.MetricType
    analyze_categories: _datafusion_common_pb2.ExplainAnalyzeCategoriesNode
    format: _datafusion_common_pb2.ExplainFormat
    def __init__(self, input: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., verbose: _Optional[bool] = ..., analyze_level: _Optional[_Union[_datafusion_common_pb2.MetricType, str]] = ..., analyze_categories: _Optional[_Union[_datafusion_common_pb2.ExplainAnalyzeCategoriesNode, _Mapping]] = ..., format: _Optional[_Union[_datafusion_common_pb2.ExplainFormat, str]] = ...) -> None: ...

class ExplainNode(_message.Message):
    __slots__ = ("input", "verbose", "format", "show_statistics")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    VERBOSE_FIELD_NUMBER: _ClassVar[int]
    FORMAT_FIELD_NUMBER: _ClassVar[int]
    SHOW_STATISTICS_FIELD_NUMBER: _ClassVar[int]
    input: LogicalPlanNode
    verbose: bool
    format: _datafusion_common_pb2.ExplainFormat
    show_statistics: bool
    def __init__(self, input: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., verbose: _Optional[bool] = ..., format: _Optional[_Union[_datafusion_common_pb2.ExplainFormat, str]] = ..., show_statistics: _Optional[bool] = ...) -> None: ...

class AggregateNode(_message.Message):
    __slots__ = ("input", "group_expr", "aggr_expr")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    GROUP_EXPR_FIELD_NUMBER: _ClassVar[int]
    AGGR_EXPR_FIELD_NUMBER: _ClassVar[int]
    input: LogicalPlanNode
    group_expr: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    aggr_expr: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    def __init__(self, input: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., group_expr: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ..., aggr_expr: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ...) -> None: ...

class WindowNode(_message.Message):
    __slots__ = ("input", "window_expr")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    WINDOW_EXPR_FIELD_NUMBER: _ClassVar[int]
    input: LogicalPlanNode
    window_expr: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    def __init__(self, input: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., window_expr: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ...) -> None: ...

class JoinNode(_message.Message):
    __slots__ = ("left", "right", "join_type", "join_constraint", "left_join_key", "right_join_key", "null_equality", "filter", "null_aware")
    LEFT_FIELD_NUMBER: _ClassVar[int]
    RIGHT_FIELD_NUMBER: _ClassVar[int]
    JOIN_TYPE_FIELD_NUMBER: _ClassVar[int]
    JOIN_CONSTRAINT_FIELD_NUMBER: _ClassVar[int]
    LEFT_JOIN_KEY_FIELD_NUMBER: _ClassVar[int]
    RIGHT_JOIN_KEY_FIELD_NUMBER: _ClassVar[int]
    NULL_EQUALITY_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    NULL_AWARE_FIELD_NUMBER: _ClassVar[int]
    left: LogicalPlanNode
    right: LogicalPlanNode
    join_type: _datafusion_common_pb2.JoinType
    join_constraint: _datafusion_common_pb2.JoinConstraint
    left_join_key: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    right_join_key: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    null_equality: _datafusion_common_pb2.NullEquality
    filter: LogicalExprNode
    null_aware: bool
    def __init__(self, left: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., right: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., join_type: _Optional[_Union[_datafusion_common_pb2.JoinType, str]] = ..., join_constraint: _Optional[_Union[_datafusion_common_pb2.JoinConstraint, str]] = ..., left_join_key: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ..., right_join_key: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ..., null_equality: _Optional[_Union[_datafusion_common_pb2.NullEquality, str]] = ..., filter: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., null_aware: _Optional[bool] = ...) -> None: ...

class DistinctNode(_message.Message):
    __slots__ = ("input",)
    INPUT_FIELD_NUMBER: _ClassVar[int]
    input: LogicalPlanNode
    def __init__(self, input: _Optional[_Union[LogicalPlanNode, _Mapping]] = ...) -> None: ...

class DistinctOnNode(_message.Message):
    __slots__ = ("on_expr", "select_expr", "sort_expr", "input")
    ON_EXPR_FIELD_NUMBER: _ClassVar[int]
    SELECT_EXPR_FIELD_NUMBER: _ClassVar[int]
    SORT_EXPR_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    on_expr: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    select_expr: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    sort_expr: _containers.RepeatedCompositeFieldContainer[SortExprNode]
    input: LogicalPlanNode
    def __init__(self, on_expr: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ..., select_expr: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ..., sort_expr: _Optional[_Iterable[_Union[SortExprNode, _Mapping]]] = ..., input: _Optional[_Union[LogicalPlanNode, _Mapping]] = ...) -> None: ...

class CopyToNode(_message.Message):
    __slots__ = ("input", "output_url", "file_type", "partition_by")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_URL_FIELD_NUMBER: _ClassVar[int]
    FILE_TYPE_FIELD_NUMBER: _ClassVar[int]
    PARTITION_BY_FIELD_NUMBER: _ClassVar[int]
    input: LogicalPlanNode
    output_url: str
    file_type: bytes
    partition_by: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, input: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., output_url: _Optional[str] = ..., file_type: _Optional[bytes] = ..., partition_by: _Optional[_Iterable[str]] = ...) -> None: ...

class FileFormatProto(_message.Message):
    __slots__ = ("kind", "encoded_file_format")
    KIND_FIELD_NUMBER: _ClassVar[int]
    ENCODED_FILE_FORMAT_FIELD_NUMBER: _ClassVar[int]
    kind: FileFormatKind
    encoded_file_format: bytes
    def __init__(self, kind: _Optional[_Union[FileFormatKind, str]] = ..., encoded_file_format: _Optional[bytes] = ...) -> None: ...

class DmlNode(_message.Message):
    __slots__ = ("dml_type", "input", "table_name", "target", "merge_into")
    class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        UPDATE: _ClassVar[DmlNode.Type]
        DELETE: _ClassVar[DmlNode.Type]
        CTAS: _ClassVar[DmlNode.Type]
        INSERT_APPEND: _ClassVar[DmlNode.Type]
        INSERT_OVERWRITE: _ClassVar[DmlNode.Type]
        INSERT_REPLACE: _ClassVar[DmlNode.Type]
        TRUNCATE: _ClassVar[DmlNode.Type]
        MERGE_INTO: _ClassVar[DmlNode.Type]
    UPDATE: DmlNode.Type
    DELETE: DmlNode.Type
    CTAS: DmlNode.Type
    INSERT_APPEND: DmlNode.Type
    INSERT_OVERWRITE: DmlNode.Type
    INSERT_REPLACE: DmlNode.Type
    TRUNCATE: DmlNode.Type
    MERGE_INTO: DmlNode.Type
    DML_TYPE_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    MERGE_INTO_FIELD_NUMBER: _ClassVar[int]
    dml_type: DmlNode.Type
    input: LogicalPlanNode
    table_name: TableReference
    target: LogicalPlanNode
    merge_into: MergeIntoOpNode
    def __init__(self, dml_type: _Optional[_Union[DmlNode.Type, str]] = ..., input: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., table_name: _Optional[_Union[TableReference, _Mapping]] = ..., target: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., merge_into: _Optional[_Union[MergeIntoOpNode, _Mapping]] = ...) -> None: ...

class MergeIntoOpNode(_message.Message):
    __slots__ = ("on", "clauses")
    ON_FIELD_NUMBER: _ClassVar[int]
    CLAUSES_FIELD_NUMBER: _ClassVar[int]
    on: LogicalExprNode
    clauses: _containers.RepeatedCompositeFieldContainer[MergeIntoClauseNode]
    def __init__(self, on: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., clauses: _Optional[_Iterable[_Union[MergeIntoClauseNode, _Mapping]]] = ...) -> None: ...

class MergeIntoClauseNode(_message.Message):
    __slots__ = ("kind", "predicate", "action")
    class Kind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        MATCHED: _ClassVar[MergeIntoClauseNode.Kind]
        NOT_MATCHED: _ClassVar[MergeIntoClauseNode.Kind]
        NOT_MATCHED_BY_TARGET: _ClassVar[MergeIntoClauseNode.Kind]
        NOT_MATCHED_BY_SOURCE: _ClassVar[MergeIntoClauseNode.Kind]
    MATCHED: MergeIntoClauseNode.Kind
    NOT_MATCHED: MergeIntoClauseNode.Kind
    NOT_MATCHED_BY_TARGET: MergeIntoClauseNode.Kind
    NOT_MATCHED_BY_SOURCE: MergeIntoClauseNode.Kind
    KIND_FIELD_NUMBER: _ClassVar[int]
    PREDICATE_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    kind: MergeIntoClauseNode.Kind
    predicate: LogicalExprNode
    action: MergeIntoActionNode
    def __init__(self, kind: _Optional[_Union[MergeIntoClauseNode.Kind, str]] = ..., predicate: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., action: _Optional[_Union[MergeIntoActionNode, _Mapping]] = ...) -> None: ...

class MergeIntoActionNode(_message.Message):
    __slots__ = ("update", "insert", "delete")
    UPDATE_FIELD_NUMBER: _ClassVar[int]
    INSERT_FIELD_NUMBER: _ClassVar[int]
    DELETE_FIELD_NUMBER: _ClassVar[int]
    update: MergeUpdateAction
    insert: MergeInsertAction
    delete: MergeDeleteAction
    def __init__(self, update: _Optional[_Union[MergeUpdateAction, _Mapping]] = ..., insert: _Optional[_Union[MergeInsertAction, _Mapping]] = ..., delete: _Optional[_Union[MergeDeleteAction, _Mapping]] = ...) -> None: ...

class MergeUpdateAction(_message.Message):
    __slots__ = ("assignments",)
    ASSIGNMENTS_FIELD_NUMBER: _ClassVar[int]
    assignments: _containers.RepeatedCompositeFieldContainer[MergeAssignment]
    def __init__(self, assignments: _Optional[_Iterable[_Union[MergeAssignment, _Mapping]]] = ...) -> None: ...

class MergeAssignment(_message.Message):
    __slots__ = ("column", "value")
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    column: str
    value: LogicalExprNode
    def __init__(self, column: _Optional[str] = ..., value: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class MergeInsertAction(_message.Message):
    __slots__ = ("columns", "values")
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedScalarFieldContainer[str]
    values: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    def __init__(self, columns: _Optional[_Iterable[str]] = ..., values: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ...) -> None: ...

class MergeDeleteAction(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class UnnestNode(_message.Message):
    __slots__ = ("input", "exec_columns", "list_type_columns", "struct_type_columns", "dependency_indices", "schema", "options")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    EXEC_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    LIST_TYPE_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    STRUCT_TYPE_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    DEPENDENCY_INDICES_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    input: LogicalPlanNode
    exec_columns: _containers.RepeatedCompositeFieldContainer[_datafusion_common_pb2.Column]
    list_type_columns: _containers.RepeatedCompositeFieldContainer[ColumnUnnestListItem]
    struct_type_columns: _containers.RepeatedScalarFieldContainer[int]
    dependency_indices: _containers.RepeatedScalarFieldContainer[int]
    schema: _datafusion_common_pb2.DfSchema
    options: UnnestOptions
    def __init__(self, input: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., exec_columns: _Optional[_Iterable[_Union[_datafusion_common_pb2.Column, _Mapping]]] = ..., list_type_columns: _Optional[_Iterable[_Union[ColumnUnnestListItem, _Mapping]]] = ..., struct_type_columns: _Optional[_Iterable[int]] = ..., dependency_indices: _Optional[_Iterable[int]] = ..., schema: _Optional[_Union[_datafusion_common_pb2.DfSchema, _Mapping]] = ..., options: _Optional[_Union[UnnestOptions, _Mapping]] = ...) -> None: ...

class ColumnUnnestListItem(_message.Message):
    __slots__ = ("input_index", "recursion")
    INPUT_INDEX_FIELD_NUMBER: _ClassVar[int]
    RECURSION_FIELD_NUMBER: _ClassVar[int]
    input_index: int
    recursion: ColumnUnnestListRecursion
    def __init__(self, input_index: _Optional[int] = ..., recursion: _Optional[_Union[ColumnUnnestListRecursion, _Mapping]] = ...) -> None: ...

class ColumnUnnestListRecursions(_message.Message):
    __slots__ = ("recursions",)
    RECURSIONS_FIELD_NUMBER: _ClassVar[int]
    recursions: _containers.RepeatedCompositeFieldContainer[ColumnUnnestListRecursion]
    def __init__(self, recursions: _Optional[_Iterable[_Union[ColumnUnnestListRecursion, _Mapping]]] = ...) -> None: ...

class ColumnUnnestListRecursion(_message.Message):
    __slots__ = ("output_column", "depth")
    OUTPUT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    DEPTH_FIELD_NUMBER: _ClassVar[int]
    output_column: _datafusion_common_pb2.Column
    depth: int
    def __init__(self, output_column: _Optional[_Union[_datafusion_common_pb2.Column, _Mapping]] = ..., depth: _Optional[int] = ...) -> None: ...

class UnnestOptions(_message.Message):
    __slots__ = ("null_handling", "recursions")
    class NullHandling(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        PRESERVE: _ClassVar[UnnestOptions.NullHandling]
        DROP: _ClassVar[UnnestOptions.NullHandling]
        PRESERVE_AND_EXPAND_EMPTY: _ClassVar[UnnestOptions.NullHandling]
    PRESERVE: UnnestOptions.NullHandling
    DROP: UnnestOptions.NullHandling
    PRESERVE_AND_EXPAND_EMPTY: UnnestOptions.NullHandling
    NULL_HANDLING_FIELD_NUMBER: _ClassVar[int]
    RECURSIONS_FIELD_NUMBER: _ClassVar[int]
    null_handling: UnnestOptions.NullHandling
    recursions: _containers.RepeatedCompositeFieldContainer[RecursionUnnestOption]
    def __init__(self, null_handling: _Optional[_Union[UnnestOptions.NullHandling, str]] = ..., recursions: _Optional[_Iterable[_Union[RecursionUnnestOption, _Mapping]]] = ...) -> None: ...

class RecursionUnnestOption(_message.Message):
    __slots__ = ("output_column", "input_column", "depth")
    OUTPUT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    INPUT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    DEPTH_FIELD_NUMBER: _ClassVar[int]
    output_column: _datafusion_common_pb2.Column
    input_column: _datafusion_common_pb2.Column
    depth: int
    def __init__(self, output_column: _Optional[_Union[_datafusion_common_pb2.Column, _Mapping]] = ..., input_column: _Optional[_Union[_datafusion_common_pb2.Column, _Mapping]] = ..., depth: _Optional[int] = ...) -> None: ...

class UnionNode(_message.Message):
    __slots__ = ("inputs",)
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    inputs: _containers.RepeatedCompositeFieldContainer[LogicalPlanNode]
    def __init__(self, inputs: _Optional[_Iterable[_Union[LogicalPlanNode, _Mapping]]] = ...) -> None: ...

class CrossJoinNode(_message.Message):
    __slots__ = ("left", "right")
    LEFT_FIELD_NUMBER: _ClassVar[int]
    RIGHT_FIELD_NUMBER: _ClassVar[int]
    left: LogicalPlanNode
    right: LogicalPlanNode
    def __init__(self, left: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., right: _Optional[_Union[LogicalPlanNode, _Mapping]] = ...) -> None: ...

class LimitNode(_message.Message):
    __slots__ = ("input", "skip", "fetch")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    SKIP_FIELD_NUMBER: _ClassVar[int]
    FETCH_FIELD_NUMBER: _ClassVar[int]
    input: LogicalPlanNode
    skip: int
    fetch: int
    def __init__(self, input: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., skip: _Optional[int] = ..., fetch: _Optional[int] = ...) -> None: ...

class SelectionExecNode(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: LogicalExprNode
    def __init__(self, expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class SubqueryAliasNode(_message.Message):
    __slots__ = ("input", "alias")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    ALIAS_FIELD_NUMBER: _ClassVar[int]
    input: LogicalPlanNode
    alias: TableReference
    def __init__(self, input: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., alias: _Optional[_Union[TableReference, _Mapping]] = ...) -> None: ...

class LogicalExprNode(_message.Message):
    __slots__ = ("column", "alias", "literal", "binary_expr", "is_null_expr", "is_not_null_expr", "not_expr", "between", "case_", "cast", "negative", "in_list", "wildcard", "try_cast", "window_expr", "aggregate_udf_expr", "scalar_udf_expr", "grouping_set", "cube", "rollup", "is_true", "is_false", "is_unknown", "is_not_true", "is_not_false", "is_not_unknown", "like", "ilike", "similar_to", "placeholder", "unnest", "scalar_subquery_expr", "higher_order_udf_expr", "lambda_variable")
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    ALIAS_FIELD_NUMBER: _ClassVar[int]
    LITERAL_FIELD_NUMBER: _ClassVar[int]
    BINARY_EXPR_FIELD_NUMBER: _ClassVar[int]
    IS_NULL_EXPR_FIELD_NUMBER: _ClassVar[int]
    IS_NOT_NULL_EXPR_FIELD_NUMBER: _ClassVar[int]
    NOT_EXPR_FIELD_NUMBER: _ClassVar[int]
    BETWEEN_FIELD_NUMBER: _ClassVar[int]
    CASE__FIELD_NUMBER: _ClassVar[int]
    CAST_FIELD_NUMBER: _ClassVar[int]
    NEGATIVE_FIELD_NUMBER: _ClassVar[int]
    IN_LIST_FIELD_NUMBER: _ClassVar[int]
    WILDCARD_FIELD_NUMBER: _ClassVar[int]
    TRY_CAST_FIELD_NUMBER: _ClassVar[int]
    WINDOW_EXPR_FIELD_NUMBER: _ClassVar[int]
    AGGREGATE_UDF_EXPR_FIELD_NUMBER: _ClassVar[int]
    SCALAR_UDF_EXPR_FIELD_NUMBER: _ClassVar[int]
    GROUPING_SET_FIELD_NUMBER: _ClassVar[int]
    CUBE_FIELD_NUMBER: _ClassVar[int]
    ROLLUP_FIELD_NUMBER: _ClassVar[int]
    IS_TRUE_FIELD_NUMBER: _ClassVar[int]
    IS_FALSE_FIELD_NUMBER: _ClassVar[int]
    IS_UNKNOWN_FIELD_NUMBER: _ClassVar[int]
    IS_NOT_TRUE_FIELD_NUMBER: _ClassVar[int]
    IS_NOT_FALSE_FIELD_NUMBER: _ClassVar[int]
    IS_NOT_UNKNOWN_FIELD_NUMBER: _ClassVar[int]
    LIKE_FIELD_NUMBER: _ClassVar[int]
    ILIKE_FIELD_NUMBER: _ClassVar[int]
    SIMILAR_TO_FIELD_NUMBER: _ClassVar[int]
    PLACEHOLDER_FIELD_NUMBER: _ClassVar[int]
    UNNEST_FIELD_NUMBER: _ClassVar[int]
    SCALAR_SUBQUERY_EXPR_FIELD_NUMBER: _ClassVar[int]
    HIGHER_ORDER_UDF_EXPR_FIELD_NUMBER: _ClassVar[int]
    LAMBDA_FIELD_NUMBER: _ClassVar[int]
    LAMBDA_VARIABLE_FIELD_NUMBER: _ClassVar[int]
    column: _datafusion_common_pb2.Column
    alias: AliasNode
    literal: _datafusion_common_pb2.ScalarValue
    binary_expr: BinaryExprNode
    is_null_expr: IsNull
    is_not_null_expr: IsNotNull
    not_expr: Not
    between: BetweenNode
    case_: CaseNode
    cast: CastNode
    negative: NegativeNode
    in_list: InListNode
    wildcard: Wildcard
    try_cast: TryCastNode
    window_expr: WindowExprNode
    aggregate_udf_expr: AggregateUDFExprNode
    scalar_udf_expr: ScalarUDFExprNode
    grouping_set: GroupingSetNode
    cube: CubeNode
    rollup: RollupNode
    is_true: IsTrue
    is_false: IsFalse
    is_unknown: IsUnknown
    is_not_true: IsNotTrue
    is_not_false: IsNotFalse
    is_not_unknown: IsNotUnknown
    like: LikeNode
    ilike: ILikeNode
    similar_to: SimilarToNode
    placeholder: PlaceholderNode
    unnest: Unnest
    scalar_subquery_expr: ScalarSubqueryExprNode
    higher_order_udf_expr: HigherOrderUDFExprNode
    lambda_variable: LambdaVariable
    def __init__(self, column: _Optional[_Union[_datafusion_common_pb2.Column, _Mapping]] = ..., alias: _Optional[_Union[AliasNode, _Mapping]] = ..., literal: _Optional[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]] = ..., binary_expr: _Optional[_Union[BinaryExprNode, _Mapping]] = ..., is_null_expr: _Optional[_Union[IsNull, _Mapping]] = ..., is_not_null_expr: _Optional[_Union[IsNotNull, _Mapping]] = ..., not_expr: _Optional[_Union[Not, _Mapping]] = ..., between: _Optional[_Union[BetweenNode, _Mapping]] = ..., case_: _Optional[_Union[CaseNode, _Mapping]] = ..., cast: _Optional[_Union[CastNode, _Mapping]] = ..., negative: _Optional[_Union[NegativeNode, _Mapping]] = ..., in_list: _Optional[_Union[InListNode, _Mapping]] = ..., wildcard: _Optional[_Union[Wildcard, _Mapping]] = ..., try_cast: _Optional[_Union[TryCastNode, _Mapping]] = ..., window_expr: _Optional[_Union[WindowExprNode, _Mapping]] = ..., aggregate_udf_expr: _Optional[_Union[AggregateUDFExprNode, _Mapping]] = ..., scalar_udf_expr: _Optional[_Union[ScalarUDFExprNode, _Mapping]] = ..., grouping_set: _Optional[_Union[GroupingSetNode, _Mapping]] = ..., cube: _Optional[_Union[CubeNode, _Mapping]] = ..., rollup: _Optional[_Union[RollupNode, _Mapping]] = ..., is_true: _Optional[_Union[IsTrue, _Mapping]] = ..., is_false: _Optional[_Union[IsFalse, _Mapping]] = ..., is_unknown: _Optional[_Union[IsUnknown, _Mapping]] = ..., is_not_true: _Optional[_Union[IsNotTrue, _Mapping]] = ..., is_not_false: _Optional[_Union[IsNotFalse, _Mapping]] = ..., is_not_unknown: _Optional[_Union[IsNotUnknown, _Mapping]] = ..., like: _Optional[_Union[LikeNode, _Mapping]] = ..., ilike: _Optional[_Union[ILikeNode, _Mapping]] = ..., similar_to: _Optional[_Union[SimilarToNode, _Mapping]] = ..., placeholder: _Optional[_Union[PlaceholderNode, _Mapping]] = ..., unnest: _Optional[_Union[Unnest, _Mapping]] = ..., scalar_subquery_expr: _Optional[_Union[ScalarSubqueryExprNode, _Mapping]] = ..., higher_order_udf_expr: _Optional[_Union[HigherOrderUDFExprNode, _Mapping]] = ..., lambda_variable: _Optional[_Union[LambdaVariable, _Mapping]] = ..., **kwargs) -> None: ...

class Wildcard(_message.Message):
    __slots__ = ("qualifier",)
    QUALIFIER_FIELD_NUMBER: _ClassVar[int]
    qualifier: TableReference
    def __init__(self, qualifier: _Optional[_Union[TableReference, _Mapping]] = ...) -> None: ...

class SubqueryNode(_message.Message):
    __slots__ = ("subquery", "outer_ref_columns")
    SUBQUERY_FIELD_NUMBER: _ClassVar[int]
    OUTER_REF_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    subquery: LogicalPlanNode
    outer_ref_columns: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    def __init__(self, subquery: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., outer_ref_columns: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ...) -> None: ...

class ScalarSubqueryExprNode(_message.Message):
    __slots__ = ("subquery",)
    SUBQUERY_FIELD_NUMBER: _ClassVar[int]
    subquery: SubqueryNode
    def __init__(self, subquery: _Optional[_Union[SubqueryNode, _Mapping]] = ...) -> None: ...

class PlaceholderNode(_message.Message):
    __slots__ = ("id", "data_type", "nullable", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
    NULLABLE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    data_type: _datafusion_common_pb2.ArrowType
    nullable: bool
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, id: _Optional[str] = ..., data_type: _Optional[_Union[_datafusion_common_pb2.ArrowType, _Mapping]] = ..., nullable: _Optional[bool] = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class LogicalExprList(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    def __init__(self, expr: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ...) -> None: ...

class GroupingSetNode(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: _containers.RepeatedCompositeFieldContainer[LogicalExprList]
    def __init__(self, expr: _Optional[_Iterable[_Union[LogicalExprList, _Mapping]]] = ...) -> None: ...

class CubeNode(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    def __init__(self, expr: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ...) -> None: ...

class RollupNode(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    def __init__(self, expr: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ...) -> None: ...

class NamedStructField(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: _datafusion_common_pb2.ScalarValue
    def __init__(self, name: _Optional[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]] = ...) -> None: ...

class ListIndex(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: LogicalExprNode
    def __init__(self, key: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class ListRange(_message.Message):
    __slots__ = ("start", "stop", "stride")
    START_FIELD_NUMBER: _ClassVar[int]
    STOP_FIELD_NUMBER: _ClassVar[int]
    STRIDE_FIELD_NUMBER: _ClassVar[int]
    start: LogicalExprNode
    stop: LogicalExprNode
    stride: LogicalExprNode
    def __init__(self, start: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., stop: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., stride: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class IsNull(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: LogicalExprNode
    def __init__(self, expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class IsNotNull(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: LogicalExprNode
    def __init__(self, expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class IsTrue(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: LogicalExprNode
    def __init__(self, expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class IsFalse(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: LogicalExprNode
    def __init__(self, expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class IsUnknown(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: LogicalExprNode
    def __init__(self, expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class IsNotTrue(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: LogicalExprNode
    def __init__(self, expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class IsNotFalse(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: LogicalExprNode
    def __init__(self, expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class IsNotUnknown(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: LogicalExprNode
    def __init__(self, expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class Not(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: LogicalExprNode
    def __init__(self, expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class AliasNode(_message.Message):
    __slots__ = ("expr", "alias", "relation", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    EXPR_FIELD_NUMBER: _ClassVar[int]
    ALIAS_FIELD_NUMBER: _ClassVar[int]
    RELATION_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    expr: LogicalExprNode
    alias: str
    relation: _containers.RepeatedCompositeFieldContainer[TableReference]
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., alias: _Optional[str] = ..., relation: _Optional[_Iterable[_Union[TableReference, _Mapping]]] = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class BinaryExprNode(_message.Message):
    __slots__ = ("operands", "op")
    OPERANDS_FIELD_NUMBER: _ClassVar[int]
    OP_FIELD_NUMBER: _ClassVar[int]
    operands: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    op: str
    def __init__(self, operands: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ..., op: _Optional[str] = ...) -> None: ...

class NegativeNode(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: LogicalExprNode
    def __init__(self, expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class Unnest(_message.Message):
    __slots__ = ("exprs", "outer")
    EXPRS_FIELD_NUMBER: _ClassVar[int]
    OUTER_FIELD_NUMBER: _ClassVar[int]
    exprs: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    outer: bool
    def __init__(self, exprs: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ..., outer: _Optional[bool] = ...) -> None: ...

class InListNode(_message.Message):
    __slots__ = ("expr", "list", "negated")
    EXPR_FIELD_NUMBER: _ClassVar[int]
    LIST_FIELD_NUMBER: _ClassVar[int]
    NEGATED_FIELD_NUMBER: _ClassVar[int]
    expr: LogicalExprNode
    list: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    negated: bool
    def __init__(self, expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., list: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ..., negated: _Optional[bool] = ...) -> None: ...

class AggregateUDFExprNode(_message.Message):
    __slots__ = ("fun_name", "args", "distinct", "filter", "order_by", "fun_definition", "null_treatment")
    FUN_NAME_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    DISTINCT_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    FUN_DEFINITION_FIELD_NUMBER: _ClassVar[int]
    NULL_TREATMENT_FIELD_NUMBER: _ClassVar[int]
    fun_name: str
    args: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    distinct: bool
    filter: LogicalExprNode
    order_by: _containers.RepeatedCompositeFieldContainer[SortExprNode]
    fun_definition: bytes
    null_treatment: NullTreatment
    def __init__(self, fun_name: _Optional[str] = ..., args: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ..., distinct: _Optional[bool] = ..., filter: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., order_by: _Optional[_Iterable[_Union[SortExprNode, _Mapping]]] = ..., fun_definition: _Optional[bytes] = ..., null_treatment: _Optional[_Union[NullTreatment, str]] = ...) -> None: ...

class ScalarUDFExprNode(_message.Message):
    __slots__ = ("fun_name", "args", "fun_definition")
    FUN_NAME_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    FUN_DEFINITION_FIELD_NUMBER: _ClassVar[int]
    fun_name: str
    args: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    fun_definition: bytes
    def __init__(self, fun_name: _Optional[str] = ..., args: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ..., fun_definition: _Optional[bytes] = ...) -> None: ...

class HigherOrderUDFExprNode(_message.Message):
    __slots__ = ("fun_name", "args", "fun_definition")
    FUN_NAME_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    FUN_DEFINITION_FIELD_NUMBER: _ClassVar[int]
    fun_name: str
    args: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    fun_definition: bytes
    def __init__(self, fun_name: _Optional[str] = ..., args: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ..., fun_definition: _Optional[bytes] = ...) -> None: ...

class Lambda(_message.Message):
    __slots__ = ("params", "body")
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    params: _containers.RepeatedScalarFieldContainer[str]
    body: LogicalExprNode
    def __init__(self, params: _Optional[_Iterable[str]] = ..., body: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class LambdaVariable(_message.Message):
    __slots__ = ("name", "field")
    NAME_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    name: str
    field: _datafusion_common_pb2.Field
    def __init__(self, name: _Optional[str] = ..., field: _Optional[_Union[_datafusion_common_pb2.Field, _Mapping]] = ...) -> None: ...

class WindowExprNode(_message.Message):
    __slots__ = ("udaf", "udwf", "exprs", "partition_by", "order_by", "window_frame", "fun_definition", "null_treatment", "distinct", "filter")
    UDAF_FIELD_NUMBER: _ClassVar[int]
    UDWF_FIELD_NUMBER: _ClassVar[int]
    EXPRS_FIELD_NUMBER: _ClassVar[int]
    PARTITION_BY_FIELD_NUMBER: _ClassVar[int]
    ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    WINDOW_FRAME_FIELD_NUMBER: _ClassVar[int]
    FUN_DEFINITION_FIELD_NUMBER: _ClassVar[int]
    NULL_TREATMENT_FIELD_NUMBER: _ClassVar[int]
    DISTINCT_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    udaf: str
    udwf: str
    exprs: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    partition_by: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    order_by: _containers.RepeatedCompositeFieldContainer[SortExprNode]
    window_frame: WindowFrame
    fun_definition: bytes
    null_treatment: NullTreatment
    distinct: bool
    filter: LogicalExprNode
    def __init__(self, udaf: _Optional[str] = ..., udwf: _Optional[str] = ..., exprs: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ..., partition_by: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ..., order_by: _Optional[_Iterable[_Union[SortExprNode, _Mapping]]] = ..., window_frame: _Optional[_Union[WindowFrame, _Mapping]] = ..., fun_definition: _Optional[bytes] = ..., null_treatment: _Optional[_Union[NullTreatment, str]] = ..., distinct: _Optional[bool] = ..., filter: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class BetweenNode(_message.Message):
    __slots__ = ("expr", "negated", "low", "high")
    EXPR_FIELD_NUMBER: _ClassVar[int]
    NEGATED_FIELD_NUMBER: _ClassVar[int]
    LOW_FIELD_NUMBER: _ClassVar[int]
    HIGH_FIELD_NUMBER: _ClassVar[int]
    expr: LogicalExprNode
    negated: bool
    low: LogicalExprNode
    high: LogicalExprNode
    def __init__(self, expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., negated: _Optional[bool] = ..., low: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., high: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class LikeNode(_message.Message):
    __slots__ = ("negated", "expr", "pattern", "escape_char")
    NEGATED_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    PATTERN_FIELD_NUMBER: _ClassVar[int]
    ESCAPE_CHAR_FIELD_NUMBER: _ClassVar[int]
    negated: bool
    expr: LogicalExprNode
    pattern: LogicalExprNode
    escape_char: str
    def __init__(self, negated: _Optional[bool] = ..., expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., pattern: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., escape_char: _Optional[str] = ...) -> None: ...

class ILikeNode(_message.Message):
    __slots__ = ("negated", "expr", "pattern", "escape_char")
    NEGATED_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    PATTERN_FIELD_NUMBER: _ClassVar[int]
    ESCAPE_CHAR_FIELD_NUMBER: _ClassVar[int]
    negated: bool
    expr: LogicalExprNode
    pattern: LogicalExprNode
    escape_char: str
    def __init__(self, negated: _Optional[bool] = ..., expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., pattern: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., escape_char: _Optional[str] = ...) -> None: ...

class SimilarToNode(_message.Message):
    __slots__ = ("negated", "expr", "pattern", "escape_char")
    NEGATED_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    PATTERN_FIELD_NUMBER: _ClassVar[int]
    ESCAPE_CHAR_FIELD_NUMBER: _ClassVar[int]
    negated: bool
    expr: LogicalExprNode
    pattern: LogicalExprNode
    escape_char: str
    def __init__(self, negated: _Optional[bool] = ..., expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., pattern: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., escape_char: _Optional[str] = ...) -> None: ...

class CaseNode(_message.Message):
    __slots__ = ("expr", "when_then_expr", "else_expr")
    EXPR_FIELD_NUMBER: _ClassVar[int]
    WHEN_THEN_EXPR_FIELD_NUMBER: _ClassVar[int]
    ELSE_EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: LogicalExprNode
    when_then_expr: _containers.RepeatedCompositeFieldContainer[WhenThen]
    else_expr: LogicalExprNode
    def __init__(self, expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., when_then_expr: _Optional[_Iterable[_Union[WhenThen, _Mapping]]] = ..., else_expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class WhenThen(_message.Message):
    __slots__ = ("when_expr", "then_expr")
    WHEN_EXPR_FIELD_NUMBER: _ClassVar[int]
    THEN_EXPR_FIELD_NUMBER: _ClassVar[int]
    when_expr: LogicalExprNode
    then_expr: LogicalExprNode
    def __init__(self, when_expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., then_expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ...) -> None: ...

class CastNode(_message.Message):
    __slots__ = ("expr", "arrow_type", "metadata", "nullable")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    EXPR_FIELD_NUMBER: _ClassVar[int]
    ARROW_TYPE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    NULLABLE_FIELD_NUMBER: _ClassVar[int]
    expr: LogicalExprNode
    arrow_type: _datafusion_common_pb2.ArrowType
    metadata: _containers.ScalarMap[str, str]
    nullable: bool
    def __init__(self, expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., arrow_type: _Optional[_Union[_datafusion_common_pb2.ArrowType, _Mapping]] = ..., metadata: _Optional[_Mapping[str, str]] = ..., nullable: _Optional[bool] = ...) -> None: ...

class TryCastNode(_message.Message):
    __slots__ = ("expr", "arrow_type", "metadata", "nullable")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    EXPR_FIELD_NUMBER: _ClassVar[int]
    ARROW_TYPE_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    NULLABLE_FIELD_NUMBER: _ClassVar[int]
    expr: LogicalExprNode
    arrow_type: _datafusion_common_pb2.ArrowType
    metadata: _containers.ScalarMap[str, str]
    nullable: bool
    def __init__(self, expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., arrow_type: _Optional[_Union[_datafusion_common_pb2.ArrowType, _Mapping]] = ..., metadata: _Optional[_Mapping[str, str]] = ..., nullable: _Optional[bool] = ...) -> None: ...

class SortExprNode(_message.Message):
    __slots__ = ("expr", "asc", "nulls_first")
    EXPR_FIELD_NUMBER: _ClassVar[int]
    ASC_FIELD_NUMBER: _ClassVar[int]
    NULLS_FIRST_FIELD_NUMBER: _ClassVar[int]
    expr: LogicalExprNode
    asc: bool
    nulls_first: bool
    def __init__(self, expr: _Optional[_Union[LogicalExprNode, _Mapping]] = ..., asc: _Optional[bool] = ..., nulls_first: _Optional[bool] = ...) -> None: ...

class WindowFrame(_message.Message):
    __slots__ = ("window_frame_units", "start_bound", "bound")
    WINDOW_FRAME_UNITS_FIELD_NUMBER: _ClassVar[int]
    START_BOUND_FIELD_NUMBER: _ClassVar[int]
    BOUND_FIELD_NUMBER: _ClassVar[int]
    window_frame_units: WindowFrameUnits
    start_bound: WindowFrameBound
    bound: WindowFrameBound
    def __init__(self, window_frame_units: _Optional[_Union[WindowFrameUnits, str]] = ..., start_bound: _Optional[_Union[WindowFrameBound, _Mapping]] = ..., bound: _Optional[_Union[WindowFrameBound, _Mapping]] = ...) -> None: ...

class WindowFrameBound(_message.Message):
    __slots__ = ("window_frame_bound_type", "bound_value")
    WINDOW_FRAME_BOUND_TYPE_FIELD_NUMBER: _ClassVar[int]
    BOUND_VALUE_FIELD_NUMBER: _ClassVar[int]
    window_frame_bound_type: WindowFrameBoundType
    bound_value: _datafusion_common_pb2.ScalarValue
    def __init__(self, window_frame_bound_type: _Optional[_Union[WindowFrameBoundType, str]] = ..., bound_value: _Optional[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]] = ...) -> None: ...

class FixedSizeBinary(_message.Message):
    __slots__ = ("length",)
    LENGTH_FIELD_NUMBER: _ClassVar[int]
    length: int
    def __init__(self, length: _Optional[int] = ...) -> None: ...

class AnalyzedLogicalPlanType(_message.Message):
    __slots__ = ("analyzer_name",)
    ANALYZER_NAME_FIELD_NUMBER: _ClassVar[int]
    analyzer_name: str
    def __init__(self, analyzer_name: _Optional[str] = ...) -> None: ...

class OptimizedLogicalPlanType(_message.Message):
    __slots__ = ("optimizer_name",)
    OPTIMIZER_NAME_FIELD_NUMBER: _ClassVar[int]
    optimizer_name: str
    def __init__(self, optimizer_name: _Optional[str] = ...) -> None: ...

class OptimizedPhysicalPlanType(_message.Message):
    __slots__ = ("optimizer_name",)
    OPTIMIZER_NAME_FIELD_NUMBER: _ClassVar[int]
    optimizer_name: str
    def __init__(self, optimizer_name: _Optional[str] = ...) -> None: ...

class PlanType(_message.Message):
    __slots__ = ("InitialLogicalPlan", "AnalyzedLogicalPlan", "FinalAnalyzedLogicalPlan", "OptimizedLogicalPlan", "FinalLogicalPlan", "InitialPhysicalPlan", "InitialPhysicalPlanWithStats", "InitialPhysicalPlanWithSchema", "OptimizedPhysicalPlan", "FinalPhysicalPlan", "FinalPhysicalPlanWithStats", "FinalPhysicalPlanWithSchema", "PhysicalPlanError")
    INITIALLOGICALPLAN_FIELD_NUMBER: _ClassVar[int]
    ANALYZEDLOGICALPLAN_FIELD_NUMBER: _ClassVar[int]
    FINALANALYZEDLOGICALPLAN_FIELD_NUMBER: _ClassVar[int]
    OPTIMIZEDLOGICALPLAN_FIELD_NUMBER: _ClassVar[int]
    FINALLOGICALPLAN_FIELD_NUMBER: _ClassVar[int]
    INITIALPHYSICALPLAN_FIELD_NUMBER: _ClassVar[int]
    INITIALPHYSICALPLANWITHSTATS_FIELD_NUMBER: _ClassVar[int]
    INITIALPHYSICALPLANWITHSCHEMA_FIELD_NUMBER: _ClassVar[int]
    OPTIMIZEDPHYSICALPLAN_FIELD_NUMBER: _ClassVar[int]
    FINALPHYSICALPLAN_FIELD_NUMBER: _ClassVar[int]
    FINALPHYSICALPLANWITHSTATS_FIELD_NUMBER: _ClassVar[int]
    FINALPHYSICALPLANWITHSCHEMA_FIELD_NUMBER: _ClassVar[int]
    PHYSICALPLANERROR_FIELD_NUMBER: _ClassVar[int]
    InitialLogicalPlan: _datafusion_common_pb2.EmptyMessage
    AnalyzedLogicalPlan: AnalyzedLogicalPlanType
    FinalAnalyzedLogicalPlan: _datafusion_common_pb2.EmptyMessage
    OptimizedLogicalPlan: OptimizedLogicalPlanType
    FinalLogicalPlan: _datafusion_common_pb2.EmptyMessage
    InitialPhysicalPlan: _datafusion_common_pb2.EmptyMessage
    InitialPhysicalPlanWithStats: _datafusion_common_pb2.EmptyMessage
    InitialPhysicalPlanWithSchema: _datafusion_common_pb2.EmptyMessage
    OptimizedPhysicalPlan: OptimizedPhysicalPlanType
    FinalPhysicalPlan: _datafusion_common_pb2.EmptyMessage
    FinalPhysicalPlanWithStats: _datafusion_common_pb2.EmptyMessage
    FinalPhysicalPlanWithSchema: _datafusion_common_pb2.EmptyMessage
    PhysicalPlanError: _datafusion_common_pb2.EmptyMessage
    def __init__(self, InitialLogicalPlan: _Optional[_Union[_datafusion_common_pb2.EmptyMessage, _Mapping]] = ..., AnalyzedLogicalPlan: _Optional[_Union[AnalyzedLogicalPlanType, _Mapping]] = ..., FinalAnalyzedLogicalPlan: _Optional[_Union[_datafusion_common_pb2.EmptyMessage, _Mapping]] = ..., OptimizedLogicalPlan: _Optional[_Union[OptimizedLogicalPlanType, _Mapping]] = ..., FinalLogicalPlan: _Optional[_Union[_datafusion_common_pb2.EmptyMessage, _Mapping]] = ..., InitialPhysicalPlan: _Optional[_Union[_datafusion_common_pb2.EmptyMessage, _Mapping]] = ..., InitialPhysicalPlanWithStats: _Optional[_Union[_datafusion_common_pb2.EmptyMessage, _Mapping]] = ..., InitialPhysicalPlanWithSchema: _Optional[_Union[_datafusion_common_pb2.EmptyMessage, _Mapping]] = ..., OptimizedPhysicalPlan: _Optional[_Union[OptimizedPhysicalPlanType, _Mapping]] = ..., FinalPhysicalPlan: _Optional[_Union[_datafusion_common_pb2.EmptyMessage, _Mapping]] = ..., FinalPhysicalPlanWithStats: _Optional[_Union[_datafusion_common_pb2.EmptyMessage, _Mapping]] = ..., FinalPhysicalPlanWithSchema: _Optional[_Union[_datafusion_common_pb2.EmptyMessage, _Mapping]] = ..., PhysicalPlanError: _Optional[_Union[_datafusion_common_pb2.EmptyMessage, _Mapping]] = ...) -> None: ...

class StringifiedPlan(_message.Message):
    __slots__ = ("plan_type", "plan")
    PLAN_TYPE_FIELD_NUMBER: _ClassVar[int]
    PLAN_FIELD_NUMBER: _ClassVar[int]
    plan_type: PlanType
    plan: str
    def __init__(self, plan_type: _Optional[_Union[PlanType, _Mapping]] = ..., plan: _Optional[str] = ...) -> None: ...

class BareTableReference(_message.Message):
    __slots__ = ("table",)
    TABLE_FIELD_NUMBER: _ClassVar[int]
    table: str
    def __init__(self, table: _Optional[str] = ...) -> None: ...

class PartialTableReference(_message.Message):
    __slots__ = ("schema", "table")
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    schema: str
    table: str
    def __init__(self, schema: _Optional[str] = ..., table: _Optional[str] = ...) -> None: ...

class FullTableReference(_message.Message):
    __slots__ = ("catalog", "schema", "table")
    CATALOG_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    catalog: str
    schema: str
    table: str
    def __init__(self, catalog: _Optional[str] = ..., schema: _Optional[str] = ..., table: _Optional[str] = ...) -> None: ...

class TableReference(_message.Message):
    __slots__ = ("bare", "partial", "full")
    BARE_FIELD_NUMBER: _ClassVar[int]
    PARTIAL_FIELD_NUMBER: _ClassVar[int]
    FULL_FIELD_NUMBER: _ClassVar[int]
    bare: BareTableReference
    partial: PartialTableReference
    full: FullTableReference
    def __init__(self, bare: _Optional[_Union[BareTableReference, _Mapping]] = ..., partial: _Optional[_Union[PartialTableReference, _Mapping]] = ..., full: _Optional[_Union[FullTableReference, _Mapping]] = ...) -> None: ...

class PhysicalPlanNode(_message.Message):
    __slots__ = ("parquet_scan", "csv_scan", "empty", "projection", "global_limit", "local_limit", "aggregate", "hash_join", "sort", "coalesce_batches", "filter", "merge", "repartition", "window", "cross_join", "avro_scan", "extension", "union", "explain", "sort_preserving_merge", "nested_loop_join", "analyze", "json_sink", "symmetric_hash_join", "interleave", "placeholder_row", "csv_sink", "parquet_sink", "unnest", "json_scan", "cooperative", "generate_series", "sort_merge_join", "memory_scan", "async_func", "buffer", "arrow_scan", "scalar_subquery")
    PARQUET_SCAN_FIELD_NUMBER: _ClassVar[int]
    CSV_SCAN_FIELD_NUMBER: _ClassVar[int]
    EMPTY_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_FIELD_NUMBER: _ClassVar[int]
    GLOBAL_LIMIT_FIELD_NUMBER: _ClassVar[int]
    LOCAL_LIMIT_FIELD_NUMBER: _ClassVar[int]
    AGGREGATE_FIELD_NUMBER: _ClassVar[int]
    HASH_JOIN_FIELD_NUMBER: _ClassVar[int]
    SORT_FIELD_NUMBER: _ClassVar[int]
    COALESCE_BATCHES_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    MERGE_FIELD_NUMBER: _ClassVar[int]
    REPARTITION_FIELD_NUMBER: _ClassVar[int]
    WINDOW_FIELD_NUMBER: _ClassVar[int]
    CROSS_JOIN_FIELD_NUMBER: _ClassVar[int]
    AVRO_SCAN_FIELD_NUMBER: _ClassVar[int]
    EXTENSION_FIELD_NUMBER: _ClassVar[int]
    UNION_FIELD_NUMBER: _ClassVar[int]
    EXPLAIN_FIELD_NUMBER: _ClassVar[int]
    SORT_PRESERVING_MERGE_FIELD_NUMBER: _ClassVar[int]
    NESTED_LOOP_JOIN_FIELD_NUMBER: _ClassVar[int]
    ANALYZE_FIELD_NUMBER: _ClassVar[int]
    JSON_SINK_FIELD_NUMBER: _ClassVar[int]
    SYMMETRIC_HASH_JOIN_FIELD_NUMBER: _ClassVar[int]
    INTERLEAVE_FIELD_NUMBER: _ClassVar[int]
    PLACEHOLDER_ROW_FIELD_NUMBER: _ClassVar[int]
    CSV_SINK_FIELD_NUMBER: _ClassVar[int]
    PARQUET_SINK_FIELD_NUMBER: _ClassVar[int]
    UNNEST_FIELD_NUMBER: _ClassVar[int]
    JSON_SCAN_FIELD_NUMBER: _ClassVar[int]
    COOPERATIVE_FIELD_NUMBER: _ClassVar[int]
    GENERATE_SERIES_FIELD_NUMBER: _ClassVar[int]
    SORT_MERGE_JOIN_FIELD_NUMBER: _ClassVar[int]
    MEMORY_SCAN_FIELD_NUMBER: _ClassVar[int]
    ASYNC_FUNC_FIELD_NUMBER: _ClassVar[int]
    BUFFER_FIELD_NUMBER: _ClassVar[int]
    ARROW_SCAN_FIELD_NUMBER: _ClassVar[int]
    SCALAR_SUBQUERY_FIELD_NUMBER: _ClassVar[int]
    parquet_scan: ParquetScanExecNode
    csv_scan: CsvScanExecNode
    empty: EmptyExecNode
    projection: ProjectionExecNode
    global_limit: GlobalLimitExecNode
    local_limit: LocalLimitExecNode
    aggregate: AggregateExecNode
    hash_join: HashJoinExecNode
    sort: SortExecNode
    coalesce_batches: CoalesceBatchesExecNode
    filter: FilterExecNode
    merge: CoalescePartitionsExecNode
    repartition: RepartitionExecNode
    window: WindowAggExecNode
    cross_join: CrossJoinExecNode
    avro_scan: AvroScanExecNode
    extension: PhysicalExtensionNode
    union: UnionExecNode
    explain: ExplainExecNode
    sort_preserving_merge: SortPreservingMergeExecNode
    nested_loop_join: NestedLoopJoinExecNode
    analyze: AnalyzeExecNode
    json_sink: JsonSinkExecNode
    symmetric_hash_join: SymmetricHashJoinExecNode
    interleave: InterleaveExecNode
    placeholder_row: PlaceholderRowExecNode
    csv_sink: CsvSinkExecNode
    parquet_sink: ParquetSinkExecNode
    unnest: UnnestExecNode
    json_scan: JsonScanExecNode
    cooperative: CooperativeExecNode
    generate_series: GenerateSeriesNode
    sort_merge_join: SortMergeJoinExecNode
    memory_scan: MemoryScanExecNode
    async_func: AsyncFuncExecNode
    buffer: BufferExecNode
    arrow_scan: ArrowScanExecNode
    scalar_subquery: ScalarSubqueryExecNode
    def __init__(self, parquet_scan: _Optional[_Union[ParquetScanExecNode, _Mapping]] = ..., csv_scan: _Optional[_Union[CsvScanExecNode, _Mapping]] = ..., empty: _Optional[_Union[EmptyExecNode, _Mapping]] = ..., projection: _Optional[_Union[ProjectionExecNode, _Mapping]] = ..., global_limit: _Optional[_Union[GlobalLimitExecNode, _Mapping]] = ..., local_limit: _Optional[_Union[LocalLimitExecNode, _Mapping]] = ..., aggregate: _Optional[_Union[AggregateExecNode, _Mapping]] = ..., hash_join: _Optional[_Union[HashJoinExecNode, _Mapping]] = ..., sort: _Optional[_Union[SortExecNode, _Mapping]] = ..., coalesce_batches: _Optional[_Union[CoalesceBatchesExecNode, _Mapping]] = ..., filter: _Optional[_Union[FilterExecNode, _Mapping]] = ..., merge: _Optional[_Union[CoalescePartitionsExecNode, _Mapping]] = ..., repartition: _Optional[_Union[RepartitionExecNode, _Mapping]] = ..., window: _Optional[_Union[WindowAggExecNode, _Mapping]] = ..., cross_join: _Optional[_Union[CrossJoinExecNode, _Mapping]] = ..., avro_scan: _Optional[_Union[AvroScanExecNode, _Mapping]] = ..., extension: _Optional[_Union[PhysicalExtensionNode, _Mapping]] = ..., union: _Optional[_Union[UnionExecNode, _Mapping]] = ..., explain: _Optional[_Union[ExplainExecNode, _Mapping]] = ..., sort_preserving_merge: _Optional[_Union[SortPreservingMergeExecNode, _Mapping]] = ..., nested_loop_join: _Optional[_Union[NestedLoopJoinExecNode, _Mapping]] = ..., analyze: _Optional[_Union[AnalyzeExecNode, _Mapping]] = ..., json_sink: _Optional[_Union[JsonSinkExecNode, _Mapping]] = ..., symmetric_hash_join: _Optional[_Union[SymmetricHashJoinExecNode, _Mapping]] = ..., interleave: _Optional[_Union[InterleaveExecNode, _Mapping]] = ..., placeholder_row: _Optional[_Union[PlaceholderRowExecNode, _Mapping]] = ..., csv_sink: _Optional[_Union[CsvSinkExecNode, _Mapping]] = ..., parquet_sink: _Optional[_Union[ParquetSinkExecNode, _Mapping]] = ..., unnest: _Optional[_Union[UnnestExecNode, _Mapping]] = ..., json_scan: _Optional[_Union[JsonScanExecNode, _Mapping]] = ..., cooperative: _Optional[_Union[CooperativeExecNode, _Mapping]] = ..., generate_series: _Optional[_Union[GenerateSeriesNode, _Mapping]] = ..., sort_merge_join: _Optional[_Union[SortMergeJoinExecNode, _Mapping]] = ..., memory_scan: _Optional[_Union[MemoryScanExecNode, _Mapping]] = ..., async_func: _Optional[_Union[AsyncFuncExecNode, _Mapping]] = ..., buffer: _Optional[_Union[BufferExecNode, _Mapping]] = ..., arrow_scan: _Optional[_Union[ArrowScanExecNode, _Mapping]] = ..., scalar_subquery: _Optional[_Union[ScalarSubqueryExecNode, _Mapping]] = ...) -> None: ...

class PartitionColumn(_message.Message):
    __slots__ = ("name", "arrow_type")
    NAME_FIELD_NUMBER: _ClassVar[int]
    ARROW_TYPE_FIELD_NUMBER: _ClassVar[int]
    name: str
    arrow_type: _datafusion_common_pb2.ArrowType
    def __init__(self, name: _Optional[str] = ..., arrow_type: _Optional[_Union[_datafusion_common_pb2.ArrowType, _Mapping]] = ...) -> None: ...

class FileSinkConfig(_message.Message):
    __slots__ = ("object_store_url", "file_groups", "table_paths", "output_schema", "table_partition_cols", "keep_partition_by_columns", "insert_op", "file_extension", "file_output_mode")
    OBJECT_STORE_URL_FIELD_NUMBER: _ClassVar[int]
    FILE_GROUPS_FIELD_NUMBER: _ClassVar[int]
    TABLE_PATHS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    TABLE_PARTITION_COLS_FIELD_NUMBER: _ClassVar[int]
    KEEP_PARTITION_BY_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    INSERT_OP_FIELD_NUMBER: _ClassVar[int]
    FILE_EXTENSION_FIELD_NUMBER: _ClassVar[int]
    FILE_OUTPUT_MODE_FIELD_NUMBER: _ClassVar[int]
    object_store_url: str
    file_groups: _containers.RepeatedCompositeFieldContainer[PartitionedFile]
    table_paths: _containers.RepeatedScalarFieldContainer[str]
    output_schema: _datafusion_common_pb2.Schema
    table_partition_cols: _containers.RepeatedCompositeFieldContainer[PartitionColumn]
    keep_partition_by_columns: bool
    insert_op: InsertOp
    file_extension: str
    file_output_mode: FileOutputMode
    def __init__(self, object_store_url: _Optional[str] = ..., file_groups: _Optional[_Iterable[_Union[PartitionedFile, _Mapping]]] = ..., table_paths: _Optional[_Iterable[str]] = ..., output_schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., table_partition_cols: _Optional[_Iterable[_Union[PartitionColumn, _Mapping]]] = ..., keep_partition_by_columns: _Optional[bool] = ..., insert_op: _Optional[_Union[InsertOp, str]] = ..., file_extension: _Optional[str] = ..., file_output_mode: _Optional[_Union[FileOutputMode, str]] = ...) -> None: ...

class JsonSink(_message.Message):
    __slots__ = ("config", "writer_options")
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    WRITER_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    config: FileSinkConfig
    writer_options: _datafusion_common_pb2.JsonWriterOptions
    def __init__(self, config: _Optional[_Union[FileSinkConfig, _Mapping]] = ..., writer_options: _Optional[_Union[_datafusion_common_pb2.JsonWriterOptions, _Mapping]] = ...) -> None: ...

class JsonSinkExecNode(_message.Message):
    __slots__ = ("input", "sink", "sink_schema", "sort_order")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    SINK_FIELD_NUMBER: _ClassVar[int]
    SINK_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    SORT_ORDER_FIELD_NUMBER: _ClassVar[int]
    input: PhysicalPlanNode
    sink: JsonSink
    sink_schema: _datafusion_common_pb2.Schema
    sort_order: PhysicalSortExprNodeCollection
    def __init__(self, input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., sink: _Optional[_Union[JsonSink, _Mapping]] = ..., sink_schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., sort_order: _Optional[_Union[PhysicalSortExprNodeCollection, _Mapping]] = ...) -> None: ...

class CsvSink(_message.Message):
    __slots__ = ("config", "writer_options")
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    WRITER_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    config: FileSinkConfig
    writer_options: _datafusion_common_pb2.CsvWriterOptions
    def __init__(self, config: _Optional[_Union[FileSinkConfig, _Mapping]] = ..., writer_options: _Optional[_Union[_datafusion_common_pb2.CsvWriterOptions, _Mapping]] = ...) -> None: ...

class CsvSinkExecNode(_message.Message):
    __slots__ = ("input", "sink", "sink_schema", "sort_order")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    SINK_FIELD_NUMBER: _ClassVar[int]
    SINK_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    SORT_ORDER_FIELD_NUMBER: _ClassVar[int]
    input: PhysicalPlanNode
    sink: CsvSink
    sink_schema: _datafusion_common_pb2.Schema
    sort_order: PhysicalSortExprNodeCollection
    def __init__(self, input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., sink: _Optional[_Union[CsvSink, _Mapping]] = ..., sink_schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., sort_order: _Optional[_Union[PhysicalSortExprNodeCollection, _Mapping]] = ...) -> None: ...

class ParquetSink(_message.Message):
    __slots__ = ("config", "parquet_options")
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    PARQUET_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    config: FileSinkConfig
    parquet_options: _datafusion_common_pb2.TableParquetOptions
    def __init__(self, config: _Optional[_Union[FileSinkConfig, _Mapping]] = ..., parquet_options: _Optional[_Union[_datafusion_common_pb2.TableParquetOptions, _Mapping]] = ...) -> None: ...

class ParquetSinkExecNode(_message.Message):
    __slots__ = ("input", "sink", "sink_schema", "sort_order")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    SINK_FIELD_NUMBER: _ClassVar[int]
    SINK_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    SORT_ORDER_FIELD_NUMBER: _ClassVar[int]
    input: PhysicalPlanNode
    sink: ParquetSink
    sink_schema: _datafusion_common_pb2.Schema
    sort_order: PhysicalSortExprNodeCollection
    def __init__(self, input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., sink: _Optional[_Union[ParquetSink, _Mapping]] = ..., sink_schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., sort_order: _Optional[_Union[PhysicalSortExprNodeCollection, _Mapping]] = ...) -> None: ...

class UnnestExecNode(_message.Message):
    __slots__ = ("input", "schema", "list_type_columns", "struct_type_columns", "options")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    LIST_TYPE_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    STRUCT_TYPE_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    input: PhysicalPlanNode
    schema: _datafusion_common_pb2.Schema
    list_type_columns: _containers.RepeatedCompositeFieldContainer[ListUnnest]
    struct_type_columns: _containers.RepeatedScalarFieldContainer[int]
    options: UnnestOptions
    def __init__(self, input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., list_type_columns: _Optional[_Iterable[_Union[ListUnnest, _Mapping]]] = ..., struct_type_columns: _Optional[_Iterable[int]] = ..., options: _Optional[_Union[UnnestOptions, _Mapping]] = ...) -> None: ...

class ListUnnest(_message.Message):
    __slots__ = ("index_in_input_schema", "depth")
    INDEX_IN_INPUT_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    DEPTH_FIELD_NUMBER: _ClassVar[int]
    index_in_input_schema: int
    depth: int
    def __init__(self, index_in_input_schema: _Optional[int] = ..., depth: _Optional[int] = ...) -> None: ...

class PhysicalExtensionNode(_message.Message):
    __slots__ = ("node", "inputs")
    NODE_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    node: bytes
    inputs: _containers.RepeatedCompositeFieldContainer[PhysicalPlanNode]
    def __init__(self, node: _Optional[bytes] = ..., inputs: _Optional[_Iterable[_Union[PhysicalPlanNode, _Mapping]]] = ...) -> None: ...

class PhysicalExprNode(_message.Message):
    __slots__ = ("expr_id", "column", "literal", "binary_expr", "aggregate_expr", "is_null_expr", "is_not_null_expr", "not_expr", "case_", "cast", "sort", "negative", "in_list", "try_cast", "window_expr", "scalar_udf", "like_expr", "extension", "unknown_column", "hash_expr", "scalar_subquery", "dynamic_filter", "higher_order_udf", "lambda_variable", "range_expr")
    EXPR_ID_FIELD_NUMBER: _ClassVar[int]
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    LITERAL_FIELD_NUMBER: _ClassVar[int]
    BINARY_EXPR_FIELD_NUMBER: _ClassVar[int]
    AGGREGATE_EXPR_FIELD_NUMBER: _ClassVar[int]
    IS_NULL_EXPR_FIELD_NUMBER: _ClassVar[int]
    IS_NOT_NULL_EXPR_FIELD_NUMBER: _ClassVar[int]
    NOT_EXPR_FIELD_NUMBER: _ClassVar[int]
    CASE__FIELD_NUMBER: _ClassVar[int]
    CAST_FIELD_NUMBER: _ClassVar[int]
    SORT_FIELD_NUMBER: _ClassVar[int]
    NEGATIVE_FIELD_NUMBER: _ClassVar[int]
    IN_LIST_FIELD_NUMBER: _ClassVar[int]
    TRY_CAST_FIELD_NUMBER: _ClassVar[int]
    WINDOW_EXPR_FIELD_NUMBER: _ClassVar[int]
    SCALAR_UDF_FIELD_NUMBER: _ClassVar[int]
    LIKE_EXPR_FIELD_NUMBER: _ClassVar[int]
    EXTENSION_FIELD_NUMBER: _ClassVar[int]
    UNKNOWN_COLUMN_FIELD_NUMBER: _ClassVar[int]
    HASH_EXPR_FIELD_NUMBER: _ClassVar[int]
    SCALAR_SUBQUERY_FIELD_NUMBER: _ClassVar[int]
    DYNAMIC_FILTER_FIELD_NUMBER: _ClassVar[int]
    HIGHER_ORDER_UDF_FIELD_NUMBER: _ClassVar[int]
    LAMBDA_FIELD_NUMBER: _ClassVar[int]
    LAMBDA_VARIABLE_FIELD_NUMBER: _ClassVar[int]
    RANGE_EXPR_FIELD_NUMBER: _ClassVar[int]
    expr_id: int
    column: PhysicalColumn
    literal: _datafusion_common_pb2.ScalarValue
    binary_expr: PhysicalBinaryExprNode
    aggregate_expr: PhysicalAggregateExprNode
    is_null_expr: PhysicalIsNull
    is_not_null_expr: PhysicalIsNotNull
    not_expr: PhysicalNot
    case_: PhysicalCaseNode
    cast: PhysicalCastNode
    sort: PhysicalSortExprNode
    negative: PhysicalNegativeNode
    in_list: PhysicalInListNode
    try_cast: PhysicalTryCastNode
    window_expr: PhysicalWindowExprNode
    scalar_udf: PhysicalScalarUdfNode
    like_expr: PhysicalLikeExprNode
    extension: PhysicalExtensionExprNode
    unknown_column: UnknownColumn
    hash_expr: PhysicalHashExprNode
    scalar_subquery: PhysicalScalarSubqueryExprNode
    dynamic_filter: PhysicalDynamicFilterNode
    higher_order_udf: PhysicalHigherOrderUdfNode
    lambda_variable: PhysicalLambdaVariableExprNode
    range_expr: PhysicalRangeExprNode
    def __init__(self, expr_id: _Optional[int] = ..., column: _Optional[_Union[PhysicalColumn, _Mapping]] = ..., literal: _Optional[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]] = ..., binary_expr: _Optional[_Union[PhysicalBinaryExprNode, _Mapping]] = ..., aggregate_expr: _Optional[_Union[PhysicalAggregateExprNode, _Mapping]] = ..., is_null_expr: _Optional[_Union[PhysicalIsNull, _Mapping]] = ..., is_not_null_expr: _Optional[_Union[PhysicalIsNotNull, _Mapping]] = ..., not_expr: _Optional[_Union[PhysicalNot, _Mapping]] = ..., case_: _Optional[_Union[PhysicalCaseNode, _Mapping]] = ..., cast: _Optional[_Union[PhysicalCastNode, _Mapping]] = ..., sort: _Optional[_Union[PhysicalSortExprNode, _Mapping]] = ..., negative: _Optional[_Union[PhysicalNegativeNode, _Mapping]] = ..., in_list: _Optional[_Union[PhysicalInListNode, _Mapping]] = ..., try_cast: _Optional[_Union[PhysicalTryCastNode, _Mapping]] = ..., window_expr: _Optional[_Union[PhysicalWindowExprNode, _Mapping]] = ..., scalar_udf: _Optional[_Union[PhysicalScalarUdfNode, _Mapping]] = ..., like_expr: _Optional[_Union[PhysicalLikeExprNode, _Mapping]] = ..., extension: _Optional[_Union[PhysicalExtensionExprNode, _Mapping]] = ..., unknown_column: _Optional[_Union[UnknownColumn, _Mapping]] = ..., hash_expr: _Optional[_Union[PhysicalHashExprNode, _Mapping]] = ..., scalar_subquery: _Optional[_Union[PhysicalScalarSubqueryExprNode, _Mapping]] = ..., dynamic_filter: _Optional[_Union[PhysicalDynamicFilterNode, _Mapping]] = ..., higher_order_udf: _Optional[_Union[PhysicalHigherOrderUdfNode, _Mapping]] = ..., lambda_variable: _Optional[_Union[PhysicalLambdaVariableExprNode, _Mapping]] = ..., range_expr: _Optional[_Union[PhysicalRangeExprNode, _Mapping]] = ..., **kwargs) -> None: ...

class PhysicalDynamicFilterNode(_message.Message):
    __slots__ = ("children", "remapped_children", "generation", "inner_expr", "is_complete")
    CHILDREN_FIELD_NUMBER: _ClassVar[int]
    REMAPPED_CHILDREN_FIELD_NUMBER: _ClassVar[int]
    GENERATION_FIELD_NUMBER: _ClassVar[int]
    INNER_EXPR_FIELD_NUMBER: _ClassVar[int]
    IS_COMPLETE_FIELD_NUMBER: _ClassVar[int]
    children: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    remapped_children: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    generation: int
    inner_expr: PhysicalExprNode
    is_complete: bool
    def __init__(self, children: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ..., remapped_children: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ..., generation: _Optional[int] = ..., inner_expr: _Optional[_Union[PhysicalExprNode, _Mapping]] = ..., is_complete: _Optional[bool] = ...) -> None: ...

class PhysicalScalarUdfNode(_message.Message):
    __slots__ = ("name", "args", "fun_definition", "return_type", "nullable", "return_field_name")
    NAME_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    FUN_DEFINITION_FIELD_NUMBER: _ClassVar[int]
    RETURN_TYPE_FIELD_NUMBER: _ClassVar[int]
    NULLABLE_FIELD_NUMBER: _ClassVar[int]
    RETURN_FIELD_NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    args: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    fun_definition: bytes
    return_type: _datafusion_common_pb2.ArrowType
    nullable: bool
    return_field_name: str
    def __init__(self, name: _Optional[str] = ..., args: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ..., fun_definition: _Optional[bytes] = ..., return_type: _Optional[_Union[_datafusion_common_pb2.ArrowType, _Mapping]] = ..., nullable: _Optional[bool] = ..., return_field_name: _Optional[str] = ...) -> None: ...

class PhysicalHigherOrderUdfNode(_message.Message):
    __slots__ = ("name", "args", "fun_definition")
    NAME_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    FUN_DEFINITION_FIELD_NUMBER: _ClassVar[int]
    name: str
    args: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    fun_definition: bytes
    def __init__(self, name: _Optional[str] = ..., args: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ..., fun_definition: _Optional[bytes] = ...) -> None: ...

class PhysicalLambdaExprNode(_message.Message):
    __slots__ = ("params", "body")
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    params: _containers.RepeatedScalarFieldContainer[str]
    body: PhysicalExprNode
    def __init__(self, params: _Optional[_Iterable[str]] = ..., body: _Optional[_Union[PhysicalExprNode, _Mapping]] = ...) -> None: ...

class PhysicalLambdaVariableExprNode(_message.Message):
    __slots__ = ("index", "field")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    index: int
    field: _datafusion_common_pb2.Field
    def __init__(self, index: _Optional[int] = ..., field: _Optional[_Union[_datafusion_common_pb2.Field, _Mapping]] = ...) -> None: ...

class PhysicalAggregateExprNode(_message.Message):
    __slots__ = ("user_defined_aggr_function", "expr", "ordering_req", "distinct", "ignore_nulls", "fun_definition", "human_display", "is_reversed")
    USER_DEFINED_AGGR_FUNCTION_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    ORDERING_REQ_FIELD_NUMBER: _ClassVar[int]
    DISTINCT_FIELD_NUMBER: _ClassVar[int]
    IGNORE_NULLS_FIELD_NUMBER: _ClassVar[int]
    FUN_DEFINITION_FIELD_NUMBER: _ClassVar[int]
    HUMAN_DISPLAY_FIELD_NUMBER: _ClassVar[int]
    IS_REVERSED_FIELD_NUMBER: _ClassVar[int]
    user_defined_aggr_function: str
    expr: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    ordering_req: _containers.RepeatedCompositeFieldContainer[PhysicalSortExprNode]
    distinct: bool
    ignore_nulls: bool
    fun_definition: bytes
    human_display: str
    is_reversed: bool
    def __init__(self, user_defined_aggr_function: _Optional[str] = ..., expr: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ..., ordering_req: _Optional[_Iterable[_Union[PhysicalSortExprNode, _Mapping]]] = ..., distinct: _Optional[bool] = ..., ignore_nulls: _Optional[bool] = ..., fun_definition: _Optional[bytes] = ..., human_display: _Optional[str] = ..., is_reversed: _Optional[bool] = ...) -> None: ...

class PhysicalWindowExprNode(_message.Message):
    __slots__ = ("user_defined_aggr_function", "user_defined_window_function", "args", "partition_by", "order_by", "window_frame", "name", "fun_definition", "ignore_nulls", "distinct")
    USER_DEFINED_AGGR_FUNCTION_FIELD_NUMBER: _ClassVar[int]
    USER_DEFINED_WINDOW_FUNCTION_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    PARTITION_BY_FIELD_NUMBER: _ClassVar[int]
    ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    WINDOW_FRAME_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    FUN_DEFINITION_FIELD_NUMBER: _ClassVar[int]
    IGNORE_NULLS_FIELD_NUMBER: _ClassVar[int]
    DISTINCT_FIELD_NUMBER: _ClassVar[int]
    user_defined_aggr_function: str
    user_defined_window_function: str
    args: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    partition_by: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    order_by: _containers.RepeatedCompositeFieldContainer[PhysicalSortExprNode]
    window_frame: WindowFrame
    name: str
    fun_definition: bytes
    ignore_nulls: bool
    distinct: bool
    def __init__(self, user_defined_aggr_function: _Optional[str] = ..., user_defined_window_function: _Optional[str] = ..., args: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ..., partition_by: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ..., order_by: _Optional[_Iterable[_Union[PhysicalSortExprNode, _Mapping]]] = ..., window_frame: _Optional[_Union[WindowFrame, _Mapping]] = ..., name: _Optional[str] = ..., fun_definition: _Optional[bytes] = ..., ignore_nulls: _Optional[bool] = ..., distinct: _Optional[bool] = ...) -> None: ...

class PhysicalIsNull(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: PhysicalExprNode
    def __init__(self, expr: _Optional[_Union[PhysicalExprNode, _Mapping]] = ...) -> None: ...

class PhysicalIsNotNull(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: PhysicalExprNode
    def __init__(self, expr: _Optional[_Union[PhysicalExprNode, _Mapping]] = ...) -> None: ...

class PhysicalNot(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: PhysicalExprNode
    def __init__(self, expr: _Optional[_Union[PhysicalExprNode, _Mapping]] = ...) -> None: ...

class PhysicalAliasNode(_message.Message):
    __slots__ = ("expr", "alias")
    EXPR_FIELD_NUMBER: _ClassVar[int]
    ALIAS_FIELD_NUMBER: _ClassVar[int]
    expr: PhysicalExprNode
    alias: str
    def __init__(self, expr: _Optional[_Union[PhysicalExprNode, _Mapping]] = ..., alias: _Optional[str] = ...) -> None: ...

class PhysicalBinaryExprNode(_message.Message):
    __slots__ = ("l", "r", "op", "operands")
    L_FIELD_NUMBER: _ClassVar[int]
    R_FIELD_NUMBER: _ClassVar[int]
    OP_FIELD_NUMBER: _ClassVar[int]
    OPERANDS_FIELD_NUMBER: _ClassVar[int]
    l: PhysicalExprNode
    r: PhysicalExprNode
    op: str
    operands: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    def __init__(self, l: _Optional[_Union[PhysicalExprNode, _Mapping]] = ..., r: _Optional[_Union[PhysicalExprNode, _Mapping]] = ..., op: _Optional[str] = ..., operands: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ...) -> None: ...

class PhysicalDateTimeIntervalExprNode(_message.Message):
    __slots__ = ("l", "r", "op")
    L_FIELD_NUMBER: _ClassVar[int]
    R_FIELD_NUMBER: _ClassVar[int]
    OP_FIELD_NUMBER: _ClassVar[int]
    l: PhysicalExprNode
    r: PhysicalExprNode
    op: str
    def __init__(self, l: _Optional[_Union[PhysicalExprNode, _Mapping]] = ..., r: _Optional[_Union[PhysicalExprNode, _Mapping]] = ..., op: _Optional[str] = ...) -> None: ...

class PhysicalLikeExprNode(_message.Message):
    __slots__ = ("negated", "case_insensitive", "expr", "pattern")
    NEGATED_FIELD_NUMBER: _ClassVar[int]
    CASE_INSENSITIVE_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    PATTERN_FIELD_NUMBER: _ClassVar[int]
    negated: bool
    case_insensitive: bool
    expr: PhysicalExprNode
    pattern: PhysicalExprNode
    def __init__(self, negated: _Optional[bool] = ..., case_insensitive: _Optional[bool] = ..., expr: _Optional[_Union[PhysicalExprNode, _Mapping]] = ..., pattern: _Optional[_Union[PhysicalExprNode, _Mapping]] = ...) -> None: ...

class PhysicalSortExprNode(_message.Message):
    __slots__ = ("expr", "asc", "nulls_first")
    EXPR_FIELD_NUMBER: _ClassVar[int]
    ASC_FIELD_NUMBER: _ClassVar[int]
    NULLS_FIRST_FIELD_NUMBER: _ClassVar[int]
    expr: PhysicalExprNode
    asc: bool
    nulls_first: bool
    def __init__(self, expr: _Optional[_Union[PhysicalExprNode, _Mapping]] = ..., asc: _Optional[bool] = ..., nulls_first: _Optional[bool] = ...) -> None: ...

class PhysicalWhenThen(_message.Message):
    __slots__ = ("when_expr", "then_expr")
    WHEN_EXPR_FIELD_NUMBER: _ClassVar[int]
    THEN_EXPR_FIELD_NUMBER: _ClassVar[int]
    when_expr: PhysicalExprNode
    then_expr: PhysicalExprNode
    def __init__(self, when_expr: _Optional[_Union[PhysicalExprNode, _Mapping]] = ..., then_expr: _Optional[_Union[PhysicalExprNode, _Mapping]] = ...) -> None: ...

class PhysicalInListNode(_message.Message):
    __slots__ = ("expr", "list", "negated")
    EXPR_FIELD_NUMBER: _ClassVar[int]
    LIST_FIELD_NUMBER: _ClassVar[int]
    NEGATED_FIELD_NUMBER: _ClassVar[int]
    expr: PhysicalExprNode
    list: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    negated: bool
    def __init__(self, expr: _Optional[_Union[PhysicalExprNode, _Mapping]] = ..., list: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ..., negated: _Optional[bool] = ...) -> None: ...

class PhysicalCaseNode(_message.Message):
    __slots__ = ("expr", "when_then_expr", "else_expr")
    EXPR_FIELD_NUMBER: _ClassVar[int]
    WHEN_THEN_EXPR_FIELD_NUMBER: _ClassVar[int]
    ELSE_EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: PhysicalExprNode
    when_then_expr: _containers.RepeatedCompositeFieldContainer[PhysicalWhenThen]
    else_expr: PhysicalExprNode
    def __init__(self, expr: _Optional[_Union[PhysicalExprNode, _Mapping]] = ..., when_then_expr: _Optional[_Iterable[_Union[PhysicalWhenThen, _Mapping]]] = ..., else_expr: _Optional[_Union[PhysicalExprNode, _Mapping]] = ...) -> None: ...

class PhysicalTryCastNode(_message.Message):
    __slots__ = ("expr", "arrow_type")
    EXPR_FIELD_NUMBER: _ClassVar[int]
    ARROW_TYPE_FIELD_NUMBER: _ClassVar[int]
    expr: PhysicalExprNode
    arrow_type: _datafusion_common_pb2.ArrowType
    def __init__(self, expr: _Optional[_Union[PhysicalExprNode, _Mapping]] = ..., arrow_type: _Optional[_Union[_datafusion_common_pb2.ArrowType, _Mapping]] = ...) -> None: ...

class PhysicalCastNode(_message.Message):
    __slots__ = ("expr", "arrow_type")
    EXPR_FIELD_NUMBER: _ClassVar[int]
    ARROW_TYPE_FIELD_NUMBER: _ClassVar[int]
    expr: PhysicalExprNode
    arrow_type: _datafusion_common_pb2.ArrowType
    def __init__(self, expr: _Optional[_Union[PhysicalExprNode, _Mapping]] = ..., arrow_type: _Optional[_Union[_datafusion_common_pb2.ArrowType, _Mapping]] = ...) -> None: ...

class PhysicalNegativeNode(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: PhysicalExprNode
    def __init__(self, expr: _Optional[_Union[PhysicalExprNode, _Mapping]] = ...) -> None: ...

class PhysicalExtensionExprNode(_message.Message):
    __slots__ = ("expr", "inputs")
    EXPR_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    expr: bytes
    inputs: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    def __init__(self, expr: _Optional[bytes] = ..., inputs: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ...) -> None: ...

class PhysicalHashExprNode(_message.Message):
    __slots__ = ("on_columns", "seed0", "description")
    ON_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    SEED0_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    on_columns: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    seed0: int
    description: str
    def __init__(self, on_columns: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ..., seed0: _Optional[int] = ..., description: _Optional[str] = ...) -> None: ...

class PhysicalRangeExprNode(_message.Message):
    __slots__ = ("sort_expr", "split_point")
    SORT_EXPR_FIELD_NUMBER: _ClassVar[int]
    SPLIT_POINT_FIELD_NUMBER: _ClassVar[int]
    sort_expr: _containers.RepeatedCompositeFieldContainer[PhysicalSortExprNode]
    split_point: _containers.RepeatedCompositeFieldContainer[PhysicalRangeSplitPoint]
    def __init__(self, sort_expr: _Optional[_Iterable[_Union[PhysicalSortExprNode, _Mapping]]] = ..., split_point: _Optional[_Iterable[_Union[PhysicalRangeSplitPoint, _Mapping]]] = ...) -> None: ...

class FilterExecNode(_message.Message):
    __slots__ = ("input", "expr", "default_filter_selectivity", "projection", "batch_size", "fetch")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_FILTER_SELECTIVITY_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_FIELD_NUMBER: _ClassVar[int]
    BATCH_SIZE_FIELD_NUMBER: _ClassVar[int]
    FETCH_FIELD_NUMBER: _ClassVar[int]
    input: PhysicalPlanNode
    expr: PhysicalExprNode
    default_filter_selectivity: int
    projection: _containers.RepeatedScalarFieldContainer[int]
    batch_size: int
    fetch: int
    def __init__(self, input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., expr: _Optional[_Union[PhysicalExprNode, _Mapping]] = ..., default_filter_selectivity: _Optional[int] = ..., projection: _Optional[_Iterable[int]] = ..., batch_size: _Optional[int] = ..., fetch: _Optional[int] = ...) -> None: ...

class FileGroup(_message.Message):
    __slots__ = ("files",)
    FILES_FIELD_NUMBER: _ClassVar[int]
    files: _containers.RepeatedCompositeFieldContainer[PartitionedFile]
    def __init__(self, files: _Optional[_Iterable[_Union[PartitionedFile, _Mapping]]] = ...) -> None: ...

class ScanLimit(_message.Message):
    __slots__ = ("limit",)
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    limit: int
    def __init__(self, limit: _Optional[int] = ...) -> None: ...

class PhysicalSortExprNodeCollection(_message.Message):
    __slots__ = ("physical_sort_expr_nodes",)
    PHYSICAL_SORT_EXPR_NODES_FIELD_NUMBER: _ClassVar[int]
    physical_sort_expr_nodes: _containers.RepeatedCompositeFieldContainer[PhysicalSortExprNode]
    def __init__(self, physical_sort_expr_nodes: _Optional[_Iterable[_Union[PhysicalSortExprNode, _Mapping]]] = ...) -> None: ...

class ProjectionExpr(_message.Message):
    __slots__ = ("alias", "expr")
    ALIAS_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    alias: str
    expr: PhysicalExprNode
    def __init__(self, alias: _Optional[str] = ..., expr: _Optional[_Union[PhysicalExprNode, _Mapping]] = ...) -> None: ...

class ProjectionExprs(_message.Message):
    __slots__ = ("projections",)
    PROJECTIONS_FIELD_NUMBER: _ClassVar[int]
    projections: _containers.RepeatedCompositeFieldContainer[ProjectionExpr]
    def __init__(self, projections: _Optional[_Iterable[_Union[ProjectionExpr, _Mapping]]] = ...) -> None: ...

class FileScanExecConf(_message.Message):
    __slots__ = ("file_groups", "schema", "projection", "limit", "statistics", "table_partition_cols", "object_store_url", "output_ordering", "constraints", "batch_size", "projection_exprs", "output_partitioning")
    FILE_GROUPS_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    STATISTICS_FIELD_NUMBER: _ClassVar[int]
    TABLE_PARTITION_COLS_FIELD_NUMBER: _ClassVar[int]
    OBJECT_STORE_URL_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_ORDERING_FIELD_NUMBER: _ClassVar[int]
    CONSTRAINTS_FIELD_NUMBER: _ClassVar[int]
    BATCH_SIZE_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_EXPRS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_PARTITIONING_FIELD_NUMBER: _ClassVar[int]
    file_groups: _containers.RepeatedCompositeFieldContainer[FileGroup]
    schema: _datafusion_common_pb2.Schema
    projection: _containers.RepeatedScalarFieldContainer[int]
    limit: ScanLimit
    statistics: _datafusion_common_pb2.Statistics
    table_partition_cols: _containers.RepeatedScalarFieldContainer[str]
    object_store_url: str
    output_ordering: _containers.RepeatedCompositeFieldContainer[PhysicalSortExprNodeCollection]
    constraints: _datafusion_common_pb2.Constraints
    batch_size: int
    projection_exprs: ProjectionExprs
    output_partitioning: Partitioning
    def __init__(self, file_groups: _Optional[_Iterable[_Union[FileGroup, _Mapping]]] = ..., schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., projection: _Optional[_Iterable[int]] = ..., limit: _Optional[_Union[ScanLimit, _Mapping]] = ..., statistics: _Optional[_Union[_datafusion_common_pb2.Statistics, _Mapping]] = ..., table_partition_cols: _Optional[_Iterable[str]] = ..., object_store_url: _Optional[str] = ..., output_ordering: _Optional[_Iterable[_Union[PhysicalSortExprNodeCollection, _Mapping]]] = ..., constraints: _Optional[_Union[_datafusion_common_pb2.Constraints, _Mapping]] = ..., batch_size: _Optional[int] = ..., projection_exprs: _Optional[_Union[ProjectionExprs, _Mapping]] = ..., output_partitioning: _Optional[_Union[Partitioning, _Mapping]] = ...) -> None: ...

class ParquetScanExecNode(_message.Message):
    __slots__ = ("base_conf", "predicate", "parquet_options")
    BASE_CONF_FIELD_NUMBER: _ClassVar[int]
    PREDICATE_FIELD_NUMBER: _ClassVar[int]
    PARQUET_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    base_conf: FileScanExecConf
    predicate: PhysicalExprNode
    parquet_options: _datafusion_common_pb2.TableParquetOptions
    def __init__(self, base_conf: _Optional[_Union[FileScanExecConf, _Mapping]] = ..., predicate: _Optional[_Union[PhysicalExprNode, _Mapping]] = ..., parquet_options: _Optional[_Union[_datafusion_common_pb2.TableParquetOptions, _Mapping]] = ...) -> None: ...

class CsvScanExecNode(_message.Message):
    __slots__ = ("base_conf", "has_header", "delimiter", "quote", "escape", "comment", "newlines_in_values", "truncate_rows")
    BASE_CONF_FIELD_NUMBER: _ClassVar[int]
    HAS_HEADER_FIELD_NUMBER: _ClassVar[int]
    DELIMITER_FIELD_NUMBER: _ClassVar[int]
    QUOTE_FIELD_NUMBER: _ClassVar[int]
    ESCAPE_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    NEWLINES_IN_VALUES_FIELD_NUMBER: _ClassVar[int]
    TRUNCATE_ROWS_FIELD_NUMBER: _ClassVar[int]
    base_conf: FileScanExecConf
    has_header: bool
    delimiter: str
    quote: str
    escape: str
    comment: str
    newlines_in_values: bool
    truncate_rows: bool
    def __init__(self, base_conf: _Optional[_Union[FileScanExecConf, _Mapping]] = ..., has_header: _Optional[bool] = ..., delimiter: _Optional[str] = ..., quote: _Optional[str] = ..., escape: _Optional[str] = ..., comment: _Optional[str] = ..., newlines_in_values: _Optional[bool] = ..., truncate_rows: _Optional[bool] = ...) -> None: ...

class JsonScanExecNode(_message.Message):
    __slots__ = ("base_conf",)
    BASE_CONF_FIELD_NUMBER: _ClassVar[int]
    base_conf: FileScanExecConf
    def __init__(self, base_conf: _Optional[_Union[FileScanExecConf, _Mapping]] = ...) -> None: ...

class AvroScanExecNode(_message.Message):
    __slots__ = ("base_conf",)
    BASE_CONF_FIELD_NUMBER: _ClassVar[int]
    base_conf: FileScanExecConf
    def __init__(self, base_conf: _Optional[_Union[FileScanExecConf, _Mapping]] = ...) -> None: ...

class ArrowScanExecNode(_message.Message):
    __slots__ = ("base_conf",)
    BASE_CONF_FIELD_NUMBER: _ClassVar[int]
    base_conf: FileScanExecConf
    def __init__(self, base_conf: _Optional[_Union[FileScanExecConf, _Mapping]] = ...) -> None: ...

class MemoryScanExecNode(_message.Message):
    __slots__ = ("partitions", "schema", "projection", "sort_information", "show_sizes", "fetch")
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_FIELD_NUMBER: _ClassVar[int]
    SORT_INFORMATION_FIELD_NUMBER: _ClassVar[int]
    SHOW_SIZES_FIELD_NUMBER: _ClassVar[int]
    FETCH_FIELD_NUMBER: _ClassVar[int]
    partitions: _containers.RepeatedScalarFieldContainer[bytes]
    schema: _datafusion_common_pb2.Schema
    projection: _containers.RepeatedScalarFieldContainer[int]
    sort_information: _containers.RepeatedCompositeFieldContainer[PhysicalSortExprNodeCollection]
    show_sizes: bool
    fetch: int
    def __init__(self, partitions: _Optional[_Iterable[bytes]] = ..., schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., projection: _Optional[_Iterable[int]] = ..., sort_information: _Optional[_Iterable[_Union[PhysicalSortExprNodeCollection, _Mapping]]] = ..., show_sizes: _Optional[bool] = ..., fetch: _Optional[int] = ...) -> None: ...

class CooperativeExecNode(_message.Message):
    __slots__ = ("input",)
    INPUT_FIELD_NUMBER: _ClassVar[int]
    input: PhysicalPlanNode
    def __init__(self, input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ...) -> None: ...

class HashJoinExecNode(_message.Message):
    __slots__ = ("left", "right", "on", "join_type", "partition_mode", "null_equality", "filter", "projection", "null_aware", "dynamic_filter", "fetch")
    LEFT_FIELD_NUMBER: _ClassVar[int]
    RIGHT_FIELD_NUMBER: _ClassVar[int]
    ON_FIELD_NUMBER: _ClassVar[int]
    JOIN_TYPE_FIELD_NUMBER: _ClassVar[int]
    PARTITION_MODE_FIELD_NUMBER: _ClassVar[int]
    NULL_EQUALITY_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_FIELD_NUMBER: _ClassVar[int]
    NULL_AWARE_FIELD_NUMBER: _ClassVar[int]
    DYNAMIC_FILTER_FIELD_NUMBER: _ClassVar[int]
    FETCH_FIELD_NUMBER: _ClassVar[int]
    left: PhysicalPlanNode
    right: PhysicalPlanNode
    on: _containers.RepeatedCompositeFieldContainer[JoinOn]
    join_type: _datafusion_common_pb2.JoinType
    partition_mode: PartitionMode
    null_equality: _datafusion_common_pb2.NullEquality
    filter: JoinFilter
    projection: _containers.RepeatedScalarFieldContainer[int]
    null_aware: bool
    dynamic_filter: PhysicalExprNode
    fetch: int
    def __init__(self, left: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., right: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., on: _Optional[_Iterable[_Union[JoinOn, _Mapping]]] = ..., join_type: _Optional[_Union[_datafusion_common_pb2.JoinType, str]] = ..., partition_mode: _Optional[_Union[PartitionMode, str]] = ..., null_equality: _Optional[_Union[_datafusion_common_pb2.NullEquality, str]] = ..., filter: _Optional[_Union[JoinFilter, _Mapping]] = ..., projection: _Optional[_Iterable[int]] = ..., null_aware: _Optional[bool] = ..., dynamic_filter: _Optional[_Union[PhysicalExprNode, _Mapping]] = ..., fetch: _Optional[int] = ...) -> None: ...

class SymmetricHashJoinExecNode(_message.Message):
    __slots__ = ("left", "right", "on", "join_type", "partition_mode", "null_equality", "filter", "left_sort_exprs", "right_sort_exprs")
    LEFT_FIELD_NUMBER: _ClassVar[int]
    RIGHT_FIELD_NUMBER: _ClassVar[int]
    ON_FIELD_NUMBER: _ClassVar[int]
    JOIN_TYPE_FIELD_NUMBER: _ClassVar[int]
    PARTITION_MODE_FIELD_NUMBER: _ClassVar[int]
    NULL_EQUALITY_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    LEFT_SORT_EXPRS_FIELD_NUMBER: _ClassVar[int]
    RIGHT_SORT_EXPRS_FIELD_NUMBER: _ClassVar[int]
    left: PhysicalPlanNode
    right: PhysicalPlanNode
    on: _containers.RepeatedCompositeFieldContainer[JoinOn]
    join_type: _datafusion_common_pb2.JoinType
    partition_mode: StreamPartitionMode
    null_equality: _datafusion_common_pb2.NullEquality
    filter: JoinFilter
    left_sort_exprs: _containers.RepeatedCompositeFieldContainer[PhysicalSortExprNode]
    right_sort_exprs: _containers.RepeatedCompositeFieldContainer[PhysicalSortExprNode]
    def __init__(self, left: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., right: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., on: _Optional[_Iterable[_Union[JoinOn, _Mapping]]] = ..., join_type: _Optional[_Union[_datafusion_common_pb2.JoinType, str]] = ..., partition_mode: _Optional[_Union[StreamPartitionMode, str]] = ..., null_equality: _Optional[_Union[_datafusion_common_pb2.NullEquality, str]] = ..., filter: _Optional[_Union[JoinFilter, _Mapping]] = ..., left_sort_exprs: _Optional[_Iterable[_Union[PhysicalSortExprNode, _Mapping]]] = ..., right_sort_exprs: _Optional[_Iterable[_Union[PhysicalSortExprNode, _Mapping]]] = ...) -> None: ...

class InterleaveExecNode(_message.Message):
    __slots__ = ("inputs",)
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    inputs: _containers.RepeatedCompositeFieldContainer[PhysicalPlanNode]
    def __init__(self, inputs: _Optional[_Iterable[_Union[PhysicalPlanNode, _Mapping]]] = ...) -> None: ...

class UnionExecNode(_message.Message):
    __slots__ = ("inputs",)
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    inputs: _containers.RepeatedCompositeFieldContainer[PhysicalPlanNode]
    def __init__(self, inputs: _Optional[_Iterable[_Union[PhysicalPlanNode, _Mapping]]] = ...) -> None: ...

class ExplainExecNode(_message.Message):
    __slots__ = ("schema", "stringified_plans", "verbose")
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    STRINGIFIED_PLANS_FIELD_NUMBER: _ClassVar[int]
    VERBOSE_FIELD_NUMBER: _ClassVar[int]
    schema: _datafusion_common_pb2.Schema
    stringified_plans: _containers.RepeatedCompositeFieldContainer[StringifiedPlan]
    verbose: bool
    def __init__(self, schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., stringified_plans: _Optional[_Iterable[_Union[StringifiedPlan, _Mapping]]] = ..., verbose: _Optional[bool] = ...) -> None: ...

class AnalyzeExecNode(_message.Message):
    __slots__ = ("verbose", "show_statistics", "input", "schema", "has_metric_categories", "metric_categories", "format")
    VERBOSE_FIELD_NUMBER: _ClassVar[int]
    SHOW_STATISTICS_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    HAS_METRIC_CATEGORIES_FIELD_NUMBER: _ClassVar[int]
    METRIC_CATEGORIES_FIELD_NUMBER: _ClassVar[int]
    FORMAT_FIELD_NUMBER: _ClassVar[int]
    verbose: bool
    show_statistics: bool
    input: PhysicalPlanNode
    schema: _datafusion_common_pb2.Schema
    has_metric_categories: bool
    metric_categories: _containers.RepeatedScalarFieldContainer[str]
    format: _datafusion_common_pb2.ExplainFormat
    def __init__(self, verbose: _Optional[bool] = ..., show_statistics: _Optional[bool] = ..., input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., has_metric_categories: _Optional[bool] = ..., metric_categories: _Optional[_Iterable[str]] = ..., format: _Optional[_Union[_datafusion_common_pb2.ExplainFormat, str]] = ...) -> None: ...

class CrossJoinExecNode(_message.Message):
    __slots__ = ("left", "right")
    LEFT_FIELD_NUMBER: _ClassVar[int]
    RIGHT_FIELD_NUMBER: _ClassVar[int]
    left: PhysicalPlanNode
    right: PhysicalPlanNode
    def __init__(self, left: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., right: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ...) -> None: ...

class PhysicalColumn(_message.Message):
    __slots__ = ("name", "index")
    NAME_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    name: str
    index: int
    def __init__(self, name: _Optional[str] = ..., index: _Optional[int] = ...) -> None: ...

class UnknownColumn(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class JoinOn(_message.Message):
    __slots__ = ("left", "right")
    LEFT_FIELD_NUMBER: _ClassVar[int]
    RIGHT_FIELD_NUMBER: _ClassVar[int]
    left: PhysicalExprNode
    right: PhysicalExprNode
    def __init__(self, left: _Optional[_Union[PhysicalExprNode, _Mapping]] = ..., right: _Optional[_Union[PhysicalExprNode, _Mapping]] = ...) -> None: ...

class EmptyExecNode(_message.Message):
    __slots__ = ("schema", "partitions")
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    schema: _datafusion_common_pb2.Schema
    partitions: int
    def __init__(self, schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., partitions: _Optional[int] = ...) -> None: ...

class PlaceholderRowExecNode(_message.Message):
    __slots__ = ("schema", "partitions")
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    schema: _datafusion_common_pb2.Schema
    partitions: int
    def __init__(self, schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., partitions: _Optional[int] = ...) -> None: ...

class ProjectionExecNode(_message.Message):
    __slots__ = ("input", "expr", "expr_name")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    EXPR_NAME_FIELD_NUMBER: _ClassVar[int]
    input: PhysicalPlanNode
    expr: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    expr_name: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., expr: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ..., expr_name: _Optional[_Iterable[str]] = ...) -> None: ...

class PartiallySortedInputOrderMode(_message.Message):
    __slots__ = ("columns",)
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, columns: _Optional[_Iterable[int]] = ...) -> None: ...

class WindowAggExecNode(_message.Message):
    __slots__ = ("input", "window_expr", "partition_keys", "linear", "partially_sorted", "sorted")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    WINDOW_EXPR_FIELD_NUMBER: _ClassVar[int]
    PARTITION_KEYS_FIELD_NUMBER: _ClassVar[int]
    LINEAR_FIELD_NUMBER: _ClassVar[int]
    PARTIALLY_SORTED_FIELD_NUMBER: _ClassVar[int]
    SORTED_FIELD_NUMBER: _ClassVar[int]
    input: PhysicalPlanNode
    window_expr: _containers.RepeatedCompositeFieldContainer[PhysicalWindowExprNode]
    partition_keys: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    linear: _datafusion_common_pb2.EmptyMessage
    partially_sorted: PartiallySortedInputOrderMode
    sorted: _datafusion_common_pb2.EmptyMessage
    def __init__(self, input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., window_expr: _Optional[_Iterable[_Union[PhysicalWindowExprNode, _Mapping]]] = ..., partition_keys: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ..., linear: _Optional[_Union[_datafusion_common_pb2.EmptyMessage, _Mapping]] = ..., partially_sorted: _Optional[_Union[PartiallySortedInputOrderMode, _Mapping]] = ..., sorted: _Optional[_Union[_datafusion_common_pb2.EmptyMessage, _Mapping]] = ...) -> None: ...

class MaybeFilter(_message.Message):
    __slots__ = ("expr",)
    EXPR_FIELD_NUMBER: _ClassVar[int]
    expr: PhysicalExprNode
    def __init__(self, expr: _Optional[_Union[PhysicalExprNode, _Mapping]] = ...) -> None: ...

class MaybePhysicalSortExprs(_message.Message):
    __slots__ = ("sort_expr",)
    SORT_EXPR_FIELD_NUMBER: _ClassVar[int]
    sort_expr: _containers.RepeatedCompositeFieldContainer[PhysicalSortExprNode]
    def __init__(self, sort_expr: _Optional[_Iterable[_Union[PhysicalSortExprNode, _Mapping]]] = ...) -> None: ...

class AggLimit(_message.Message):
    __slots__ = ("limit", "descending")
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    DESCENDING_FIELD_NUMBER: _ClassVar[int]
    limit: int
    descending: bool
    def __init__(self, limit: _Optional[int] = ..., descending: _Optional[bool] = ...) -> None: ...

class AggregateExecNode(_message.Message):
    __slots__ = ("group_expr", "aggr_expr", "mode", "input", "group_expr_name", "aggr_expr_name", "input_schema", "null_expr", "groups", "filter_expr", "limit", "has_grouping_set", "dynamic_filter", "schema")
    GROUP_EXPR_FIELD_NUMBER: _ClassVar[int]
    AGGR_EXPR_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    GROUP_EXPR_NAME_FIELD_NUMBER: _ClassVar[int]
    AGGR_EXPR_NAME_FIELD_NUMBER: _ClassVar[int]
    INPUT_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    NULL_EXPR_FIELD_NUMBER: _ClassVar[int]
    GROUPS_FIELD_NUMBER: _ClassVar[int]
    FILTER_EXPR_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    HAS_GROUPING_SET_FIELD_NUMBER: _ClassVar[int]
    DYNAMIC_FILTER_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    group_expr: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    aggr_expr: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    mode: AggregateMode
    input: PhysicalPlanNode
    group_expr_name: _containers.RepeatedScalarFieldContainer[str]
    aggr_expr_name: _containers.RepeatedScalarFieldContainer[str]
    input_schema: _datafusion_common_pb2.Schema
    null_expr: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    groups: _containers.RepeatedScalarFieldContainer[bool]
    filter_expr: _containers.RepeatedCompositeFieldContainer[MaybeFilter]
    limit: AggLimit
    has_grouping_set: bool
    dynamic_filter: PhysicalExprNode
    schema: _datafusion_common_pb2.Schema
    def __init__(self, group_expr: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ..., aggr_expr: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ..., mode: _Optional[_Union[AggregateMode, str]] = ..., input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., group_expr_name: _Optional[_Iterable[str]] = ..., aggr_expr_name: _Optional[_Iterable[str]] = ..., input_schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., null_expr: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ..., groups: _Optional[_Iterable[bool]] = ..., filter_expr: _Optional[_Iterable[_Union[MaybeFilter, _Mapping]]] = ..., limit: _Optional[_Union[AggLimit, _Mapping]] = ..., has_grouping_set: _Optional[bool] = ..., dynamic_filter: _Optional[_Union[PhysicalExprNode, _Mapping]] = ..., schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ...) -> None: ...

class GlobalLimitExecNode(_message.Message):
    __slots__ = ("input", "skip", "fetch", "required_ordering")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    SKIP_FIELD_NUMBER: _ClassVar[int]
    FETCH_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_ORDERING_FIELD_NUMBER: _ClassVar[int]
    input: PhysicalPlanNode
    skip: int
    fetch: int
    required_ordering: _containers.RepeatedCompositeFieldContainer[PhysicalSortExprNode]
    def __init__(self, input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., skip: _Optional[int] = ..., fetch: _Optional[int] = ..., required_ordering: _Optional[_Iterable[_Union[PhysicalSortExprNode, _Mapping]]] = ...) -> None: ...

class LocalLimitExecNode(_message.Message):
    __slots__ = ("input", "fetch", "required_ordering")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    FETCH_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_ORDERING_FIELD_NUMBER: _ClassVar[int]
    input: PhysicalPlanNode
    fetch: int
    required_ordering: _containers.RepeatedCompositeFieldContainer[PhysicalSortExprNode]
    def __init__(self, input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., fetch: _Optional[int] = ..., required_ordering: _Optional[_Iterable[_Union[PhysicalSortExprNode, _Mapping]]] = ...) -> None: ...

class SortExecNode(_message.Message):
    __slots__ = ("input", "expr", "fetch", "preserve_partitioning", "dynamic_filter")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    FETCH_FIELD_NUMBER: _ClassVar[int]
    PRESERVE_PARTITIONING_FIELD_NUMBER: _ClassVar[int]
    DYNAMIC_FILTER_FIELD_NUMBER: _ClassVar[int]
    input: PhysicalPlanNode
    expr: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    fetch: int
    preserve_partitioning: bool
    dynamic_filter: PhysicalExprNode
    def __init__(self, input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., expr: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ..., fetch: _Optional[int] = ..., preserve_partitioning: _Optional[bool] = ..., dynamic_filter: _Optional[_Union[PhysicalExprNode, _Mapping]] = ...) -> None: ...

class SortPreservingMergeExecNode(_message.Message):
    __slots__ = ("input", "expr", "fetch")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    FETCH_FIELD_NUMBER: _ClassVar[int]
    input: PhysicalPlanNode
    expr: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    fetch: int
    def __init__(self, input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., expr: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ..., fetch: _Optional[int] = ...) -> None: ...

class NestedLoopJoinExecNode(_message.Message):
    __slots__ = ("left", "right", "join_type", "filter", "projection")
    LEFT_FIELD_NUMBER: _ClassVar[int]
    RIGHT_FIELD_NUMBER: _ClassVar[int]
    JOIN_TYPE_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_FIELD_NUMBER: _ClassVar[int]
    left: PhysicalPlanNode
    right: PhysicalPlanNode
    join_type: _datafusion_common_pb2.JoinType
    filter: JoinFilter
    projection: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, left: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., right: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., join_type: _Optional[_Union[_datafusion_common_pb2.JoinType, str]] = ..., filter: _Optional[_Union[JoinFilter, _Mapping]] = ..., projection: _Optional[_Iterable[int]] = ...) -> None: ...

class CoalesceBatchesExecNode(_message.Message):
    __slots__ = ("input", "target_batch_size", "fetch")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    TARGET_BATCH_SIZE_FIELD_NUMBER: _ClassVar[int]
    FETCH_FIELD_NUMBER: _ClassVar[int]
    input: PhysicalPlanNode
    target_batch_size: int
    fetch: int
    def __init__(self, input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., target_batch_size: _Optional[int] = ..., fetch: _Optional[int] = ...) -> None: ...

class CoalescePartitionsExecNode(_message.Message):
    __slots__ = ("input", "fetch")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    FETCH_FIELD_NUMBER: _ClassVar[int]
    input: PhysicalPlanNode
    fetch: int
    def __init__(self, input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., fetch: _Optional[int] = ...) -> None: ...

class PhysicalHashRepartition(_message.Message):
    __slots__ = ("hash_expr", "partition_count")
    HASH_EXPR_FIELD_NUMBER: _ClassVar[int]
    PARTITION_COUNT_FIELD_NUMBER: _ClassVar[int]
    hash_expr: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    partition_count: int
    def __init__(self, hash_expr: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ..., partition_count: _Optional[int] = ...) -> None: ...

class PhysicalRangePartitioning(_message.Message):
    __slots__ = ("sort_expr", "split_point")
    SORT_EXPR_FIELD_NUMBER: _ClassVar[int]
    SPLIT_POINT_FIELD_NUMBER: _ClassVar[int]
    sort_expr: _containers.RepeatedCompositeFieldContainer[PhysicalSortExprNode]
    split_point: _containers.RepeatedCompositeFieldContainer[PhysicalRangeSplitPoint]
    def __init__(self, sort_expr: _Optional[_Iterable[_Union[PhysicalSortExprNode, _Mapping]]] = ..., split_point: _Optional[_Iterable[_Union[PhysicalRangeSplitPoint, _Mapping]]] = ...) -> None: ...

class PhysicalRangeSplitPoint(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: _containers.RepeatedCompositeFieldContainer[_datafusion_common_pb2.ScalarValue]
    def __init__(self, value: _Optional[_Iterable[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]]] = ...) -> None: ...

class RepartitionExecNode(_message.Message):
    __slots__ = ("input", "partitioning", "preserve_order")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    PARTITIONING_FIELD_NUMBER: _ClassVar[int]
    PRESERVE_ORDER_FIELD_NUMBER: _ClassVar[int]
    input: PhysicalPlanNode
    partitioning: Partitioning
    preserve_order: bool
    def __init__(self, input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., partitioning: _Optional[_Union[Partitioning, _Mapping]] = ..., preserve_order: _Optional[bool] = ...) -> None: ...

class Partitioning(_message.Message):
    __slots__ = ("round_robin", "hash", "unknown", "range")
    ROUND_ROBIN_FIELD_NUMBER: _ClassVar[int]
    HASH_FIELD_NUMBER: _ClassVar[int]
    UNKNOWN_FIELD_NUMBER: _ClassVar[int]
    RANGE_FIELD_NUMBER: _ClassVar[int]
    round_robin: int
    hash: PhysicalHashRepartition
    unknown: int
    range: PhysicalRangePartitioning
    def __init__(self, round_robin: _Optional[int] = ..., hash: _Optional[_Union[PhysicalHashRepartition, _Mapping]] = ..., unknown: _Optional[int] = ..., range: _Optional[_Union[PhysicalRangePartitioning, _Mapping]] = ...) -> None: ...

class JoinFilter(_message.Message):
    __slots__ = ("expression", "column_indices", "schema")
    EXPRESSION_FIELD_NUMBER: _ClassVar[int]
    COLUMN_INDICES_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    expression: PhysicalExprNode
    column_indices: _containers.RepeatedCompositeFieldContainer[ColumnIndex]
    schema: _datafusion_common_pb2.Schema
    def __init__(self, expression: _Optional[_Union[PhysicalExprNode, _Mapping]] = ..., column_indices: _Optional[_Iterable[_Union[ColumnIndex, _Mapping]]] = ..., schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ...) -> None: ...

class ColumnIndex(_message.Message):
    __slots__ = ("index", "side")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    SIDE_FIELD_NUMBER: _ClassVar[int]
    index: int
    side: _datafusion_common_pb2.JoinSide
    def __init__(self, index: _Optional[int] = ..., side: _Optional[_Union[_datafusion_common_pb2.JoinSide, str]] = ...) -> None: ...

class PartitionedFile(_message.Message):
    __slots__ = ("path", "size", "last_modified_ns", "partition_values", "range", "statistics", "arrow_schema")
    PATH_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    LAST_MODIFIED_NS_FIELD_NUMBER: _ClassVar[int]
    PARTITION_VALUES_FIELD_NUMBER: _ClassVar[int]
    RANGE_FIELD_NUMBER: _ClassVar[int]
    STATISTICS_FIELD_NUMBER: _ClassVar[int]
    ARROW_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    path: str
    size: int
    last_modified_ns: int
    partition_values: _containers.RepeatedCompositeFieldContainer[_datafusion_common_pb2.ScalarValue]
    range: FileRange
    statistics: _datafusion_common_pb2.Statistics
    arrow_schema: _datafusion_common_pb2.Schema
    def __init__(self, path: _Optional[str] = ..., size: _Optional[int] = ..., last_modified_ns: _Optional[int] = ..., partition_values: _Optional[_Iterable[_Union[_datafusion_common_pb2.ScalarValue, _Mapping]]] = ..., range: _Optional[_Union[FileRange, _Mapping]] = ..., statistics: _Optional[_Union[_datafusion_common_pb2.Statistics, _Mapping]] = ..., arrow_schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ...) -> None: ...

class FileRange(_message.Message):
    __slots__ = ("start", "end")
    START_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    start: int
    end: int
    def __init__(self, start: _Optional[int] = ..., end: _Optional[int] = ...) -> None: ...

class PartitionStats(_message.Message):
    __slots__ = ("num_rows", "num_batches", "num_bytes", "column_stats")
    NUM_ROWS_FIELD_NUMBER: _ClassVar[int]
    NUM_BATCHES_FIELD_NUMBER: _ClassVar[int]
    NUM_BYTES_FIELD_NUMBER: _ClassVar[int]
    COLUMN_STATS_FIELD_NUMBER: _ClassVar[int]
    num_rows: int
    num_batches: int
    num_bytes: int
    column_stats: _containers.RepeatedCompositeFieldContainer[_datafusion_common_pb2.ColumnStats]
    def __init__(self, num_rows: _Optional[int] = ..., num_batches: _Optional[int] = ..., num_bytes: _Optional[int] = ..., column_stats: _Optional[_Iterable[_Union[_datafusion_common_pb2.ColumnStats, _Mapping]]] = ...) -> None: ...

class RecursiveQueryNode(_message.Message):
    __slots__ = ("name", "static_term", "recursive_term", "is_distinct")
    NAME_FIELD_NUMBER: _ClassVar[int]
    STATIC_TERM_FIELD_NUMBER: _ClassVar[int]
    RECURSIVE_TERM_FIELD_NUMBER: _ClassVar[int]
    IS_DISTINCT_FIELD_NUMBER: _ClassVar[int]
    name: str
    static_term: LogicalPlanNode
    recursive_term: LogicalPlanNode
    is_distinct: bool
    def __init__(self, name: _Optional[str] = ..., static_term: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., recursive_term: _Optional[_Union[LogicalPlanNode, _Mapping]] = ..., is_distinct: _Optional[bool] = ...) -> None: ...

class CteWorkTableScanNode(_message.Message):
    __slots__ = ("name", "schema")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    name: str
    schema: _datafusion_common_pb2.Schema
    def __init__(self, name: _Optional[str] = ..., schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ...) -> None: ...

class EmptyTableScanNode(_message.Message):
    __slots__ = ("table_name", "schema", "projection", "filters")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_FIELD_NUMBER: _ClassVar[int]
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    table_name: TableReference
    schema: _datafusion_common_pb2.Schema
    projection: ProjectionColumns
    filters: _containers.RepeatedCompositeFieldContainer[LogicalExprNode]
    def __init__(self, table_name: _Optional[_Union[TableReference, _Mapping]] = ..., schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., projection: _Optional[_Union[ProjectionColumns, _Mapping]] = ..., filters: _Optional[_Iterable[_Union[LogicalExprNode, _Mapping]]] = ...) -> None: ...

class GenerateSeriesArgsContainsNull(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: GenerateSeriesName
    def __init__(self, name: _Optional[_Union[GenerateSeriesName, str]] = ...) -> None: ...

class GenerateSeriesArgsInt64(_message.Message):
    __slots__ = ("start", "end", "step", "include_end", "name")
    START_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_END_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    start: int
    end: int
    step: int
    include_end: bool
    name: GenerateSeriesName
    def __init__(self, start: _Optional[int] = ..., end: _Optional[int] = ..., step: _Optional[int] = ..., include_end: _Optional[bool] = ..., name: _Optional[_Union[GenerateSeriesName, str]] = ...) -> None: ...

class GenerateSeriesArgsTimestamp(_message.Message):
    __slots__ = ("start", "end", "step", "tz", "include_end", "name")
    START_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    TZ_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_END_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    start: int
    end: int
    step: _datafusion_common_pb2.IntervalMonthDayNanoValue
    tz: str
    include_end: bool
    name: GenerateSeriesName
    def __init__(self, start: _Optional[int] = ..., end: _Optional[int] = ..., step: _Optional[_Union[_datafusion_common_pb2.IntervalMonthDayNanoValue, _Mapping]] = ..., tz: _Optional[str] = ..., include_end: _Optional[bool] = ..., name: _Optional[_Union[GenerateSeriesName, str]] = ...) -> None: ...

class GenerateSeriesArgsDate(_message.Message):
    __slots__ = ("start", "end", "step", "include_end", "name")
    START_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_END_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    start: int
    end: int
    step: _datafusion_common_pb2.IntervalMonthDayNanoValue
    include_end: bool
    name: GenerateSeriesName
    def __init__(self, start: _Optional[int] = ..., end: _Optional[int] = ..., step: _Optional[_Union[_datafusion_common_pb2.IntervalMonthDayNanoValue, _Mapping]] = ..., include_end: _Optional[bool] = ..., name: _Optional[_Union[GenerateSeriesName, str]] = ...) -> None: ...

class GenerateSeriesNode(_message.Message):
    __slots__ = ("schema", "target_batch_size", "contains_null", "int64_args", "timestamp_args", "date_args")
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    TARGET_BATCH_SIZE_FIELD_NUMBER: _ClassVar[int]
    CONTAINS_NULL_FIELD_NUMBER: _ClassVar[int]
    INT64_ARGS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_ARGS_FIELD_NUMBER: _ClassVar[int]
    DATE_ARGS_FIELD_NUMBER: _ClassVar[int]
    schema: _datafusion_common_pb2.Schema
    target_batch_size: int
    contains_null: GenerateSeriesArgsContainsNull
    int64_args: GenerateSeriesArgsInt64
    timestamp_args: GenerateSeriesArgsTimestamp
    date_args: GenerateSeriesArgsDate
    def __init__(self, schema: _Optional[_Union[_datafusion_common_pb2.Schema, _Mapping]] = ..., target_batch_size: _Optional[int] = ..., contains_null: _Optional[_Union[GenerateSeriesArgsContainsNull, _Mapping]] = ..., int64_args: _Optional[_Union[GenerateSeriesArgsInt64, _Mapping]] = ..., timestamp_args: _Optional[_Union[GenerateSeriesArgsTimestamp, _Mapping]] = ..., date_args: _Optional[_Union[GenerateSeriesArgsDate, _Mapping]] = ...) -> None: ...

class SortMergeJoinExecNode(_message.Message):
    __slots__ = ("left", "right", "on", "join_type", "filter", "sort_options", "null_equality")
    LEFT_FIELD_NUMBER: _ClassVar[int]
    RIGHT_FIELD_NUMBER: _ClassVar[int]
    ON_FIELD_NUMBER: _ClassVar[int]
    JOIN_TYPE_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    SORT_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    NULL_EQUALITY_FIELD_NUMBER: _ClassVar[int]
    left: PhysicalPlanNode
    right: PhysicalPlanNode
    on: _containers.RepeatedCompositeFieldContainer[JoinOn]
    join_type: _datafusion_common_pb2.JoinType
    filter: JoinFilter
    sort_options: _containers.RepeatedCompositeFieldContainer[SortExprNode]
    null_equality: _datafusion_common_pb2.NullEquality
    def __init__(self, left: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., right: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., on: _Optional[_Iterable[_Union[JoinOn, _Mapping]]] = ..., join_type: _Optional[_Union[_datafusion_common_pb2.JoinType, str]] = ..., filter: _Optional[_Union[JoinFilter, _Mapping]] = ..., sort_options: _Optional[_Iterable[_Union[SortExprNode, _Mapping]]] = ..., null_equality: _Optional[_Union[_datafusion_common_pb2.NullEquality, str]] = ...) -> None: ...

class AsyncFuncExecNode(_message.Message):
    __slots__ = ("input", "async_exprs", "async_expr_names")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    ASYNC_EXPRS_FIELD_NUMBER: _ClassVar[int]
    ASYNC_EXPR_NAMES_FIELD_NUMBER: _ClassVar[int]
    input: PhysicalPlanNode
    async_exprs: _containers.RepeatedCompositeFieldContainer[PhysicalExprNode]
    async_expr_names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., async_exprs: _Optional[_Iterable[_Union[PhysicalExprNode, _Mapping]]] = ..., async_expr_names: _Optional[_Iterable[str]] = ...) -> None: ...

class BufferExecNode(_message.Message):
    __slots__ = ("input", "capacity")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    CAPACITY_FIELD_NUMBER: _ClassVar[int]
    input: PhysicalPlanNode
    capacity: int
    def __init__(self, input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., capacity: _Optional[int] = ...) -> None: ...

class ScalarSubqueryExecNode(_message.Message):
    __slots__ = ("input", "subqueries")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    SUBQUERIES_FIELD_NUMBER: _ClassVar[int]
    input: PhysicalPlanNode
    subqueries: _containers.RepeatedCompositeFieldContainer[PhysicalPlanNode]
    def __init__(self, input: _Optional[_Union[PhysicalPlanNode, _Mapping]] = ..., subqueries: _Optional[_Iterable[_Union[PhysicalPlanNode, _Mapping]]] = ...) -> None: ...

class PhysicalScalarSubqueryExprNode(_message.Message):
    __slots__ = ("data_type", "nullable", "index")
    DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
    NULLABLE_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    data_type: _datafusion_common_pb2.ArrowType
    nullable: bool
    index: int
    def __init__(self, data_type: _Optional[_Union[_datafusion_common_pb2.ArrowType, _Mapping]] = ..., nullable: _Optional[bool] = ..., index: _Optional[int] = ...) -> None: ...
