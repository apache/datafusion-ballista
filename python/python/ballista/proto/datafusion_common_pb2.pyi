from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class JoinType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INNER: _ClassVar[JoinType]
    LEFT: _ClassVar[JoinType]
    RIGHT: _ClassVar[JoinType]
    FULL: _ClassVar[JoinType]
    LEFTSEMI: _ClassVar[JoinType]
    LEFTANTI: _ClassVar[JoinType]
    RIGHTSEMI: _ClassVar[JoinType]
    RIGHTANTI: _ClassVar[JoinType]
    LEFTMARK: _ClassVar[JoinType]
    RIGHTMARK: _ClassVar[JoinType]

class JoinConstraint(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ON: _ClassVar[JoinConstraint]
    USING: _ClassVar[JoinConstraint]

class NullEquality(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NULL_EQUALS_NOTHING: _ClassVar[NullEquality]
    NULL_EQUALS_NULL: _ClassVar[NullEquality]

class TimeUnit(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    Second: _ClassVar[TimeUnit]
    Millisecond: _ClassVar[TimeUnit]
    Microsecond: _ClassVar[TimeUnit]
    Nanosecond: _ClassVar[TimeUnit]

class IntervalUnit(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    YearMonth: _ClassVar[IntervalUnit]
    DayTime: _ClassVar[IntervalUnit]
    MonthDayNano: _ClassVar[IntervalUnit]

class UnionMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    sparse: _ClassVar[UnionMode]
    dense: _ClassVar[UnionMode]

class CompressionTypeVariant(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    GZIP: _ClassVar[CompressionTypeVariant]
    BZIP2: _ClassVar[CompressionTypeVariant]
    XZ: _ClassVar[CompressionTypeVariant]
    ZSTD: _ClassVar[CompressionTypeVariant]
    UNCOMPRESSED: _ClassVar[CompressionTypeVariant]

class CsvQuoteStyle(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NECESSARY: _ClassVar[CsvQuoteStyle]
    ALWAYS: _ClassVar[CsvQuoteStyle]
    NON_NUMERIC: _ClassVar[CsvQuoteStyle]
    NEVER: _ClassVar[CsvQuoteStyle]

class JoinSide(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LEFT_SIDE: _ClassVar[JoinSide]
    RIGHT_SIDE: _ClassVar[JoinSide]
    NONE: _ClassVar[JoinSide]

class PrecisionInfo(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EXACT: _ClassVar[PrecisionInfo]
    INEXACT: _ClassVar[PrecisionInfo]
    ABSENT: _ClassVar[PrecisionInfo]

class ExplainFormat(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EXPLAIN_FORMAT_INDENT: _ClassVar[ExplainFormat]
    EXPLAIN_FORMAT_TREE: _ClassVar[ExplainFormat]
    EXPLAIN_FORMAT_PGJSON: _ClassVar[ExplainFormat]
    EXPLAIN_FORMAT_GRAPHVIZ: _ClassVar[ExplainFormat]

class MetricType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    METRIC_TYPE_SUMMARY: _ClassVar[MetricType]
    METRIC_TYPE_DEV: _ClassVar[MetricType]

class MetricCategory(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    METRIC_CATEGORY_ROWS: _ClassVar[MetricCategory]
    METRIC_CATEGORY_BYTES: _ClassVar[MetricCategory]
    METRIC_CATEGORY_TIMING: _ClassVar[MetricCategory]
    METRIC_CATEGORY_UNCATEGORIZED: _ClassVar[MetricCategory]
INNER: JoinType
LEFT: JoinType
RIGHT: JoinType
FULL: JoinType
LEFTSEMI: JoinType
LEFTANTI: JoinType
RIGHTSEMI: JoinType
RIGHTANTI: JoinType
LEFTMARK: JoinType
RIGHTMARK: JoinType
ON: JoinConstraint
USING: JoinConstraint
NULL_EQUALS_NOTHING: NullEquality
NULL_EQUALS_NULL: NullEquality
Second: TimeUnit
Millisecond: TimeUnit
Microsecond: TimeUnit
Nanosecond: TimeUnit
YearMonth: IntervalUnit
DayTime: IntervalUnit
MonthDayNano: IntervalUnit
sparse: UnionMode
dense: UnionMode
GZIP: CompressionTypeVariant
BZIP2: CompressionTypeVariant
XZ: CompressionTypeVariant
ZSTD: CompressionTypeVariant
UNCOMPRESSED: CompressionTypeVariant
NECESSARY: CsvQuoteStyle
ALWAYS: CsvQuoteStyle
NON_NUMERIC: CsvQuoteStyle
NEVER: CsvQuoteStyle
LEFT_SIDE: JoinSide
RIGHT_SIDE: JoinSide
NONE: JoinSide
EXACT: PrecisionInfo
INEXACT: PrecisionInfo
ABSENT: PrecisionInfo
EXPLAIN_FORMAT_INDENT: ExplainFormat
EXPLAIN_FORMAT_TREE: ExplainFormat
EXPLAIN_FORMAT_PGJSON: ExplainFormat
EXPLAIN_FORMAT_GRAPHVIZ: ExplainFormat
METRIC_TYPE_SUMMARY: MetricType
METRIC_TYPE_DEV: MetricType
METRIC_CATEGORY_ROWS: MetricCategory
METRIC_CATEGORY_BYTES: MetricCategory
METRIC_CATEGORY_TIMING: MetricCategory
METRIC_CATEGORY_UNCATEGORIZED: MetricCategory

class ColumnRelation(_message.Message):
    __slots__ = ("relation",)
    RELATION_FIELD_NUMBER: _ClassVar[int]
    relation: str
    def __init__(self, relation: _Optional[str] = ...) -> None: ...

class Column(_message.Message):
    __slots__ = ("name", "relation")
    NAME_FIELD_NUMBER: _ClassVar[int]
    RELATION_FIELD_NUMBER: _ClassVar[int]
    name: str
    relation: ColumnRelation
    def __init__(self, name: _Optional[str] = ..., relation: _Optional[_Union[ColumnRelation, _Mapping]] = ...) -> None: ...

class DfField(_message.Message):
    __slots__ = ("field", "qualifier")
    FIELD_FIELD_NUMBER: _ClassVar[int]
    QUALIFIER_FIELD_NUMBER: _ClassVar[int]
    field: Field
    qualifier: ColumnRelation
    def __init__(self, field: _Optional[_Union[Field, _Mapping]] = ..., qualifier: _Optional[_Union[ColumnRelation, _Mapping]] = ...) -> None: ...

class DfSchema(_message.Message):
    __slots__ = ("columns", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedCompositeFieldContainer[DfField]
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, columns: _Optional[_Iterable[_Union[DfField, _Mapping]]] = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class CsvFormat(_message.Message):
    __slots__ = ("options",)
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    options: CsvOptions
    def __init__(self, options: _Optional[_Union[CsvOptions, _Mapping]] = ...) -> None: ...

class ParquetFormat(_message.Message):
    __slots__ = ("options",)
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    options: TableParquetOptions
    def __init__(self, options: _Optional[_Union[TableParquetOptions, _Mapping]] = ...) -> None: ...

class AvroFormat(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class NdJsonFormat(_message.Message):
    __slots__ = ("options",)
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    options: JsonOptions
    def __init__(self, options: _Optional[_Union[JsonOptions, _Mapping]] = ...) -> None: ...

class ArrowFormat(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class PrimaryKeyConstraint(_message.Message):
    __slots__ = ("indices",)
    INDICES_FIELD_NUMBER: _ClassVar[int]
    indices: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, indices: _Optional[_Iterable[int]] = ...) -> None: ...

class UniqueConstraint(_message.Message):
    __slots__ = ("indices",)
    INDICES_FIELD_NUMBER: _ClassVar[int]
    indices: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, indices: _Optional[_Iterable[int]] = ...) -> None: ...

class Constraint(_message.Message):
    __slots__ = ("primary_key", "unique")
    PRIMARY_KEY_FIELD_NUMBER: _ClassVar[int]
    UNIQUE_FIELD_NUMBER: _ClassVar[int]
    primary_key: PrimaryKeyConstraint
    unique: UniqueConstraint
    def __init__(self, primary_key: _Optional[_Union[PrimaryKeyConstraint, _Mapping]] = ..., unique: _Optional[_Union[UniqueConstraint, _Mapping]] = ...) -> None: ...

class Constraints(_message.Message):
    __slots__ = ("constraints",)
    CONSTRAINTS_FIELD_NUMBER: _ClassVar[int]
    constraints: _containers.RepeatedCompositeFieldContainer[Constraint]
    def __init__(self, constraints: _Optional[_Iterable[_Union[Constraint, _Mapping]]] = ...) -> None: ...

class AvroOptions(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ArrowOptions(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Schema(_message.Message):
    __slots__ = ("columns", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedCompositeFieldContainer[Field]
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, columns: _Optional[_Iterable[_Union[Field, _Mapping]]] = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class Field(_message.Message):
    __slots__ = ("name", "arrow_type", "nullable", "children", "metadata")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    NAME_FIELD_NUMBER: _ClassVar[int]
    ARROW_TYPE_FIELD_NUMBER: _ClassVar[int]
    NULLABLE_FIELD_NUMBER: _ClassVar[int]
    CHILDREN_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    name: str
    arrow_type: ArrowType
    nullable: bool
    children: _containers.RepeatedCompositeFieldContainer[Field]
    metadata: _containers.ScalarMap[str, str]
    def __init__(self, name: _Optional[str] = ..., arrow_type: _Optional[_Union[ArrowType, _Mapping]] = ..., nullable: _Optional[bool] = ..., children: _Optional[_Iterable[_Union[Field, _Mapping]]] = ..., metadata: _Optional[_Mapping[str, str]] = ...) -> None: ...

class Timestamp(_message.Message):
    __slots__ = ("time_unit", "timezone")
    TIME_UNIT_FIELD_NUMBER: _ClassVar[int]
    TIMEZONE_FIELD_NUMBER: _ClassVar[int]
    time_unit: TimeUnit
    timezone: str
    def __init__(self, time_unit: _Optional[_Union[TimeUnit, str]] = ..., timezone: _Optional[str] = ...) -> None: ...

class Decimal32Type(_message.Message):
    __slots__ = ("precision", "scale")
    PRECISION_FIELD_NUMBER: _ClassVar[int]
    SCALE_FIELD_NUMBER: _ClassVar[int]
    precision: int
    scale: int
    def __init__(self, precision: _Optional[int] = ..., scale: _Optional[int] = ...) -> None: ...

class Decimal64Type(_message.Message):
    __slots__ = ("precision", "scale")
    PRECISION_FIELD_NUMBER: _ClassVar[int]
    SCALE_FIELD_NUMBER: _ClassVar[int]
    precision: int
    scale: int
    def __init__(self, precision: _Optional[int] = ..., scale: _Optional[int] = ...) -> None: ...

class Decimal128Type(_message.Message):
    __slots__ = ("precision", "scale")
    PRECISION_FIELD_NUMBER: _ClassVar[int]
    SCALE_FIELD_NUMBER: _ClassVar[int]
    precision: int
    scale: int
    def __init__(self, precision: _Optional[int] = ..., scale: _Optional[int] = ...) -> None: ...

class Decimal256Type(_message.Message):
    __slots__ = ("precision", "scale")
    PRECISION_FIELD_NUMBER: _ClassVar[int]
    SCALE_FIELD_NUMBER: _ClassVar[int]
    precision: int
    scale: int
    def __init__(self, precision: _Optional[int] = ..., scale: _Optional[int] = ...) -> None: ...

class List(_message.Message):
    __slots__ = ("field_type",)
    FIELD_TYPE_FIELD_NUMBER: _ClassVar[int]
    field_type: Field
    def __init__(self, field_type: _Optional[_Union[Field, _Mapping]] = ...) -> None: ...

class FixedSizeList(_message.Message):
    __slots__ = ("field_type", "list_size")
    FIELD_TYPE_FIELD_NUMBER: _ClassVar[int]
    LIST_SIZE_FIELD_NUMBER: _ClassVar[int]
    field_type: Field
    list_size: int
    def __init__(self, field_type: _Optional[_Union[Field, _Mapping]] = ..., list_size: _Optional[int] = ...) -> None: ...

class Dictionary(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: ArrowType
    value: ArrowType
    def __init__(self, key: _Optional[_Union[ArrowType, _Mapping]] = ..., value: _Optional[_Union[ArrowType, _Mapping]] = ...) -> None: ...

class Struct(_message.Message):
    __slots__ = ("sub_field_types",)
    SUB_FIELD_TYPES_FIELD_NUMBER: _ClassVar[int]
    sub_field_types: _containers.RepeatedCompositeFieldContainer[Field]
    def __init__(self, sub_field_types: _Optional[_Iterable[_Union[Field, _Mapping]]] = ...) -> None: ...

class Map(_message.Message):
    __slots__ = ("field_type", "keys_sorted")
    FIELD_TYPE_FIELD_NUMBER: _ClassVar[int]
    KEYS_SORTED_FIELD_NUMBER: _ClassVar[int]
    field_type: Field
    keys_sorted: bool
    def __init__(self, field_type: _Optional[_Union[Field, _Mapping]] = ..., keys_sorted: _Optional[bool] = ...) -> None: ...

class RunEndEncoded(_message.Message):
    __slots__ = ("run_ends_field", "values_field")
    RUN_ENDS_FIELD_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_FIELD_NUMBER: _ClassVar[int]
    run_ends_field: Field
    values_field: Field
    def __init__(self, run_ends_field: _Optional[_Union[Field, _Mapping]] = ..., values_field: _Optional[_Union[Field, _Mapping]] = ...) -> None: ...

class Union(_message.Message):
    __slots__ = ("union_types", "union_mode", "type_ids")
    UNION_TYPES_FIELD_NUMBER: _ClassVar[int]
    UNION_MODE_FIELD_NUMBER: _ClassVar[int]
    TYPE_IDS_FIELD_NUMBER: _ClassVar[int]
    union_types: _containers.RepeatedCompositeFieldContainer[Field]
    union_mode: UnionMode
    type_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, union_types: _Optional[_Iterable[_Union[Field, _Mapping]]] = ..., union_mode: _Optional[_Union[UnionMode, str]] = ..., type_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class ScalarNestedValue(_message.Message):
    __slots__ = ("ipc_message", "arrow_data", "schema", "dictionaries")
    class Dictionary(_message.Message):
        __slots__ = ("ipc_message", "arrow_data")
        IPC_MESSAGE_FIELD_NUMBER: _ClassVar[int]
        ARROW_DATA_FIELD_NUMBER: _ClassVar[int]
        ipc_message: bytes
        arrow_data: bytes
        def __init__(self, ipc_message: _Optional[bytes] = ..., arrow_data: _Optional[bytes] = ...) -> None: ...
    IPC_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ARROW_DATA_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    DICTIONARIES_FIELD_NUMBER: _ClassVar[int]
    ipc_message: bytes
    arrow_data: bytes
    schema: Schema
    dictionaries: _containers.RepeatedCompositeFieldContainer[ScalarNestedValue.Dictionary]
    def __init__(self, ipc_message: _Optional[bytes] = ..., arrow_data: _Optional[bytes] = ..., schema: _Optional[_Union[Schema, _Mapping]] = ..., dictionaries: _Optional[_Iterable[_Union[ScalarNestedValue.Dictionary, _Mapping]]] = ...) -> None: ...

class ScalarTime32Value(_message.Message):
    __slots__ = ("time32_second_value", "time32_millisecond_value")
    TIME32_SECOND_VALUE_FIELD_NUMBER: _ClassVar[int]
    TIME32_MILLISECOND_VALUE_FIELD_NUMBER: _ClassVar[int]
    time32_second_value: int
    time32_millisecond_value: int
    def __init__(self, time32_second_value: _Optional[int] = ..., time32_millisecond_value: _Optional[int] = ...) -> None: ...

class ScalarTime64Value(_message.Message):
    __slots__ = ("time64_microsecond_value", "time64_nanosecond_value")
    TIME64_MICROSECOND_VALUE_FIELD_NUMBER: _ClassVar[int]
    TIME64_NANOSECOND_VALUE_FIELD_NUMBER: _ClassVar[int]
    time64_microsecond_value: int
    time64_nanosecond_value: int
    def __init__(self, time64_microsecond_value: _Optional[int] = ..., time64_nanosecond_value: _Optional[int] = ...) -> None: ...

class ScalarTimestampValue(_message.Message):
    __slots__ = ("time_microsecond_value", "time_nanosecond_value", "time_second_value", "time_millisecond_value", "timezone")
    TIME_MICROSECOND_VALUE_FIELD_NUMBER: _ClassVar[int]
    TIME_NANOSECOND_VALUE_FIELD_NUMBER: _ClassVar[int]
    TIME_SECOND_VALUE_FIELD_NUMBER: _ClassVar[int]
    TIME_MILLISECOND_VALUE_FIELD_NUMBER: _ClassVar[int]
    TIMEZONE_FIELD_NUMBER: _ClassVar[int]
    time_microsecond_value: int
    time_nanosecond_value: int
    time_second_value: int
    time_millisecond_value: int
    timezone: str
    def __init__(self, time_microsecond_value: _Optional[int] = ..., time_nanosecond_value: _Optional[int] = ..., time_second_value: _Optional[int] = ..., time_millisecond_value: _Optional[int] = ..., timezone: _Optional[str] = ...) -> None: ...

class ScalarDictionaryValue(_message.Message):
    __slots__ = ("index_type", "value")
    INDEX_TYPE_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    index_type: ArrowType
    value: ScalarValue
    def __init__(self, index_type: _Optional[_Union[ArrowType, _Mapping]] = ..., value: _Optional[_Union[ScalarValue, _Mapping]] = ...) -> None: ...

class ScalarRunEndEncodedValue(_message.Message):
    __slots__ = ("run_ends_field", "values_field", "value")
    RUN_ENDS_FIELD_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    run_ends_field: Field
    values_field: Field
    value: ScalarValue
    def __init__(self, run_ends_field: _Optional[_Union[Field, _Mapping]] = ..., values_field: _Optional[_Union[Field, _Mapping]] = ..., value: _Optional[_Union[ScalarValue, _Mapping]] = ...) -> None: ...

class IntervalDayTimeValue(_message.Message):
    __slots__ = ("days", "milliseconds")
    DAYS_FIELD_NUMBER: _ClassVar[int]
    MILLISECONDS_FIELD_NUMBER: _ClassVar[int]
    days: int
    milliseconds: int
    def __init__(self, days: _Optional[int] = ..., milliseconds: _Optional[int] = ...) -> None: ...

class IntervalMonthDayNanoValue(_message.Message):
    __slots__ = ("months", "days", "nanos")
    MONTHS_FIELD_NUMBER: _ClassVar[int]
    DAYS_FIELD_NUMBER: _ClassVar[int]
    NANOS_FIELD_NUMBER: _ClassVar[int]
    months: int
    days: int
    nanos: int
    def __init__(self, months: _Optional[int] = ..., days: _Optional[int] = ..., nanos: _Optional[int] = ...) -> None: ...

class UnionField(_message.Message):
    __slots__ = ("field_id", "field")
    FIELD_ID_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    field_id: int
    field: Field
    def __init__(self, field_id: _Optional[int] = ..., field: _Optional[_Union[Field, _Mapping]] = ...) -> None: ...

class UnionValue(_message.Message):
    __slots__ = ("value_id", "value", "fields", "mode")
    VALUE_ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    value_id: int
    value: ScalarValue
    fields: _containers.RepeatedCompositeFieldContainer[UnionField]
    mode: UnionMode
    def __init__(self, value_id: _Optional[int] = ..., value: _Optional[_Union[ScalarValue, _Mapping]] = ..., fields: _Optional[_Iterable[_Union[UnionField, _Mapping]]] = ..., mode: _Optional[_Union[UnionMode, str]] = ...) -> None: ...

class ScalarFixedSizeBinary(_message.Message):
    __slots__ = ("values", "length")
    VALUES_FIELD_NUMBER: _ClassVar[int]
    LENGTH_FIELD_NUMBER: _ClassVar[int]
    values: bytes
    length: int
    def __init__(self, values: _Optional[bytes] = ..., length: _Optional[int] = ...) -> None: ...

class ScalarValue(_message.Message):
    __slots__ = ("null_value", "bool_value", "utf8_value", "large_utf8_value", "utf8_view_value", "int8_value", "int16_value", "int32_value", "int64_value", "uint8_value", "uint16_value", "uint32_value", "uint64_value", "float32_value", "float64_value", "date_32_value", "time32_value", "large_list_value", "list_value", "fixed_size_list_value", "list_view_value", "large_list_view_value", "struct_value", "map_value", "decimal32_value", "decimal64_value", "decimal128_value", "decimal256_value", "date_64_value", "interval_yearmonth_value", "duration_second_value", "duration_millisecond_value", "duration_microsecond_value", "duration_nanosecond_value", "timestamp_value", "dictionary_value", "binary_value", "large_binary_value", "binary_view_value", "time64_value", "interval_daytime_value", "interval_month_day_nano", "fixed_size_binary_value", "union_value", "run_end_encoded_value")
    NULL_VALUE_FIELD_NUMBER: _ClassVar[int]
    BOOL_VALUE_FIELD_NUMBER: _ClassVar[int]
    UTF8_VALUE_FIELD_NUMBER: _ClassVar[int]
    LARGE_UTF8_VALUE_FIELD_NUMBER: _ClassVar[int]
    UTF8_VIEW_VALUE_FIELD_NUMBER: _ClassVar[int]
    INT8_VALUE_FIELD_NUMBER: _ClassVar[int]
    INT16_VALUE_FIELD_NUMBER: _ClassVar[int]
    INT32_VALUE_FIELD_NUMBER: _ClassVar[int]
    INT64_VALUE_FIELD_NUMBER: _ClassVar[int]
    UINT8_VALUE_FIELD_NUMBER: _ClassVar[int]
    UINT16_VALUE_FIELD_NUMBER: _ClassVar[int]
    UINT32_VALUE_FIELD_NUMBER: _ClassVar[int]
    UINT64_VALUE_FIELD_NUMBER: _ClassVar[int]
    FLOAT32_VALUE_FIELD_NUMBER: _ClassVar[int]
    FLOAT64_VALUE_FIELD_NUMBER: _ClassVar[int]
    DATE_32_VALUE_FIELD_NUMBER: _ClassVar[int]
    TIME32_VALUE_FIELD_NUMBER: _ClassVar[int]
    LARGE_LIST_VALUE_FIELD_NUMBER: _ClassVar[int]
    LIST_VALUE_FIELD_NUMBER: _ClassVar[int]
    FIXED_SIZE_LIST_VALUE_FIELD_NUMBER: _ClassVar[int]
    LIST_VIEW_VALUE_FIELD_NUMBER: _ClassVar[int]
    LARGE_LIST_VIEW_VALUE_FIELD_NUMBER: _ClassVar[int]
    STRUCT_VALUE_FIELD_NUMBER: _ClassVar[int]
    MAP_VALUE_FIELD_NUMBER: _ClassVar[int]
    DECIMAL32_VALUE_FIELD_NUMBER: _ClassVar[int]
    DECIMAL64_VALUE_FIELD_NUMBER: _ClassVar[int]
    DECIMAL128_VALUE_FIELD_NUMBER: _ClassVar[int]
    DECIMAL256_VALUE_FIELD_NUMBER: _ClassVar[int]
    DATE_64_VALUE_FIELD_NUMBER: _ClassVar[int]
    INTERVAL_YEARMONTH_VALUE_FIELD_NUMBER: _ClassVar[int]
    DURATION_SECOND_VALUE_FIELD_NUMBER: _ClassVar[int]
    DURATION_MILLISECOND_VALUE_FIELD_NUMBER: _ClassVar[int]
    DURATION_MICROSECOND_VALUE_FIELD_NUMBER: _ClassVar[int]
    DURATION_NANOSECOND_VALUE_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_VALUE_FIELD_NUMBER: _ClassVar[int]
    DICTIONARY_VALUE_FIELD_NUMBER: _ClassVar[int]
    BINARY_VALUE_FIELD_NUMBER: _ClassVar[int]
    LARGE_BINARY_VALUE_FIELD_NUMBER: _ClassVar[int]
    BINARY_VIEW_VALUE_FIELD_NUMBER: _ClassVar[int]
    TIME64_VALUE_FIELD_NUMBER: _ClassVar[int]
    INTERVAL_DAYTIME_VALUE_FIELD_NUMBER: _ClassVar[int]
    INTERVAL_MONTH_DAY_NANO_FIELD_NUMBER: _ClassVar[int]
    FIXED_SIZE_BINARY_VALUE_FIELD_NUMBER: _ClassVar[int]
    UNION_VALUE_FIELD_NUMBER: _ClassVar[int]
    RUN_END_ENCODED_VALUE_FIELD_NUMBER: _ClassVar[int]
    null_value: ArrowType
    bool_value: bool
    utf8_value: str
    large_utf8_value: str
    utf8_view_value: str
    int8_value: int
    int16_value: int
    int32_value: int
    int64_value: int
    uint8_value: int
    uint16_value: int
    uint32_value: int
    uint64_value: int
    float32_value: float
    float64_value: float
    date_32_value: int
    time32_value: ScalarTime32Value
    large_list_value: ScalarNestedValue
    list_value: ScalarNestedValue
    fixed_size_list_value: ScalarNestedValue
    list_view_value: ScalarNestedValue
    large_list_view_value: ScalarNestedValue
    struct_value: ScalarNestedValue
    map_value: ScalarNestedValue
    decimal32_value: Decimal32
    decimal64_value: Decimal64
    decimal128_value: Decimal128
    decimal256_value: Decimal256
    date_64_value: int
    interval_yearmonth_value: int
    duration_second_value: int
    duration_millisecond_value: int
    duration_microsecond_value: int
    duration_nanosecond_value: int
    timestamp_value: ScalarTimestampValue
    dictionary_value: ScalarDictionaryValue
    binary_value: bytes
    large_binary_value: bytes
    binary_view_value: bytes
    time64_value: ScalarTime64Value
    interval_daytime_value: IntervalDayTimeValue
    interval_month_day_nano: IntervalMonthDayNanoValue
    fixed_size_binary_value: ScalarFixedSizeBinary
    union_value: UnionValue
    run_end_encoded_value: ScalarRunEndEncodedValue
    def __init__(self, null_value: _Optional[_Union[ArrowType, _Mapping]] = ..., bool_value: _Optional[bool] = ..., utf8_value: _Optional[str] = ..., large_utf8_value: _Optional[str] = ..., utf8_view_value: _Optional[str] = ..., int8_value: _Optional[int] = ..., int16_value: _Optional[int] = ..., int32_value: _Optional[int] = ..., int64_value: _Optional[int] = ..., uint8_value: _Optional[int] = ..., uint16_value: _Optional[int] = ..., uint32_value: _Optional[int] = ..., uint64_value: _Optional[int] = ..., float32_value: _Optional[float] = ..., float64_value: _Optional[float] = ..., date_32_value: _Optional[int] = ..., time32_value: _Optional[_Union[ScalarTime32Value, _Mapping]] = ..., large_list_value: _Optional[_Union[ScalarNestedValue, _Mapping]] = ..., list_value: _Optional[_Union[ScalarNestedValue, _Mapping]] = ..., fixed_size_list_value: _Optional[_Union[ScalarNestedValue, _Mapping]] = ..., list_view_value: _Optional[_Union[ScalarNestedValue, _Mapping]] = ..., large_list_view_value: _Optional[_Union[ScalarNestedValue, _Mapping]] = ..., struct_value: _Optional[_Union[ScalarNestedValue, _Mapping]] = ..., map_value: _Optional[_Union[ScalarNestedValue, _Mapping]] = ..., decimal32_value: _Optional[_Union[Decimal32, _Mapping]] = ..., decimal64_value: _Optional[_Union[Decimal64, _Mapping]] = ..., decimal128_value: _Optional[_Union[Decimal128, _Mapping]] = ..., decimal256_value: _Optional[_Union[Decimal256, _Mapping]] = ..., date_64_value: _Optional[int] = ..., interval_yearmonth_value: _Optional[int] = ..., duration_second_value: _Optional[int] = ..., duration_millisecond_value: _Optional[int] = ..., duration_microsecond_value: _Optional[int] = ..., duration_nanosecond_value: _Optional[int] = ..., timestamp_value: _Optional[_Union[ScalarTimestampValue, _Mapping]] = ..., dictionary_value: _Optional[_Union[ScalarDictionaryValue, _Mapping]] = ..., binary_value: _Optional[bytes] = ..., large_binary_value: _Optional[bytes] = ..., binary_view_value: _Optional[bytes] = ..., time64_value: _Optional[_Union[ScalarTime64Value, _Mapping]] = ..., interval_daytime_value: _Optional[_Union[IntervalDayTimeValue, _Mapping]] = ..., interval_month_day_nano: _Optional[_Union[IntervalMonthDayNanoValue, _Mapping]] = ..., fixed_size_binary_value: _Optional[_Union[ScalarFixedSizeBinary, _Mapping]] = ..., union_value: _Optional[_Union[UnionValue, _Mapping]] = ..., run_end_encoded_value: _Optional[_Union[ScalarRunEndEncodedValue, _Mapping]] = ...) -> None: ...

class Decimal32(_message.Message):
    __slots__ = ("value", "p", "s")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    P_FIELD_NUMBER: _ClassVar[int]
    S_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    p: int
    s: int
    def __init__(self, value: _Optional[bytes] = ..., p: _Optional[int] = ..., s: _Optional[int] = ...) -> None: ...

class Decimal64(_message.Message):
    __slots__ = ("value", "p", "s")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    P_FIELD_NUMBER: _ClassVar[int]
    S_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    p: int
    s: int
    def __init__(self, value: _Optional[bytes] = ..., p: _Optional[int] = ..., s: _Optional[int] = ...) -> None: ...

class Decimal128(_message.Message):
    __slots__ = ("value", "p", "s")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    P_FIELD_NUMBER: _ClassVar[int]
    S_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    p: int
    s: int
    def __init__(self, value: _Optional[bytes] = ..., p: _Optional[int] = ..., s: _Optional[int] = ...) -> None: ...

class Decimal256(_message.Message):
    __slots__ = ("value", "p", "s")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    P_FIELD_NUMBER: _ClassVar[int]
    S_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    p: int
    s: int
    def __init__(self, value: _Optional[bytes] = ..., p: _Optional[int] = ..., s: _Optional[int] = ...) -> None: ...

class ArrowType(_message.Message):
    __slots__ = ("NONE", "BOOL", "UINT8", "INT8", "UINT16", "INT16", "UINT32", "INT32", "UINT64", "INT64", "FLOAT16", "FLOAT32", "FLOAT64", "UTF8", "UTF8_VIEW", "LARGE_UTF8", "BINARY", "BINARY_VIEW", "FIXED_SIZE_BINARY", "LARGE_BINARY", "DATE32", "DATE64", "DURATION", "TIMESTAMP", "TIME32", "TIME64", "INTERVAL", "DECIMAL32", "DECIMAL64", "DECIMAL128", "DECIMAL256", "LIST", "LARGE_LIST", "FIXED_SIZE_LIST", "LIST_VIEW", "LARGE_LIST_VIEW", "STRUCT", "UNION", "DICTIONARY", "MAP", "RUN_END_ENCODED")
    NONE_FIELD_NUMBER: _ClassVar[int]
    BOOL_FIELD_NUMBER: _ClassVar[int]
    UINT8_FIELD_NUMBER: _ClassVar[int]
    INT8_FIELD_NUMBER: _ClassVar[int]
    UINT16_FIELD_NUMBER: _ClassVar[int]
    INT16_FIELD_NUMBER: _ClassVar[int]
    UINT32_FIELD_NUMBER: _ClassVar[int]
    INT32_FIELD_NUMBER: _ClassVar[int]
    UINT64_FIELD_NUMBER: _ClassVar[int]
    INT64_FIELD_NUMBER: _ClassVar[int]
    FLOAT16_FIELD_NUMBER: _ClassVar[int]
    FLOAT32_FIELD_NUMBER: _ClassVar[int]
    FLOAT64_FIELD_NUMBER: _ClassVar[int]
    UTF8_FIELD_NUMBER: _ClassVar[int]
    UTF8_VIEW_FIELD_NUMBER: _ClassVar[int]
    LARGE_UTF8_FIELD_NUMBER: _ClassVar[int]
    BINARY_FIELD_NUMBER: _ClassVar[int]
    BINARY_VIEW_FIELD_NUMBER: _ClassVar[int]
    FIXED_SIZE_BINARY_FIELD_NUMBER: _ClassVar[int]
    LARGE_BINARY_FIELD_NUMBER: _ClassVar[int]
    DATE32_FIELD_NUMBER: _ClassVar[int]
    DATE64_FIELD_NUMBER: _ClassVar[int]
    DURATION_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TIME32_FIELD_NUMBER: _ClassVar[int]
    TIME64_FIELD_NUMBER: _ClassVar[int]
    INTERVAL_FIELD_NUMBER: _ClassVar[int]
    DECIMAL32_FIELD_NUMBER: _ClassVar[int]
    DECIMAL64_FIELD_NUMBER: _ClassVar[int]
    DECIMAL128_FIELD_NUMBER: _ClassVar[int]
    DECIMAL256_FIELD_NUMBER: _ClassVar[int]
    LIST_FIELD_NUMBER: _ClassVar[int]
    LARGE_LIST_FIELD_NUMBER: _ClassVar[int]
    FIXED_SIZE_LIST_FIELD_NUMBER: _ClassVar[int]
    LIST_VIEW_FIELD_NUMBER: _ClassVar[int]
    LARGE_LIST_VIEW_FIELD_NUMBER: _ClassVar[int]
    STRUCT_FIELD_NUMBER: _ClassVar[int]
    UNION_FIELD_NUMBER: _ClassVar[int]
    DICTIONARY_FIELD_NUMBER: _ClassVar[int]
    MAP_FIELD_NUMBER: _ClassVar[int]
    RUN_END_ENCODED_FIELD_NUMBER: _ClassVar[int]
    NONE: EmptyMessage
    BOOL: EmptyMessage
    UINT8: EmptyMessage
    INT8: EmptyMessage
    UINT16: EmptyMessage
    INT16: EmptyMessage
    UINT32: EmptyMessage
    INT32: EmptyMessage
    UINT64: EmptyMessage
    INT64: EmptyMessage
    FLOAT16: EmptyMessage
    FLOAT32: EmptyMessage
    FLOAT64: EmptyMessage
    UTF8: EmptyMessage
    UTF8_VIEW: EmptyMessage
    LARGE_UTF8: EmptyMessage
    BINARY: EmptyMessage
    BINARY_VIEW: EmptyMessage
    FIXED_SIZE_BINARY: int
    LARGE_BINARY: EmptyMessage
    DATE32: EmptyMessage
    DATE64: EmptyMessage
    DURATION: TimeUnit
    TIMESTAMP: Timestamp
    TIME32: TimeUnit
    TIME64: TimeUnit
    INTERVAL: IntervalUnit
    DECIMAL32: Decimal32Type
    DECIMAL64: Decimal64Type
    DECIMAL128: Decimal128Type
    DECIMAL256: Decimal256Type
    LIST: List
    LARGE_LIST: List
    FIXED_SIZE_LIST: FixedSizeList
    LIST_VIEW: List
    LARGE_LIST_VIEW: List
    STRUCT: Struct
    UNION: Union
    DICTIONARY: Dictionary
    MAP: Map
    RUN_END_ENCODED: RunEndEncoded
    def __init__(self, NONE: _Optional[_Union[EmptyMessage, _Mapping]] = ..., BOOL: _Optional[_Union[EmptyMessage, _Mapping]] = ..., UINT8: _Optional[_Union[EmptyMessage, _Mapping]] = ..., INT8: _Optional[_Union[EmptyMessage, _Mapping]] = ..., UINT16: _Optional[_Union[EmptyMessage, _Mapping]] = ..., INT16: _Optional[_Union[EmptyMessage, _Mapping]] = ..., UINT32: _Optional[_Union[EmptyMessage, _Mapping]] = ..., INT32: _Optional[_Union[EmptyMessage, _Mapping]] = ..., UINT64: _Optional[_Union[EmptyMessage, _Mapping]] = ..., INT64: _Optional[_Union[EmptyMessage, _Mapping]] = ..., FLOAT16: _Optional[_Union[EmptyMessage, _Mapping]] = ..., FLOAT32: _Optional[_Union[EmptyMessage, _Mapping]] = ..., FLOAT64: _Optional[_Union[EmptyMessage, _Mapping]] = ..., UTF8: _Optional[_Union[EmptyMessage, _Mapping]] = ..., UTF8_VIEW: _Optional[_Union[EmptyMessage, _Mapping]] = ..., LARGE_UTF8: _Optional[_Union[EmptyMessage, _Mapping]] = ..., BINARY: _Optional[_Union[EmptyMessage, _Mapping]] = ..., BINARY_VIEW: _Optional[_Union[EmptyMessage, _Mapping]] = ..., FIXED_SIZE_BINARY: _Optional[int] = ..., LARGE_BINARY: _Optional[_Union[EmptyMessage, _Mapping]] = ..., DATE32: _Optional[_Union[EmptyMessage, _Mapping]] = ..., DATE64: _Optional[_Union[EmptyMessage, _Mapping]] = ..., DURATION: _Optional[_Union[TimeUnit, str]] = ..., TIMESTAMP: _Optional[_Union[Timestamp, _Mapping]] = ..., TIME32: _Optional[_Union[TimeUnit, str]] = ..., TIME64: _Optional[_Union[TimeUnit, str]] = ..., INTERVAL: _Optional[_Union[IntervalUnit, str]] = ..., DECIMAL32: _Optional[_Union[Decimal32Type, _Mapping]] = ..., DECIMAL64: _Optional[_Union[Decimal64Type, _Mapping]] = ..., DECIMAL128: _Optional[_Union[Decimal128Type, _Mapping]] = ..., DECIMAL256: _Optional[_Union[Decimal256Type, _Mapping]] = ..., LIST: _Optional[_Union[List, _Mapping]] = ..., LARGE_LIST: _Optional[_Union[List, _Mapping]] = ..., FIXED_SIZE_LIST: _Optional[_Union[FixedSizeList, _Mapping]] = ..., LIST_VIEW: _Optional[_Union[List, _Mapping]] = ..., LARGE_LIST_VIEW: _Optional[_Union[List, _Mapping]] = ..., STRUCT: _Optional[_Union[Struct, _Mapping]] = ..., UNION: _Optional[_Union[Union, _Mapping]] = ..., DICTIONARY: _Optional[_Union[Dictionary, _Mapping]] = ..., MAP: _Optional[_Union[Map, _Mapping]] = ..., RUN_END_ENCODED: _Optional[_Union[RunEndEncoded, _Mapping]] = ...) -> None: ...

class EmptyMessage(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class JsonWriterOptions(_message.Message):
    __slots__ = ("compression",)
    COMPRESSION_FIELD_NUMBER: _ClassVar[int]
    compression: CompressionTypeVariant
    def __init__(self, compression: _Optional[_Union[CompressionTypeVariant, str]] = ...) -> None: ...

class CsvWriterOptions(_message.Message):
    __slots__ = ("compression", "delimiter", "has_header", "date_format", "datetime_format", "timestamp_format", "time_format", "null_value", "quote", "escape", "double_quote", "quote_style", "ignore_leading_whitespace", "ignore_trailing_whitespace")
    COMPRESSION_FIELD_NUMBER: _ClassVar[int]
    DELIMITER_FIELD_NUMBER: _ClassVar[int]
    HAS_HEADER_FIELD_NUMBER: _ClassVar[int]
    DATE_FORMAT_FIELD_NUMBER: _ClassVar[int]
    DATETIME_FORMAT_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FORMAT_FIELD_NUMBER: _ClassVar[int]
    TIME_FORMAT_FIELD_NUMBER: _ClassVar[int]
    NULL_VALUE_FIELD_NUMBER: _ClassVar[int]
    QUOTE_FIELD_NUMBER: _ClassVar[int]
    ESCAPE_FIELD_NUMBER: _ClassVar[int]
    DOUBLE_QUOTE_FIELD_NUMBER: _ClassVar[int]
    QUOTE_STYLE_FIELD_NUMBER: _ClassVar[int]
    IGNORE_LEADING_WHITESPACE_FIELD_NUMBER: _ClassVar[int]
    IGNORE_TRAILING_WHITESPACE_FIELD_NUMBER: _ClassVar[int]
    compression: CompressionTypeVariant
    delimiter: str
    has_header: bool
    date_format: str
    datetime_format: str
    timestamp_format: str
    time_format: str
    null_value: str
    quote: str
    escape: str
    double_quote: bool
    quote_style: CsvQuoteStyle
    ignore_leading_whitespace: bool
    ignore_trailing_whitespace: bool
    def __init__(self, compression: _Optional[_Union[CompressionTypeVariant, str]] = ..., delimiter: _Optional[str] = ..., has_header: _Optional[bool] = ..., date_format: _Optional[str] = ..., datetime_format: _Optional[str] = ..., timestamp_format: _Optional[str] = ..., time_format: _Optional[str] = ..., null_value: _Optional[str] = ..., quote: _Optional[str] = ..., escape: _Optional[str] = ..., double_quote: _Optional[bool] = ..., quote_style: _Optional[_Union[CsvQuoteStyle, str]] = ..., ignore_leading_whitespace: _Optional[bool] = ..., ignore_trailing_whitespace: _Optional[bool] = ...) -> None: ...

class CsvOptions(_message.Message):
    __slots__ = ("has_header", "delimiter", "quote", "escape", "compression", "schema_infer_max_rec", "date_format", "datetime_format", "timestamp_format", "timestamp_tz_format", "time_format", "null_value", "null_regex", "comment", "double_quote", "newlines_in_values", "terminator", "truncated_rows", "compression_level", "quote_style", "ignore_leading_whitespace", "ignore_trailing_whitespace")
    HAS_HEADER_FIELD_NUMBER: _ClassVar[int]
    DELIMITER_FIELD_NUMBER: _ClassVar[int]
    QUOTE_FIELD_NUMBER: _ClassVar[int]
    ESCAPE_FIELD_NUMBER: _ClassVar[int]
    COMPRESSION_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_INFER_MAX_REC_FIELD_NUMBER: _ClassVar[int]
    DATE_FORMAT_FIELD_NUMBER: _ClassVar[int]
    DATETIME_FORMAT_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FORMAT_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_TZ_FORMAT_FIELD_NUMBER: _ClassVar[int]
    TIME_FORMAT_FIELD_NUMBER: _ClassVar[int]
    NULL_VALUE_FIELD_NUMBER: _ClassVar[int]
    NULL_REGEX_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    DOUBLE_QUOTE_FIELD_NUMBER: _ClassVar[int]
    NEWLINES_IN_VALUES_FIELD_NUMBER: _ClassVar[int]
    TERMINATOR_FIELD_NUMBER: _ClassVar[int]
    TRUNCATED_ROWS_FIELD_NUMBER: _ClassVar[int]
    COMPRESSION_LEVEL_FIELD_NUMBER: _ClassVar[int]
    QUOTE_STYLE_FIELD_NUMBER: _ClassVar[int]
    IGNORE_LEADING_WHITESPACE_FIELD_NUMBER: _ClassVar[int]
    IGNORE_TRAILING_WHITESPACE_FIELD_NUMBER: _ClassVar[int]
    has_header: bytes
    delimiter: bytes
    quote: bytes
    escape: bytes
    compression: CompressionTypeVariant
    schema_infer_max_rec: int
    date_format: str
    datetime_format: str
    timestamp_format: str
    timestamp_tz_format: str
    time_format: str
    null_value: str
    null_regex: str
    comment: bytes
    double_quote: bytes
    newlines_in_values: bytes
    terminator: bytes
    truncated_rows: bytes
    compression_level: int
    quote_style: CsvQuoteStyle
    ignore_leading_whitespace: bytes
    ignore_trailing_whitespace: bytes
    def __init__(self, has_header: _Optional[bytes] = ..., delimiter: _Optional[bytes] = ..., quote: _Optional[bytes] = ..., escape: _Optional[bytes] = ..., compression: _Optional[_Union[CompressionTypeVariant, str]] = ..., schema_infer_max_rec: _Optional[int] = ..., date_format: _Optional[str] = ..., datetime_format: _Optional[str] = ..., timestamp_format: _Optional[str] = ..., timestamp_tz_format: _Optional[str] = ..., time_format: _Optional[str] = ..., null_value: _Optional[str] = ..., null_regex: _Optional[str] = ..., comment: _Optional[bytes] = ..., double_quote: _Optional[bytes] = ..., newlines_in_values: _Optional[bytes] = ..., terminator: _Optional[bytes] = ..., truncated_rows: _Optional[bytes] = ..., compression_level: _Optional[int] = ..., quote_style: _Optional[_Union[CsvQuoteStyle, str]] = ..., ignore_leading_whitespace: _Optional[bytes] = ..., ignore_trailing_whitespace: _Optional[bytes] = ...) -> None: ...

class JsonOptions(_message.Message):
    __slots__ = ("compression", "schema_infer_max_rec", "compression_level", "newline_delimited")
    COMPRESSION_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_INFER_MAX_REC_FIELD_NUMBER: _ClassVar[int]
    COMPRESSION_LEVEL_FIELD_NUMBER: _ClassVar[int]
    NEWLINE_DELIMITED_FIELD_NUMBER: _ClassVar[int]
    compression: CompressionTypeVariant
    schema_infer_max_rec: int
    compression_level: int
    newline_delimited: bool
    def __init__(self, compression: _Optional[_Union[CompressionTypeVariant, str]] = ..., schema_infer_max_rec: _Optional[int] = ..., compression_level: _Optional[int] = ..., newline_delimited: _Optional[bool] = ...) -> None: ...

class TableParquetOptions(_message.Message):
    __slots__ = ("column_specific_options", "key_value_metadata")
    class KeyValueMetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    GLOBAL_FIELD_NUMBER: _ClassVar[int]
    COLUMN_SPECIFIC_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    KEY_VALUE_METADATA_FIELD_NUMBER: _ClassVar[int]
    column_specific_options: _containers.RepeatedCompositeFieldContainer[ParquetColumnSpecificOptions]
    key_value_metadata: _containers.ScalarMap[str, str]
    def __init__(self, column_specific_options: _Optional[_Iterable[_Union[ParquetColumnSpecificOptions, _Mapping]]] = ..., key_value_metadata: _Optional[_Mapping[str, str]] = ..., **kwargs) -> None: ...

class ParquetColumnSpecificOptions(_message.Message):
    __slots__ = ("column_name", "options")
    COLUMN_NAME_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    column_name: str
    options: ParquetColumnOptions
    def __init__(self, column_name: _Optional[str] = ..., options: _Optional[_Union[ParquetColumnOptions, _Mapping]] = ...) -> None: ...

class ParquetColumnOptions(_message.Message):
    __slots__ = ("bloom_filter_enabled", "encoding", "dictionary_enabled", "compression", "statistics_enabled", "bloom_filter_fpp", "bloom_filter_ndv")
    BLOOM_FILTER_ENABLED_FIELD_NUMBER: _ClassVar[int]
    ENCODING_FIELD_NUMBER: _ClassVar[int]
    DICTIONARY_ENABLED_FIELD_NUMBER: _ClassVar[int]
    COMPRESSION_FIELD_NUMBER: _ClassVar[int]
    STATISTICS_ENABLED_FIELD_NUMBER: _ClassVar[int]
    BLOOM_FILTER_FPP_FIELD_NUMBER: _ClassVar[int]
    BLOOM_FILTER_NDV_FIELD_NUMBER: _ClassVar[int]
    bloom_filter_enabled: bool
    encoding: str
    dictionary_enabled: bool
    compression: str
    statistics_enabled: str
    bloom_filter_fpp: float
    bloom_filter_ndv: int
    def __init__(self, bloom_filter_enabled: _Optional[bool] = ..., encoding: _Optional[str] = ..., dictionary_enabled: _Optional[bool] = ..., compression: _Optional[str] = ..., statistics_enabled: _Optional[str] = ..., bloom_filter_fpp: _Optional[float] = ..., bloom_filter_ndv: _Optional[int] = ...) -> None: ...

class ParquetOptions(_message.Message):
    __slots__ = ("enable_page_index", "pruning", "skip_metadata", "pushdown_filters", "reorder_filters", "force_filter_selections", "data_pagesize_limit", "write_batch_size", "writer_version", "allow_single_file_parallelism", "maximum_parallel_row_group_writers", "maximum_buffered_record_batches_per_stream", "bloom_filter_on_read", "bloom_filter_on_write", "schema_force_view_types", "binary_as_string", "skip_arrow_metadata", "metadata_size_hint", "compression", "dictionary_enabled", "statistics_enabled", "column_index_truncate_length", "statistics_truncate_length", "encoding", "bloom_filter_fpp", "bloom_filter_ndv", "dictionary_page_size_limit", "data_page_row_count_limit", "max_row_group_size", "max_in_list_size", "created_by", "coerce_int96", "max_predicate_cache_size", "max_row_group_bytes", "content_defined_chunking", "coerce_int96_tz")
    ENABLE_PAGE_INDEX_FIELD_NUMBER: _ClassVar[int]
    PRUNING_FIELD_NUMBER: _ClassVar[int]
    SKIP_METADATA_FIELD_NUMBER: _ClassVar[int]
    PUSHDOWN_FILTERS_FIELD_NUMBER: _ClassVar[int]
    REORDER_FILTERS_FIELD_NUMBER: _ClassVar[int]
    FORCE_FILTER_SELECTIONS_FIELD_NUMBER: _ClassVar[int]
    DATA_PAGESIZE_LIMIT_FIELD_NUMBER: _ClassVar[int]
    WRITE_BATCH_SIZE_FIELD_NUMBER: _ClassVar[int]
    WRITER_VERSION_FIELD_NUMBER: _ClassVar[int]
    ALLOW_SINGLE_FILE_PARALLELISM_FIELD_NUMBER: _ClassVar[int]
    MAXIMUM_PARALLEL_ROW_GROUP_WRITERS_FIELD_NUMBER: _ClassVar[int]
    MAXIMUM_BUFFERED_RECORD_BATCHES_PER_STREAM_FIELD_NUMBER: _ClassVar[int]
    BLOOM_FILTER_ON_READ_FIELD_NUMBER: _ClassVar[int]
    BLOOM_FILTER_ON_WRITE_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FORCE_VIEW_TYPES_FIELD_NUMBER: _ClassVar[int]
    BINARY_AS_STRING_FIELD_NUMBER: _ClassVar[int]
    SKIP_ARROW_METADATA_FIELD_NUMBER: _ClassVar[int]
    METADATA_SIZE_HINT_FIELD_NUMBER: _ClassVar[int]
    COMPRESSION_FIELD_NUMBER: _ClassVar[int]
    DICTIONARY_ENABLED_FIELD_NUMBER: _ClassVar[int]
    STATISTICS_ENABLED_FIELD_NUMBER: _ClassVar[int]
    COLUMN_INDEX_TRUNCATE_LENGTH_FIELD_NUMBER: _ClassVar[int]
    STATISTICS_TRUNCATE_LENGTH_FIELD_NUMBER: _ClassVar[int]
    ENCODING_FIELD_NUMBER: _ClassVar[int]
    BLOOM_FILTER_FPP_FIELD_NUMBER: _ClassVar[int]
    BLOOM_FILTER_NDV_FIELD_NUMBER: _ClassVar[int]
    DICTIONARY_PAGE_SIZE_LIMIT_FIELD_NUMBER: _ClassVar[int]
    DATA_PAGE_ROW_COUNT_LIMIT_FIELD_NUMBER: _ClassVar[int]
    MAX_ROW_GROUP_SIZE_FIELD_NUMBER: _ClassVar[int]
    MAX_IN_LIST_SIZE_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    COERCE_INT96_FIELD_NUMBER: _ClassVar[int]
    MAX_PREDICATE_CACHE_SIZE_FIELD_NUMBER: _ClassVar[int]
    MAX_ROW_GROUP_BYTES_FIELD_NUMBER: _ClassVar[int]
    CONTENT_DEFINED_CHUNKING_FIELD_NUMBER: _ClassVar[int]
    COERCE_INT96_TZ_FIELD_NUMBER: _ClassVar[int]
    enable_page_index: bool
    pruning: bool
    skip_metadata: bool
    pushdown_filters: bool
    reorder_filters: bool
    force_filter_selections: bool
    data_pagesize_limit: int
    write_batch_size: int
    writer_version: str
    allow_single_file_parallelism: bool
    maximum_parallel_row_group_writers: int
    maximum_buffered_record_batches_per_stream: int
    bloom_filter_on_read: bool
    bloom_filter_on_write: bool
    schema_force_view_types: bool
    binary_as_string: bool
    skip_arrow_metadata: bool
    metadata_size_hint: int
    compression: str
    dictionary_enabled: bool
    statistics_enabled: str
    column_index_truncate_length: int
    statistics_truncate_length: int
    encoding: str
    bloom_filter_fpp: float
    bloom_filter_ndv: int
    dictionary_page_size_limit: int
    data_page_row_count_limit: int
    max_row_group_size: int
    max_in_list_size: int
    created_by: str
    coerce_int96: str
    max_predicate_cache_size: int
    max_row_group_bytes: int
    content_defined_chunking: ParquetCdcOptions
    coerce_int96_tz: str
    def __init__(self, enable_page_index: _Optional[bool] = ..., pruning: _Optional[bool] = ..., skip_metadata: _Optional[bool] = ..., pushdown_filters: _Optional[bool] = ..., reorder_filters: _Optional[bool] = ..., force_filter_selections: _Optional[bool] = ..., data_pagesize_limit: _Optional[int] = ..., write_batch_size: _Optional[int] = ..., writer_version: _Optional[str] = ..., allow_single_file_parallelism: _Optional[bool] = ..., maximum_parallel_row_group_writers: _Optional[int] = ..., maximum_buffered_record_batches_per_stream: _Optional[int] = ..., bloom_filter_on_read: _Optional[bool] = ..., bloom_filter_on_write: _Optional[bool] = ..., schema_force_view_types: _Optional[bool] = ..., binary_as_string: _Optional[bool] = ..., skip_arrow_metadata: _Optional[bool] = ..., metadata_size_hint: _Optional[int] = ..., compression: _Optional[str] = ..., dictionary_enabled: _Optional[bool] = ..., statistics_enabled: _Optional[str] = ..., column_index_truncate_length: _Optional[int] = ..., statistics_truncate_length: _Optional[int] = ..., encoding: _Optional[str] = ..., bloom_filter_fpp: _Optional[float] = ..., bloom_filter_ndv: _Optional[int] = ..., dictionary_page_size_limit: _Optional[int] = ..., data_page_row_count_limit: _Optional[int] = ..., max_row_group_size: _Optional[int] = ..., max_in_list_size: _Optional[int] = ..., created_by: _Optional[str] = ..., coerce_int96: _Optional[str] = ..., max_predicate_cache_size: _Optional[int] = ..., max_row_group_bytes: _Optional[int] = ..., content_defined_chunking: _Optional[_Union[ParquetCdcOptions, _Mapping]] = ..., coerce_int96_tz: _Optional[str] = ...) -> None: ...

class ParquetCdcOptions(_message.Message):
    __slots__ = ("enabled", "min_chunk_size", "max_chunk_size", "norm_level")
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    MIN_CHUNK_SIZE_FIELD_NUMBER: _ClassVar[int]
    MAX_CHUNK_SIZE_FIELD_NUMBER: _ClassVar[int]
    NORM_LEVEL_FIELD_NUMBER: _ClassVar[int]
    enabled: bool
    min_chunk_size: int
    max_chunk_size: int
    norm_level: int
    def __init__(self, enabled: _Optional[bool] = ..., min_chunk_size: _Optional[int] = ..., max_chunk_size: _Optional[int] = ..., norm_level: _Optional[int] = ...) -> None: ...

class Precision(_message.Message):
    __slots__ = ("precision_info", "val")
    PRECISION_INFO_FIELD_NUMBER: _ClassVar[int]
    VAL_FIELD_NUMBER: _ClassVar[int]
    precision_info: PrecisionInfo
    val: ScalarValue
    def __init__(self, precision_info: _Optional[_Union[PrecisionInfo, str]] = ..., val: _Optional[_Union[ScalarValue, _Mapping]] = ...) -> None: ...

class Statistics(_message.Message):
    __slots__ = ("num_rows", "total_byte_size", "column_stats")
    NUM_ROWS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_BYTE_SIZE_FIELD_NUMBER: _ClassVar[int]
    COLUMN_STATS_FIELD_NUMBER: _ClassVar[int]
    num_rows: Precision
    total_byte_size: Precision
    column_stats: _containers.RepeatedCompositeFieldContainer[ColumnStats]
    def __init__(self, num_rows: _Optional[_Union[Precision, _Mapping]] = ..., total_byte_size: _Optional[_Union[Precision, _Mapping]] = ..., column_stats: _Optional[_Iterable[_Union[ColumnStats, _Mapping]]] = ...) -> None: ...

class ColumnStats(_message.Message):
    __slots__ = ("min_value", "max_value", "sum_value", "null_count", "distinct_count", "byte_size")
    MIN_VALUE_FIELD_NUMBER: _ClassVar[int]
    MAX_VALUE_FIELD_NUMBER: _ClassVar[int]
    SUM_VALUE_FIELD_NUMBER: _ClassVar[int]
    NULL_COUNT_FIELD_NUMBER: _ClassVar[int]
    DISTINCT_COUNT_FIELD_NUMBER: _ClassVar[int]
    BYTE_SIZE_FIELD_NUMBER: _ClassVar[int]
    min_value: Precision
    max_value: Precision
    sum_value: Precision
    null_count: Precision
    distinct_count: Precision
    byte_size: Precision
    def __init__(self, min_value: _Optional[_Union[Precision, _Mapping]] = ..., max_value: _Optional[_Union[Precision, _Mapping]] = ..., sum_value: _Optional[_Union[Precision, _Mapping]] = ..., null_count: _Optional[_Union[Precision, _Mapping]] = ..., distinct_count: _Optional[_Union[Precision, _Mapping]] = ..., byte_size: _Optional[_Union[Precision, _Mapping]] = ...) -> None: ...

class ExplainAnalyzeCategoriesNode(_message.Message):
    __slots__ = ("all", "only")
    ALL_FIELD_NUMBER: _ClassVar[int]
    ONLY_FIELD_NUMBER: _ClassVar[int]
    all: bool
    only: _containers.RepeatedScalarFieldContainer[MetricCategory]
    def __init__(self, all: _Optional[bool] = ..., only: _Optional[_Iterable[_Union[MetricCategory, str]]] = ...) -> None: ...
