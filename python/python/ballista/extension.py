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

from datafusion import SessionContext, DataFrame, ParquetWriterOptions
from datafusion.dataframe import Compression

from typing import (
    Union,
)


from ._internal_ballista import create_ballista_data_frame
from ._internal_ballista import ParquetColumnOptions as ParquetColumnOptionsInternal
from ._internal_ballista import ParquetWriterOptions as ParquetWriterOptionsInternal
import pathlib

# DataFrame execution methods which should be automatically
# overridden.

OVERRIDDEN_EXECUTION_METHODS = [
    "show",
    "count",
    "collect",
    "collect_partitioned",
    "write_json",
    "to_arrow_table",
    "to_pandas",
    "to_pydict",
    "to_polars",
    "to_pylist",
    "_repr_html_",
    "execute_stream",
    "execute_stream_partitioned",
]


# class used to redefine DataFrame object
# intercepting execution methods and methods
# which returns `DataFrame`
class RedefiningDataFrameMeta(type):
    def __new__(cls, name, bases, attrs):
        # wrapper function intercept all execution functions
        # replacing ordinary DataFrame with Ballista data frame
        def __wrap_dataframe_execution(func):
            def method_wrapper(*args, **kwargs):
                slf, *argz = args
                df = slf._to_internal_df()

                return getattr(df, func)(*argz, **kwargs)

            return method_wrapper

        # wrapper function intercepts all methods which
        # return DataFrame and wraps DataFrame with DistributedDataFrame
        def __wrap_dataframe_result(func):
            def method_wrapper(*args, **kwargs):
                address = args[0].address
                session_id = args[0].session_id
                df = func(*args, **kwargs)
                return DistributedDataFrame(df, session_id, address)

            return method_wrapper

        for base_name, base_value in bases[0].__dict__.items():
            #
            # TODO: could we not use 'DataFrame' as a string here?
            #
            if (
                callable(base_value)
                and not base_name.startswith("__")
                and base_value.__annotations__.get("return") == "DataFrame"
            ):
                #
                # functions which return DataFrame are redefined
                # to return DistributedDataFrame
                #
                attrs[base_name] = __wrap_dataframe_result(base_value)

        # TODO: we could do better here
        for function in OVERRIDDEN_EXECUTION_METHODS:
            attrs[function] = __wrap_dataframe_execution(function)

        return super().__new__(cls, name, bases, attrs)


class RedefiningSessionContextMeta(type):
    def __new__(cls, name, bases, attrs):
        def __wrap_dataframe_result(func):
            def method_wrapper(*args, **kwargs):
                address = args[0].address
                session_id = args[0].session_id
                df = func(*args, **kwargs)
                return DistributedDataFrame(df, session_id, address)

            return method_wrapper

        for base_name, base_value in bases[0].__dict__.items():
            #
            # could we not use 'DataFrame' as a string here?
            #
            if (
                callable(base_value)
                and not base_name.startswith("__")
                and base_value.__annotations__.get("return") == "DataFrame"
            ):
                #
                # functions which return DataFrame are redefined
                # to return DistributedDataFrame
                #
                attrs[base_name] = __wrap_dataframe_result(base_value)

        return super().__new__(cls, name, bases, attrs)


# main difference between this class and DataFrame is that
# operations which would execute logical plan will
# serialize it and invoke ballista client to execute it
#
# this class keeps reference to remote ballista


class DistributedDataFrame(DataFrame, metaclass=RedefiningDataFrameMeta):
    def __init__(self, df: DataFrame, session_id: str, address: str):
        super().__init__(df.df)
        self.address = address
        self.session_id = session_id

    #
    # this will create a ballista dataframe, which has ballista
    # session context, and ballista planner.
    #
    def _to_internal_df(self):
        blob_plan = self.logical_plan().to_proto()
        df = create_ballista_data_frame(blob_plan, self.address, self.session_id)
        return df

    def write_csv(self, path, with_header=False):
        df = self._to_internal_df()
        df.write_csv(path, with_header)

    def write_parquet_with_options(
        self,
        path: str,
        options: ParquetWriterOptions,
    ):
        options_internal = ParquetWriterOptionsInternal(
            options.data_pagesize_limit,
            options.write_batch_size,
            options.writer_version,
            options.skip_arrow_metadata,
            options.compression,
            options.dictionary_enabled,
            options.dictionary_page_size_limit,
            options.statistics_enabled,
            options.max_row_group_size,
            options.created_by,
            options.column_index_truncate_length,
            options.statistics_truncate_length,
            options.data_page_row_count_limit,
            options.encoding,
            options.bloom_filter_on_write,
            options.bloom_filter_fpp,
            options.bloom_filter_ndv,
            options.allow_single_file_parallelism,
            options.maximum_parallel_row_group_writers,
            options.maximum_buffered_record_batches_per_stream,
        )

        column_specific_options_internal = {}
        for column, opts in (options.column_specific_options or {}).items():
            column_specific_options_internal[column] = ParquetColumnOptionsInternal(
                bloom_filter_enabled=opts.bloom_filter_enabled,
                encoding=opts.encoding,
                dictionary_enabled=opts.dictionary_enabled,
                compression=opts.compression,
                statistics_enabled=opts.statistics_enabled,
                bloom_filter_fpp=opts.bloom_filter_fpp,
                bloom_filter_ndv=opts.bloom_filter_ndv,
            )

        # raw_write_options = (
        #     write_options._raw_write_options if write_options is not None else None
        # )

        df = self._to_internal_df()

        df.write_parquet_with_options(
            str(path),
            options_internal,
            column_specific_options_internal,
            # raw_write_options,
        )

    def write_parquet(
        self,
        path: Union[str, pathlib.Path],
        compression: Union[str, Compression, ParquetWriterOptions] = Compression.ZSTD,
        compression_level: Union[int, None] = None,
    ) -> None:
        if isinstance(compression, ParquetWriterOptions):
            if compression_level is not None:
                msg = "compression_level should be None when using ParquetWriterOptions"
                raise ValueError(msg)
            self.write_parquet_with_options(path, compression)
            return

        if isinstance(compression, str):
            compression = Compression.from_str(compression)

        if (
            compression in {Compression.GZIP, Compression.BROTLI, Compression.ZSTD}
            and compression_level is None
        ):
            compression_level = compression.get_default_level()
        df = self._to_internal_df()
        df.write_parquet(str(path), compression.value, compression_level)


class BallistaSessionContext(SessionContext, metaclass=RedefiningSessionContextMeta):
    def __init__(self, address: str, config=None, runtime=None):
        super().__init__(config, runtime)
        self.address = address
        self.session_id = self.session_id()
