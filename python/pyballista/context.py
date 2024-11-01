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

from contextlib import ContextDecorator
import json
import os
import time
from typing import Iterable
import pathlib

from datafusion import SessionContext
import pyarrow as pa
import pyballista

from typing import List, Any

class BallistaContext:
    def __init__(self, df_ctx: SessionContext = SessionContext()):
        self.df_ctx = df_ctx
        self.ctx = pyballista.SessionContext(df_ctx)
        
    def standalone(self) -> SessionContext:
        ctx = self.ctx.local()
        
        return ctx
        
    def remote(self, url: str) -> SessionContext:
        ctx = self.ctx.remote(url)
        
        return ctx
            
    def sql(self, sql: str) -> pa.RecordBatch:
            # TODO we should parse sql and inspect the plan rather than
            # perform a string comparison here
            sql_str = sql.lower()
            if "create view" in sql_str or "drop view" in sql_str:
                self.ctx.sql(sql)
                return []
    
            df = self.ctx.sql(sql)
            return df
    
    def read_csv(
        self, 
        path: str | pathlib.Path | list[str] | list[pathlib.Path], 
        schema: pa.Schema = None, 
        has_header: bool = False, 
        delimeter: str = ",", 
        schema_infer_max_records: int = 1000, 
        file_extension: str = ".csv", 
        table_partition_cols: list[tuple[str, str]] | None = None, 
        file_compression_type: str | None = None
    ):
        return self.ctx.read_csv(
            path, 
            schema, 
            has_header, 
            delimeter, 
            schema_infer_max_records, 
            file_extension, 
            table_partition_cols, 
            file_compression_type
        )
        
    def register_csv(
        self,
        name: str,
        path: str | pathlib.Path | list[str] | list[pathlib.Path], 
        schema: pa.Schema | None = None, 
        has_header: bool = False, 
        delimeter: str = ",", 
        schema_infer_max_records: int = 1000, 
        file_extension: str = ".csv", 
        file_compression_type: str | None = None
    ):
        return self.ctx.register_csv(
            name,
            path,
            schema, 
            has_header, 
            delimeter, 
            schema_infer_max_records,
            file_extension,
            file_compression_type
        )
    
    def read_parquet(
        self,
        path: str | pathlib.Path,
        table_partition_cols: list[tuple[str, str]] | None = None,
        parquet_pruning: bool = True,
        file_extension: str = ".parquet",
        skip_metadata: bool = True,
        schema: pa.Schema | None = None,
        file_sort_order: list[list[str]] | None = None,
    ):
        """Read a Parquet source into a :py:class:`~datafusion.dataframe.Dataframe`.

        Args:
            path: Path to the Parquet file.
            table_partition_cols: Partition columns.
            parquet_pruning: Whether the parquet reader should use the predicate
                to prune row groups.
            file_extension: File extension; only files with this extension are
                selected for data input.
            skip_metadata: Whether the parquet reader should skip any metadata
                that may be in the file schema. This can help avoid schema
                conflicts due to metadata.
            schema: An optional schema representing the parquet files. If None,
                the parquet reader will try to infer it based on data in the
                file.
            file_sort_order: Sort order for the file.

        Returns:
            DataFrame representation of the read Parquet files
        """
        if table_partition_cols is None:
            table_partition_cols = []
        return self.ctx.read_parquet(
            str(path),
            table_partition_cols,
            parquet_pruning,
            file_extension,
            skip_metadata,
            schema,
            file_sort_order,
        )
        
    def register_parquet(
        self,
        name: str,
        path: str | pathlib.Path,
        table_partition_cols: list[tuple[str, str]] | None = None,
        parquet_pruning: bool = True,
        file_extension: str = ".parquet",
        skip_metadata: bool = True,
        schema: pa.Schema | None = None,
        file_sort_order: list[list[str]] | None = None,
    ) -> None:
        """Register a Parquet file as a table.

        The registered table can be referenced from SQL statement executed
        against this context.

        Args:
            name: Name of the table to register.
            path: Path to the Parquet file.
            table_partition_cols: Partition columns.
            parquet_pruning: Whether the parquet reader should use the
                predicate to prune row groups.
            file_extension: File extension; only files with this extension are
                selected for data input.
            skip_metadata: Whether the parquet reader should skip any metadata
                that may be in the file schema. This can help avoid schema
                conflicts due to metadata.
            schema: The data source schema.
            file_sort_order: Sort order for the file.
        """
        if table_partition_cols is None:
            table_partition_cols = []
        self.ctx.register_parquet(
            name,
            str(path),
            table_partition_cols,
            parquet_pruning,
            file_extension,
            skip_metadata,
            schema,
            file_sort_order,
        )
        
    def functions(self):
        self.ctx.functions
    
    