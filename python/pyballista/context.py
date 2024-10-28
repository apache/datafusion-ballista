from contextlib import ContextDecorator
import json
import os
import time
from typing import Iterable

import pyarrow as pa

import pyballista
from pyballista import BallistaContext
from typing import List, Any
from datafusion import SessionContext


class BallistaContext:
    def __init__(self, df_ctx: SessionContext):
        self.df_ctx = df_ctx
        self.ctx = BallistaContext(df_ctx)
        
    def register_csv(self, table_name: str, path: str, has_header: bool):
        self.ctx.register_csv(table_name, path, has_header)
    
    def register_parquet(self, table_name: str, path: str):
        self.ctx.register_parquet(table_name, path)