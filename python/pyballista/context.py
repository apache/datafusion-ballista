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