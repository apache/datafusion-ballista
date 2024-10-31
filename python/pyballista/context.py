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

from typing import List, Any

class BallistaContext:
    def __init__(self, mode: str = "standalone", host: str = "0.0.0.0", port: int = 50050, concurrent_tasks: int = 4):
        if mode == "standalone":
            self.ctx = pyballista.BallistaContext().standalone(concurrent_tasks)
        else:
            self.ctx = pyballista.BallistaContext().remote(host, port)
            
    def sql(self, sql: str) -> pa.RecordBatch:
            # TODO we should parse sql and inspect the plan rather than
            # perform a string comparison here
            sql_str = sql.lower()
            if "create view" in sql_str or "drop view" in sql_str:
                self.ctx.sql(sql)
                return []
    
            df = self.ctx.sql(sql)
            return df