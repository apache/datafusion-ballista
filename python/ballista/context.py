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

from _typeshed import Self
from datafusion import SessionContext
from ballista import SessionConfig, SessionStateBuilder, SessionState, Ballista

from typing import List, Any
       
class SessionConfig:
    def __new__(cls):
        return super().__new__(cls)
        
    def __init__(self):
        self.session_config = SessionConfig()
        
    def set_str(self, key: str, value: str):
        self.session_config.set_str(key, value)
        
class SessionStateBuilder:
    def __new__(cls):
        return super().__new__(cls)
        
    def __init__(self) -> None:
        self.state = SessionStateBuilder()
        
    def with_config(self, config: SessionConfig) -> SessionStateBuilder:
        self.with_config(config)
        
        return self
        
    def build(self) -> SessionState:
        self.build()
        
class SessionState:
    def __new__(cls):
        return super().__new__(cls)
        
class Ballista:
    def __new__(cls):
        return super().__new__(cls)
        
    def __init__(self) -> None:
        self.state = Ballista()
        
    def standalone(self):
        self.standalone()