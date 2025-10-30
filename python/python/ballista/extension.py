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

from datafusion import SessionContext
from datafusion import DataFrame
from ._internal_ballista import PyBallistaRemoteExecutor

class RedefiningMeta(type):
    def __new__(cls, name, bases, attrs):
        def __wrap_dataframe_result(func):  
            def method_wrapper(*args, **kwargs):
                address = args[0].address
                df = func(*args, **kwargs)
                return DistributedDataFrame(df,address)
            return method_wrapper

        for base_name, base_value in bases[0].__dict__.items():
            #
            # could we not use 'DataFrame' as a string here?
            #
            if callable(base_value) and not base_name.startswith('__') and base_value.__annotations__.get('return') == 'DataFrame':
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

class DistributedDataFrame(DataFrame, metaclass = RedefiningMeta):
    def __init__(self, df: DataFrame, address: str):
        super().__init__(df.df)
        self.address = address

    def show(self, num: int = 20) -> None:    
        blob_plan = self.logical_plan().to_proto()
        PyBallistaRemoteExecutor.show(blob_plan, self.address)
    
    def collect(self):
        blob_plan = self.logical_plan().to_proto()
        batches = PyBallistaRemoteExecutor.collect(blob_plan, self.address)

        return batches

    # TODO we would need to override write methods
    #      and few others here 

class BallistaSessionContext(SessionContext, metaclass = RedefiningMeta):
    def __init__(self, address: str, config = None, runtime = None):
        super().__init__(config, runtime)
        self.address = address
