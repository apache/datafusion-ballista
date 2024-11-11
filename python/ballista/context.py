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
from ballista import Ballista, BallistaConfig, BallistaConfigBuilder

from typing import List, Any
       
class BallistaConfigBuilder:
    def __init__(self) -> None:
        pass
    
    def set(self, k: str, v: str):
        BallistaConfigBuilder.set(self, k, v)
        
    def build() -> BallistaConfig:
        BallistaConfig()
       
class BallistaConfig:
    def __init__(self):
        self.config = self.with_settings({})
        
    def builder(self) -> BallistaConfig:
        builder = self.builder()
        
        return builder
    
    def with_settings(self, settings: dict) -> BallistaConfig:
        self.with_settings(settings)
        
        return self
        
    def settings(self) -> None:
        self.settings()
        
    def default_shuffle_partitions(self):
        self.default_shuffle_partitions()
    
    def default_batch_size(self):
        self.default_batch_size()
        
    def hash_join_single_partition_threshold(self):
        self.hash_join_single_partition_threshold()
        
    def default_grpc_client_max_message_size(self):
        self.default_grpc_client_max_message_size()
        
    def repartition_joins(self):
        self.repartition_joins()
        
    def repartition_aggregations(self):
        self.repartition_aggregations()
        
    def repartition_windows(self):
        self.repartition_windows()
        
    def parquet_pruning(self):
        self.parquet_pruning()
        
    def collect_statistics(self):
        self.collect_statistics()
        
    def default_standalone_parallelism(self):
        self.default_standalone_parallelism()
        
    def default_with_information_schema(self):
        self.default_with_information_schema()
        
class Ballista:
    def __init__(self):
        self.config = BallistaConfig()
    
    def configuration(self, settings: dict):
        self.config = BallistaConfig().builder().with_settings(settings)
    
    def standalone(self) -> SessionContext:
        return self.standalone()
        
    def remote(self, url: str) -> SessionContext:
        return self.remote(url)
        
    def settings(self):
        self.config.settings()
        
    def default_shuffle_partitions(self):
        self.config.default_shuffle_partitions()
        
    def default_batch_size(self):
        self.config.default_batch_size()
        
    def hash_join_single_partition_threshold(self):
        self.config.hash_join_single_partition_threshold()
        
    def default_grpc_client_max_message_size(self):
        self.config.default_grpc_client_max_message_size()
        
    def repartition_joins(self):
        self.config.repartition_joins()
        
    def repartion_aggregations(self):
        self.config.repartition_aggregations()
        
    def repartition_windows(self):
        self.config.repartition_windows()
        
    def parquet_pruning(self):
        self.config.parquet_pruning()
        
    def collect_statistics(self):
        self.config.collect_statistics()
    
    def default_standalone_parallelism(self):
        self.config.default_standalone_parallelism()
        
    def default_with_information_schema(self):
        self.config.default_with_information_schema()