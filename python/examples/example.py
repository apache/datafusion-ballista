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

from ballista import Ballista, BallistaBuilder
from datafusion.context import SessionContext

# Ballista will initiate with an empty config
# set config variables with `set()`
ballista = BallistaBuilder()\
    .set("ballista.job.name", "example ballista")\
    .set("ballista.shuffle.partitions", "16")\
    .set("ballista.executor.cpus", "4")\
    .build()
    
# Show the Ballista Config
print(ballista.show_config())

# Build a standalone Cluster (use `remote()`)
# for remote cluster
ctx: SessionContext = ballista.standalone()
#ctx_remote: SessionContext = ballista.remote("remote_ip", 50050)

# Select 1 to verify its working
ctx.sql("SELECT 1").show()
#ctx_remote.sql("SELECT 2").show()