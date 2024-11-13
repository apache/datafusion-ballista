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

from ballista import Ballista, SessionConfig, SessionStateBuilder
from datafusion.context import SessionContext

# Ballista will initiate with an empty config
ballista = Ballista()
config = SessionConfig()\
    .set_str("BALLISTA_DEFAULT_SHUFFLE_PARTITIONS", "4")
    
# Build the state
state = SessionStateBuilder()\
    .with_config(config)\
    .build()

# Create the context
ctx: SessionContext = Ballista().standalone()

ctx.sql("SELECT 1").show()

"""
# Define custom settings
job_settings = {
    "BALLISTA_JOB_NAME": "Example Ballista Job",
    "DEFAULT_SHUFFLE_PARTITIONS": "2"
}

ballista.configuration(job_settings)

# But you can also set your own config
print("New Ballista Config: ", ballista.settings())

# Or you can check default settings in BallistaConfig
print("Default Shuffle Partitions: ", ballista.default_shuffle_partitions())
# Create the Ballista Context [standalone or remote]
ctx: SessionContext  = ballista.standalone() # Ballista.remote()

# Register our parquet file to perform SQL operations
ctx.register_parquet("test_parquet", "./testdata/test.parquet")

# Select the data from our test parquet file
test_parquet = ctx.sql("SELECT * FROM test_parquet")

# Show our test parquet data
print(test_parquet.show())

# To perform dataframe operations, read in data
test_csv = ctx.read_csv("./testdata/test.csv", has_header=False)

# Show the dataframe
test_csv.show()
"""
