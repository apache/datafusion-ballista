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

from ballista import StandaloneBallista, RemoteBallista
from datafusion.context import SessionContext

# Create the Ballista Context [standalone or remote]
ctx: SessionContext = StandaloneBallista.build()

# Register our parquet file to perform SQL operations
ctx.register_parquet("test_parquet", "./testdata/test.parquet")

# Select the data from our test parquet file
test_parquet = ctx.sql("SELECT * FROM test_parquet")

# Show our test parquet data
print(test_parquet.show())

# To perform daatframe operations, read in data
test_csv = ctx.read_csv("./testdata/test.csv", has_header=False)

# Show the dataframe
test_csv.show()