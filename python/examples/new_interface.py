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
# %%
from ballista import BallistaSessionContext, BallistaExecutor, BallistaScheduler
from datafusion import col, lit, DataFrame, ParquetWriterOptions
from datafusion import functions as f
# from datafusion import SessionContext

scheduler = BallistaScheduler()
scheduler.start()

executor = BallistaExecutor()
executor.start()

# we replace
# ctx = SessionContext()
# with
ctx = BallistaSessionContext(address="df://127.0.0.1:50050")

ctx.sql("create external table t stored as parquet location '../testdata/test.parquet'")

# %%
df: DataFrame = ctx.table("t")

# %%
df.show()
# %%

# this did not work previously
df.filter(col("id") > lit(4)).show()
# %%
df.explain()
# %%
ctx.sql("select * from t limit 3").show()
# %%
df.collect()
# %%
df.collect_partitioned()
# %%
df0 = ctx.sql("SELECT 1 as r")

df0.aggregate([f.col("r")], [f.count_star()])
df0.show()
# %%
df.count()
# %%
# df.select("id").write_json("/Users/user/git/datafusion_ballista/python/target/a.json")
# %%
# df.select("id").write_csv("/Users/user/git/datafusion_ballista/python/target/a.cvs")
# %%
df.write_parquet_with_options(
    "/Users/marko/git/datafusion_ballista/python/target/a.parquet",
    ParquetWriterOptions(),
)
# %%
df.write_parquet("/Users/marko/git/datafusion_ballista/python/target/b.parquet")
# %%
