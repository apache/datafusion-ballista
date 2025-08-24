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

from ballista import BallistaBuilder
from datafusion.context import SessionContext

ctx: SessionContext = (
    BallistaBuilder()
    .config("datafusion.catalog.information_schema", "true")
    .config("ballista.job.name", "example ballista")
    .standalone()
)


ctx.sql("SELECT 1").show()

# %%
ctx.sql("SHOW TABLES").show()
# %%
ctx.sql(
    "select name, value from information_schema.df_settings where name like 'ballista.job.name'"
).show()


# %%
