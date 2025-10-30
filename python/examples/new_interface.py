# %%
from ballista import BallistaSessionContext, BallistaExecutor, BallistaScheduler
from datafusion import col, lit, DataFrame
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
df : DataFrame = ctx.table("t")

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

from datafusion import functions as f
df0 = ctx.sql("SELECT 1 as r")

df0.aggregate(
    [f.col("r")], [f.count_star()]
)
df0.show()
# %%
