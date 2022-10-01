import sys
import time

query = sys.argv[1]
path = sys.argv[2]
table_ext = ""
if len(sys.argv) > 3:
    table_ext = sys.argv[3]

import ballista
ctx = ballista.BallistaContext("localhost", 50050)

tables = ["part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region"]

for table in tables:
    table_path = path + "/" + table
    if len(table_ext) > 0:
        table_path = table_path + "." + table_ext
    print("Registering table", table, "at path", table_path)
    ctx.register_parquet(table, table_path)

with open("queries/" + query + ".sql", 'r') as file:
    sql = file.read()

import time

start = time.time()

df = ctx.sql(sql)
df.show()

end = time.time()
print(end - start)


