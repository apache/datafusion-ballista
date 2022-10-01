import sys
import time
import argparse

parser = argparse.ArgumentParser(description='Run SQL benchmarks.')
parser.add_argument('--query', help='query to run, such as q1')
parser.add_argument('--path', help='path to data files')
parser.add_argument('--ext', default='', help='optional file extension, such as parquet')

args = parser.parse_args()

query = args.query
path = args.path
table_ext = args.ext

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
print("Query", query, "took", end - start, "second(s)")


