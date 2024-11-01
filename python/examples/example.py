from datafusion import col
from pyballista.context import BallistaContext
import pyarrow as pa

import csv
from datafusion.object_store import AmazonS3
from datafusion.context import SessionContext
import os

# Create the Ballista Context [standalone or remote]
ctx = BallistaContext().standalone()

# Register our parquet file to perform SQL operations
ctx.register_parquet("test_parquet", "./testdata/test.parquet")

# Select the data from our test parquet file
test_parquet = ctx.sql("""
    SELECT * FROM test_parquet
""")

# Show our test parquet data
print(test_parquet.show())

# To perform daatframe operations, read in data
test_csv = ctx.read_csv("./testdata/test.csv", has_header=False)

# Show the dataframe
test_csv.show()