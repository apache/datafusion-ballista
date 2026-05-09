<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Jupyter Notebooks

Ballista works well in Jupyter notebooks. DataFrames automatically render as formatted
HTML tables when displayed in a notebook cell.

## Basic Usage

```python
from ballista import BallistaSessionContext

ctx = BallistaSessionContext("df://localhost:50050")

ctx.register_parquet("trips", "/path/to/trips.parquet")

# The result renders as an HTML table when this is the last expression in a cell
ctx.sql("SELECT * FROM trips LIMIT 10")
```

When a DataFrame is the last expression in a cell, Jupyter automatically calls its
`_repr_html_()` method, which renders a styled table with formatted column headers,
expandable cells for long text, and scrollable display for wide tables.

## Converting Results

DataFrames can be converted to various formats for further analysis:

```python
df = ctx.sql("SELECT * FROM trips WHERE fare_amount > 50")

pandas_df = df.to_pandas()
arrow_table = df.to_arrow_table()
polars_df = df.to_polars()
batches = df.collect()
```

## Example Workflow

```python
# Cell 1: Setup
from ballista import BallistaSessionContext
from datafusion import col, lit

ctx = BallistaSessionContext("df://localhost:50050")
ctx.register_parquet("orders", "/data/orders.parquet")
ctx.register_parquet("customers", "/data/customers.parquet")

# Cell 2: Explore the data
df = ctx.sql("SELECT * FROM orders LIMIT 5")

# Cell 3: Run analysis — DataFrame renders as an HTML table
df = ctx.sql("""
    SELECT
        c.name,
        COUNT(*) as order_count,
        SUM(o.amount) as total_spent
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    GROUP BY c.name
    ORDER BY total_spent DESC
    LIMIT 10
""")

# Cell 4: Convert to Pandas for visualization
import matplotlib.pyplot as plt

pandas_df = df.to_pandas()
pandas_df.plot(kind='bar', x='name', y='total_spent')
plt.show()
```

## Running a Local Cluster in a Notebook

For development and testing, you can start a local cluster directly from a notebook:

```python
from ballista import BallistaSessionContext, setup_test_cluster

host, port = setup_test_cluster()
ctx = BallistaSessionContext(f"df://{host}:{port}")
```
