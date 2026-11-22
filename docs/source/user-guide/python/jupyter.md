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
HTML tables when displayed in a notebook cell, and the `ballista.jupyter` extension
adds SQL magic commands, plan visualization and progress reporting.

Install the Jupyter extras with:

```bash
pip install "ballista[jupyter]"
```

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

## SQL Magic Commands

Load the extension and connect to a cluster:

```python
%load_ext ballista.jupyter

%ballista connect df://localhost:50050
```

The magic commands use their own `SessionContext`, created by `%ballista connect`.
Tables registered on a context created with `BallistaSessionContext` are not visible to
them, register tables with `%register` instead. The scheduler is only contacted when a
query runs, so `%ballista connect` does not check that the cluster is reachable.

| Command                             | Description                                        |
| ----------------------------------- | -------------------------------------------------- |
| `%ballista connect <url>`           | Connect to a cluster, `df://` is added if missing  |
| `%ballista disconnect`              | Drop the connection                                |
| `%ballista status`                  | Show the address, session id and query count       |
| `%ballista tables`                  | List registered tables by schema                   |
| `%ballista schema <table>`          | Show the columns of a table                        |
| `%ballista history`                 | Show the last 10 queries with their planning time  |
| `%register <format> <table> <path>` | Register a `parquet`, `csv`, `json` or `avro` file |
| `%sql <query>`                      | Run a single-line query                            |
| `%%sql [var]`                       | Run the cell as a query, store the result in `var` |

```python
%register parquet orders /data/orders.parquet

%sql SELECT COUNT(*) FROM orders
```

```python
%%sql top_customers
SELECT customer_id, SUM(amount) AS total
FROM orders
GROUP BY customer_id
ORDER BY total DESC
LIMIT 10
```

`%sql` and `%%sql` return a `datafusion.DataFrame`, which renders as a table. The most
recent result is also available as `_last_result`.

## Visualizing Query Plans

`explain_visual` renders the logical plan of a DataFrame as a graph, without running
the query:

```python
from ballista.jupyter import explain_visual

df = ctx.sql("SELECT * FROM orders WHERE amount > 100")
explain_visual(df)

# save as SVG, or as Graphviz DOT
explain_visual(df).save("plan.svg")
explain_visual(df).save("plan.dot")
```

The SVG is rendered with the Graphviz `dot` command, for example installed with
`brew install graphviz` or `apt install graphviz`. Without it, the plan is shown as
text. For runtime metrics use `df.explain(analyze=True)`, which runs the query on the
cluster.

## Progress Reporting

`collect_with_progress` collects a DataFrame and shows the elapsed time while the query
runs:

```python
from ballista.jupyter import collect_with_progress

batches = collect_with_progress(df)
```

Outside Jupyter, or to handle progress yourself, pass a callback. `progress` is `-1.0`
while the query runs and `1.0` when it completes:

```python
def on_progress(status: str, progress: float):
    print(status)

batches = collect_with_progress(df, callback=on_progress, poll_interval=1.0)
```

## Example Notebooks

The [`python/examples`](https://github.com/apache/datafusion-ballista/tree/main/python/examples)
directory contains notebooks for getting started, the DataFrame API and distributed
queries.

## Running a Local Cluster in a Notebook

For development and testing, you can start a local cluster directly from a notebook:

```python
from ballista import BallistaSessionContext, setup_test_cluster

host, port = setup_test_cluster()
ctx = BallistaSessionContext(f"df://{host}:{port}")
```
