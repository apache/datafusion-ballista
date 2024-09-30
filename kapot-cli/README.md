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

# kapot Command-line Interface

[kapot][kapot] is a distributed query execution framework, written in Rust, that uses Apache Arrow as its in-memory format.

The kapot CLI allows SQL queries to be executed by an in-process DataFusion context, or by a distributed
kapot context.

```ignore
USAGE:
    kapot-cli [OPTIONS]

OPTIONS:
    -c, --batch-size <BATCH_SIZE>
            The batch size of each query, or use kapot default

        --color
            Enables console syntax highlighting

        --concurrent-tasks <CONCURRENT_TASKS>
            The max concurrent tasks, only for kapot local mode. Default: all available cores

    -f, --file <FILE>...
            Execute commands from file(s), then exit

        --format <FORMAT>
            [default: table] [possible values: csv, tsv, table, json, nd-json, automatic]

    -h, --help
            Print help information

        --host <HOST>
            kapot scheduler host

    -p, --data-path <DATA_PATH>
            Path to your data, default to current directory

        --port <PORT>
            kapot scheduler port

    -q, --quiet
            Reduce printing other than the results and work quietly

    -r, --rc <RC>...
            Run the provided files on startup instead of ~/.kapotrc

    -V, --version
            Print version information
```

## Example

Create a CSV file to query.

```bash,ignore
$ echo "1,2" > data.csv
```

```sql,ignore
$ kapot-cli

kapot CLI v0.12.0

> CREATE EXTERNAL TABLE foo (a INT, b INT) STORED AS CSV LOCATION 'data.csv';
0 rows in set. Query took 0.001 seconds.

> SELECT * FROM foo;
+---+---+
| a | b |
+---+---+
| 1 | 2 |
+---+---+
1 row in set. Query took 0.017 seconds.

> \q
```

## kapot-Cli

If you want to execute the SQL in kapot by `kapot-cli`, you must build/compile `kapot-cli` first.

```bash
cd arrow-kapot/kapot-cli
cargo build
cargo install --path .
```

The kapot CLI can connect to a kapot scheduler for query execution.

```bash
kapot-cli --host localhost --port 50050
```

[kapot]: https://crates.io/crates/kapot
