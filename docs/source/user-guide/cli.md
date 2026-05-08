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

# Ballista Command-line Interface

The Ballista CLI allows SQL queries to be executed against a Ballista cluster, or in standalone mode in a single
process.

Use Cargo to install:

```bash
cargo install ballista-cli
```

## Usage

```
USAGE:
    ballista-cli [OPTIONS]

OPTIONS:
    -c, --batch-size <BATCH_SIZE>    The batch size of each query, or use Ballista default
    -f, --file <FILE>...             Execute commands from file(s), then exit
        --format <FORMAT>            [default: table] [possible values: csv, tsv, table, json,
                                     nd-json]
    -h, --help                       Print help information
        --host <HOST>                Ballista scheduler host
    -p, --data-path <DATA_PATH>      Path to your data, default to current directory
        --port <PORT>                Ballista scheduler port
    -q, --quiet                      Reduce printing other than the results and work quietly
    -r, --rc <RC>...                 Run the provided files on startup instead of ~/.ballistarc
        --tui                        Enables terminal user interface (requires `tui` feature)
    -V, --version                    Print version information
```

## Example

Create a CSV file to query.

```bash
$ echo "1,2" > data.csv
```

## Run Ballista CLI in Distributed Mode

The CLI can connect to a Ballista scheduler for query execution.

```bash
ballista-cli --host localhost --port 50050
```

## Run Ballista CLI in Standalone Mode

It is also possible to run the CLI in standalone mode, where it will create a scheduler and executor in-process.

```bash
$ ballista-cli

Ballista CLI v52.0.0

> CREATE EXTERNAL TABLE foo (a INT, b INT) STORED AS CSV LOCATION 'data.csv';
0 rows in set. Query took 0.001 seconds.

> SELECT * FROM foo;
+---+---+
| a | b |
+---+---+
| 1 | 2 |
+---+---+
1 row in set. Query took 0.017 seconds.
```

## Cli commands

Available commands inside Ballista CLI are:

- Quit

```bash
> \q
```

- Help

```bash
> \?
```

- ListTables

```bash
> \d
```

- DescribeTable

```bash
> \d table_name
```

- QuietMode

```
> \quiet [true|false]
```

- list function

```bash
> \h
```

- Search and describe function

```bash
> \h function_table
```

## Terminal User Interface (TUI)

When Ballista CLI is built with the `tui` feature, you can launch an interactive terminal user interface
that provides a visual overview of the Ballista cluster.

### Launching the TUI

There are two ways to start the TUI:

1. Directly via command-line flag:

```bash
ballista-cli --tui
```

2. From within the Ballista CLI interactive shell:

```bash
ballista-cli
> \tui
```

### TUI Features

The TUI provides the following views:

- **Executors**: Lists all registered executors with their host, port, CPU cores, memory, and current job count. Supports sorting by any column.
- **Jobs**: Displays active and completed jobs with their status, start time, and duration. Supports sorting, job search (`/`), and shows job details on selection.
- **Job Stages**: When viewing a job, press `Enter` to see its execution stages with input/output rows, elapsed compute, and task percentiles.
- **Stage Tasks & Plan**: Within the Job Stages view, press `Enter` to see individual task details or `p` to view the stage execution plan.
- **Job Plans**: For completed jobs, press `p` to view the Stage, Physical, or Logical query plans.
- **Job Stages Graph**: Press `g` to visualize the job's stage execution graph.
- **Metrics**: Fetches and displays Prometheus metrics from the scheduler, including query execution statistics.
- **Scheduler Info**: Shows the current scheduler state and configuration.

### TUI Navigation

#### Global Keybindings

| Key         | Action                                  |
| ----------- | --------------------------------------- |
| `j`         | Switch to Jobs view                     |
| `e`         | Switch to Executors view                |
| `m`         | Switch to Metrics view                  |
| `i`         | Show Scheduler Info popup               |
| `?` / `h`   | Show help overlay with all key bindings |
| `q` / `Esc` | Quit the TUI                            |

#### Jobs View Keybindings

| Key                   | Action                                                                                      |
| --------------------- | ------------------------------------------------------------------------------------------- |
| `↑` / `↓`             | Navigate rows in the jobs table                                                             |
| `1` / `2` / `3` / ... | Sort by first/second/third/... column (press again to reverse, third press removes sorting) |
| `/`                   | Search jobs                                                                                 |
| `Enter`               | Open Job Stages popup for the selected job                                                  |
| `g`                   | View job stages graph (DOT visualization) for completed jobs                                |
| `c`                   | Cancel the selected job (if cancelable)                                                     |
| `p`                   | View job plans (Stage / Physical / Logical) for completed jobs                              |

#### Job Stages Popup Keybindings

| Key       | Action                                     |
| --------- | ------------------------------------------ |
| `↑` / `↓` | Navigate stages                            |
| `Enter`   | View tasks for the selected stage          |
| `p`       | View execution plan for the selected stage |
| `Esc`     | Close popup                                |

#### Stage Tasks / Plan Popup Keybindings

| Key   | Action                     |
| ----- | -------------------------- |
| `Esc` | Return to Job Stages popup |

#### Job Stages Graph Popup Keybindings

| Key       | Action         |
| --------- | -------------- |
| `↑` / `↓` | Scroll up/down |
| `Esc`     | Close popup    |

#### Job Plans Popup Keybindings

| Key       | Action             |
| --------- | ------------------ |
| `s`       | Show Stage plan    |
| `p`       | Show Physical plan |
| `l`       | Show Logical plan  |
| `↑` / `↓` | Scroll up/down     |
| `Esc`     | Close popup        |

#### Executors View Keybindings

| Key             | Action                            |
| --------------- | --------------------------------- |
| `1` / `2` / `3` | Sort by first/second/third column |

#### Metrics View Keybindings

| Key | Action         |
| --- | -------------- |
| `/` | Search metrics |

### TUI Configuration

The TUI reads its configuration from a YAML file located at the platform-specific config directory:

| Platform | Example Path                                      |
| -------- | ------------------------------------------------- |
| Linux    | `~/.config/ballista/tui.yaml`                     |
| macOS    | `~/Library/Application Support/ballista/tui.yaml` |
| Windows  | `%LOCALAPPDATA%\ballista\tui.yaml`                |

Create the file manually if it does not exist. The following settings are available:

```yaml
tick_interval_ms: 2000

scheduler:
  url: http://localhost:50050

http:
  timeout: 2000
```

- `tick_interval_ms`: How often the TUI refreshes data from the scheduler (milliseconds).
- `scheduler.url`: The Ballista scheduler HTTP endpoint.
- `http.timeout`: HTTP request timeout in milliseconds.

Environment variables prefixed with `BALLISTA_` also override these values. For example:

```bash
BALLISTA_SCHEDULER_URL=http://localhost:50051 ballista-cli --tui
```

The TUI connects to the scheduler via HTTP and refreshes data automatically every few seconds.
