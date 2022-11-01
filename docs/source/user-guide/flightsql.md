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

# Using FlightSQL to Connect to Ballista

One of the easiest ways to start with Ballista is to plug it into your existing data infrastructure using support for Arrow Flight SQL JDBC.

Getting started involves these main steps:

1. [Installing prerequisites](#prereq)
2. Build the [Ballista rust code](#rust)
3. Build and run the [Ballista docker containers](#docker)
4. Build the [Arrow Flight SQL JDBC Driver](#jdbc)
5. [Install the driver](#tool) into your favorite JDBC tool
6. Run a ["hello, world!"](#hello) query
7. Register a table and run more complicated queries

## <a name="prereq"/>Prerequisites

### Ubuntu

```shell
sudo apt-get update
sudo apt-get install -y docker.io docker-compose
```

### MacOS

```shell
brew install docker docker-compose
```

### Windows

```shell
choco install docker-desktop docker-compose
```

## <a name="rust"/>Building Ballista

To build in docker (non-linux systems):

```shell
git clone https://github.com/apache/arrow-ballista.git
dev/build-ballista-rust.sh
```

Or in linux-based systems with the correct dependencies installed, one can simply:

```shell
cargo build --release --all --features flight-sql
```

## <a name="docker"/> Run Docker Containers

```shell
source dev/build-ballista-docker.sh
docker-compose up
```

## <a name="jdbc"/>Download the FlightSQL JDBC Driver

Download the [FlightSQL JDBC Driver](https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/10.0.0/flight-sql-jdbc-driver-10.0.0.jar)
from Maven Central.

## <a name="tool"/>Use the Driver in your Favorite Data Tool

The important pieces of information:

| Key              | Value                                              |
| ---------------- | -------------------------------------------------- |
| Driver file      | flight-sql-jdbc-driver-10.0.0-SNAPSHOT.jar         |
| Class Name       | org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver |
| Authentication   | User & Password                                    |
| Username         | admin                                              |
| Password         | password                                           |
| Advanced Options | useEncryption=false                                |
| URL              | jdbc:arrow-flight://127.0.0.1:50050                |

## <a name="hello"/>Run a "Hello, World!" Query

```sql
select 'Hello from Arrow Ballista!' as greeting;
```

## <a name="complex"/>Run a Complex Query

In order to run queries against data, tables need to be "registered" with the current session (and re-registered upon each new connection).

To register a table, find a `.csv`, `.json`, or `.parquet` file for testing, and use the syntax below:

```sql
create external table customer stored as CSV with header row
    location '/path/to/customer.csv';
```

Once the table has been registered, all the normal SQL queries can be performed:

```sql
select * from customer;
```

🎉 Happy querying! 🎉
