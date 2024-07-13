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
2. Run the [Ballista docker container](#docker)
3. Download the [Arrow Flight SQL JDBC Driver](#jdbc)
4. [Install the driver](#tool) into your favorite JDBC tool
5. Run a ["hello, world!"](#hello) query
6. Register a table and run more complicated queries

## <a name="prereq"/>Prerequisites

### Ubuntu

```shell
sudo apt-get update
sudo apt-get install -y docker.io
```

### MacOS

```shell
brew install docker
```

### Windows

```shell
choco install docker-desktop
```

## <a name="docker"/> Run Docker Container

```shell
docker run -p 50050:50050 --rm ghcr.io/apache/arrow-ballista-standalone:0.10.0
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

To register the built-in demo table, use the syntax below:

```sql
create external table taxi stored as parquet location '/data/yellow_tripdata_2022-01.parquet';
```

Once the table has been registered, all the normal SQL queries can be performed:

```sql
select * from taxi limit 10;
```

🎉 Happy querying! 🎉
