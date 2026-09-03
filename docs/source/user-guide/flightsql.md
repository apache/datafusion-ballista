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

# Arrow Flight SQL

Ballista can serve [Arrow Flight SQL] directly from the scheduler, so any
Flight SQL client can run distributed queries without linking against Ballista:
Python's [ADBC] driver, the Arrow Flight SQL JDBC driver, and BI tools that
speak either.

Clients send SQL text. The scheduler plans it, distributes execution across the
cluster, and returns one Flight endpoint per output partition. Results are
streamed back **through the scheduler**, so clients never need to reach
executors and the frontend works unchanged behind NAT, Docker, Kubernetes, and
load balancers.

## Enabling it

Flight SQL is a non-default compile-time feature and is also off at runtime, so
a scheduler never starts serving SQL by accident.

```bash
cargo build --release -p ballista-scheduler --features flight-sql
RUST_LOG=info ./target/release/ballista-scheduler --flight-sql
```

Start one or more executors as usual:

```bash
RUST_LOG=info ./target/release/ballista-executor -c 4 -p 50051
```

Flight SQL is served on the scheduler's existing gRPC port (50050 by default) —
there is no second port to expose.

```{warning}
**Ballista ships no authentication.** With `--flight-sql` and no authenticator,
any client that can reach the scheduler port can run queries, and all
unauthenticated clients share a single session (and therefore a single
catalog). The scheduler logs a warning at startup when this is the case.

There is a second reason not to expose the port: Flight tickets carry the
executor address the scheduler should fetch a partition from, and the scheduler
does not verify that the address belongs to the cluster. A client that forges a
ticket can make the scheduler open a gRPC connection to an arbitrary host and
relay the response. This is a property of the existing Flight result proxy
rather than of Flight SQL, but enabling Flight SQL is what makes the port
something you would otherwise consider exposing.

Do not expose the port outside a trusted network. See
[Authentication](#authentication) below.
```

## Querying from Python with ADBC

Install the driver:

```bash
pip install adbc-driver-flightsql pyarrow
```

Connect to the scheduler and run a query. There is no Ballista Python package
involved here — this is the plain ADBC driver talking to the cluster:

```python
import adbc_driver_flightsql.dbapi as flight_sql

with flight_sql.connect("grpc://localhost:50050") as conn:
    with conn.cursor() as cur:
        # Register a table. DDL runs on the scheduler and lands in the
        # session's catalog.
        cur.execute(
            """
            CREATE EXTERNAL TABLE trips
            STORED AS PARQUET
            LOCATION '/data/yellow_tripdata_2022-01.parquet'
            """
        )

        # This one is planned by the scheduler and executed across the cluster.
        cur.execute(
            """
            SELECT payment_type, COUNT(*) AS trips, AVG(total_amount) AS avg_fare
            FROM trips
            GROUP BY payment_type
            ORDER BY trips DESC
            """
        )
        table = cur.fetch_arrow_table()

print(table)
```

`fetch_arrow_table()` reads every partition the scheduler advertised, so the
result arrives as Arrow the whole way from the executors — no row-by-row
conversion.

A runnable version of this, including cluster setup, is in
[`examples/python/adbc_flight_sql.py`][example].

### Introspection

The driver's metadata calls are answered from the scheduler's real DataFusion
catalog, so what you see is what you can query:

```python
with flight_sql.connect("grpc://localhost:50050") as conn:
    print(conn.adbc_get_table_types())
    print(conn.adbc_get_objects(depth="tables").read_all())

    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        print(cur.description)
```

### Streaming large results

`fetch_arrow_table()` materializes everything. For results that do not fit in
memory, take the record batch reader instead and consume it incrementally:

```python
with conn.cursor() as cur:
    cur.execute("SELECT * FROM trips")
    reader = cur.fetch_record_batch()
    for batch in reader:
        ...  # one Arrow RecordBatch at a time
```

## Connecting with JDBC

Download the [Arrow Flight SQL JDBC driver] from Maven Central and point your
tool at the scheduler:

| Setting          | Value                                                |
| ---------------- | ---------------------------------------------------- |
| Driver class     | `org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver` |
| URL              | `jdbc:arrow-flight-sql://localhost:50050`            |
| Advanced options | `useEncryption=false`                                |

`useEncryption=false` is required because Ballista serves Flight SQL over plain
gRPC; put a TLS-terminating proxy in front of it if you need encryption.

## Authentication

The frontend has no built-in credentials. Ballista's default authenticator
accepts every handshake, which is why the startup warning exists.

To require authentication, implement
`ballista_flight_sql::Authenticator` and supply it when constructing the
service. Because the trait sees the request metadata, HTTP Basic credentials
(the Flight convention) and bearer tokens both work:

```rust
use ballista_flight_sql::{Authenticator, Identity};
use tonic::{Status, metadata::MetadataMap};

struct MyAuth;

#[async_trait::async_trait]
impl Authenticator for MyAuth {
    async fn authenticate(&self, headers: &MetadataMap) -> Result<Identity, Status> {
        // Validate `headers["authorization"]` however your deployment requires.
        Ok(Identity::user("alice"))
    }
}
```

With an authenticator installed, clients must complete the Flight handshake and
send the returned bearer token on every request; each handshake gets its own
Ballista session, so catalogs are no longer shared.

Alternatively, terminate authentication in a Tonic interceptor or a proxy in
front of the scheduler and leave the frontend as-is.

## Sessions and the catalog

- A client that completes a handshake gets a **private session**. Tables it
  creates with DDL are visible only to that connection.
- Clients that do not authenticate share **one anonymous session**.
- Sessions, prepared statements, and unredeemed result tickets expire after 30
  minutes idle.
- Embedders can pre-populate the catalog for every session through the
  scheduler's `SessionBuilder`, so users do not have to re-run
  `CREATE EXTERNAL TABLE` on each connection. See
  [Extending Ballista components](extending-components.md).

## Limitations

These are known gaps, tracked in [#2298]:

- **`GetFlightInfo` blocks until the query finishes.** `PollFlightInfo` is not
  implemented, so a long query can hit a client-side deadline. Raise your
  client's timeout for TPC-H-scale queries.
- **No bound parameters.** Prepared statements are supported, but
  `DoPutPreparedStatementQuery` parameter binding is not, so
  `cur.execute(sql, parameters=...)` will fail.
- **No write path.** `INSERT`, `UPDATE`, `DELETE`, and `COPY` are rejected with
  a clear error rather than silently executing on the scheduler. `CREATE TABLE
  AS SELECT` is rejected for the same reason — it would run its query on the
  scheduler instead of the cluster. Other DDL, including `CREATE EXTERNAL
  TABLE` and `CREATE VIEW`, is supported.
- **No transactions or savepoints.**
- **No Substrait.** `CommandStatementSubstraitPlan` is not implemented, even
  when the scheduler's `substrait` feature is enabled.
- **Primary/foreign key metadata is not implemented**, so tools that browse
  relationships will show none.

[Arrow Flight SQL]: https://arrow.apache.org/docs/format/FlightSql.html
[ADBC]: https://arrow.apache.org/adbc/
[Arrow Flight SQL JDBC driver]: https://central.sonatype.com/artifact/org.apache.arrow/flight-sql-jdbc-driver
[#2298]: https://github.com/apache/datafusion-ballista/issues/2298
[example]: https://github.com/apache/datafusion-ballista/blob/main/examples/python/adbc_flight_sql.py
