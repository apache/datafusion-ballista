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

# Ballista Arrow Flight SQL frontend

An [Arrow Flight SQL] frontend for Ballista, so that generic SQL clients —
JDBC/ODBC tools, BI tools, and Python's `adbc_driver_flightsql` — can run
distributed queries without linking against Ballista.

Clients send SQL text. The frontend plans it against the session's DataFusion
catalog, submits the plan through a [`QueryBackend`](backend::QueryBackend),
and returns one Flight endpoint per output partition. Redeeming a ticket is
proxied to the executor that holds the partition, so **no executor address is
ever exposed to the client** and the frontend works unchanged behind NAT,
Docker, Kubernetes, and load balancers.

This crate deliberately knows nothing about `ballista-scheduler`. The scheduler
implements `QueryBackend` behind its `flight-sql` feature; embedders running
their own Tonic server can implement it too.

## Usage

The scheduler wires this up for you — start it with the `flight-sql` feature
compiled in and `--flight-sql` at runtime:

```bash
cargo build --release -p ballista-scheduler --features flight-sql
./target/release/ballista-scheduler --flight-sql
```

Then connect any Flight SQL client to the scheduler's port (50050 by default).
See the [Flight SQL user guide] for a Python ADBC walkthrough.

## Scope

Implemented: SQL query execution, DDL, catalog and schema introspection,
`SqlInfo`/`XdbcTypeInfo`, prepared statements without parameter binding, query
cancellation, and pluggable authentication.

Not implemented: bound parameters, `PollFlightInfo` (so `GetFlightInfo` blocks
until the query finishes), transactions, Substrait, and the distributed write
path (`INSERT`/`UPDATE`/`DELETE`/`COPY`).

## Security

The default authenticator accepts every handshake, and the scheduler logs a
warning when it is in use. Separately, the Flight proxy this crate reuses trusts
the executor address carried in a partition-fetch ticket, so a client that
forges a ticket can make the scheduler dial an arbitrary host. Treat the port as
trusted-network-only until both are addressed.

[Arrow Flight SQL]: https://arrow.apache.org/docs/format/FlightSql.html
[Flight SQL user guide]: https://datafusion.apache.org/ballista/user-guide/flightsql.html
