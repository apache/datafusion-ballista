// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Protocol-level tests for the Flight SQL frontend, driven through the
//! `FlightSqlService` trait against a stub backend.
//!
//! These deliberately avoid a live cluster so they run anywhere; the
//! scheduler's `tests/flight_sql.rs` covers the same surface against real
//! executors. Everything here is asserted the way a client sees it: tickets
//! are decoded from the wire bytes rather than through crate internals, so
//! the tests fail if the wire format changes.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, Any,
    CommandGetSqlInfo, CommandPreparedStatementQuery, CommandStatementQuery,
    TicketStatementQuery,
};
use arrow_flight::{Action, FlightDescriptor, FlightInfo, Ticket};
use async_trait::async_trait;
use ballista_core::error::Result;
use ballista_core::flight_proxy_service::BallistaFlightProxyService;
use ballista_core::serde::decode_protobuf;
use ballista_core::serde::protobuf::{ExecutorMetadata, PartitionId, PartitionLocation};
use ballista_core::serde::scheduler::{
    Action as BallistaAction, ShuffleFileKind, ShuffleLayout,
};
use ballista_flight_sql::backend::{QueryBackend, QueryResult};
use ballista_flight_sql::{
    ANONYMOUS_SESSION, AnonymousAuthenticator, Authenticator, BallistaFlightSqlService,
    Identity,
};
use dashmap::DashMap;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::empty::EmptyTable;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use prost::Message;
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

/// A backend that never contacts a cluster: it hands out real
/// `SessionContext`s so planning is genuine, and fabricates the partition
/// locations a completed job would have produced.
#[derive(Default)]
struct StubBackend {
    sessions: DashMap<String, Arc<SessionContext>>,
    executed: DashMap<String, usize>,
}

const JOB_ID: &str = "job-1";

/// How many output partitions every stub job reports.
const PARTITIONS: u32 = 3;

#[async_trait]
impl QueryBackend for StubBackend {
    async fn session(&self, session_id: &str) -> Result<Arc<SessionContext>> {
        Ok(self
            .sessions
            .entry(session_id.to_string())
            .or_insert_with(|| Arc::new(SessionContext::new()))
            .clone())
    }

    async fn close_session(&self, session_id: &str) -> Result<()> {
        self.sessions.remove(session_id);
        Ok(())
    }

    async fn execute(
        &self,
        _job_name: &str,
        _ctx: Arc<SessionContext>,
        plan: LogicalPlan,
    ) -> Result<QueryResult> {
        *self.executed.entry(JOB_ID.to_string()).or_insert(0) += 1;

        let partitions = (0..PARTITIONS)
            .map(|partition_id| PartitionLocation {
                map_partition_id: 0,
                partition_id: Some(PartitionId {
                    job_id: JOB_ID.to_string(),
                    stage_id: 3,
                    partition_id,
                }),
                executor_meta: Some(ExecutorMetadata {
                    id: "executor-1".to_string(),
                    host: "executor-host".to_string(),
                    port: 50051,
                    grpc_port: 50052,
                    specification: None,
                    os_info: None,
                }),
                partition_stats: None,
                file_id: None,
                is_sort_shuffle: false,
            })
            .collect();

        Ok(QueryResult {
            job_id: JOB_ID.to_string(),
            schema: Arc::new(plan.schema().as_arrow().clone()),
            partitions,
        })
    }
}

/// Rejects everything, to check that the frontend actually consults the
/// authenticator and refuses tokenless requests when one is installed.
struct DenyAll;

#[async_trait]
impl Authenticator for DenyAll {
    async fn authenticate(
        &self,
        _headers: &MetadataMap,
    ) -> std::result::Result<Identity, Status> {
        Err(Status::unauthenticated("nope"))
    }
}

type Service = BallistaFlightSqlService<StubBackend>;

fn make_service(backend: Arc<StubBackend>) -> Service {
    BallistaFlightSqlService::new(
        backend,
        BallistaFlightProxyService::new(4_194_304, 4_194_304, false, None),
    )
}

/// Serves `service` on a local port, so tests can take part in the real
/// handshake, which needs a streaming request only a transport can build.
async fn serve(service: Arc<Service>) -> Channel {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::from_arc(service))
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
    );
    Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap()
}

/// Performs a handshake and returns the bearer token the server issued.
async fn handshake(channel: &Channel) -> String {
    let mut client = FlightSqlServiceClient::new(channel.clone());
    client.handshake("user", "").await.expect("handshake");
    client.token().expect("the server issues a token").clone()
}

fn with_token<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

fn descriptor() -> Request<FlightDescriptor> {
    Request::new(FlightDescriptor::new_cmd(vec![]))
}

fn statement(query: &str) -> CommandStatementQuery {
    CommandStatementQuery {
        query: query.to_string(),
        transaction_id: None,
    }
}

/// `expect_err` needs `Debug` on the success type, and Flight's stream
/// responses do not have it.
fn expect_err<T>(result: std::result::Result<T, Status>, msg: &str) -> Status {
    match result {
        Ok(_) => panic!("{msg}"),
        Err(status) => status,
    }
}

/// Pulls the ticket out of an endpoint exactly as a Flight client would.
fn statement_ticket(info: &FlightInfo, index: usize) -> TicketStatementQuery {
    let ticket = info.endpoint[index].ticket.as_ref().expect("ticket");
    let any = Any::decode(&*ticket.ticket).expect("ticket is an Any");
    any.unpack().expect("unpackable").expect("statement ticket")
}

fn ticket_for(info: &FlightInfo, index: usize) -> Ticket {
    info.endpoint[index].ticket.clone().expect("ticket")
}

async fn register_table(backend: &StubBackend, session_id: &str) {
    let ctx = backend.session(session_id).await.unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    ctx.register_table("people", Arc::new(EmptyTable::new(schema)))
        .unwrap();
}

#[tokio::test]
async fn select_produces_one_endpoint_per_partition_with_no_location() {
    let backend = Arc::new(StubBackend::default());
    register_table(&backend, ANONYMOUS_SESSION).await;
    let service = make_service(backend.clone());

    let info = service
        .get_flight_info_statement(statement("SELECT id FROM people"), descriptor())
        .await
        .expect("query planned and submitted")
        .into_inner();

    assert_eq!(info.endpoint.len(), PARTITIONS as usize);

    let mut seen = HashSet::new();
    for index in 0..info.endpoint.len() {
        assert!(
            info.endpoint[index].location.is_empty(),
            "an endpoint with no location tells the client to reuse its existing \
             connection; advertising the executor is what broke #1012"
        );

        // The ticket must carry the executor-facing fetch action, so the proxy
        // can redeem it without any extra server-side lookup.
        let handle = statement_ticket(&info, index).statement_handle;
        let action =
            decode_protobuf(&handle[1..]).expect("handle wraps a Ballista action");
        let BallistaAction::FetchPartition {
            job_id,
            stage_id,
            partition_id,
            host,
            port,
            ..
        } = action;
        assert_eq!(job_id.to_string(), JOB_ID);
        assert_eq!(stage_id, 3);
        assert_eq!(host, "executor-host");
        assert_eq!(port, 50051);
        seen.insert(partition_id);
    }
    assert_eq!(
        seen,
        (0..PARTITIONS as usize).collect(),
        "each endpoint must name a different partition"
    );
}

#[tokio::test]
async fn ddl_runs_on_the_scheduler_and_its_result_is_single_use() {
    let backend = Arc::new(StubBackend::default());
    let service = make_service(backend.clone());

    let info = service
        .get_flight_info_statement(statement("CREATE SCHEMA reporting"), descriptor())
        .await
        .expect("DDL executes")
        .into_inner();

    // DDL never reaches the cluster.
    assert!(backend.executed.is_empty());

    // The schema really was created in the session the client will query.
    let ctx = backend.session(ANONYMOUS_SESSION).await.unwrap();
    assert!(
        ctx.catalog("datafusion")
            .unwrap()
            .schema("reporting")
            .is_some()
    );

    let ticket = statement_ticket(&info, 0);
    assert_eq!(
        ticket.statement_handle[0], 1,
        "DDL results use the local-result tag"
    );

    service
        .do_get_statement(ticket.clone(), Request::new(ticket_for(&info, 0)))
        .await
        .expect("first fetch succeeds");

    let err = expect_err(
        service
            .do_get_statement(ticket, Request::new(ticket_for(&info, 0)))
            .await,
        "a ticket is redeemable once",
    );
    assert_eq!(err.code(), Code::NotFound);
}

/// Each of these would run on the scheduler rather than the cluster, or write
/// through a path that does not exist. They must be refused, and must not take
/// effect.
#[tokio::test]
async fn statements_that_cannot_be_distributed_are_refused() {
    let backend = Arc::new(StubBackend::default());
    register_table(&backend, ANONYMOUS_SESSION).await;
    let service = make_service(backend.clone());

    // `EXECUTE` needs something to execute. `PREPARE` itself only records the
    // plan in the session, so it is allowed.
    service
        .get_flight_info_statement(
            statement("PREPARE q AS SELECT id FROM people"),
            descriptor(),
        )
        .await
        .expect("PREPARE only edits the session");

    for query in [
        "INSERT INTO people VALUES (1, 'x')",
        "CREATE TABLE big AS SELECT id FROM people",
        "COPY people TO 'people.csv' STORED AS CSV",
        "EXECUTE q",
        "EXPLAIN ANALYZE INSERT INTO people VALUES (1, 'x')",
    ] {
        let err = expect_err(
            service
                .get_flight_info_statement(statement(query), descriptor())
                .await,
            &format!("{query} must be refused"),
        );
        assert_eq!(err.code(), Code::Unimplemented, "{query}: {err}");
    }

    assert!(backend.executed.is_empty());
    let ctx = backend.session(ANONYMOUS_SESSION).await.unwrap();
    assert!(
        !ctx.table_exist("big").unwrap(),
        "the refused CTAS must not have taken effect"
    );
}

#[tokio::test]
async fn queries_mixing_information_schema_with_other_tables_are_refused() {
    let backend = Arc::new(StubBackend::default());
    let ctx = Arc::new(SessionContext::new_with_config(
        SessionConfig::new().with_information_schema(true),
    ));
    backend.sessions.insert(ANONYMOUS_SESSION.to_string(), ctx);
    register_table(&backend, ANONYMOUS_SESSION).await;
    let service = make_service(backend.clone());

    for query in [
        "SELECT table_name FROM information_schema.tables \
         WHERE table_name IN (SELECT name FROM people)",
        "SELECT name FROM people \
         WHERE name IN (SELECT table_name FROM information_schema.tables)",
        "SELECT t.table_name FROM information_schema.tables t \
         JOIN people p ON t.table_name = p.name",
    ] {
        let err = expect_err(
            service
                .get_flight_info_statement(statement(query), descriptor())
                .await,
            &format!("{query} must be refused"),
        );
        assert_eq!(err.code(), Code::Unimplemented, "{query}: {err}");
        assert!(
            err.message().contains("information_schema together"),
            "{err}"
        );
    }

    assert!(backend.executed.is_empty());
}

#[tokio::test]
async fn prepared_statements_are_planned_once_and_expire_on_close() {
    let backend = Arc::new(StubBackend::default());
    register_table(&backend, ANONYMOUS_SESSION).await;
    let service = make_service(backend.clone());

    let prepared = service
        .do_action_create_prepared_statement(
            ActionCreatePreparedStatementRequest {
                query: "SELECT id FROM people".to_string(),
                transaction_id: None,
            },
            Request::new(Action::default()),
        )
        .await
        .expect("statement prepared");

    assert!(
        !prepared.dataset_schema.is_empty(),
        "clients need the result schema before executing"
    );

    let handle = prepared.prepared_statement_handle.clone();
    service
        .get_flight_info_prepared_statement(
            CommandPreparedStatementQuery {
                prepared_statement_handle: handle.clone(),
            },
            descriptor(),
        )
        .await
        .expect("prepared statement executes");

    service
        .do_action_close_prepared_statement(
            ActionClosePreparedStatementRequest {
                prepared_statement_handle: handle.clone(),
            },
            Request::new(Action::default()),
        )
        .await
        .expect("closes");

    let err = service
        .get_flight_info_prepared_statement(
            CommandPreparedStatementQuery {
                prepared_statement_handle: handle,
            },
            descriptor(),
        )
        .await
        .expect_err("a closed handle must not linger");
    assert_eq!(err.code(), Code::NotFound);
}

/// With an authenticator installed, every Flight SQL handler that reads data
/// or changes state refuses a request that carries no token.
#[tokio::test]
async fn an_authenticator_makes_tokenless_requests_fail() {
    let backend = Arc::new(StubBackend::default());
    let service = make_service(backend).with_authenticator(Arc::new(DenyAll));
    assert!(!service.allows_anonymous());
    assert!(AnonymousAuthenticator.allows_anonymous());

    let handle = b"some-handle".to_vec();
    let local_ticket = TicketStatementQuery {
        statement_handle: [&[1u8][..], b"some-result"].concat().into(),
    };

    let codes = [
        service
            .get_flight_info_statement(statement("SELECT 1"), descriptor())
            .await
            .map(|_| ()),
        service
            .do_get_statement(local_ticket, Request::new(Ticket::default()))
            .await
            .map(|_| ()),
        service
            .do_action_create_prepared_statement(
                ActionCreatePreparedStatementRequest {
                    query: "SELECT 1".to_string(),
                    transaction_id: None,
                },
                Request::new(Action::default()),
            )
            .await
            .map(|_| ()),
        service
            .get_flight_info_prepared_statement(
                CommandPreparedStatementQuery {
                    prepared_statement_handle: handle.clone().into(),
                },
                descriptor(),
            )
            .await
            .map(|_| ()),
        service
            .do_action_close_prepared_statement(
                ActionClosePreparedStatementRequest {
                    prepared_statement_handle: handle.into(),
                },
                Request::new(Action::default()),
            )
            .await,
        service
            .get_flight_info_sql_info(CommandGetSqlInfo { info: vec![] }, descriptor())
            .await
            .map(|_| ()),
    ]
    .map(|result| result.map_err(|status| status.code()));

    for (index, code) in codes.iter().enumerate() {
        assert_eq!(*code, Err(Code::Unauthenticated), "handler {index}");
    }
}

#[tokio::test]
async fn unknown_tokens_are_rejected() {
    let backend = Arc::new(StubBackend::default());
    let service = make_service(backend);

    let err = service
        .get_flight_info_statement(
            statement("SELECT 1"),
            with_token(FlightDescriptor::new_cmd(vec![]), "not-a-real-token"),
        )
        .await
        .expect_err("a stale token must not silently fall back to a shared session");

    assert_eq!(err.code(), Code::Unauthenticated);
}

/// The frontend replaces the standalone Flight proxy when mounted, so
/// Ballista's own client tickets must still reach the proxy through
/// arrow-flight's `do_get` dispatcher, which must not mistake them for Flight
/// SQL commands.
#[tokio::test]
async fn ballista_client_tickets_reach_the_proxy() {
    let backend = Arc::new(StubBackend::default());
    let service = make_service(backend);

    // An empty ticket decodes as an `Any` with no type, so the dispatcher
    // hands it to the fallback, where the proxy rejects it as a client error.
    let err = expect_err(
        FlightService::do_get(&service, Request::new(Ticket::default())).await,
        "an unrecognized ticket is a client error",
    );
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");

    // A well-formed Ballista ticket is handed to the proxy, which then fails
    // to dial the (nonexistent) executor. Reaching a connection error is the
    // assertion: it proves the ticket was routed and accepted.
    let action = BallistaAction::FetchPartition {
        job_id: "job".into(),
        stage_id: 1,
        partition_id: 0,
        host: "127.0.0.1".to_string(),
        port: 1,
        file_id: None,
        layout: ShuffleLayout::Passthrough,
        file_kind: ShuffleFileKind::Data,
        byte_ranges: vec![],
    };
    let encoded: ballista_core::serde::protobuf::Action = action.try_into().unwrap();
    let err = expect_err(
        FlightService::do_get(
            &service,
            Request::new(Ticket {
                ticket: encoded.encode_to_vec().into(),
            }),
        )
        .await,
        "no executor is listening",
    );
    assert_ne!(
        err.code(),
        Code::InvalidArgument,
        "the ticket itself must be accepted: {err}"
    );
}

/// Each handshake gets its own session: catalog changes, prepared statements
/// and results in one are invisible to another.
#[tokio::test]
async fn sessions_are_isolated_per_token() {
    let backend = Arc::new(StubBackend::default());
    let service = Arc::new(make_service(backend.clone()));
    let channel = serve(service.clone()).await;

    let alice = handshake(&channel).await;
    let bob = handshake(&channel).await;
    assert_ne!(alice, bob);

    let info = service
        .get_flight_info_statement(
            statement("CREATE VIEW v AS SELECT 1 AS a"),
            with_token(FlightDescriptor::new_cmd(vec![]), &alice),
        )
        .await
        .expect("DDL runs in alice's session")
        .into_inner();

    let err = service
        .get_flight_info_statement(
            statement("SELECT a FROM v"),
            with_token(FlightDescriptor::new_cmd(vec![]), &bob),
        )
        .await
        .expect_err("bob cannot see alice's view");
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");

    let ticket = statement_ticket(&info, 0);
    let err = expect_err(
        service
            .do_get_statement(ticket.clone(), with_token(ticket_for(&info, 0), &bob))
            .await,
        "bob cannot redeem alice's result",
    );
    assert_eq!(err.code(), Code::NotFound);
    service
        .do_get_statement(ticket, with_token(ticket_for(&info, 0), &alice))
        .await
        .expect("bob's attempt did not consume alice's result");

    let prepared = service
        .do_action_create_prepared_statement(
            ActionCreatePreparedStatementRequest {
                query: "SELECT a FROM v".to_string(),
                transaction_id: None,
            },
            with_token(Action::default(), &alice),
        )
        .await
        .expect("alice prepares a statement");
    let handle = prepared.prepared_statement_handle;

    service
        .do_action_close_prepared_statement(
            ActionClosePreparedStatementRequest {
                prepared_statement_handle: handle.clone(),
            },
            with_token(Action::default(), &bob),
        )
        .await
        .expect("closing an unknown handle is not an error");
    let err = service
        .get_flight_info_prepared_statement(
            CommandPreparedStatementQuery {
                prepared_statement_handle: handle.clone(),
            },
            with_token(FlightDescriptor::new_cmd(vec![]), &bob),
        )
        .await
        .expect_err("bob cannot run alice's prepared statement");
    assert_eq!(err.code(), Code::NotFound);

    service
        .get_flight_info_prepared_statement(
            CommandPreparedStatementQuery {
                prepared_statement_handle: handle,
            },
            with_token(FlightDescriptor::new_cmd(vec![]), &alice),
        )
        .await
        .expect("bob's close did not remove alice's statement");
}
