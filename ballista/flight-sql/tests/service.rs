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
//! executors. Everything here is asserted the way a client sees it — tickets
//! are decoded from the wire bytes rather than through crate internals — so
//! the tests fail if the wire format changes.

use std::sync::Arc;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    ActionCreatePreparedStatementRequest, Any, CommandGetCatalogs, CommandGetTables,
    CommandStatementQuery, TicketStatementQuery,
};
use arrow_flight::{Action, FlightDescriptor, FlightInfo, Ticket};
use async_trait::async_trait;
use ballista_core::error::{BallistaError, Result};
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
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::empty::EmptyTable;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use prost::Message;
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};

/// A backend that never contacts a cluster: it hands out real
/// `SessionContext`s so planning is genuine, and fabricates the partition
/// locations a completed job would have produced.
#[derive(Default)]
struct StubBackend {
    sessions: DashMap<String, Arc<SessionContext>>,
    executed: DashMap<String, usize>,
    cancelled: DashMap<String, usize>,
}

const JOB_ID: &str = "job-1";

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

        Ok(QueryResult {
            job_id: JOB_ID.to_string(),
            schema: Arc::new(plan.schema().as_arrow().clone()),
            partitions: vec![PartitionLocation {
                map_partition_id: 0,
                partition_id: Some(PartitionId {
                    job_id: JOB_ID.to_string(),
                    stage_id: 3,
                    partition_id: 7,
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
            }],
        })
    }

    async fn cancel(&self, job_id: &str) -> Result<()> {
        *self.cancelled.entry(job_id.to_string()).or_insert(0) += 1;
        Ok(())
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

fn make_service(backend: Arc<StubBackend>) -> BallistaFlightSqlService<StubBackend> {
    BallistaFlightSqlService::new(
        backend,
        BallistaFlightProxyService::new(4_194_304, 4_194_304, false, None),
    )
}

fn descriptor() -> Request<FlightDescriptor> {
    Request::new(FlightDescriptor::new_cmd(vec![]))
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
fn statement_handle(info: &FlightInfo, index: usize) -> Vec<u8> {
    let ticket = info.endpoint[index].ticket.as_ref().expect("ticket");
    let any = Any::decode(&*ticket.ticket).expect("ticket is an Any");
    let statement: TicketStatementQuery =
        any.unpack().expect("unpackable").expect("statement ticket");
    statement.statement_handle.to_vec()
}

fn ticket_for(info: &FlightInfo, index: usize) -> Request<Ticket> {
    Request::new(info.endpoint[index].ticket.clone().expect("ticket"))
}

type DoGetStream = <BallistaFlightSqlService<StubBackend> as FlightService>::DoGetStream;

async fn collect(response: tonic::Response<DoGetStream>) -> Vec<RecordBatch> {
    let stream = response
        .into_inner()
        .map_err(|e| FlightError::Tonic(Box::new(e)));
    FlightRecordBatchStream::new_from_flight_data(stream)
        .try_collect()
        .await
        .expect("results decode")
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
        .get_flight_info_statement(
            CommandStatementQuery {
                query: "SELECT id FROM people".to_string(),
                transaction_id: None,
            },
            descriptor(),
        )
        .await
        .expect("query planned and submitted")
        .into_inner();

    assert_eq!(info.endpoint.len(), 1);
    assert!(
        info.endpoint[0].location.is_empty(),
        "an endpoint with no location tells the client to reuse its existing \
         connection; advertising the executor is what broke #1012"
    );

    // The ticket must carry the executor-facing fetch action, so the proxy can
    // redeem it without any extra server-side lookup.
    let handle = statement_handle(&info, 0);
    let action = decode_protobuf(&handle[1..]).expect("handle wraps a Ballista action");
    match action {
        BallistaAction::FetchPartition {
            job_id,
            stage_id,
            partition_id,
            host,
            port,
            ..
        } => {
            assert_eq!(job_id.to_string(), JOB_ID);
            assert_eq!(stage_id, 3);
            assert_eq!(partition_id, 7);
            assert_eq!(host, "executor-host");
            assert_eq!(port, 50051);
        }
    }
}

#[tokio::test]
async fn ddl_runs_on_the_scheduler_and_its_result_is_single_use() {
    let backend = Arc::new(StubBackend::default());
    let service = make_service(backend.clone());

    let info = service
        .get_flight_info_statement(
            CommandStatementQuery {
                query: "CREATE SCHEMA reporting".to_string(),
                transaction_id: None,
            },
            descriptor(),
        )
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

    let handle = statement_handle(&info, 0);
    assert_eq!(handle[0], 1, "DDL results use the local-result tag");

    let ticket = TicketStatementQuery {
        statement_handle: handle.into(),
    };
    service
        .do_get_statement(ticket.clone(), ticket_for(&info, 0))
        .await
        .expect("first fetch succeeds");

    let err = expect_err(
        service.do_get_statement(ticket, ticket_for(&info, 0)).await,
        "a ticket is redeemable once",
    );
    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn writes_are_refused_rather_than_run_on_the_scheduler() {
    let backend = Arc::new(StubBackend::default());
    register_table(&backend, ANONYMOUS_SESSION).await;
    let service = make_service(backend.clone());

    let err = service
        .get_flight_info_statement(
            CommandStatementQuery {
                query: "INSERT INTO people VALUES (1, 'x')".to_string(),
                transaction_id: None,
            },
            descriptor(),
        )
        .await
        .expect_err("the distributed write path is not implemented");

    assert_eq!(err.code(), tonic::Code::Unimplemented);
    assert!(backend.executed.is_empty());
}

/// `CREATE TABLE AS SELECT` arrives as DDL, and DDL runs on the scheduler. It
/// must be refused rather than quietly executed on a single node.
#[tokio::test]
async fn ctas_is_refused_rather_than_run_on_one_node() {
    let backend = Arc::new(StubBackend::default());
    register_table(&backend, ANONYMOUS_SESSION).await;
    let service = make_service(backend.clone());

    let err = expect_err(
        service
            .get_flight_info_statement(
                CommandStatementQuery {
                    query: "CREATE TABLE big AS SELECT id FROM people".to_string(),
                    transaction_id: None,
                },
                descriptor(),
            )
            .await,
        "CTAS would give up distribution",
    );

    assert_eq!(err.code(), tonic::Code::Unimplemented);

    let ctx = backend.session(ANONYMOUS_SESSION).await.unwrap();
    assert!(
        !ctx.table_exist("big").unwrap(),
        "the refused statement must not have taken effect"
    );
}

#[tokio::test]
async fn catalog_metadata_reflects_the_session() {
    let backend = Arc::new(StubBackend::default());
    register_table(&backend, ANONYMOUS_SESSION).await;
    let service = make_service(backend.clone());

    let command = CommandGetTables {
        catalog: None,
        db_schema_filter_pattern: None,
        table_name_filter_pattern: None,
        table_types: vec![],
        include_schema: true,
    };
    let batches = collect(
        service
            .do_get_tables(command, Request::new(Ticket::default()))
            .await
            .expect("tables listed"),
    )
    .await;

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 1,
        "the table registered in the session must be listed"
    );

    let batches = collect(
        service
            .do_get_catalogs(CommandGetCatalogs {}, Request::new(Ticket::default()))
            .await
            .expect("catalogs listed"),
    )
    .await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1);
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
            arrow_flight::sql::CommandPreparedStatementQuery {
                prepared_statement_handle: handle.clone(),
            },
            descriptor(),
        )
        .await
        .expect("prepared statement executes");

    service
        .do_action_close_prepared_statement(
            arrow_flight::sql::ActionClosePreparedStatementRequest {
                prepared_statement_handle: handle.clone(),
            },
            Request::new(Action::default()),
        )
        .await
        .expect("closes");

    let err = service
        .get_flight_info_prepared_statement(
            arrow_flight::sql::CommandPreparedStatementQuery {
                prepared_statement_handle: handle,
            },
            descriptor(),
        )
        .await
        .expect_err("a closed handle must not linger");
    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn an_authenticator_makes_tokenless_requests_fail() {
    let backend = Arc::new(StubBackend::default());
    let service = make_service(backend).with_authenticator(Arc::new(DenyAll));

    let err = service
        .get_flight_info_statement(
            CommandStatementQuery {
                query: "SELECT 1".to_string(),
                transaction_id: None,
            },
            descriptor(),
        )
        .await
        .expect_err("no anonymous fallback when an authenticator is installed");

    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn the_default_service_allows_anonymous_access() {
    let backend = Arc::new(StubBackend::default());
    let service = make_service(backend);
    assert!(service.allows_anonymous());

    let strict = make_service(Arc::new(StubBackend::default()))
        .with_authenticator(Arc::new(DenyAll));
    assert!(!strict.allows_anonymous());

    // And the shipped default really is the permissive one, so the scheduler's
    // startup warning is not dead code.
    assert!(AnonymousAuthenticator.allows_anonymous());
}

#[tokio::test]
async fn cancelling_a_query_reaches_the_backend() {
    let backend = Arc::new(StubBackend::default());
    register_table(&backend, ANONYMOUS_SESSION).await;
    let service = make_service(backend.clone());

    let info = service
        .get_flight_info_statement(
            CommandStatementQuery {
                query: "SELECT id FROM people".to_string(),
                transaction_id: None,
            },
            descriptor(),
        )
        .await
        .expect("query submitted")
        .into_inner();

    service
        .do_action_cancel_query(
            arrow_flight::sql::ActionCancelQueryRequest {
                info: info.encode_to_vec().into(),
            },
            Request::new(Action::default()),
        )
        .await
        .expect("cancellation accepted");

    assert_eq!(
        backend.cancelled.get(JOB_ID).map(|v| *v),
        Some(1),
        "cancel must be routed to the job the FlightInfo describes"
    );
}

#[tokio::test]
async fn unknown_tokens_are_rejected() {
    let backend = Arc::new(StubBackend::default());
    let service = make_service(backend);

    let mut request = descriptor();
    request
        .metadata_mut()
        .insert("authorization", "Bearer not-a-real-token".parse().unwrap());

    let err = service
        .get_flight_info_statement(
            CommandStatementQuery {
                query: "SELECT 1".to_string(),
                transaction_id: None,
            },
            request,
        )
        .await
        .expect_err("a stale token must not silently fall back to a shared session");

    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

/// The frontend replaces the standalone Flight proxy when mounted, so
/// Ballista's own client tickets must still be recognised. The fallback is
/// only reachable if arrow-flight's dispatcher does not mistake a Ballista
/// action for a Flight SQL command.
#[tokio::test]
async fn ballista_client_tickets_are_recognised_by_the_fallback() {
    let backend = Arc::new(StubBackend::default());
    let service = make_service(backend);

    let garbage = Ticket {
        ticket: vec![0xff, 0xff, 0xff].into(),
    };
    let err = expect_err(
        service
            .do_get_fallback(Request::new(garbage), Any::default())
            .await,
        "an unrecognized ticket is a client error",
    );
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // A well-formed Ballista ticket gets past validation and is handed to the
    // proxy, which then fails to dial the (nonexistent) executor. Reaching a
    // connection error is the assertion: it proves the ticket was accepted.
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
        service
            .do_get_fallback(
                Request::new(Ticket {
                    ticket: encoded.encode_to_vec().into(),
                }),
                Any::default(),
            )
            .await,
        "no executor is listening",
    );
    assert_ne!(
        err.code(),
        tonic::Code::InvalidArgument,
        "the ticket itself must be accepted: {err}"
    );
}

#[tokio::test]
async fn sessions_are_isolated_per_token() -> std::result::Result<(), BallistaError> {
    let backend = Arc::new(StubBackend::default());
    let _service = make_service(backend.clone());

    // The anonymous session and a handshake session are different sessions, so
    // a table registered in one is invisible to the other.
    register_table(&backend, ANONYMOUS_SESSION).await;
    let other = backend.session("some-other-session").await?;
    assert!(other.table_exist("people").is_ok_and(|exists| !exists));

    Ok(())
}
