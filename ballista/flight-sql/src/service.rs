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

//! The Arrow Flight SQL frontend itself.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    ActionCancelQueryRequest, ActionCancelQueryResult,
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, Any, CommandGetCatalogs, CommandGetDbSchemas,
    CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandGetXdbcTypeInfo,
    CommandPreparedStatementQuery, CommandStatementQuery, CommandStatementUpdate,
    ProstMessageExt, SqlInfo, TicketStatementQuery,
    metadata::{SqlInfoData, XdbcTypeInfoData},
    server::PeekableFlightDataStream,
};
use arrow_flight::{
    Action, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, IpcMessage, SchemaAsIpc, Ticket,
};
use ballista_core::error::BallistaError;
use ballista_core::flight_proxy_service::BallistaFlightProxyService;
use ballista_core::serde::protobuf::PartitionLocation;
use ballista_core::serde::scheduler::{
    Action as BallistaAction, ShuffleFileKind, ShuffleLayout,
};
use ballista_core::serde::{decode_protobuf, protobuf};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::SessionContext;
use futures::{Stream, TryStreamExt};
use prost::Message;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

use crate::auth::{AnonymousAuthenticator, Authenticator};
use crate::backend::QueryBackend;
use crate::metadata;
use crate::session::{LocalResult, Prepared, SessionStore};
use crate::ticket::StatementHandle;

/// Session shared by every client that connects without authenticating.
///
/// Anonymous clients cannot be told apart, so they necessarily share catalog
/// state. Configure an [`Authenticator`] to get a session per connection.
const ANONYMOUS_SESSION: &str = "flight-sql-anonymous";

/// How long a session, prepared statement, or unredeemed local result may sit
/// idle before it is discarded.
const DEFAULT_TTL: Duration = Duration::from_secs(30 * 60);

/// How often expired handles are swept.
const REAP_INTERVAL: Duration = Duration::from_secs(60);

type DoGetStream =
    Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

/// Serves Arrow Flight SQL on behalf of a Ballista cluster.
///
/// Clients send SQL text; the frontend plans it against the session's catalog,
/// submits the plan through a [`QueryBackend`], and hands back one
/// `FlightEndpoint` per output partition. `DoGet` on those tickets is proxied
/// to the executor holding the partition, so clients never need to reach
/// executors themselves — the failure mode that made the pre-46.0.0
/// implementation unusable behind NAT, Docker, and Kubernetes.
pub struct BallistaFlightSqlService<B: QueryBackend> {
    backend: Arc<B>,
    proxy: BallistaFlightProxyService,
    auth: Arc<dyn Authenticator>,
    store: Arc<SessionStore>,
    sql_info: SqlInfoData,
    xdbc_info: XdbcTypeInfoData,
    advertised_endpoint: Option<String>,
}

impl<B: QueryBackend> BallistaFlightSqlService<B> {
    /// Builds a frontend over `backend`, using `proxy` to stream partition
    /// data back from executors.
    ///
    /// The service authenticates nobody until an [`Authenticator`] is supplied
    /// via [`with_authenticator`](Self::with_authenticator).
    pub fn new(backend: Arc<B>, proxy: BallistaFlightProxyService) -> Self {
        let store = Arc::new(SessionStore::new(DEFAULT_TTL));

        let backend_for_reaper = backend.clone();
        store.spawn_reaper(REAP_INTERVAL, move |session_id| {
            let backend = backend_for_reaper.clone();
            async move {
                if let Err(e) = backend.close_session(&session_id).await {
                    log::warn!("flight-sql: failed to close session {session_id}: {e}");
                }
            }
        });

        Self {
            backend,
            proxy,
            auth: Arc::new(AnonymousAuthenticator),
            store,
            sql_info: metadata::sql_info(),
            xdbc_info: metadata::xdbc_type_info(),
            advertised_endpoint: None,
        }
    }

    /// Installs an authenticator. Without one, every handshake is accepted and
    /// unauthenticated clients share a single session.
    pub fn with_authenticator(mut self, auth: Arc<dyn Authenticator>) -> Self {
        self.auth = auth;
        self
    }

    /// Overrides the idle TTL for sessions, prepared statements, and
    /// unredeemed results.
    pub fn with_session_ttl(mut self, ttl: Duration) -> Self {
        self.store = Arc::new(SessionStore::new(ttl));

        let backend = self.backend.clone();
        self.store.spawn_reaper(REAP_INTERVAL, move |session_id| {
            let backend = backend.clone();
            async move {
                if let Err(e) = backend.close_session(&session_id).await {
                    log::warn!("flight-sql: failed to close session {session_id}: {e}");
                }
            }
        });

        self
    }

    /// Sets the `grpc://host:port` clients should dial to redeem tickets.
    ///
    /// Leave this unset unless clients cannot reach the address they used to
    /// call `GetFlightInfo`. Endpoints with no location tell the client to
    /// reuse its existing connection, which is what makes the frontend work
    /// unchanged behind a load balancer or an ingress.
    pub fn with_advertised_endpoint(mut self, endpoint: Option<String>) -> Self {
        self.advertised_endpoint = endpoint;
        self
    }

    /// True when the service will accept unauthenticated clients, which the
    /// scheduler logs at startup.
    pub fn allows_anonymous(&self) -> bool {
        self.auth.allows_anonymous()
    }

    /// Resolves the Ballista session for a request from its bearer token.
    fn session_id(&self, metadata: &MetadataMap) -> Result<String, Status> {
        match bearer_token(metadata) {
            Some(token) => self.store.session(&token).ok_or_else(|| {
                Status::unauthenticated(
                    "unknown or expired session token; re-run the Flight handshake",
                )
            }),
            None if self.auth.allows_anonymous() => Ok(ANONYMOUS_SESSION.to_string()),
            None => Err(Status::unauthenticated(
                "missing bearer token; authenticate with the Flight handshake first",
            )),
        }
    }

    async fn session_context(
        &self,
        metadata: &MetadataMap,
    ) -> Result<(String, Arc<SessionContext>), Status> {
        let session_id = self.session_id(metadata)?;
        let ctx = self
            .backend
            .session(&session_id)
            .await
            .map_err(|e| Status::internal(format!("failed to open session: {e}")))?;
        Ok((session_id, ctx))
    }

    /// Plans `sql` against the session's catalog.
    async fn plan(ctx: &SessionContext, sql: &str) -> Result<LogicalPlan, Status> {
        ctx.state()
            .create_logical_plan(sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("failed to plan query: {e}")))
    }

    /// Runs a planned statement and describes where to collect its results.
    async fn flight_info_for(
        &self,
        ctx: Arc<SessionContext>,
        plan: LogicalPlan,
        descriptor: FlightDescriptor,
        job_name: &str,
    ) -> Result<FlightInfo, Status> {
        if let Some(reason) = unsupported_reason(&plan) {
            return Err(Status::unimplemented(reason));
        }

        if runs_on_scheduler(&plan) {
            let (schema, batches) = execute_locally(&ctx, plan).await?;
            let handle = Uuid::new_v4().to_string();
            self.store.insert_result(
                handle.clone(),
                LocalResult {
                    schema: schema.clone(),
                    batches,
                },
            );

            let endpoint = self.endpoint(StatementHandle::Local(handle));
            return build_flight_info(&schema, vec![endpoint], descriptor);
        }

        let result = self
            .backend
            .execute(job_name, ctx, plan)
            .await
            .map_err(query_failed)?;

        let endpoints = result
            .partitions
            .into_iter()
            .map(|location| {
                partition_handle(location).map(|handle| self.endpoint(handle))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| Status::internal(format!("invalid partition location: {e}")))?;

        log::debug!(
            "flight-sql: job {} produced {} endpoint(s)",
            result.job_id,
            endpoints.len()
        );

        build_flight_info(&result.schema, endpoints, descriptor)
    }

    fn endpoint(&self, handle: StatementHandle) -> FlightEndpoint {
        let ticket = TicketStatementQuery {
            statement_handle: handle.encode().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: ticket.as_any().encode_to_vec().into(),
        });

        match &self.advertised_endpoint {
            Some(location) => endpoint.with_location(location.clone()),
            // No location means "fetch from the server that gave you this
            // FlightInfo", so no cluster-internal address ever reaches a client.
            None => endpoint,
        }
    }

    /// Serves a metadata command by round-tripping the command itself as the
    /// ticket, so `DoGet` lands back on the matching handler.
    fn metadata_info<C: ProstMessageExt>(
        command: C,
        schema: SchemaRef,
        descriptor: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: command.as_any().encode_to_vec().into(),
        });
        build_flight_info(&schema, vec![endpoint], descriptor).map(Response::new)
    }
}

/// Statements the scheduler executes itself rather than distributing.
///
/// DDL mutates the session catalog and produces no data, and `SET`-style
/// statements only touch session config; shipping either to executors would be
/// meaningless.
///
/// Check [`unsupported_reason`] first: `CREATE TABLE AS SELECT` is also DDL,
/// but running it here would quietly execute its query on a single node.
fn runs_on_scheduler(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Ddl(_) | LogicalPlan::Statement(_))
}

/// Explains why a plan cannot be served, or `None` if it can.
fn unsupported_reason(plan: &LogicalPlan) -> Option<&'static str> {
    match plan {
        LogicalPlan::Dml(_) => Some(
            "Ballista Flight SQL does not support INSERT/UPDATE/DELETE; \
             the distributed write path is not implemented",
        ),
        LogicalPlan::Copy(_) => Some(
            "Ballista Flight SQL does not support COPY; \
             the distributed write path is not implemented",
        ),
        // CTAS reaches us as DDL, and DDL runs on the scheduler. Executing the
        // query part there would silently give up distribution on exactly the
        // kind of statement a user runs over a large table.
        LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(_)) => Some(
            "Ballista Flight SQL does not support CREATE TABLE AS SELECT, \
             because it would execute on the scheduler rather than the cluster; \
             use CREATE EXTERNAL TABLE over data the executors can read",
        ),
        _ => None,
    }
}

async fn execute_locally(
    ctx: &SessionContext,
    plan: LogicalPlan,
) -> Result<(SchemaRef, Vec<RecordBatch>), Status> {
    let df = ctx
        .execute_logical_plan(plan)
        .await
        .map_err(|e| Status::internal(format!("failed to execute statement: {e}")))?;
    let planned_schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    let batches = df
        .collect()
        .await
        .map_err(|e| Status::internal(format!("failed to execute statement: {e}")))?;

    // Prefer the schema the data actually carries; DataFusion's DDL results
    // are empty and their frame schema is not always the same object.
    let schema = batches
        .first()
        .map(|batch| batch.schema())
        .unwrap_or(planned_schema);

    Ok((schema, batches))
}

/// Turns a shuffle partition into the ticket payload the Flight proxy already
/// knows how to redeem.
fn partition_handle(
    location: PartitionLocation,
) -> Result<StatementHandle, BallistaError> {
    let partition_id = location.partition_id.ok_or_else(|| {
        BallistaError::Internal("partition location has no partition id".to_string())
    })?;
    let executor = location.executor_meta.ok_or_else(|| {
        BallistaError::Internal("partition location has no executor metadata".to_string())
    })?;

    let action = BallistaAction::FetchPartition {
        job_id: partition_id.job_id.into(),
        stage_id: partition_id.stage_id as usize,
        partition_id: partition_id.partition_id as usize,
        host: executor.host,
        port: executor.port as u16,
        file_id: location.file_id,
        // Mirrors `PartitionLocation::layout()`. We map the protobuf message
        // directly rather than going through its `TryInto`, because that
        // conversion also requires `partition_stats`, which a partition fetch
        // does not need.
        layout: if location.is_sort_shuffle {
            ShuffleLayout::Sort
        } else {
            ShuffleLayout::Passthrough
        },
        // A Flight client wants the whole partition: the data file, in full.
        file_kind: ShuffleFileKind::Data,
        byte_ranges: vec![],
    };

    let encoded: protobuf::Action = action.try_into()?;
    Ok(StatementHandle::Partition(encoded.encode_to_vec()))
}

fn build_flight_info(
    schema: &Schema,
    endpoints: Vec<FlightEndpoint>,
    descriptor: FlightDescriptor,
) -> Result<FlightInfo, Status> {
    FlightInfo::new()
        .try_with_schema(schema)
        .map_err(|e| Status::internal(format!("failed to encode result schema: {e}")))
        .map(|info| info.with_descriptor(descriptor).with_endpoints(endpoints))
}

fn batch_response(schema: SchemaRef, batches: Vec<RecordBatch>) -> Response<DoGetStream> {
    let stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(futures::stream::iter(batches.into_iter().map(Ok)))
        .map_err(|e| Status::internal(format!("failed to encode results: {e}")));

    Response::new(Box::pin(stream) as DoGetStream)
}

fn bearer_token(metadata: &MetadataMap) -> Option<String> {
    let value = metadata.get("authorization")?.to_str().ok()?;
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        .map(str::to_string)
}

fn query_failed(e: BallistaError) -> Status {
    // A failed job is the client's problem to see, not an opaque 500.
    Status::internal(format!("query execution failed: {e}"))
}

fn encode_schema(schema: &Schema) -> Result<Vec<u8>, Status> {
    let message: IpcMessage = SchemaAsIpc::new(schema, &IpcWriteOptions::default())
        .try_into()
        .map_err(|e| Status::internal(format!("failed to encode schema: {e}")))?;
    Ok(message.0.to_vec())
}

#[tonic::async_trait]
impl<B: QueryBackend> FlightSqlService for BallistaFlightSqlService<B> {
    type FlightService = BallistaFlightSqlService<B>;

    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        let identity = self.auth.authenticate(request.metadata()).await?;

        let token = Uuid::new_v4().to_string();
        let session_id = format!("flight-sql-{}", Uuid::new_v4());

        // Create the session eagerly so a failure surfaces at handshake time
        // rather than on the client's first query.
        self.backend
            .session(&session_id)
            .await
            .map_err(|e| Status::internal(format!("failed to open session: {e}")))?;
        self.store.insert_session(token.clone(), session_id.clone());

        log::debug!(
            "flight-sql: handshake for {:?} bound to session {session_id}",
            identity.user
        );

        let result = HandshakeResponse {
            protocol_version: 0,
            payload: token.clone().into(),
        };
        let stream = futures::stream::once(async move { Ok(result) });

        let mut response = Response::new(Box::pin(stream) as _);
        response.metadata_mut().insert(
            "authorization",
            format!("Bearer {token}")
                .parse()
                .map_err(|_| Status::internal("failed to encode session token"))?,
        );
        Ok(response)
    }

    /// Redeems tickets that are not Flight SQL commands.
    ///
    /// Ballista's own Rust client fetches shuffle output with a
    /// `ballista.protobuf.Action` ticket. When the Flight SQL frontend is
    /// mounted it replaces the standalone proxy on the scheduler's port, so it
    /// has to keep serving those tickets.
    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        _message: Any,
    ) -> Result<Response<DoGetStream>, Status> {
        decode_protobuf(&request.get_ref().ticket)
            .map_err(|e| Status::invalid_argument(format!("unrecognized ticket: {e}")))?;
        self.proxy.do_get(request).await
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let (_, ctx) = self.session_context(request.metadata()).await?;
        let plan = Self::plan(&ctx, &query.query).await?;
        let descriptor = request.into_inner();

        self.flight_info_for(ctx, plan, descriptor, &job_name(&query.query))
            .await
            .map(Response::new)
    }

    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let handle = prepared_handle(&query.prepared_statement_handle)?;
        let prepared = self.store.prepared(&handle).ok_or_else(|| {
            Status::not_found("unknown or expired prepared statement handle")
        })?;

        let ctx = self
            .backend
            .session(&prepared.session_id)
            .await
            .map_err(|e| Status::internal(format!("failed to open session: {e}")))?;
        let descriptor = request.into_inner();

        self.flight_info_for(ctx, prepared.plan, descriptor, "flight-sql prepared")
            .await
            .map(Response::new)
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.into_builder().schema();
        Self::metadata_info(query, schema, request.into_inner())
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder().schema();
        Self::metadata_info(query, schema, request.into_inner())
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder().schema();
        Self::metadata_info(query, schema, request.into_inner())
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.into_builder().schema();
        Self::metadata_info(query, schema, request.into_inner())
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder(&self.sql_info).schema();
        Self::metadata_info(query, schema, request.into_inner())
    }

    async fn get_flight_info_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = query.into_builder(&self.xdbc_info).schema();
        Self::metadata_info(query, schema, request.into_inner())
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let handle = StatementHandle::decode(&ticket.statement_handle)
            .map_err(|e| Status::invalid_argument(format!("invalid ticket: {e}")))?;

        match handle {
            StatementHandle::Partition(action) => {
                // Unwrap back to the executor-facing ticket and let the proxy
                // redeem it. The proxy dials the executor with a fresh request,
                // so the client's credentials are not forwarded onwards.
                self.proxy
                    .do_get(Request::new(Ticket {
                        ticket: action.into(),
                    }))
                    .await
            }
            StatementHandle::Local(handle) => {
                let result = self.store.take_result(&handle).ok_or_else(|| {
                    Status::not_found("result already consumed or expired")
                })?;
                Ok(batch_response(result.schema, result.batches))
            }
        }
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let (_, ctx) = self.session_context(request.metadata()).await?;
        let (schema, batches) = metadata::one(metadata::catalogs(&ctx, query)?);
        Ok(batch_response(schema, batches))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let (_, ctx) = self.session_context(request.metadata()).await?;
        let (schema, batches) = metadata::one(metadata::db_schemas(&ctx, query)?);
        Ok(batch_response(schema, batches))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let (_, ctx) = self.session_context(request.metadata()).await?;
        let (schema, batches) = metadata::one(metadata::tables(&ctx, query).await?);
        Ok(batch_response(schema, batches))
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let (schema, batches) = metadata::one(metadata::table_types(query)?);
        Ok(batch_response(schema, batches))
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let batch = query
            .into_builder(&self.sql_info)
            .build()
            .map_err(|e| Status::internal(format!("failed to build SqlInfo: {e}")))?;
        let (schema, batches) = metadata::one(batch);
        Ok(batch_response(schema, batches))
    }

    async fn do_get_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let batch = query
            .into_builder(&self.xdbc_info)
            .build()
            .map_err(|e| Status::internal(format!("failed to build type info: {e}")))?;
        let (schema, batches) = metadata::one(batch);
        Ok(batch_response(schema, batches))
    }

    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let (_, ctx) = self.session_context(request.metadata()).await?;
        let plan = Self::plan(&ctx, &ticket.query).await?;

        if let Some(reason) = unsupported_reason(&plan) {
            return Err(Status::unimplemented(reason));
        }
        if !runs_on_scheduler(&plan) {
            return Err(Status::unimplemented(
                "Ballista Flight SQL supports DDL and session statements on this path; \
                 the distributed write path is not implemented",
            ));
        }

        execute_locally(&ctx, plan).await?;
        // Only DDL and session statements reach here, and neither reports an
        // affected-row count.
        Ok(0)
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let (session_id, ctx) = self.session_context(request.metadata()).await?;
        let plan = Self::plan(&ctx, &query.query).await?;
        let schema = plan.schema().as_arrow().clone();

        let handle = Uuid::new_v4().to_string();
        self.store
            .insert_prepared(handle.clone(), Prepared { session_id, plan });

        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.into_bytes().into(),
            dataset_schema: encode_schema(&schema)?.into(),
            // Bound parameters are not supported yet, so the parameter schema
            // is empty rather than absent: clients read "no parameters".
            parameter_schema: encode_schema(&Schema::empty())?.into(),
        })
    }

    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        let handle = prepared_handle(&query.prepared_statement_handle)?;
        self.store.remove_prepared(&handle);
        Ok(())
    }

    async fn do_action_cancel_query(
        &self,
        query: ActionCancelQueryRequest,
        _request: Request<Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        let job_id = job_id_from_flight_info(&query.info)?;

        self.backend.cancel(&job_id).await.map_err(|e| {
            Status::internal(format!("failed to cancel job {job_id}: {e}"))
        })?;

        Ok(ActionCancelQueryResult {
            // `CancelResult::Cancelled`; the generated enum is not re-exported
            // from `arrow_flight::sql`.
            result: 1,
        })
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {
        // The frontend serves a fixed capability set built at construction, so
        // there is nothing to register at runtime.
    }
}

fn job_name(sql: &str) -> String {
    const MAX: usize = 120;
    let trimmed = sql.trim();
    match trimmed.char_indices().nth(MAX) {
        Some((end, _)) => format!("{}…", &trimmed[..end]),
        None => trimmed.to_string(),
    }
}

fn prepared_handle(bytes: &[u8]) -> Result<String, Status> {
    std::str::from_utf8(bytes).map(str::to_string).map_err(|_| {
        Status::invalid_argument("prepared statement handle is not valid UTF-8")
    })
}

/// Recovers the Ballista job id from the `FlightInfo` a client echoes back
/// when cancelling.
fn job_id_from_flight_info(info: &[u8]) -> Result<String, Status> {
    let info = FlightInfo::decode(info)
        .map_err(|e| Status::invalid_argument(format!("invalid FlightInfo: {e}")))?;

    let ticket = info
        .endpoint
        .first()
        .and_then(|endpoint| endpoint.ticket.as_ref())
        .ok_or_else(|| Status::invalid_argument("FlightInfo carries no endpoint"))?;

    let any = Any::decode(&*ticket.ticket)
        .map_err(|e| Status::invalid_argument(format!("invalid ticket: {e}")))?;
    let statement: TicketStatementQuery = any
        .unpack()
        .map_err(|e| Status::invalid_argument(format!("invalid ticket: {e}")))?
        .ok_or_else(|| Status::invalid_argument("ticket is not a statement ticket"))?;

    match StatementHandle::decode(&statement.statement_handle)
        .map_err(|e| Status::invalid_argument(format!("invalid ticket: {e}")))?
    {
        StatementHandle::Partition(action) => {
            match decode_protobuf(&action)
                .map_err(|e| Status::invalid_argument(format!("invalid ticket: {e}")))?
            {
                BallistaAction::FetchPartition { job_id, .. } => Ok(job_id.to_string()),
            }
        }
        StatementHandle::Local(_) => Err(Status::invalid_argument(
            "this query did not run on the cluster and cannot be cancelled",
        )),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn bearer_token_is_read_case_insensitively() {
        let mut metadata = MetadataMap::new();
        metadata.insert("authorization", "Bearer abc".parse().unwrap());
        assert_eq!(bearer_token(&metadata), Some("abc".to_string()));

        let mut metadata = MetadataMap::new();
        metadata.insert("authorization", "bearer abc".parse().unwrap());
        assert_eq!(bearer_token(&metadata), Some("abc".to_string()));

        assert_eq!(bearer_token(&MetadataMap::new()), None);
    }

    #[test]
    fn basic_credentials_are_not_mistaken_for_a_token() {
        let mut metadata = MetadataMap::new();
        metadata.insert("authorization", "Basic dXNlcjpwYXNz".parse().unwrap());
        assert_eq!(bearer_token(&metadata), None);
    }

    #[test]
    fn job_names_are_bounded() {
        let name = job_name(&"x".repeat(1000));
        assert!(name.chars().count() <= 121, "{name}");
    }

    /// A Ballista `FetchPartition` ticket must survive `Any::decode` inside
    /// arrow-flight's `do_get` dispatcher and reach `do_get_fallback`, which is
    /// what keeps the Rust client working when the Flight SQL frontend
    /// replaces the standalone proxy.
    #[test]
    fn ballista_tickets_route_to_the_fallback() {
        let action = BallistaAction::FetchPartition {
            job_id: "job".into(),
            stage_id: 1,
            partition_id: 2,
            host: "executor".to_string(),
            port: 50051,
            file_id: None,
            layout: ShuffleLayout::Passthrough,
            file_kind: ShuffleFileKind::Data,
            byte_ranges: vec![],
        };
        let encoded: protobuf::Action = action.try_into().unwrap();
        let bytes = encoded.encode_to_vec();

        let any = Any::decode(&*bytes).expect("must decode as Any");
        assert_eq!(
            any.type_url, "",
            "a Ballista action must not masquerade as a Flight SQL command"
        );

        // And the fallback can still recover the original action.
        assert!(decode_protobuf(&bytes).is_ok());
    }

    #[test]
    fn ddl_runs_on_the_scheduler_and_writes_are_refused() {
        use datafusion::common::DFSchema;
        use datafusion::logical_expr::{DdlStatement, DropTable};

        let drop = LogicalPlan::Ddl(DdlStatement::DropTable(DropTable {
            name: "t".into(),
            if_exists: false,
            schema: Arc::new(DFSchema::empty()),
        }));
        assert!(runs_on_scheduler(&drop));
        assert!(unsupported_reason(&drop).is_none());
    }
}
