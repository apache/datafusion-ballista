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

use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, CommandGetCatalogs, CommandGetCrossReference,
    CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys,
    CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables,
    CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementQuery,
    CommandStatementUpdate, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, Location, Ticket,
};
use log::{debug, error};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::{Response, Status, Streaming};

use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::scheduler_server::SchedulerServer;
use arrow_flight::SchemaAsIpc;
use ballista_core::config::BallistaConfig;
use ballista_core::serde::protobuf;
use ballista_core::serde::protobuf::job_status;
use ballista_core::serde::protobuf::CompletedJob;
use ballista_core::serde::protobuf::JobStatus;
use ballista_core::serde::protobuf::PhysicalPlanNode;
use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::writer::{IpcDataGenerator, IpcWriteOptions};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion_proto::protobuf::LogicalPlanNode;
use prost::Message;
use tokio::time::sleep;
use uuid::Uuid;

pub struct FlightSqlServiceImpl {
    server: SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
    _statements: Arc<Mutex<HashMap<Uuid, LogicalPlan>>>,
}

impl FlightSqlServiceImpl {
    pub fn new(server: SchedulerServer<LogicalPlanNode, PhysicalPlanNode>) -> Self {
        Self {
            server,
            _statements: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn create_ctx(&self) -> Result<Arc<SessionContext>, Status> {
        let config_builder = BallistaConfig::builder();
        let config = config_builder
            .build()
            .map_err(|e| Status::internal(format!("Error building config: {}", e)))?;
        let ctx = self
            .server
            .state
            .session_manager
            .create_session(&config)
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to create SessionContext: {:?}", e))
            })?;
        Ok(ctx)
    }

    async fn prepare_statement(
        query: CommandStatementQuery,
        ctx: &Arc<SessionContext>,
    ) -> Result<LogicalPlan, Status> {
        let plan = ctx
            .sql(query.query.as_str())
            .await
            .and_then(|df| df.to_logical_plan())
            .map_err(|e| Status::internal(format!("Error building plan: {}", e)))?;
        Ok(plan)
    }

    async fn check_job(&self, job_id: &String) -> Result<Option<CompletedJob>, Status> {
        let status = self
            .server
            .state
            .task_manager
            .get_job_status(job_id)
            .await
            .map_err(|e| {
                let msg = format!("Error getting status for job {}: {:?}", job_id, e);
                error!("{}", msg);
                Status::internal(msg)
            })?;
        let status: JobStatus = match status {
            Some(status) => status,
            None => {
                let msg = format!("Error getting status for job {}!", job_id);
                error!("{}", msg);
                Err(Status::internal(msg))?
            }
        };
        let status: job_status::Status = match status.status {
            Some(status) => status,
            None => {
                let msg = format!("Error getting status for job {}!", job_id);
                error!("{}", msg);
                Err(Status::internal(msg))?
            }
        };
        match status {
            job_status::Status::Queued(_) => Ok(None),
            job_status::Status::Running(_) => Ok(None),
            job_status::Status::Failed(e) => Err(Status::internal(format!(
                "Error building plan: {}",
                e.error
            )))?,
            job_status::Status::Completed(comp) => Ok(Some(comp)),
        }
    }

    async fn job_to_fetch_part(
        &self,
        completed: CompletedJob,
        num_rows: &mut i64,
        num_bytes: &mut i64,
    ) -> Result<Vec<FlightEndpoint>, Status> {
        let mut fieps: Vec<_> = vec![];
        for loc in completed.partition_location.iter() {
            let fetch = if let Some(ref id) = loc.partition_id {
                let fetch = protobuf::FetchPartition {
                    job_id: id.job_id.clone(),
                    stage_id: id.stage_id,
                    partition_id: id.partition_id,
                    path: loc.path.clone(),
                };
                protobuf::Action {
                    action_type: Some(protobuf::action::ActionType::FetchPartition(
                        fetch,
                    )),
                    settings: vec![],
                }
            } else {
                Err(Status::internal("Error getting partition ID".to_string()))?
            };
            let authority = if let Some(ref md) = loc.executor_meta {
                format!("{}:{}", md.host, md.port)
            } else {
                Err(Status::internal(
                    "Invalid partition location, missing executor metadata".to_string(),
                ))?
            };
            if let Some(ref stats) = loc.partition_stats {
                *num_rows += stats.num_rows;
                *num_bytes += stats.num_bytes;
            } else {
                Err(Status::internal("Error getting stats".to_string()))?
            }
            let loc = Location {
                uri: format!("grpc+tcp://{}", authority),
            };
            let buf = fetch.encode_to_vec();
            let ticket = Ticket { ticket: buf };
            let fiep = FlightEndpoint {
                ticket: Some(ticket),
                location: vec![loc],
            };
            fieps.push(fiep);
        }
        Ok(fieps)
    }

    async fn enqueue_job(
        &self,
        ctx: Arc<SessionContext>,
        plan: &LogicalPlan,
    ) -> Result<String, Status> {
        let job_id = self.server.state.task_manager.generate_job_id();
        self.server
            .state
            .task_manager
            .queue_job(&job_id)
            .await
            .map_err(|e| {
                let msg = format!("Failed to queue job {}: {:?}", job_id, e);
                error!("{}", msg);

                Status::internal(msg)
            })?;
        let query_stage_event_sender = self
            .server
            .query_stage_event_loop
            .get_sender()
            .map_err(|e| {
                Status::internal(format!(
                    "Could not get query stage event sender due to: {}",
                    e
                ))
            })?;
        query_stage_event_sender
            .post_event(QueryStageSchedulerEvent::JobQueued {
                job_id: job_id.clone(),
                session_id: ctx.session_id().clone(),
                session_ctx: ctx,
                plan: Box::new(plan.clone()),
            })
            .await
            .map_err(|e| {
                let msg =
                    format!("Failed to send JobQueued event for {}: {:?}", job_id, e);
                error!("{}", msg);
                Status::internal(msg)
            })?;
        Ok(job_id)
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;
    // get_flight_info
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("Got query:\n{}", query.query);

        let ctx = self.create_ctx().await?;
        let plan = Self::prepare_statement(query, &ctx).await?;
        let job_id = self.enqueue_job(ctx, &plan).await?;

        // let handle = Uuid::new_v4();
        // let mut statements = self.statements.try_lock()
        //     .map_err(|e| Status::internal(format!("Error locking statements: {}", e)))?;
        // statements.insert(handle, plan.clone());

        // poll for job completion
        let mut num_rows = 0;
        let mut num_bytes = 0;
        let fieps = loop {
            sleep(Duration::from_millis(100)).await;
            let completed = if let Some(comp) = self.check_job(&job_id).await? {
                comp
            } else {
                continue;
            };
            let fieps = self
                .job_to_fetch_part(completed, &mut num_rows, &mut num_bytes)
                .await?;
            break fieps;
        };

        // transform schema
        let arrow_schema: Schema = (&**plan.schema()).into();
        let options = IpcWriteOptions::default();
        let pair = SchemaAsIpc::new(&arrow_schema, &options);
        let data_gen = IpcDataGenerator::default();
        let encoded_data = data_gen.schema_to_bytes(pair.0, pair.1);
        let mut schema_bytes = vec![];
        arrow::ipc::writer::write_message(&mut schema_bytes, encoded_data, pair.1)
            .map_err(|e| Status::internal(format!("Error encoding schema: {}", e)))?;

        // Generate response
        let flight_desc = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: vec![],
            path: vec![],
        };
        let info = FlightInfo {
            schema: schema_bytes,
            flight_descriptor: Some(flight_desc),
            endpoint: fieps,
            total_records: num_rows,
            total_bytes: num_bytes,
        };
        let resp = Response::new(info);
        debug!("Responding to query...");
        Ok(resp)
    }

    async fn get_flight_info_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "Implement get_flight_info_prepared_statement",
        ))
    }
    async fn get_flight_info_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Implement get_flight_info_catalogs"))
    }
    async fn get_flight_info_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Implement get_flight_info_schemas"))
    }
    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Implement get_flight_info_tables"))
    }
    async fn get_flight_info_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "Implement get_flight_info_table_types",
        ))
    }
    async fn get_flight_info_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        // TODO: implement for FlightSQL JDBC to work
        Err(Status::unimplemented("Implement CommandGetSqlInfo"))
    }
    async fn get_flight_info_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "Implement get_flight_info_primary_keys",
        ))
    }
    async fn get_flight_info_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "Implement get_flight_info_exported_keys",
        ))
    }
    async fn get_flight_info_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "Implement get_flight_info_imported_keys",
        ))
    }
    async fn get_flight_info_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "Implement get_flight_info_cross_reference",
        ))
    }
    // do_get
    async fn do_get_statement(
        &self,
        _ticket: TicketStatementQuery,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // let handle = Uuid::from_slice(&ticket.statement_handle)
        //     .map_err(|e| Status::internal(format!("Error decoding ticket: {}", e)))?;
        // let statements = self.statements.try_lock()
        //     .map_err(|e| Status::internal(format!("Error decoding ticket: {}", e)))?;
        // let plan = statements.get(&handle);
        Err(Status::unimplemented("Implement do_get_statement"))
    }

    async fn do_get_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Implement do_get_prepared_statement"))
    }
    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Implement do_get_catalogs"))
    }
    async fn do_get_schemas(
        &self,
        _query: CommandGetDbSchemas,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Implement do_get_schemas"))
    }
    async fn do_get_tables(
        &self,
        _query: CommandGetTables,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Implement do_get_tables"))
    }
    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Implement do_get_table_types"))
    }
    async fn do_get_sql_info(
        &self,
        _query: CommandGetSqlInfo,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Implement do_get_sql_info"))
    }
    async fn do_get_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Implement do_get_primary_keys"))
    }
    async fn do_get_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Implement do_get_exported_keys"))
    }
    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Implement do_get_imported_keys"))
    }
    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Implement do_get_cross_reference"))
    }
    // do_put
    async fn do_put_statement_update(
        &self,
        _ticket: CommandStatementUpdate,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented("Implement do_put_statement_update"))
    }
    async fn do_put_prepared_statement_query(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Streaming<FlightData>,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        Err(Status::unimplemented(
            "Implement do_put_prepared_statement_query",
        ))
    }
    async fn do_put_prepared_statement_update(
        &self,
        _query: CommandPreparedStatementUpdate,
        _request: Streaming<FlightData>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "Implement do_put_prepared_statement_update",
        ))
    }
    // do_action
    async fn do_action_create_prepared_statement(
        &self,
        _query: ActionCreatePreparedStatementRequest,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        // TODO: implement for Flight SQL JDBC driver to work
        Err(Status::unimplemented(
            "Implement do_action_create_prepared_statement",
        ))
    }
    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
    ) {
        unimplemented!("Implement do_action_close_prepared_statement")
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}
