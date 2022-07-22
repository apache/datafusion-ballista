use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use arrow_flight::{FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, Location, Ticket};
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::{ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult, CommandGetCatalogs, CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys, CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTables, CommandGetTableTypes, CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementQuery, CommandStatementUpdate, ProstMessageExt, SqlInfo, TicketStatementQuery};
use arrow_flight::sql::server::FlightSqlService;
use log::debug;
use tonic::{Response, Status, Streaming};
use prost::Message;

use crate::scheduler_server::SchedulerServer;
use datafusion_proto::protobuf::LogicalPlanNode;
use ballista_core::{
    serde::protobuf::{PhysicalPlanNode},
};
use ballista_core::config::BallistaConfig;
use arrow_flight::SchemaAsIpc;
use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::writer::{IpcDataGenerator, IpcWriteOptions};
use datafusion::logical_expr::LogicalPlan;
use uuid::{Uuid};

#[derive(Clone)]
pub struct FlightSqlServiceImpl {
    server: SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
    statements: Arc<Mutex<HashMap<Uuid, LogicalPlan>>>,
}

impl FlightSqlServiceImpl {
    pub fn new(server: SchedulerServer<LogicalPlanNode, PhysicalPlanNode>) -> Self {
        Self { server, statements: Arc::new(Mutex::new(HashMap::new())) }
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;
    // get_flight_info
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("Got query:\n{}", query.query);

        // Run query
        let config_builder = BallistaConfig::builder();
        let config = config_builder.build()
            .map_err(|e| Status::internal(format!("Error building config: {}", e)))?;
        let ctx = self.server
            .state
            .session_manager
            .create_session(&config)
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "Failed to create SessionContext: {:?}",
                    e
                ))
            })?;
        let plan = ctx
            .sql(&query.query.as_str())
            .await
            .and_then(|df| df.to_logical_plan())
            .map_err(|e| Status::internal(format!("Error building plan: {}", e)))?;

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
        let handle = Uuid::new_v4();
        let ticket = TicketStatementQuery { statement_handle: handle.as_bytes().to_vec() };
        let mut statements = self.statements.try_lock()
            .map_err(|e| Status::internal(format!("Error locking statements: {}", e)))?;
        statements.insert(handle, plan);
        let buf = ticket.as_any().encode_to_vec();
        let ticket = Ticket { ticket: buf };
        let fiep = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![Location { uri: "grpc+tcp://0.0.0.0:50050".to_string() }],
        };
        let flight_desc = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: vec![],
            path: vec![]
        };
        let info = FlightInfo {
            schema: schema_bytes,
            flight_descriptor: Some(flight_desc),
            endpoint: vec![fiep],
            total_records: -1,
            total_bytes: -1,
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
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    // do_get
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let handle = Uuid::from_slice(&ticket.statement_handle)
            .map_err(|e| Status::internal(format!("Error decoding ticket: {}", e)))?;
        let statements = self.statements.try_lock()
            .map_err(|e| Status::internal(format!("Error decoding ticket: {}", e)))?;
        let plan = statements.get(&handle);
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_get_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_get_schemas(
        &self,
        _query: CommandGetDbSchemas,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_get_tables(
        &self,
        _query: CommandGetTables,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_get_sql_info(
        &self,
        _query: CommandGetSqlInfo,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_get_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_get_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    // do_put
    async fn do_put_statement_update(
        &self,
        _ticket: CommandStatementUpdate,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_put_prepared_statement_query(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Streaming<FlightData>,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_put_prepared_statement_update(
        &self,
        _query: CommandPreparedStatementUpdate,
        _request: Streaming<FlightData>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    // do_action
    async fn do_action_create_prepared_statement(
        &self,
        _query: ActionCreatePreparedStatementRequest,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
    ) {
        unimplemented!("Not yet implemented")
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}
