use arrow_flight::{FlightData, FlightDescriptor, FlightInfo, IpcMessage};
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::{ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult, CommandGetCatalogs, CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys, CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTables, CommandGetTableTypes, CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementQuery, CommandStatementUpdate, SqlInfo, TicketStatementQuery};
use arrow_flight::sql::server::FlightSqlService;
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::ipc;
use datafusion::arrow::ipc::{FieldBuilder, SchemaBuilder};
use datafusion::parquet::data_type::AsBytes;
use log::debug;
use tonic::{Response, Status, Streaming};
use prost::Message;

use crate::scheduler_server::SchedulerServer;
use datafusion_proto::protobuf::LogicalPlanNode;
use flatbuffers::{FlatBufferBuilder, Vector};
use ballista_core::{
    serde::protobuf::{PhysicalPlanNode},
};
use ballista_core::config::BallistaConfig;

#[derive(Clone)]
pub struct FlightSqlServiceImpl {
    server: SchedulerServer<LogicalPlanNode, PhysicalPlanNode>,
}

impl FlightSqlServiceImpl {
    pub fn new(server: SchedulerServer<LogicalPlanNode, PhysicalPlanNode>) -> Self {
        Self { server }
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

        let mut fbb = FlatBufferBuilder::new();

        let mut sb = SchemaBuilder::new(&mut fbb);

        fbb.start_vector(0);
        let mut fb = FieldBuilder::new(&mut fbb);
        let field_name = fbb.create_string("EXPR$0");
        fb.add_name(field_name);
        let field = fb.finish();

        fbb.push()
        
        let fields = fbb.create_vector(&[field]);
        
        sb.add_fields(fields);
        let schema = sb.finish();
        let bytes = Vec::from(schema.to_le_bytes());

        // Generate response
        let mut buf = vec![];
        let ticket = TicketStatementQuery { statement_handle: vec![] };
        ticket.encode(&mut buf)
            .map_err(|e| Status::internal(format!("Error encoding message: {}", e)))?;
        let total_records = 0;
        let total_bytes = 0;
        let ep = vec![];
        let flight_desc = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: bytes,
            path: vec![]
        };
        let msg = IpcMessage(buf);
        let info = FlightInfo::new(msg, Some(flight_desc), ep, total_records, total_bytes);
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
        _ticket: TicketStatementQuery,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
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
