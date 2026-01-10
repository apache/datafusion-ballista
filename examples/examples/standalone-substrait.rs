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

use ballista::datafusion::common::Result;
use ballista_core::serde::protobuf::execute_query_params::Query::SubstraitPlan;
use ballista_core::serde::protobuf::ExecuteQueryParams;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::utils::{create_grpc_client_connection, GrpcClientConfig};
use duckdb::Connection;

/// Example of passing Substrait plans to Ballista standalone instance.
/// datafusion-substrait is used here to compile the Substrait plan, but any front-end
/// can be substituted.
#[tokio::main]
async fn main() -> Result<()> {
    // 1. Initialize a connection
    let conn = Connection::open_in_memory().expect("Can't open database");

    // 2. Load the Substrait extension
    // (In modern DuckDB, this is often autoloaded, but explicit is safer in Rust)
    conn.execute_batch("INSTALL substrait FROM community;\
    ").expect("Can't load substrait");

    // 3. Create some dummy data to plan against
    conn.execute_batch("CREATE TABLE users (id INTEGER, name TEXT);").expect("Can't create table");

    // 4. Generate the Substrait Plan
    // get_substrait returns a BLOB (Vec<u8> in Rust)
    let sql = "SELECT * FROM users WHERE id > 42";
    let mut stmt = conn.prepare("SELECT plan FROM get_substrait(?)").expect("Can't select from table");

    let plan_bytes: Vec<u8> = stmt.query_row([sql], |row| row.get(0)).expect("Can't serialize plan");

    println!("Generated Substrait plan with {} bytes", plan_bytes.len());

    let connection = create_grpc_client_connection("df://localhost:50050".to_owned(), &GrpcClientConfig::default()).await.expect("Error creating client");
    let mut scheduler = SchedulerGrpcClient::new(connection);

    let execute_query_params = ExecuteQueryParams {
        session_id: uuid::Uuid::new_v4().to_string(),
        settings: vec![],
        operation_id: uuid::Uuid::now_v7().to_string(),
        query: Some(SubstraitPlan(plan_bytes)),
    };

    let response = scheduler.execute_query(execute_query_params).await.expect("Error executing query");
    response.into_inner().result.expect("Failed query");

    Ok(())
}