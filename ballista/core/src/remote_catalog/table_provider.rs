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
//

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{exec_err, Result};
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::sync::Arc;

/// A stub provider to encapsulate a table that exists in the scheduler's catalog, to allow
/// the Ballista client to perform planning
#[derive(Debug)]
pub struct RemoteTableProvider {
    catalog_name: String,
    schema_name: String,
    table_name: String,
    schema: SchemaRef,
}

impl RemoteTableProvider {
    pub fn new(
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        schema: SchemaRef,
    ) -> Self {
        RemoteTableProvider {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            schema,
        }
    }

    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

#[async_trait::async_trait]
impl TableProvider for RemoteTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        exec_err!("{}.{}.{} is a stub table implementation to be resolved on the Ballista scheduler. It should not be scanned on the client. This is a bug.",
        self.catalog_name, self.schema_name, self.table_name)
    }
}
