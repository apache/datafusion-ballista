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

use pyo3::exceptions::PyValueError;
/// Implements a Datafusion TableProvider that delegates to a PyArrow Dataset
/// This allows us to use PyArrow Datasets as Datafusion tables while pushing down projections and filters
use pyo3::prelude::*;
use pyo3::types::PyType;

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::datasource::datasource::TableProviderFilterPushDown;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

use crate::dataset_exec::DatasetExec;
use crate::pyarrow_filter_expression::PyArrowFilterExpression;

// Wraps a pyarrow.dataset.Dataset class and implements a Datafusion TableProvider around it
#[derive(Debug, Clone)]
pub(crate) struct Dataset {
    dataset: PyObject,
}

impl Dataset {
    // Creates a Python PyArrow.Dataset
    pub fn new(dataset: &PyAny, py: Python) -> PyResult<Self> {
        // Ensure that we were passed an instance of pyarrow.dataset.Dataset
        let ds = PyModule::import(py, "pyarrow.dataset")?;
        let ds_type: &PyType = ds.getattr("Dataset")?.downcast()?;
        if dataset.is_instance(ds_type)? {
            Ok(Dataset {
                dataset: dataset.into(),
            })
        } else {
            Err(PyValueError::new_err(
                "dataset argument must be a pyarrow.dataset.Dataset object",
            ))
        }
    }
}

#[async_trait]
impl TableProvider for Dataset {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef {
        Python::with_gil(|py| {
            let dataset = self.dataset.as_ref(py);
            // This can panic but since we checked that self.dataset is a pyarrow.dataset.Dataset it should never
            Arc::new(
                dataset
                    .getattr("schema")
                    .unwrap()
                    .extract::<PyArrowType<_>>()
                    .unwrap()
                    .0,
            )
        })
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Create an ExecutionPlan that will scan the table.
    /// The table provider will be usually responsible of grouping
    /// the source data into partitions that can be efficiently
    /// parallelized or distributed.
    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        // limit can be used to reduce the amount scanned
        // from the datasource as a performance optimization.
        // If set, it contains the amount of rows needed by the `LogicalPlan`,
        // The datasource should return *at least* this number of rows if available.
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Python::with_gil(|py| {
            let plan: Arc<dyn ExecutionPlan> = Arc::new(
                DatasetExec::new(
                    py,
                    self.dataset.as_ref(py),
                    projection.clone(),
                    filters,
                )
                .map_err(|err| DataFusionError::External(Box::new(err)))?,
            );
            Ok(plan)
        })
    }

    /// Tests whether the table provider can make use of a filter expression
    /// to optimise data retrieval.
    fn supports_filter_pushdown(
        &self,
        filter: &Expr,
    ) -> DFResult<TableProviderFilterPushDown> {
        match PyArrowFilterExpression::try_from(filter) {
            Ok(_) => Ok(TableProviderFilterPushDown::Exact),
            _ => Ok(TableProviderFilterPushDown::Unsupported),
        }
    }
}
