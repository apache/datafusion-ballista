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

/// Implements a Datafusion physical ExecutionPlan that delegates to a PyArrow Dataset
/// This actually performs the projection, filtering and scanning of a Dataset
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyIterator, PyList};

use std::any::Any;
use std::sync::Arc;

use futures::stream;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError as InnerDataFusionError, Result as DFResult};
use datafusion::execution::context::TaskContext;
use datafusion::logical_plan::{combine_filters, Expr};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};

use crate::errors::DataFusionError;
use crate::pyarrow_filter_expression::PyArrowFilterExpression;

struct PyArrowBatchesAdapter {
    batches: Py<PyIterator>,
}

impl Iterator for PyArrowBatchesAdapter {
    type Item = ArrowResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        Python::with_gil(|py| {
            let mut batches: &PyIterator = self.batches.as_ref(py);
            Some(
                batches
                    .next()?
                    .and_then(|batch| Ok(batch.extract::<PyArrowType<_>>()?.0))
                    .map_err(|err| ArrowError::ExternalError(Box::new(err))),
            )
        })
    }
}

// Wraps a pyarrow.dataset.Dataset class and implements a Datafusion ExecutionPlan around it
#[derive(Debug, Clone)]
pub(crate) struct DatasetExec {
    dataset: PyObject,
    schema: SchemaRef,
    fragments: Py<PyList>,
    columns: Option<Vec<String>>,
    filter_expr: Option<PyObject>,
    projected_statistics: Statistics,
}

impl DatasetExec {
    pub fn new(
        py: Python,
        dataset: &PyAny,
        projection: Option<Vec<usize>>,
        filters: &[Expr],
    ) -> Result<Self, DataFusionError> {
        let columns: Option<Result<Vec<String>, DataFusionError>> = projection.map(|p| {
            p.iter()
                .map(|index| {
                    let name: String = dataset
                        .getattr("schema")?
                        .call_method1("field", (*index,))?
                        .getattr("name")?
                        .extract()?;
                    Ok(name)
                })
                .collect()
        });
        let columns: Option<Vec<String>> = columns.transpose()?;
        let filter_expr: Option<PyObject> = combine_filters(filters)
            .map(|filters| {
                PyArrowFilterExpression::try_from(&filters)
                    .map(|filter_expr| filter_expr.inner().clone_ref(py))
            })
            .transpose()?;

        let kwargs = PyDict::new(py);

        kwargs.set_item("columns", columns.clone())?;
        kwargs.set_item(
            "filter",
            filter_expr.as_ref().map(|expr| expr.clone_ref(py)),
        )?;

        let scanner = dataset.call_method("scanner", (), Some(kwargs))?;

        let schema = Arc::new(
            scanner
                .getattr("projected_schema")?
                .extract::<PyArrowType<_>>()?
                .0,
        );

        let builtins = Python::import(py, "builtins")?;
        let pylist = builtins.getattr("list")?;

        // Get the fragments or partitions of the dataset
        let fragments_iterator: &PyAny = dataset.call_method1(
            "get_fragments",
            (filter_expr.as_ref().map(|expr| expr.clone_ref(py)),),
        )?;

        let fragments: &PyList = pylist
            .call1((fragments_iterator,))?
            .downcast()
            .map_err(PyErr::from)?;

        Ok(DatasetExec {
            dataset: dataset.into(),
            schema,
            fragments: fragments.into(),
            columns,
            filter_expr,
            projected_statistics: Default::default(),
        })
    }
}

impl ExecutionPlan for DatasetExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Python::with_gil(|py| {
            let fragments = self.fragments.as_ref(py);
            Partitioning::UnknownPartitioning(fragments.len())
        })
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let batch_size = context.session_config().batch_size();
        Python::with_gil(|py| {
            let dataset = self.dataset.as_ref(py);
            let fragments = self.fragments.as_ref(py);
            let fragment = fragments
                .get_item(partition)
                .map_err(|err| InnerDataFusionError::External(Box::new(err)))?;

            // We need to pass the dataset schema to unify the fragment and dataset schema per PyArrow docs
            let dataset_schema = dataset
                .getattr("schema")
                .map_err(|err| InnerDataFusionError::External(Box::new(err)))?;
            let kwargs = PyDict::new(py);
            kwargs
                .set_item("columns", self.columns.clone())
                .map_err(|err| InnerDataFusionError::External(Box::new(err)))?;
            kwargs
                .set_item(
                    "filter",
                    self.filter_expr.as_ref().map(|expr| expr.clone_ref(py)),
                )
                .map_err(|err| InnerDataFusionError::External(Box::new(err)))?;
            kwargs
                .set_item("batch_size", batch_size)
                .map_err(|err| InnerDataFusionError::External(Box::new(err)))?;
            let scanner = fragment
                .call_method("scanner", (dataset_schema,), Some(kwargs))
                .map_err(|err| InnerDataFusionError::External(Box::new(err)))?;
            let schema: SchemaRef = Arc::new(
                scanner
                    .getattr("projected_schema")
                    .and_then(|schema| Ok(schema.extract::<PyArrowType<_>>()?.0))
                    .map_err(|err| InnerDataFusionError::External(Box::new(err)))?,
            );
            let record_batches: &PyIterator = scanner
                .call_method0("to_batches")
                .map_err(|err| InnerDataFusionError::External(Box::new(err)))?
                .iter()
                .map_err(|err| InnerDataFusionError::External(Box::new(err)))?;

            let record_batches = PyArrowBatchesAdapter {
                batches: record_batches.into(),
            };

            let record_batch_stream = stream::iter(record_batches);
            let record_batch_stream: SendableRecordBatchStream =
                Box::pin(RecordBatchStreamAdapter::new(schema, record_batch_stream));
            Ok(record_batch_stream)
        })
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        Python::with_gil(|py| {
            let number_of_fragments = self.fragments.as_ref(py).len();
            match t {
                DisplayFormatType::Default => {
                    let projected_columns: Vec<String> = self
                        .schema
                        .fields()
                        .iter()
                        .map(|x| x.name().to_owned())
                        .collect();
                    if let Some(filter_expr) = &self.filter_expr {
                        let filter_expr =
                            filter_expr.as_ref(py).str().or(Err(std::fmt::Error))?;
                        write!(
                            f,
                            "DatasetExec: number_of_fragments={}, filter_expr={}, projection=[{}]",
                            number_of_fragments,
                            filter_expr,
                            projected_columns.join(", "),
                        )
                    } else {
                        write!(
                            f,
                            "DatasetExec: number_of_fragments={}, projection=[{}]",
                            number_of_fragments,
                            projected_columns.join(", "),
                        )
                    }
                }
            }
        })
    }

    fn statistics(&self) -> Statistics {
        self.projected_statistics.clone()
    }
}
