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

use std::path::PathBuf;
use std::{collections::HashSet, sync::Arc};

use uuid::Uuid;

use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::datasource::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions};

use crate::catalog::{PyCatalog, PyTable};
use crate::dataframe::PyDataFrame;
use crate::dataset::Dataset;
use crate::errors::DataFusionError;
use crate::udf::PyScalarUDF;
use crate::utils::wait_for_future;

/// `PySessionContext` is able to plan and execute DataFusion plans.
/// It has a powerful optimizer, a physical planner for local execution, and a
/// multi-threaded execution engine to perform the execution.
#[pyclass(name = "SessionContext", module = "ballista", subclass, unsendable)]
pub(crate) struct PySessionContext {
    ctx: SessionContext,
}

#[pymethods]
impl PySessionContext {
    #[allow(clippy::too_many_arguments)]
    #[args(
        default_catalog = "\"datafusion\"",
        default_schema = "\"public\"",
        create_default_catalog_and_schema = "true",
        information_schema = "false",
        repartition_joins = "true",
        repartition_aggregations = "true",
        repartition_windows = "true",
        parquet_pruning = "true",
        target_partitions = "None"
    )]
    #[new]
    fn new(
        default_catalog: &str,
        default_schema: &str,
        create_default_catalog_and_schema: bool,
        information_schema: bool,
        repartition_joins: bool,
        repartition_aggregations: bool,
        repartition_windows: bool,
        parquet_pruning: bool,
        target_partitions: Option<usize>,
        // TODO: config_options
    ) -> Self {
        let cfg = SessionConfig::new()
            .create_default_catalog_and_schema(create_default_catalog_and_schema)
            .with_default_catalog_and_schema(default_catalog, default_schema)
            .with_information_schema(information_schema)
            .with_repartition_joins(repartition_joins)
            .with_repartition_aggregations(repartition_aggregations)
            .with_repartition_windows(repartition_windows)
            .with_parquet_pruning(parquet_pruning);

        let cfg_full = match target_partitions {
            None => cfg,
            Some(x) => cfg.with_target_partitions(x),
        };

        PySessionContext {
            ctx: SessionContext::with_config(cfg_full),
        }
    }

    /// Returns a PyDataFrame whose plan corresponds to the SQL statement.
    fn sql(&mut self, query: &str, py: Python) -> PyResult<PyDataFrame> {
        let result = self.ctx.sql(query);
        let df = wait_for_future(py, result).map_err(DataFusionError::from)?;
        Ok(PyDataFrame::new(df))
    }

    fn create_dataframe(
        &mut self,
        partitions: PyArrowType<Vec<Vec<RecordBatch>>>,
    ) -> PyResult<PyDataFrame> {
        let table = MemTable::try_new(partitions.0[0][0].schema(), partitions.0)
            .map_err(DataFusionError::from)?;

        // generate a random (unique) name for this table
        // table name cannot start with numeric digit
        let name = "c".to_owned()
            + Uuid::new_v4()
                .to_simple()
                .encode_lower(&mut Uuid::encode_buffer());

        self.ctx
            .register_table(&*name, Arc::new(table))
            .map_err(DataFusionError::from)?;
        let table = self.ctx.table(&*name).map_err(DataFusionError::from)?;

        let df = PyDataFrame::new(table);
        Ok(df)
    }

    fn register_table(&mut self, name: &str, table: &PyTable) -> PyResult<()> {
        self.ctx
            .register_table(name, table.table())
            .map_err(DataFusionError::from)?;
        Ok(())
    }

    fn deregister_table(&mut self, name: &str) -> PyResult<()> {
        self.ctx
            .deregister_table(name)
            .map_err(DataFusionError::from)?;
        Ok(())
    }

    fn register_record_batches(
        &mut self,
        name: &str,
        partitions: PyArrowType<Vec<Vec<RecordBatch>>>,
    ) -> PyResult<()> {
        let schema = partitions.0[0][0].schema();
        let table = MemTable::try_new(schema, partitions.0)?;
        self.ctx
            .register_table(name, Arc::new(table))
            .map_err(DataFusionError::from)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[args(
        table_partition_cols = "vec![]",
        parquet_pruning = "true",
        file_extension = "\".parquet\""
    )]
    fn register_parquet(
        &mut self,
        name: &str,
        path: &str,
        table_partition_cols: Vec<String>,
        parquet_pruning: bool,
        file_extension: &str,
        py: Python,
    ) -> PyResult<()> {
        let mut options = ParquetReadOptions::default()
            .table_partition_cols(table_partition_cols)
            .parquet_pruning(parquet_pruning);
        options.file_extension = file_extension;
        let result = self.ctx.register_parquet(name, path, options);
        wait_for_future(py, result).map_err(DataFusionError::from)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[args(
        schema = "None",
        has_header = "true",
        delimiter = "\",\"",
        schema_infer_max_records = "1000",
        file_extension = "\".csv\""
    )]
    fn register_csv(
        &mut self,
        name: &str,
        path: PathBuf,
        schema: Option<PyArrowType<Schema>>,
        has_header: bool,
        delimiter: &str,
        schema_infer_max_records: usize,
        file_extension: &str,
        py: Python,
    ) -> PyResult<()> {
        let path = path
            .to_str()
            .ok_or_else(|| PyValueError::new_err("Unable to convert path to a string"))?;
        let delimiter = delimiter.as_bytes();
        if delimiter.len() != 1 {
            return Err(PyValueError::new_err(
                "Delimiter must be a single character",
            ));
        }

        let mut options = CsvReadOptions::new()
            .has_header(has_header)
            .delimiter(delimiter[0])
            .schema_infer_max_records(schema_infer_max_records)
            .file_extension(file_extension);
        options.schema = schema.as_ref().map(|x| &x.0);

        let result = self.ctx.register_csv(name, path, options);
        wait_for_future(py, result).map_err(DataFusionError::from)?;

        Ok(())
    }

    // Registers a PyArrow.Dataset
    fn register_dataset(&self, name: &str, dataset: &PyAny, py: Python) -> PyResult<()> {
        let table: Arc<dyn TableProvider> = Arc::new(Dataset::new(dataset, py)?);

        self.ctx
            .register_table(name, table)
            .map_err(DataFusionError::from)?;

        Ok(())
    }

    fn register_udf(&mut self, udf: PyScalarUDF) -> PyResult<()> {
        self.ctx.register_udf(udf.function);
        Ok(())
    }

    #[args(name = "\"datafusion\"")]
    fn catalog(&self, name: &str) -> PyResult<PyCatalog> {
        match self.ctx.catalog(name) {
            Some(catalog) => Ok(PyCatalog::new(catalog)),
            None => Err(PyKeyError::new_err(format!(
                "Catalog with name {} doesn't exist.",
                &name
            ))),
        }
    }

    fn tables(&self) -> HashSet<String> {
        #[allow(deprecated)]
        self.ctx.tables().unwrap()
    }

    fn table(&self, name: &str) -> PyResult<PyDataFrame> {
        Ok(PyDataFrame::new(self.ctx.table(name)?))
    }

    fn empty_table(&self) -> PyResult<PyDataFrame> {
        Ok(PyDataFrame::new(self.ctx.read_empty()?))
    }
}
