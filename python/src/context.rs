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

use crate::utils::to_pyerr;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::path::PathBuf;

use ballista::prelude::*;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::prelude::*;
use datafusion_python::catalog::PyTable;
use datafusion_python::context::{
    convert_table_partition_cols, parse_file_compression_type,
};
use datafusion_python::dataframe::PyDataFrame;
use datafusion_python::errors::DataFusionError;
use datafusion_python::expr::sort_expr::PySortExpr;
use datafusion_python::sql::logical::PyLogicalPlan;
use datafusion_python::utils::wait_for_future;

/// PyBallista session context. This is largely a duplicate of
/// DataFusion's PySessionContext, with the main difference being
/// that this operates on a BallistaContext instead of DataFusion's
/// SessionContext. We could probably add extra extension points to
/// DataFusion to allow for a pluggable context and remove much of
/// this code.
#[pyclass(name = "SessionContext", module = "pyballista", subclass)]
pub struct PySessionContext {
    ctx: BallistaContext,
}

#[pymethods]
impl PySessionContext {
    /// Create a new SessionContext by connecting to a Ballista scheduler process.
    #[new]
    pub fn new(host: &str, port: u16, py: Python) -> PyResult<Self> {
        let config = BallistaConfig::new().unwrap();
        let ballista_context = BallistaContext::remote(host, port, &config);
        let ctx = wait_for_future(py, ballista_context).map_err(to_pyerr)?;
        Ok(Self { ctx })
    }

    pub fn sql(&mut self, query: &str, py: Python) -> PyResult<PyDataFrame> {
        let result = self.ctx.sql(query);
        let df = wait_for_future(py, result)?;
        Ok(PyDataFrame::new(df))
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (path, schema=None, table_partition_cols=vec![], file_extension=".avro"))]
    pub fn read_avro(
        &self,
        path: &str,
        schema: Option<PyArrowType<Schema>>,
        table_partition_cols: Vec<(String, String)>,
        file_extension: &str,
        py: Python,
    ) -> PyResult<PyDataFrame> {
        let mut options = AvroReadOptions::default()
            .table_partition_cols(convert_table_partition_cols(table_partition_cols)?);
        options.file_extension = file_extension;
        let df = if let Some(schema) = schema {
            options.schema = Some(&schema.0);
            let read_future = self.ctx.read_avro(path, options);
            wait_for_future(py, read_future).map_err(DataFusionError::from)?
        } else {
            let read_future = self.ctx.read_avro(path, options);
            wait_for_future(py, read_future).map_err(DataFusionError::from)?
        };
        Ok(PyDataFrame::new(df))
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        path,
        schema=None,
        has_header=true,
        delimiter=",",
        schema_infer_max_records=1000,
        file_extension=".csv",
        table_partition_cols=vec![],
        file_compression_type=None))]
    pub fn read_csv(
        &self,
        path: PathBuf,
        schema: Option<PyArrowType<Schema>>,
        has_header: bool,
        delimiter: &str,
        schema_infer_max_records: usize,
        file_extension: &str,
        table_partition_cols: Vec<(String, String)>,
        file_compression_type: Option<String>,
        py: Python,
    ) -> PyResult<PyDataFrame> {
        let path = path
            .to_str()
            .ok_or_else(|| PyValueError::new_err("Unable to convert path to a string"))?;

        let delimiter = delimiter.as_bytes();
        if delimiter.len() != 1 {
            return Err(PyValueError::new_err(
                "Delimiter must be a single character",
            ));
        };

        let mut options = CsvReadOptions::new()
            .has_header(has_header)
            .delimiter(delimiter[0])
            .schema_infer_max_records(schema_infer_max_records)
            .file_extension(file_extension)
            .table_partition_cols(convert_table_partition_cols(table_partition_cols)?)
            .file_compression_type(parse_file_compression_type(file_compression_type)?);

        if let Some(py_schema) = schema {
            options.schema = Some(&py_schema.0);
            let result = self.ctx.read_csv(path, options);
            let df = PyDataFrame::new(wait_for_future(py, result)?);
            Ok(df)
        } else {
            let result = self.ctx.read_csv(path, options);
            let df = PyDataFrame::new(wait_for_future(py, result)?);
            Ok(df)
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (path, schema=None, schema_infer_max_records=1000, file_extension=".json", table_partition_cols=vec![], file_compression_type=None))]
    pub fn read_json(
        &mut self,
        path: PathBuf,
        schema: Option<PyArrowType<Schema>>,
        schema_infer_max_records: usize,
        file_extension: &str,
        table_partition_cols: Vec<(String, String)>,
        file_compression_type: Option<String>,
        py: Python,
    ) -> PyResult<PyDataFrame> {
        let path = path
            .to_str()
            .ok_or_else(|| PyValueError::new_err("Unable to convert path to a string"))?;
        let mut options = NdJsonReadOptions::default()
            .table_partition_cols(convert_table_partition_cols(table_partition_cols)?)
            .file_compression_type(parse_file_compression_type(file_compression_type)?);
        options.schema_infer_max_records = schema_infer_max_records;
        options.file_extension = file_extension;
        let df = if let Some(schema) = schema {
            options.schema = Some(&schema.0);
            let result = self.ctx.read_json(path, options);
            wait_for_future(py, result).map_err(DataFusionError::from)?
        } else {
            let result = self.ctx.read_json(path, options);
            wait_for_future(py, result).map_err(DataFusionError::from)?
        };
        Ok(PyDataFrame::new(df))
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        path,
        table_partition_cols=vec![],
        parquet_pruning=true,
        file_extension=".parquet",
        skip_metadata=true,
        schema=None,
        file_sort_order=None))]
    pub fn read_parquet(
        &self,
        path: &str,
        table_partition_cols: Vec<(String, String)>,
        parquet_pruning: bool,
        file_extension: &str,
        skip_metadata: bool,
        schema: Option<PyArrowType<Schema>>,
        file_sort_order: Option<Vec<Vec<PySortExpr>>>,
        py: Python,
    ) -> PyResult<PyDataFrame> {
        let mut options = ParquetReadOptions::default()
            .table_partition_cols(convert_table_partition_cols(table_partition_cols)?)
            .parquet_pruning(parquet_pruning)
            .skip_metadata(skip_metadata);
        options.file_extension = file_extension;
        options.schema = schema.as_ref().map(|x| &x.0);
        options.file_sort_order = file_sort_order
            .unwrap_or_default()
            .into_iter()
            .map(|e| e.into_iter().map(|f| f.into()).collect())
            .collect();

        let result = self.ctx.read_parquet(path, options);
        let df =
            PyDataFrame::new(wait_for_future(py, result).map_err(DataFusionError::from)?);
        Ok(df)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name,
    path,
    schema=None,
    file_extension=".avro",
    table_partition_cols=vec![]))]
    pub fn register_avro(
        &mut self,
        name: &str,
        path: PathBuf,
        schema: Option<PyArrowType<Schema>>,
        file_extension: &str,
        table_partition_cols: Vec<(String, String)>,
        py: Python,
    ) -> PyResult<()> {
        let path = path
            .to_str()
            .ok_or_else(|| PyValueError::new_err("Unable to convert path to a string"))?;

        let mut options = AvroReadOptions::default()
            .table_partition_cols(convert_table_partition_cols(table_partition_cols)?);
        options.file_extension = file_extension;
        options.schema = schema.as_ref().map(|x| &x.0);

        let result = self.ctx.register_avro(name, path, options);
        wait_for_future(py, result).map_err(DataFusionError::from)?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name,
    path,
    schema=None,
    has_header=true,
    delimiter=",",
    schema_infer_max_records=1000,
    file_extension=".csv",
    file_compression_type=None))]
    pub fn register_csv(
        &mut self,
        name: &str,
        path: PathBuf,
        schema: Option<PyArrowType<Schema>>,
        has_header: bool,
        delimiter: &str,
        schema_infer_max_records: usize,
        file_extension: &str,
        file_compression_type: Option<String>,
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
            .file_extension(file_extension)
            .file_compression_type(parse_file_compression_type(file_compression_type)?);
        options.schema = schema.as_ref().map(|x| &x.0);

        let result = self.ctx.register_csv(name, path, options);
        wait_for_future(py, result).map_err(DataFusionError::from)?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, path, table_partition_cols=vec![],
    parquet_pruning=true,
    file_extension=".parquet",
    skip_metadata=true,
    schema=None,
    file_sort_order=None))]
    pub fn register_parquet(
        &mut self,
        name: &str,
        path: &str,
        table_partition_cols: Vec<(String, String)>,
        parquet_pruning: bool,
        file_extension: &str,
        skip_metadata: bool,
        schema: Option<PyArrowType<Schema>>,
        file_sort_order: Option<Vec<Vec<PySortExpr>>>,
        py: Python,
    ) -> PyResult<()> {
        let mut options = ParquetReadOptions::default()
            .table_partition_cols(convert_table_partition_cols(table_partition_cols)?)
            .parquet_pruning(parquet_pruning)
            .skip_metadata(skip_metadata);
        options.file_extension = file_extension;
        options.schema = schema.as_ref().map(|x| &x.0);
        options.file_sort_order = file_sort_order
            .unwrap_or_default()
            .into_iter()
            .map(|e| e.into_iter().map(|f| f.into()).collect())
            .collect();

        let result = self.ctx.register_parquet(name, path, options);
        wait_for_future(py, result).map_err(DataFusionError::from)?;
        Ok(())
    }

    pub fn register_table(&mut self, name: &str, table: &PyTable) -> PyResult<()> {
        self.ctx
            .register_table(name, table.table())
            .map_err(DataFusionError::from)?;
        Ok(())
    }

    pub fn execute_logical_plan(
        &mut self,
        logical_plan: PyLogicalPlan,
        py: Python,
    ) -> PyResult<PyDataFrame> {
        let result = self.ctx.execute_logical_plan(logical_plan.into());
        let df = wait_for_future(py, result).unwrap();
        Ok(PyDataFrame::new(df))
    }
}
