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

use datafusion::prelude::*;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::path::PathBuf;

use crate::utils::to_pyerr;
use ballista::prelude::*;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion_python::context::{
    convert_table_partition_cols, parse_file_compression_type,
};
use datafusion_python::dataframe::PyDataFrame;
use datafusion_python::utils::wait_for_future;

/// PySessionContext SessionContext. This is largely a duplicate of
/// DataFusion's PySessionContext, with the main difference being the
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
            //TODO Remove unwraps
            .table_partition_cols(
                convert_table_partition_cols(table_partition_cols).unwrap(),
            )
            .file_compression_type(
                parse_file_compression_type(file_compression_type).unwrap(),
            );

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
}
