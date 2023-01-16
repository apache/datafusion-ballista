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

/// Copied from https://github.com/apache/arrow-datafusion-python/pull/103

use datafusion::arrow::datatypes::DataType;
use pyo3::pyclass;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[pyclass(name = "PyDataType", module = "datafusion", subclass)]
pub struct PyDataType {
    pub(crate) data_type: DataType,
}

impl From<PyDataType> for DataType {
    fn from(data_type: PyDataType) -> DataType {
        data_type.data_type
    }
}

impl From<DataType> for PyDataType {
    fn from(data_type: DataType) -> PyDataType {
        PyDataType { data_type }
    }
}