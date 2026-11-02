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

//! Readies a separately deployed executor for Iceberg.

use std::sync::Arc;

use ballista_executor::executor_process::ExecutorProcessConfig;

use crate::{IcebergLogicalCodec, IcebergPhysicalCodec};

/// Readies an [`ExecutorProcessConfig`] for Iceberg.
pub trait ExecutorProcessConfigExt {
    /// Sets the Iceberg codecs, wrapping any codecs already set.
    fn with_iceberg(self) -> Self;
}

impl ExecutorProcessConfigExt for ExecutorProcessConfig {
    fn with_iceberg(mut self) -> Self {
        let logical = self.override_logical_codec.take();
        let physical = self.override_physical_codec.take();
        self.override_logical_codec = Some(Arc::new(
            logical.map_or_else(IcebergLogicalCodec::default, IcebergLogicalCodec::new),
        ));
        self.override_physical_codec = Some(Arc::new(
            physical
                .map_or_else(IcebergPhysicalCodec::default, IcebergPhysicalCodec::new),
        ));
        self
    }
}
