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

//! SDK-free build gate for independently built Iceberg integrations.
//!
//! Enable `contrib-iceberg` and supply provider/scan adapters from a separate
//! integration crate. The gate owns framing and codec composition; the adapter
//! owns its payload schema, SDK dependencies, table reconstruction and runtime.
//! This feature adds no Iceberg SDK to Ballista's dependency graph.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{Result, TableReference};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;

mod logical;
mod physical;

pub use logical::IcebergLogicalCodec;
pub use physical::IcebergPhysicalCodec;

// Versioned, delimited namespace. The adapter owns and versions the payload
// inside this envelope; every participant must use a compatible adapter.
const PREFIX: &[u8] = b"ballista-contrib-iceberg-v1\0";

/// Provider serialization implemented by an independently built Iceberg adapter.
pub trait IcebergTableProviderCodec: Debug + Send + Sync + 'static {
    /// Returns an unframed payload for an owned provider, or `None` to delegate.
    /// Errors for owned but unsupported providers must not fall through.
    fn try_encode(
        &self,
        table_ref: &TableReference,
        provider: &dyn TableProvider,
    ) -> Result<Option<Vec<u8>>>;

    /// Reconstructs an owned provider from its unframed payload.
    /// Malformed or unsupported payloads must return an error.
    fn decode(
        &self,
        payload: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>>;
}

/// Leaf scan serialization implemented by an independently built Iceberg adapter.
pub trait IcebergScanCodec: Debug + Send + Sync + 'static {
    /// Returns an unframed payload for an owned scan, or `None` to delegate.
    fn try_encode(&self, plan: &dyn ExecutionPlan) -> Result<Option<Vec<u8>>>;

    /// Reconstructs an Iceberg leaf scan from its unframed payload.
    /// Malformed or unsupported payloads must return an error.
    fn decode(&self, payload: &[u8], ctx: &TaskContext)
    -> Result<Arc<dyn ExecutionPlan>>;
}
