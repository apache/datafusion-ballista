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

//! Codecs that let the standalone cluster use the client's in-memory tables.
//!
//! The `.slt` files create most of their tables with `CREATE TABLE ... AS
//! VALUES`, which gives a `MemTable`, and Ballista can't serialize those. A
//! deployment would keep such tables in a catalog the scheduler shares. Here
//! the scheduler and executor run in this process, so a table or sink is
//! encoded as a reference to the object itself and decoded back into the same
//! `Arc`. `INSERT INTO` then changes the table later queries read, as it does
//! on DataFusion. Every other node Ballista can't serialize still fails, as it
//! would on a cluster.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use ballista_core::serde::BallistaPhysicalExtensionCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, Result, TableReference, not_impl_err};
use datafusion::datasource::sink::DataSinkExec;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions,
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};

/// Objects handed to the in-process scheduler or executor, by address.
/// Holding the `Arc` keeps the address from being reused.
struct Registry<T: ?Sized>(LazyLock<Mutex<HashMap<u64, Arc<T>>>>);

impl<T: ?Sized> Registry<T> {
    const fn new() -> Self {
        Self(LazyLock::new(Default::default))
    }

    fn insert(&self, value: Arc<T>) -> u64 {
        let key = Arc::as_ptr(&value) as *const () as usize as u64;
        self.0.lock().unwrap().entry(key).or_insert(value);
        key
    }

    fn get(&self, key: &[u8]) -> Result<Arc<T>> {
        let key = <[u8; 8]>::try_from(key)
            .map(u64::from_le_bytes)
            .map_err(|_| DataFusionError::Internal("bad in-process reference".into()))?;
        self.0.lock().unwrap().get(&key).cloned().ok_or_else(|| {
            DataFusionError::Internal(format!("nothing registered under {key:#x}"))
        })
    }
}

static TABLES: Registry<dyn TableProvider> = Registry::new();
static SINKS: Registry<dyn ExecutionPlan> = Registry::new();

/// Encodes a table provider that no other codec can serialize as a reference
/// to the provider itself. Compose it after Ballista's logical codec.
#[derive(Debug)]
pub struct InProcessTableCodec;

impl LogicalExtensionCodec for InProcessTableCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &TaskContext,
    ) -> Result<Extension> {
        not_impl_err!("InProcessTableCodec only encodes table providers")
    }

    fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<()> {
        not_impl_err!("InProcessTableCodec only encodes table providers")
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        _table_ref: &TableReference,
        _schema: SchemaRef,
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        TABLES.get(buf)
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        buf.extend_from_slice(&TABLES.insert(node).to_le_bytes());
        Ok(())
    }
}

/// Marks a payload as an [`InProcessSinkCodec`] reference rather than
/// Ballista's own encoding.
const SINK_MAGIC: &[u8; 8] = b"BSLTSINK";

/// Ballista's physical codec, plus: a `DataSinkExec` it can't serialize
/// (`INSERT INTO` a `MemTable` plans one with a memory sink) is encoded as a
/// reference that the in-process executor decodes back into the same sink, so
/// the insert reaches the table [`InProcessTableCodec`] shares.
#[derive(Debug, Default)]
pub struct InProcessSinkCodec {
    inner: BallistaPhysicalExtensionCodec,
}

impl PhysicalExtensionCodec for InProcessSinkCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(key) = buf.strip_prefix(SINK_MAGIC) else {
            return self.inner.try_decode(buf, inputs, ctx, proto_converter);
        };
        // The stage's input differs from the one the sink was planned with.
        SINKS.get(key)?.replace_children(
            inputs.to_vec(),
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let error = match self
            .inner
            .try_encode(Arc::clone(&node), buf, proto_converter)
        {
            Ok(()) => return Ok(()),
            Err(e) => e,
        };
        if node.downcast_ref::<DataSinkExec>().is_none() {
            return Err(error);
        }
        buf.clear();
        buf.extend_from_slice(SINK_MAGIC);
        buf.extend_from_slice(&SINKS.insert(node).to_le_bytes());
        Ok(())
    }
}
