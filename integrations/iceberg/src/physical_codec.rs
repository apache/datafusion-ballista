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

//! Sends Iceberg scans from the scheduler to executors.

use std::any::Any;
use std::sync::Arc;

use ballista_core::serde::BallistaPhysicalExtensionCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, internal_datafusion_err, not_impl_err};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::physical_plan::{
    ComposedPhysicalExtensionCodec, PhysicalExtensionCodec,
    PhysicalProtoConverterExtension,
};
use futures::FutureExt;
use iceberg::expr::Predicate;
use iceberg_datafusion::read::{
    FileScanTaskProviderBuilder, IcebergDataSource, LazyFileScanTaskProvider,
};
use serde::{Deserialize, Serialize};

use crate::wire::{self, TableWire};

#[derive(Debug, Serialize, Deserialize)]
struct ScanWire {
    table: TableWire,
    snapshot_id: Option<i64>,
    /// The scan's output schema, already projected.
    schema: SchemaRef,
    predicate: Option<Predicate>,
    fetch: Option<usize>,
}

/// A [`PhysicalExtensionCodec`] for scans of an [`IcebergDataSource`]. It
/// fails on anything else, so compose it after other codecs with
/// [`ComposedPhysicalExtensionCodec`], as [`register_iceberg`] does.
///
/// [`register_iceberg`]: crate::register_iceberg
#[derive(Debug, Default)]
pub struct IcebergPhysicalCodec;

impl IcebergPhysicalCodec {
    /// Composes `codec`, or Ballista's when `None`, with this codec after it.
    pub(crate) fn after(
        codec: Option<Arc<dyn PhysicalExtensionCodec>>,
    ) -> Arc<dyn PhysicalExtensionCodec> {
        let codec =
            codec.unwrap_or_else(|| Arc::new(BallistaPhysicalExtensionCodec::default()));
        Arc::new(ComposedPhysicalExtensionCodec::new(vec![
            codec,
            Arc::new(Self),
        ]))
    }
}

impl PhysicalExtensionCodec for IcebergPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let scan: ScanWire = wire::decode(buf)?;
        let table = scan.table.load()?;
        // A default context leaves eager file planning off, so the provider
        // discovers files when the scan runs, and building it awaits nothing.
        let tasks = FileScanTaskProviderBuilder::new(
            Arc::new(TaskContext::default()),
            table.clone(),
            Arc::clone(&scan.schema),
        )
        .with_snapshot_id(scan.snapshot_id)
        .with_predicate(scan.predicate)
        .build()
        .now_or_never()
        .ok_or_else(|| {
            internal_datafusion_err!("building a lazy Iceberg scan waited on I/O")
        })??;
        let source = IcebergDataSource::builder(
            ctx.session_config(),
            scan.schema,
            table.file_io().clone(),
            table.reader_builder(),
            tasks,
        )
        .with_fetch(scan.fetch)
        .build()?;
        Ok(DataSourceExec::from_data_source(source))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let Some(source) = node
            .downcast_ref::<DataSourceExec>()
            .and_then(|exec| exec.data_source().downcast_ref::<IcebergDataSource>())
        else {
            return not_impl_err!("{} isn't an Iceberg scan", node.name());
        };
        // Ballista can't restrict this scan to a task's partitions, so the job
        // would fail whenever its stage is split between tasks, which depends
        // on how busy the cluster is. Refuse it every time instead.
        let partitions = node.output_partitioning().partition_count();
        if partitions > 1 {
            return not_impl_err!(
                "Iceberg scans with {partitions} partitions aren't supported on Ballista yet"
            );
        }
        // Executors discover the scan's files themselves, from the table.
        let tasks = source.file_scan_task_provider() as &dyn Any;
        let Some(tasks) = tasks.downcast_ref::<LazyFileScanTaskProvider>() else {
            return not_impl_err!(
                "Iceberg scans planned eagerly, with \
                 iceberg.execution.eager_file_planning, aren't supported on Ballista yet"
            );
        };
        wire::encode(
            buf,
            &ScanWire {
                table: TableWire::new(tasks.table())?,
                snapshot_id: tasks.snapshot_id(),
                schema: source.schema(),
                predicate: tasks.predicate().cloned(),
                fetch: source.fetch(),
            },
        )
    }
}
