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

//! Workaround for missing file-format support in `datafusion-ffi` 55.2.0.
//!
//! datafusion-python replaces the codecs on an installed [`FFI_QueryPlanner`]
//! with its own, so Ballista decodes every logical plan through a
//! `ForeignLogicalExtensionCodec`. That type's `try_decode_file_format` is a
//! hard-coded `not_impl_err!`, which breaks any plan carrying a
//! `FileFormatFactory` (`LogicalPlan::Copy`, i.e. `DataFrame.write_*`).
//!
//! The file formats in those plans are written by the session's terminal
//! `DefaultLogicalExtensionCodec` without a chain envelope, so they can be
//! decoded locally. We replace the planner's `create_physical_plan` entry
//! with one that builds a temporary planner handle whose logical codec is a
//! local [`FileFormatFallbackCodec`] around the session's codec, and forwards
//! the call to `datafusion-ffi`'s own entry on that handle. The `clone` entry
//! is replaced too, so the override survives the clone datafusion-python
//! makes when it swaps codecs.
//!
//! This relies on the private field layouts of `FFI_QueryPlanner` and
//! `FFI_SessionRef`, mirrored below. It is only valid for the pinned
//! `datafusion-ffi = "=55.2.0"`.
//!
//! TODO: remove once `datafusion-ffi` delegates file formats to
//! `DefaultLogicalExtensionCodec` in `ForeignLogicalExtensionCodec`.

use async_ffi::FfiFuture;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{Result, TableReference};
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::execution::context::{QueryPlanner, SessionContext};
use datafusion::execution::{TaskContext, TaskContextProvider};
use datafusion::logical_expr::{
    AggregateUDF, Extension, HigherOrderUDF, LogicalPlan, ScalarUDF, WindowUDF,
};
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
use datafusion_ffi::query_planner::FFI_QueryPlanner;
use datafusion_ffi::util::FFI_Result;
use datafusion_proto::logical_plan::{
    DefaultLogicalExtensionCodec, LogicalExtensionCodec,
};
use stabby::vec::Vec as SVec;
use std::ffi::c_void;
use std::mem::MaybeUninit;
use std::sync::{Arc, OnceLock};

/// Same size and alignment as `datafusion_ffi::session::FFI_SessionRef`
/// (55.2.0), which is `pub(crate)`. It is only passed through by value, so
/// ownership moves to the callee and nothing here drops it. The struct is far
/// larger than two registers, so every supported C ABI passes it in memory
/// regardless of field types.
#[repr(C)]
struct FfiSessionRefBytes {
    // session_id .. physical_optimizers
    _functions: [MaybeUninit<*const c_void>; 14],
    _logical_codec: MaybeUninit<FFI_LogicalExtensionCodec>,
    _physical_codec: MaybeUninit<FFI_PhysicalExtensionCodec>,
    // clone, release, version, private_data, library_marker_id
    _tail: [MaybeUninit<*const c_void>; 5],
}

type CreatePhysicalPlanFn = unsafe extern "C" fn(
    &FFI_QueryPlanner,
    SVec<u8>,
    FfiSessionRefBytes,
) -> FfiFuture<FFI_Result<SVec<u8>>>;
type CloneFn = unsafe extern "C" fn(&FFI_QueryPlanner) -> FFI_QueryPlanner;

/// Mirror of `datafusion_ffi::query_planner::FFI_QueryPlanner` (55.2.0).
/// Only ever accessed through a pointer cast from a real `FFI_QueryPlanner`.
#[repr(C)]
#[allow(dead_code)]
struct FfiQueryPlannerLayout {
    create_physical_plan: CreatePhysicalPlanFn,
    logical_codec: FFI_LogicalExtensionCodec,
    physical_codec: FFI_PhysicalExtensionCodec,
    clone: CloneFn,
    release: unsafe extern "C" fn(&mut FFI_QueryPlanner),
    version: unsafe extern "C" fn() -> u64,
    private_data: *mut c_void,
    library_marker_id: extern "C" fn() -> usize,
}

const _: () = assert!(
    size_of::<FfiQueryPlannerLayout>() == size_of::<FFI_QueryPlanner>()
        && align_of::<FfiQueryPlannerLayout>() == align_of::<FFI_QueryPlanner>()
);

/// `datafusion-ffi`'s own clone entry, captured before it is replaced.
static ORIGINAL_CLONE: OnceLock<CloneFn> = OnceLock::new();

fn layout(planner: &FFI_QueryPlanner) -> &FfiQueryPlannerLayout {
    let layout =
        unsafe { &*(planner as *const FFI_QueryPlanner as *const FfiQueryPlannerLayout) };
    // Cheap sanity check that the mirror still lines up with the public fields.
    debug_assert_eq!(
        layout.library_marker_id as usize,
        planner.library_marker_id as usize
    );
    debug_assert_eq!(layout.version as usize, planner.version as usize);
    layout
}

/// Make `planner` decode file formats locally. `planner` must have been
/// created in this library.
pub(crate) fn decode_file_formats_locally(
    mut planner: FFI_QueryPlanner,
) -> FFI_QueryPlanner {
    let layout = unsafe {
        &mut *(&mut planner as *mut FFI_QueryPlanner as *mut FfiQueryPlannerLayout)
    };
    let original_clone = layout.clone;
    if original_clone as *const () != clone_planner as *const () {
        ORIGINAL_CLONE.get_or_init(|| original_clone);
    }
    layout.create_physical_plan = create_physical_plan;
    layout.clone = clone_planner;
    planner
}

unsafe extern "C" fn clone_planner(planner: &FFI_QueryPlanner) -> FFI_QueryPlanner {
    let original_clone = ORIGINAL_CLONE
        .get()
        .expect("FFI_QueryPlanner clone entry is captured before it is replaced");
    decode_file_formats_locally(unsafe { original_clone(planner) })
}

unsafe extern "C" fn create_physical_plan(
    planner: &FFI_QueryPlanner,
    logical_plan_serialized: SVec<u8>,
    session: FfiSessionRefBytes,
) -> FfiFuture<FFI_Result<SVec<u8>>> {
    let layout = layout(planner);
    // The planner was created in this library, so this is the native planner.
    let internal_planner: Arc<dyn QueryPlanner + Send + Sync> = planner.into();

    // Created in this library, so `datafusion-ffi` unwraps it to the native
    // codec without consulting the task context provider. The provider only
    // has to exist.
    let task_ctx_provider =
        Arc::new(SessionContext::new()) as Arc<dyn TaskContextProvider>;
    let logical_codec = FFI_LogicalExtensionCodec::new(
        Arc::new(FileFormatFallbackCodec {
            inner: (&layout.logical_codec).into(),
        }),
        None,
        &task_ctx_provider,
    );

    // A fresh handle carries `datafusion-ffi`'s own entry points, which do
    // the decode, planning and encode with the codecs we hand it.
    let forward = FFI_QueryPlanner::new_with_ffi_codecs(
        internal_planner,
        logical_codec,
        layout.physical_codec.clone(),
    );
    // The entry reads everything it needs from `forward` before returning,
    // so the handle can drop once the future is created.
    unsafe {
        (self::layout(&forward).create_physical_plan)(
            &forward,
            logical_plan_serialized,
            session,
        )
    }
}

/// Delegates to `inner`, falling back to [`DefaultLogicalExtensionCodec`]
/// for file formats, which `ForeignLogicalExtensionCodec` cannot carry.
#[derive(Debug)]
struct FileFormatFallbackCodec {
    inner: Arc<dyn LogicalExtensionCodec>,
}

impl LogicalExtensionCodec for FileFormatFallbackCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        self.inner
            .try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.inner.try_encode_table_provider(table_ref, node, buf)
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn FileFormatFactory>> {
        self.inner
            .try_decode_file_format(buf, ctx)
            .or_else(|_| DefaultLogicalExtensionCodec {}.try_decode_file_format(buf, ctx))
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn FileFormatFactory>,
    ) -> Result<()> {
        self.inner
            .try_encode_file_format(buf, Arc::clone(&node))
            .or_else(|_| {
                DefaultLogicalExtensionCodec {}.try_encode_file_format(buf, node)
            })
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.inner.try_decode_udf(name, buf)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udf(node, buf)
    }

    fn try_decode_higher_order_function(
        &self,
        name: &str,
        buf: &[u8],
    ) -> Result<Arc<HigherOrderUDF>> {
        self.inner.try_decode_higher_order_function(name, buf)
    }

    fn try_encode_higher_order_function(
        &self,
        node: &HigherOrderUDF,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.inner.try_encode_higher_order_function(node, buf)
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        self.inner.try_decode_udaf(name, buf)
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udaf(node, buf)
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        self.inner.try_decode_udwf(name, buf)
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.inner.try_encode_udwf(node, buf)
    }
}
