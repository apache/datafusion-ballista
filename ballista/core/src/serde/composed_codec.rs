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

//! Codecs that combine several extension codecs, such as one per table
//! format, by recording which one encoded each payload.
//!
//! [`ComposedPhysicalExtensionCodec`] replaces DataFusion's codec of the same
//! name, and [`ComposedLogicalExtensionCodec`] adds the logical counterpart
//! DataFusion lacks. Unlike DataFusion's, they cover every codec method, let
//! a later codec encode a function an earlier one leaves alone, keep a node
//! encoded as zero bytes with its codec, and report every codec's error.
//!
//! TODO: move both to `datafusion-proto`, replacing its
//! `ComposedPhysicalExtensionCodec`.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{
    DataFusionError, Result, TableReference, exec_datafusion_err, exec_err,
    internal_datafusion_err,
};
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{
    AggregateUDF, Extension, HigherOrderUDF, LogicalPlan, ScalarUDF, WindowUDF,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
use datafusion::physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};
use prost::Message;

/// The position of the codec that encoded `blob` in the codec list, and the
/// encoded data, in the same layout as DataFusion's
/// `ComposedPhysicalExtensionCodec`.
#[derive(Clone, PartialEq, Message)]
struct DataEncoderTuple {
    #[prost(uint32, tag = "1")]
    encoder_position: u32,
    #[prost(bytes = "vec", tag = "2")]
    blob: Vec<u8>,
}

/// A [`LogicalExtensionCodec`] that tries each of several codecs until one
/// works, such as one per table format.
///
/// Each payload records the position of the codec that encoded it, and is
/// decoded only by the codec at that position, so a codec is never offered
/// another codec's bytes. Every node must therefore list the same codecs in
/// the same order.
///
/// Each codec should fail on nodes that aren't its own, so the next one can
/// try. If none can, the error lists each codec's error. For functions and
/// file formats, a codec that succeeds without writing anything, as the
/// trait's defaults do, doesn't claim them either, so a later codec can still
/// encode them; if none writes anything, nothing is written, and DataFusion
/// looks a function up by name.
#[derive(Debug)]
pub struct ComposedLogicalExtensionCodec(Composed<dyn LogicalExtensionCodec>);

impl ComposedLogicalExtensionCodec {
    /// Combines `codecs`. Their positions are written into each payload, so
    /// add new codecs at the end.
    pub fn new(codecs: Vec<Arc<dyn LogicalExtensionCodec>>) -> Self {
        Self(Composed { codecs })
    }
}

/// A [`PhysicalExtensionCodec`] that tries each of several codecs until one
/// works, the physical counterpart of [`ComposedLogicalExtensionCodec`].
#[derive(Debug)]
pub struct ComposedPhysicalExtensionCodec(Composed<dyn PhysicalExtensionCodec>);

impl ComposedPhysicalExtensionCodec {
    /// Combines `codecs`. Their positions are written into each payload, so
    /// add new codecs at the end.
    pub fn new(codecs: Vec<Arc<dyn PhysicalExtensionCodec>>) -> Self {
        Self(Composed { codecs })
    }
}

/// The codecs, logical or physical, and how payloads are routed to them.
#[derive(Debug)]
struct Composed<C: ?Sized> {
    codecs: Vec<Arc<C>>,
}

impl<C: ?Sized> Composed<C> {
    fn decode<R>(
        &self,
        buf: &[u8],
        decode: impl Fn(&C, &[u8]) -> Result<R>,
    ) -> Result<R> {
        // Nothing was written, or codec 0 encoded a node as zero bytes, which
        // protobuf also writes as nothing: offer the empty payload to each
        // codec in turn, starting with codec 0.
        if buf.is_empty() {
            let mut errors = Vec::new();
            for codec in &self.codecs {
                match decode(codec.as_ref(), buf) {
                    Ok(decoded) => return Ok(decoded),
                    Err(e) => errors.push(e),
                }
            }
            return Err(collect("decode", errors));
        }
        let proto = DataEncoderTuple::decode(buf).map_err(|e| {
            exec_datafusion_err!("invalid composed codec payload: {e}{SAME_CODECS}")
        })?;
        let Some(codec) = self.codecs.get(proto.encoder_position as usize) else {
            return exec_err!(
                "the plan was encoded by codec {} of a composed codec, but this node \
                 has {}{SAME_CODECS}",
                proto.encoder_position,
                self.codecs.len()
            );
        };
        decode(codec.as_ref(), &proto.blob)
    }

    fn encode(
        &self,
        buf: &mut Vec<u8>,
        empty: Empty,
        encode: impl Fn(&C, &mut Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        let mut wrote_nothing = false;
        let mut errors = Vec::new();
        for (position, codec) in self.codecs.iter().enumerate() {
            let mut blob = Vec::new();
            match encode(codec.as_ref(), &mut blob) {
                Ok(()) if blob.is_empty() && empty == Empty::Declines => {
                    wrote_nothing = true
                }
                Ok(()) => {
                    let proto = DataEncoderTuple {
                        encoder_position: position as u32,
                        blob,
                    };
                    return proto
                        .encode(buf)
                        .map_err(|e| internal_datafusion_err!("{e}"));
                }
                Err(e) => errors.push(e),
            }
        }
        if wrote_nothing {
            return Ok(());
        }
        Err(collect("encode", errors))
    }
}

/// What a codec that succeeds without writing anything means.
#[derive(Clone, Copy, PartialEq)]
enum Empty {
    /// It encoded the node, as zero bytes: an all-default protobuf message
    /// is empty.
    Claims,
    /// It doesn't handle the node, as the trait's defaults do for functions
    /// and file formats.
    Declines,
}

/// Returns one codec's error as is, and several as one listing each, so the
/// error of the codec that owns a node isn't hidden by the others'.
fn collect(action: &str, mut errors: Vec<DataFusionError>) -> DataFusionError {
    match errors.len() {
        0 => empty_list(),
        1 => errors.remove(0),
        _ => {
            let errors: Vec<_> = errors
                .iter()
                .enumerate()
                .map(|(position, e)| format!("codec {position}: {e}"))
                .collect();
            exec_datafusion_err!(
                "no composed codec could {action} it: {}",
                errors.join("; ")
            )
        }
    }
}

/// Ends an error caused by nodes composing different codecs.
const SAME_CODECS: &str = ": every node must compose the same codecs in the same order";

fn empty_list() -> DataFusionError {
    DataFusionError::NotImplemented("Empty list of composed codecs".to_owned())
}

impl LogicalExtensionCodec for ComposedLogicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension> {
        self.0
            .decode(buf, |codec, data| codec.try_decode(data, inputs, ctx))
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        self.0.encode(buf, Empty::Claims, |codec, data| {
            codec.try_encode(node, data)
        })
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        self.0.decode(buf, |codec, data| {
            codec.try_decode_table_provider(data, table_ref, Arc::clone(&schema), ctx)
        })
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.0.encode(buf, Empty::Claims, |codec, data| {
            codec.try_encode_table_provider(table_ref, Arc::clone(&node), data)
        })
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn FileFormatFactory>> {
        self.0
            .decode(buf, |codec, data| codec.try_decode_file_format(data, ctx))
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn FileFormatFactory>,
    ) -> Result<()> {
        self.0.encode(buf, Empty::Declines, |codec, data| {
            codec.try_encode_file_format(data, Arc::clone(&node))
        })
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.0
            .decode(buf, |codec, data| codec.try_decode_udf(name, data))
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.0.encode(buf, Empty::Declines, |codec, data| {
            codec.try_encode_udf(node, data)
        })
    }

    fn try_decode_higher_order_function(
        &self,
        name: &str,
        buf: &[u8],
    ) -> Result<Arc<HigherOrderUDF>> {
        self.0.decode(buf, |codec, data| {
            codec.try_decode_higher_order_function(name, data)
        })
    }

    fn try_encode_higher_order_function(
        &self,
        node: &HigherOrderUDF,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.0.encode(buf, Empty::Declines, |codec, data| {
            codec.try_encode_higher_order_function(node, data)
        })
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        self.0
            .decode(buf, |codec, data| codec.try_decode_udaf(name, data))
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.0.encode(buf, Empty::Declines, |codec, data| {
            codec.try_encode_udaf(node, data)
        })
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        self.0
            .decode(buf, |codec, data| codec.try_decode_udwf(name, data))
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.0.encode(buf, Empty::Declines, |codec, data| {
            codec.try_encode_udwf(node, data)
        })
    }
}

impl PhysicalExtensionCodec for ComposedPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.0.decode(buf, |codec, data| {
            codec.try_decode(data, inputs, ctx, proto_converter)
        })
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        self.0.encode(buf, Empty::Claims, |codec, data| {
            codec.try_encode(Arc::clone(&node), data, proto_converter)
        })
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.0
            .decode(buf, |codec, data| codec.try_decode_udf(name, data))
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.0.encode(buf, Empty::Declines, |codec, data| {
            codec.try_encode_udf(node, data)
        })
    }

    fn try_decode_higher_order_function(
        &self,
        name: &str,
        buf: &[u8],
    ) -> Result<Arc<HigherOrderUDF>> {
        self.0.decode(buf, |codec, data| {
            codec.try_decode_higher_order_function(name, data)
        })
    }

    fn try_encode_higher_order_function(
        &self,
        node: &HigherOrderUDF,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.0.encode(buf, Empty::Declines, |codec, data| {
            codec.try_encode_higher_order_function(node, data)
        })
    }

    fn try_decode_expr(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn PhysicalExpr>],
        ctx: &PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.0
            .decode(buf, |codec, data| codec.try_decode_expr(data, inputs, ctx))
    }

    fn try_encode_expr(
        &self,
        node: &Arc<dyn PhysicalExpr>,
        buf: &mut Vec<u8>,
        ctx: &PhysicalExprEncodeCtx<'_>,
    ) -> Result<()> {
        self.0.encode(buf, Empty::Claims, |codec, data| {
            codec.try_encode_expr(node, data, ctx)
        })
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        self.0
            .decode(buf, |codec, data| codec.try_decode_udaf(name, data))
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.0.encode(buf, Empty::Declines, |codec, data| {
            codec.try_encode_udaf(node, data)
        })
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        self.0
            .decode(buf, |codec, data| codec.try_decode_udwf(name, data))
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.0.encode(buf, Empty::Declines, |codec, data| {
            codec.try_encode_udwf(node, data)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::not_impl_err;
    use datafusion::datasource::empty::EmptyTable;

    /// A codec that owns the table, function and window function named after
    /// it, and encodes any of them as its own name. It decodes any bytes at all into an
    /// empty table with one column of that name, as a codec that just calls
    /// `MyMessage::decode(buf)` might with another codec's bytes.
    #[derive(Debug)]
    struct Owner(&'static str);

    impl LogicalExtensionCodec for Owner {
        fn try_decode(
            &self,
            _buf: &[u8],
            _inputs: &[LogicalPlan],
            _ctx: &TaskContext,
        ) -> Result<Extension> {
            not_impl_err!("{} has no extension nodes", self.0)
        }

        fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<()> {
            not_impl_err!("{} has no extension nodes", self.0)
        }

        fn try_decode_table_provider(
            &self,
            _buf: &[u8],
            _table_ref: &TableReference,
            _schema: SchemaRef,
            _ctx: &TaskContext,
        ) -> Result<Arc<dyn TableProvider>> {
            let field = Field::new(self.0, DataType::Null, true);
            Ok(Arc::new(EmptyTable::new(Arc::new(Schema::new(vec![
                field,
            ])))))
        }

        fn try_encode_table_provider(
            &self,
            table_ref: &TableReference,
            _node: Arc<dyn TableProvider>,
            buf: &mut Vec<u8>,
        ) -> Result<()> {
            if table_ref.table() != self.0 {
                return not_impl_err!("{} doesn't handle {table_ref}", self.0);
            }
            buf.extend_from_slice(self.0.as_bytes());
            Ok(())
        }

        fn try_decode_udf(&self, name: &str, _buf: &[u8]) -> Result<Arc<ScalarUDF>> {
            let owned = datafusion::functions::all_default_functions()
                .into_iter()
                .find(|udf| udf.name() == name && name == self.0);
            match owned {
                Some(udf) => Ok(udf),
                None => not_impl_err!("{} doesn't handle {name}", self.0),
            }
        }

        fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
            if node.name() == self.0 {
                buf.extend_from_slice(self.0.as_bytes());
            }
            Ok(())
        }
    }

    impl PhysicalExtensionCodec for Owner {
        fn try_decode(
            &self,
            _buf: &[u8],
            _inputs: &[Arc<dyn ExecutionPlan>],
            _ctx: &TaskContext,
            _proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            not_impl_err!("{} has no plans", self.0)
        }

        fn try_encode(
            &self,
            _node: Arc<dyn ExecutionPlan>,
            _buf: &mut Vec<u8>,
            _proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<()> {
            not_impl_err!("{} has no plans", self.0)
        }

        fn try_decode_udwf(&self, name: &str, _buf: &[u8]) -> Result<Arc<WindowUDF>> {
            let owned = datafusion::functions_window::all_default_window_functions()
                .into_iter()
                .find(|udwf| udwf.name() == name && name == self.0);
            match owned {
                Some(udwf) => Ok(udwf),
                None => not_impl_err!("{} doesn't handle {name}", self.0),
            }
        }

        fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
            if node.name() == self.0 {
                buf.extend_from_slice(self.0.as_bytes());
            }
            Ok(())
        }
    }

    fn composed(owners: &[&'static str]) -> ComposedLogicalExtensionCodec {
        ComposedLogicalExtensionCodec::new(
            owners
                .iter()
                .map(|o| Arc::new(Owner(o)) as Arc<dyn LogicalExtensionCodec>)
                .collect(),
        )
    }

    fn encode(codec: &ComposedLogicalExtensionCodec, table: &str) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        let node = Arc::new(EmptyTable::new(Arc::new(Schema::empty())));
        codec.try_encode_table_provider(&TableReference::bare(table), node, &mut buf)?;
        Ok(buf)
    }

    /// Decodes `buf`, returning the name of the codec that decoded it.
    fn decode(codec: &ComposedLogicalExtensionCodec, buf: &[u8]) -> Result<String> {
        let table = codec.try_decode_table_provider(
            buf,
            &TableReference::bare("t"),
            Arc::new(Schema::empty()),
            &TaskContext::default(),
        )?;
        Ok(table.schema().field(0).name().clone())
    }

    #[test]
    fn should_record_the_position_of_the_encoding_codec() -> Result<()> {
        let codec = composed(&["ballista", "iceberg", "hudi"]);
        let expected = DataEncoderTuple {
            encoder_position: 2,
            blob: b"hudi".to_vec(),
        }
        .encode_to_vec();
        assert_eq!(encode(&codec, "hudi")?, expected);
        Ok(())
    }

    #[test]
    fn should_decode_only_with_the_codec_that_encoded() -> Result<()> {
        // Every codec here would decode any bytes it was offered.
        let codec = composed(&["ballista", "iceberg", "hudi"]);
        for owner in ["ballista", "iceberg", "hudi"] {
            assert_eq!(decode(&codec, &encode(&codec, owner)?)?, owner);
        }
        Ok(())
    }

    #[test]
    fn should_report_every_error_when_no_codec_handles_a_node() {
        assert_eq!(
            encode(&composed(&["iceberg", "hudi"]), "delta")
                .unwrap_err()
                .to_string(),
            "Execution error: no composed codec could encode it: codec 0: This \
             feature is not implemented: iceberg doesn't handle delta; codec 1: This \
             feature is not implemented: hudi doesn't handle delta"
        );
    }

    #[test]
    fn should_keep_a_node_encoded_as_zero_bytes_with_its_codec() -> Result<()> {
        // An owner with an empty name encodes its table as zero bytes, as an
        // all-default protobuf message would be.
        let codec = composed(&["iceberg", "", "hudi"]);
        let buf = encode(&codec, "")?;
        let expected = DataEncoderTuple {
            encoder_position: 1,
            blob: Vec::new(),
        }
        .encode_to_vec();
        assert_eq!(buf, expected);
        assert_eq!(decode(&codec, &buf)?, "");

        // At position 0 the whole payload is empty, and still decodes with
        // the codec that wrote it.
        let codec = composed(&["", "iceberg"]);
        let buf = encode(&codec, "")?;
        assert_eq!(buf, Vec::<u8>::new());
        assert_eq!(decode(&codec, &buf)?, "");
        Ok(())
    }

    #[test]
    fn should_let_a_later_physical_codec_encode_a_window_function() -> Result<()> {
        // DataFusion's composed physical codec doesn't route window functions.
        let codec = ComposedPhysicalExtensionCodec::new(vec![
            Arc::new(Owner("iceberg")),
            Arc::new(Owner("row_number")),
        ]);
        let row_number = datafusion::functions_window::row_number::row_number_udwf();
        let mut buf = Vec::new();
        codec.try_encode_udwf(&row_number, &mut buf)?;
        let expected = DataEncoderTuple {
            encoder_position: 1,
            blob: b"row_number".to_vec(),
        }
        .encode_to_vec();
        assert_eq!(buf, expected);
        assert_eq!(
            codec.try_decode_udwf("row_number", &buf)?.name(),
            "row_number"
        );
        Ok(())
    }

    #[test]
    fn should_reject_a_position_beyond_the_list() -> Result<()> {
        let buf = encode(&composed(&["ballista", "iceberg", "hudi"]), "hudi")?;
        assert_eq!(
            decode(&composed(&["ballista", "iceberg"]), &buf)
                .unwrap_err()
                .to_string(),
            "Execution error: the plan was encoded by codec 2 of a composed codec, but \
             this node has 2: every node must compose the same codecs in the same order"
        );
        Ok(())
    }

    #[test]
    fn should_let_a_later_codec_encode_a_function() -> Result<()> {
        // `iceberg` succeeds without writing anything, as the trait's default
        // does, which mustn't claim the function.
        let codec = composed(&["iceberg", "upper"]);
        let mut buf = Vec::new();
        codec.try_encode_udf(&datafusion::functions::string::upper(), &mut buf)?;
        let expected = DataEncoderTuple {
            encoder_position: 1,
            blob: b"upper".to_vec(),
        }
        .encode_to_vec();
        assert_eq!(buf, expected);
        assert_eq!(codec.try_decode_udf("upper", &buf)?.name(), "upper");
        Ok(())
    }

    #[test]
    fn should_send_functions_by_name_when_no_codec_writes_them() -> Result<()> {
        let codec = composed(&["iceberg", "upper"]);
        let mut buf = Vec::new();
        codec.try_encode_udf(&datafusion::functions::string::lower(), &mut buf)?;
        assert_eq!(buf, Vec::<u8>::new());
        // A function sent by name is offered to each codec in turn.
        assert_eq!(codec.try_decode_udf("upper", &[])?.name(), "upper");
        Ok(())
    }
}
