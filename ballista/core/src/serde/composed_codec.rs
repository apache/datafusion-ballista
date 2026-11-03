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

//! The logical counterpart of DataFusion's `ComposedPhysicalExtensionCodec`,
//! which DataFusion lacks.
//!
//! TODO: move to `datafusion-proto`, beside `ComposedPhysicalExtensionCodec`.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{
    DataFusionError, Result, TableReference, config_datafusion_err, config_err,
    exec_datafusion_err, internal_datafusion_err,
};
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{
    AggregateUDF, Extension, HigherOrderUDF, LogicalPlan, ScalarUDF, WindowUDF,
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use prost::Message;

/// The position of the codec that encoded `blob`, and the encoded data, in the
/// same layout as DataFusion's `ComposedPhysicalExtensionCodec`.
#[derive(Clone, PartialEq, Message)]
struct DataEncoderTuple {
    #[prost(uint32, tag = "1")]
    encoder_position: u32,
    #[prost(bytes = "vec", tag = "2")]
    blob: Vec<u8>,
}

/// A [`LogicalExtensionCodec`] that combines several codecs, such as one per
/// table format, in the same way as DataFusion's
/// `ComposedPhysicalExtensionCodec`.
///
/// Encoding uses the first codec that succeeds and records its position, and
/// decoding uses only the codec at that position, so every node must list the
/// same codecs in the same order.
#[derive(Debug)]
pub struct ComposedLogicalExtensionCodec {
    codecs: Vec<Arc<dyn LogicalExtensionCodec>>,
}

impl ComposedLogicalExtensionCodec {
    /// Combines `codecs`. Add new codecs at the end, since payloads record
    /// positions.
    pub fn new(codecs: Vec<Arc<dyn LogicalExtensionCodec>>) -> Self {
        Self { codecs }
    }

    fn decode_protobuf<R>(
        &self,
        buf: &[u8],
        decode: impl FnOnce(&dyn LogicalExtensionCodec, &[u8]) -> Result<R>,
    ) -> Result<R> {
        let proto = DataEncoderTuple::decode(buf).map_err(|e| {
            config_datafusion_err!(
                "invalid composed codec payload: {e}: every node must compose the \
                 same codecs in the same order"
            )
        })?;
        let Some(codec) = self.codecs.get(proto.encoder_position as usize) else {
            return config_err!(
                "the plan was encoded by codec {} of a composed codec, but this node \
                 has {}: every node must compose the same codecs in the same order",
                proto.encoder_position,
                self.codecs.len()
            );
        };
        decode(codec.as_ref(), &proto.blob)
    }

    fn encode_protobuf(
        &self,
        buf: &mut Vec<u8>,
        encode: impl Fn(&dyn LogicalExtensionCodec, &mut Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        let mut errors = Vec::new();
        for (position, codec) in self.codecs.iter().enumerate() {
            let mut blob = Vec::new();
            match encode(codec.as_ref(), &mut blob) {
                Ok(()) => {
                    let proto = DataEncoderTuple {
                        encoder_position: u32::try_from(position)
                            .map_err(|e| internal_datafusion_err!("{e}"))?,
                        blob,
                    };
                    return proto
                        .encode(buf)
                        .map_err(|e| internal_datafusion_err!("{e}"));
                }
                Err(e) => errors.push(e),
            }
        }
        Err(collect(errors))
    }
}

/// Combines the codecs' errors, so the owning codec's error isn't hidden.
fn collect(mut errors: Vec<DataFusionError>) -> DataFusionError {
    match errors.len() {
        0 => DataFusionError::NotImplemented("Empty list of composed codecs".to_owned()),
        1 => errors.remove(0),
        _ => {
            let errors: Vec<_> = errors
                .iter()
                .enumerate()
                .map(|(position, e)| format!("codec {position}: {e}"))
                .collect();
            exec_datafusion_err!(
                "no composed codec could encode it: {}",
                errors.join("; ")
            )
        }
    }
}

impl LogicalExtensionCodec for ComposedLogicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension> {
        self.decode_protobuf(buf, |codec, data| codec.try_decode(data, inputs, ctx))
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        self.encode_protobuf(buf, |codec, data| codec.try_encode(node, data))
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        self.decode_protobuf(buf, |codec, data| {
            codec.try_decode_table_provider(data, table_ref, Arc::clone(&schema), ctx)
        })
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.encode_protobuf(buf, |codec, data| {
            codec.try_encode_table_provider(table_ref, Arc::clone(&node), data)
        })
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn FileFormatFactory>> {
        self.decode_protobuf(buf, |codec, data| codec.try_decode_file_format(data, ctx))
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn FileFormatFactory>,
    ) -> Result<()> {
        self.encode_protobuf(buf, |codec, data| {
            codec.try_encode_file_format(data, Arc::clone(&node))
        })
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.decode_protobuf(buf, |codec, data| codec.try_decode_udf(name, data))
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.encode_protobuf(buf, |codec, data| codec.try_encode_udf(node, data))
    }

    fn try_decode_higher_order_function(
        &self,
        name: &str,
        buf: &[u8],
    ) -> Result<Arc<HigherOrderUDF>> {
        self.decode_protobuf(buf, |codec, data| {
            codec.try_decode_higher_order_function(name, data)
        })
    }

    fn try_encode_higher_order_function(
        &self,
        node: &HigherOrderUDF,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.encode_protobuf(buf, |codec, data| {
            codec.try_encode_higher_order_function(node, data)
        })
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        self.decode_protobuf(buf, |codec, data| codec.try_decode_udaf(name, data))
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.encode_protobuf(buf, |codec, data| codec.try_encode_udaf(node, data))
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        self.decode_protobuf(buf, |codec, data| codec.try_decode_udwf(name, data))
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.encode_protobuf(buf, |codec, data| codec.try_encode_udwf(node, data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::BallistaLogicalExtensionCodec;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::not_impl_err;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::prelude::SessionContext;
    use datafusion_proto::bytes::{
        logical_plan_from_bytes_with_extension_codec,
        logical_plan_to_bytes_with_extension_codec,
    };

    /// A codec that owns the table and the function named after it, and
    /// encodes either as its own name. It decodes any bytes at all into an
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
    fn should_decode_an_empty_payload_only_with_the_first_codec() {
        // Ballista's codec has no table providers, and `hudi` would decode
        // anything it was offered.
        let codec = ComposedLogicalExtensionCodec::new(vec![
            Arc::new(BallistaLogicalExtensionCodec::default()),
            Arc::new(Owner("hudi")),
        ]);
        assert_eq!(
            decode(&codec, &[]).unwrap_err().to_string(),
            "This feature is not implemented: LogicalExtensionCodec is not provided"
        );
    }

    #[tokio::test]
    async fn should_round_trip_a_plan_with_ballista_codec() -> Result<()> {
        // Ballista's codec encodes the CSV format, and `iceberg` the table.
        let ctx = SessionContext::new();
        let field = Field::new("iceberg", DataType::Null, true);
        let table = EmptyTable::new(Arc::new(Schema::new(vec![field])));
        ctx.register_table("iceberg", Arc::new(table))?;
        let plan = ctx
            .state()
            .create_logical_plan("COPY (SELECT * FROM iceberg) TO 'out' STORED AS CSV")
            .await?;
        let codec = ComposedLogicalExtensionCodec::new(vec![
            Arc::new(BallistaLogicalExtensionCodec::default()),
            Arc::new(Owner("iceberg")),
        ]);
        let bytes = logical_plan_to_bytes_with_extension_codec(&plan, &codec)?;
        let decoded = logical_plan_from_bytes_with_extension_codec(
            &bytes,
            &ctx.task_ctx(),
            &codec,
        )?;
        assert_eq!(
            decoded.display_indent().to_string(),
            "CopyTo: format=csv output_url=out options: ()\n  \
             Projection: iceberg.iceberg\n    \
             TableScan: iceberg"
        );
        assert_eq!(decoded, plan);
        Ok(())
    }

    #[test]
    fn should_reject_a_position_beyond_the_list() -> Result<()> {
        let buf = encode(&composed(&["ballista", "iceberg", "hudi"]), "hudi")?;
        assert_eq!(
            decode(&composed(&["ballista", "iceberg"]), &buf)
                .unwrap_err()
                .to_string(),
            "Invalid or Unsupported Configuration: the plan was encoded by codec 2 of a composed codec, but \
             this node has 2: every node must compose the same codecs in the same order"
        );
        Ok(())
    }

    #[test]
    fn should_encode_a_function_with_the_first_codec_that_succeeds() -> Result<()> {
        let upper = datafusion::functions::string::upper();

        let codec = composed(&["upper", "iceberg"]);
        let mut buf = Vec::new();
        codec.try_encode_udf(&upper, &mut buf)?;
        let expected = DataEncoderTuple {
            encoder_position: 0,
            blob: b"upper".to_vec(),
        }
        .encode_to_vec();
        assert_eq!(buf, expected);
        assert_eq!(codec.try_decode_udf("upper", &buf)?.name(), "upper");

        // `iceberg` succeeds without writing anything, as the trait's default
        // does, so the function is sent by name.
        let codec = composed(&["iceberg", "upper"]);
        let mut buf = Vec::new();
        codec.try_encode_udf(&upper, &mut buf)?;
        assert_eq!(buf, Vec::<u8>::new());
        Ok(())
    }
}
