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

//! The logical counterpart of DataFusion's `ComposedPhysicalExtensionCodec`.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{
    DataFusionError, Result, TableReference, internal_datafusion_err,
};
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{
    AggregateUDF, Extension, HigherOrderUDF, LogicalPlan, ScalarUDF, WindowUDF,
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use prost::Message;

/// The position of the codec that encoded `blob` in the codec list, and the
/// encoded data. The same message `ComposedPhysicalExtensionCodec` writes.
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
/// try. A codec that succeeds without writing anything, as the trait's
/// defaults do for functions, doesn't claim the node either, so a later codec
/// can still encode a function; if none writes anything, nothing is written,
/// and DataFusion looks the function up by name.
#[derive(Debug)]
pub struct ComposedLogicalExtensionCodec {
    codecs: Vec<Arc<dyn LogicalExtensionCodec>>,
}

impl ComposedLogicalExtensionCodec {
    /// Combines `codecs`. Their positions are written into each payload, so
    /// add new codecs at the end.
    pub fn new(codecs: Vec<Arc<dyn LogicalExtensionCodec>>) -> Self {
        Self { codecs }
    }

    fn decode_protobuf<R>(
        &self,
        buf: &[u8],
        decode: impl Fn(&dyn LogicalExtensionCodec, &[u8]) -> Result<R>,
    ) -> Result<R> {
        // Nothing was written, so there's no position to read: offer the
        // empty payload to each codec in turn.
        if buf.is_empty() {
            let mut last_err = None;
            for codec in &self.codecs {
                match decode(codec.as_ref(), buf) {
                    Ok(decoded) => return Ok(decoded),
                    Err(e) => last_err = Some(e),
                }
            }
            return Err(last_err.unwrap_or_else(empty_list));
        }
        let proto =
            DataEncoderTuple::decode(buf).map_err(|e| internal_datafusion_err!("{e}"))?;
        let codec = self
            .codecs
            .get(proto.encoder_position as usize)
            .ok_or_else(|| {
                internal_datafusion_err!(
                    "Can't find codec {} in a list of {}",
                    proto.encoder_position,
                    self.codecs.len()
                )
            })?;
        decode(codec.as_ref(), &proto.blob)
    }

    fn encode_protobuf(
        &self,
        buf: &mut Vec<u8>,
        encode: impl Fn(&dyn LogicalExtensionCodec, &mut Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        let mut wrote_nothing = false;
        let mut last_err = None;
        for (position, codec) in self.codecs.iter().enumerate() {
            let mut blob = Vec::new();
            match encode(codec.as_ref(), &mut blob) {
                Ok(()) if blob.is_empty() => wrote_nothing = true,
                Ok(()) => {
                    let proto = DataEncoderTuple {
                        encoder_position: position as u32,
                        blob,
                    };
                    return proto
                        .encode(buf)
                        .map_err(|e| internal_datafusion_err!("{e}"));
                }
                Err(e) => last_err = Some(e),
            }
        }
        match last_err {
            Some(e) if !wrote_nothing => Err(e),
            None if !wrote_nothing => Err(empty_list()),
            _ => Ok(()),
        }
    }
}

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
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::not_impl_err;
    use datafusion::datasource::empty::EmptyTable;

    /// A table format's codec: handles only the table named after it,
    /// encoding it as its own name, and decodes any bytes at all into an
    /// empty table with one column of that name, as a codec that just calls
    /// `MyMessage::decode(buf)` might with another codec's bytes.
    #[derive(Debug)]
    struct Format(&'static str);

    impl LogicalExtensionCodec for Format {
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
    }

    fn composed(formats: &[&'static str]) -> ComposedLogicalExtensionCodec {
        ComposedLogicalExtensionCodec::new(
            formats
                .iter()
                .map(|f| Arc::new(Format(f)) as Arc<dyn LogicalExtensionCodec>)
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
        for format in ["ballista", "iceberg", "hudi"] {
            assert_eq!(decode(&codec, &encode(&codec, format)?)?, format);
        }
        Ok(())
    }

    #[test]
    fn should_return_the_last_error_when_no_codec_handles_a_node() {
        assert_eq!(
            encode(&composed(&["iceberg", "hudi"]), "delta")
                .unwrap_err()
                .to_string(),
            "This feature is not implemented: hudi doesn't handle delta"
        );
    }

    #[test]
    fn should_reject_a_position_beyond_the_list() -> Result<()> {
        let buf = encode(&composed(&["ballista", "iceberg", "hudi"]), "hudi")?;
        assert_eq!(
            decode(&composed(&["ballista", "iceberg"]), &buf)
                .unwrap_err()
                .to_string(),
            "Internal error: Can't find codec 2 in a list of 2.\nThis issue was likely \
             caused by a bug in DataFusion's code. Please help us to resolve this by \
             filing a bug report in our issue tracker: \
             https://github.com/apache/datafusion/issues"
        );
        Ok(())
    }

    #[test]
    fn should_send_functions_by_name_when_no_codec_writes_them() -> Result<()> {
        let codec = composed(&["iceberg", "hudi"]);
        let mut buf = Vec::new();
        codec.try_encode_udf(&datafusion::functions::string::upper(), &mut buf)?;
        assert_eq!(buf, Vec::<u8>::new());
        Ok(())
    }
}
