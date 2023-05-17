use std::sync::Arc;

use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::TableProvider,
    error::{DataFusionError, Result},
    logical_expr::LogicalPlan,
    prelude::SessionContext,
};
use datafusion_proto::logical_plan::{
    DefaultLogicalExtensionCodec, LogicalExtensionCodec,
};
use prost::Message;

use crate::{proto, test_table::TestTable};
use datafusion::logical_expr::Extension;

#[derive(Debug)]
pub struct TestLogicalCodec {
    default_codec: Arc<DefaultLogicalExtensionCodec>,
}

impl TestLogicalCodec {
    pub fn new() -> Self {
        Self {
            default_codec: Arc::new(DefaultLogicalExtensionCodec {}),
        }
    }
}

impl LogicalExtensionCodec for TestLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &SessionContext,
    ) -> Result<Extension, DataFusionError> {
        self.default_codec.try_decode(buf, inputs, ctx)
    }

    fn try_encode(
        &self,
        node: &Extension,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        self.default_codec.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        _schema: SchemaRef,
        _ctx: &SessionContext,
    ) -> Result<Arc<dyn TableProvider>> {
        let proto = proto::TestTable::decode(buf).map_err(|e| {
            DataFusionError::Internal(format!("Failed to decode logical plan: {}", e))
        })?;

        let table = TestTable::from(proto);

        Ok(Arc::new(table))
    }

    fn try_encode_table_provider(
        &self,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        if let Some(test_table) = node.as_any().downcast_ref::<TestTable>() {
            let proto = proto::TestTable::from(test_table.clone());

            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!("Failed to encode logical plan: {}", e))
            })?;

            return Ok(());
        }

        Err(DataFusionError::Internal(
            "Failed to encode table provider, only TestTable allowed".to_owned(),
        ))
    }
}
