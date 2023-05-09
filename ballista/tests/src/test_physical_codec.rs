use std::convert::TryFrom;
use std::sync::Arc;

use ballista_core::serde::BallistaPhysicalExtensionCodec;
use datafusion::error::{DataFusionError, Result};
use datafusion::{execution::FunctionRegistry, physical_plan::ExecutionPlan};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use prost::Message;

use crate::proto::{self};
use crate::test_table_exec::TestTableExec;

#[derive(Debug)]
pub struct TestPhysicalCodec {
    ballista_codec: Arc<BallistaPhysicalExtensionCodec>,
}

impl TestPhysicalCodec {
    pub fn new() -> Self {
        Self {
            ballista_codec: Arc::new(BallistaPhysicalExtensionCodec {}),
        }
    }
}

impl PhysicalExtensionCodec for TestPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Ok(ballista_plan) = self.ballista_codec.try_decode(buf, inputs, registry) {
            return Ok(ballista_plan);
        }

        if let Ok(proto) = proto::TestTableExec::decode(buf) {
            let plan: TestTableExec = TestTableExec::try_from(proto)?;
            return Ok(Arc::new(plan));
        }

        Err(DataFusionError::Internal(
            "Failed to decode physical plan".to_owned(),
        ))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        if let Some(test_table) = node.as_any().downcast_ref::<TestTableExec>() {
            let proto = proto::TestTableExec::from(test_table.clone());
            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "Failed to encode physical plan: {}",
                    e
                ))
            })?;
            return Ok(());
        }

        self.ballista_codec.try_encode(node, buf)
    }
}
