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

mod common;

use crate::common::state_with_config;
use ballista::prelude::SessionConfigExt;
use datafusion::assert_batches_eq;
use datafusion::execution::SessionState;
use datafusion::prelude::{SessionConfig, SessionContext};

/// A session state whose Ballista job name is "Super Cool Ballista App".
fn job_name_state() -> SessionState {
    let session_config = SessionConfig::new_with_ballista()
        .with_information_schema(true)
        .with_ballista_job_name("Super Cool Ballista App");
    state_with_config(session_config)
}

/// Asserts that `ctx` reports the job name of [`job_name_state`].
async fn assert_job_name_setting(ctx: &SessionContext) -> datafusion::error::Result<()> {
    let result = ctx
        .sql("select name, value from information_schema.df_settings where name like 'ballista.job.name' order by name limit 1")
        .await?
        .collect()
        .await?;

    let expected = [
        "+-------------------+-------------------------+",
        "| name              | value                   |",
        "+-------------------+-------------------------+",
        "| ballista.job.name | Super Cool Ballista App |",
        "+-------------------+-------------------------+",
    ];

    assert_batches_eq!(expected, &result);

    Ok(())
}

#[cfg(test)]
mod remote {
    use crate::common::{ALLTYPES_ID_GT_4, ALLTYPES_ID_GT_4_SQL, register_alltypes};
    use crate::{assert_job_name_setting, job_name_state};
    use ballista::prelude::SessionContextExt;
    use datafusion::{
        assert_batches_eq, execution::SessionStateBuilder, prelude::SessionContext,
    };

    #[tokio::test]
    async fn should_execute_sql_show_with_custom_state() -> datafusion::error::Result<()>
    {
        let (host, port) = crate::common::setup_test_cluster().await;
        let url = format!("df://{host}:{port}");
        let state = SessionStateBuilder::new().with_default_features().build();

        let ctx: SessionContext = SessionContext::remote_with_state(&url, state).await?;

        register_alltypes(&ctx, "test").await;

        let result = ctx.sql(ALLTYPES_ID_GT_4_SQL).await?.collect().await?;
        assert_batches_eq!(ALLTYPES_ID_GT_4, &result);

        Ok(())
    }

    #[tokio::test]
    async fn should_execute_sql_set_configs() -> datafusion::error::Result<()> {
        let (host, port) = crate::common::setup_test_cluster().await;
        let url = format!("df://{host}:{port}");

        let ctx: SessionContext =
            SessionContext::remote_with_state(&url, job_name_state()).await?;

        assert_job_name_setting(&ctx).await
    }
}

#[cfg(test)]
#[cfg(feature = "standalone")]
mod standalone {

    use std::sync::{Arc, atomic::AtomicBool};

    use crate::common::{register_alltypes, state_with_config};
    use crate::{assert_job_name_setting, job_name_state};
    use ballista::extension::{SessionConfigExt, SessionContextExt};
    use ballista_core::serde::BallistaPhysicalExtensionCodec;
    use datafusion::{
        catalog::Session,
        common::exec_err,
        execution::{TaskContext, context::QueryPlanner},
        logical_expr::LogicalPlan,
        physical_plan::ExecutionPlan,
        prelude::{SessionConfig, SessionContext},
    };
    use datafusion_proto::{
        logical_plan::LogicalExtensionCodec,
        physical_plan::{PhysicalExtensionCodec, PhysicalProtoConverterExtension},
    };

    #[tokio::test]
    async fn should_execute_sql_set_configs() -> datafusion::error::Result<()> {
        let ctx: SessionContext =
            SessionContext::standalone_with_state(job_name_state()).await?;

        assert_job_name_setting(&ctx).await
    }

    // we testing if we can override default logical codec
    // in this specific test codec will throw exception which will
    // fail the query.
    #[tokio::test]
    async fn should_set_logical_codec() -> datafusion::error::Result<()> {
        let codec = Arc::new(BadLogicalCodec::default());

        let session_config = SessionConfig::new_with_ballista()
            .with_information_schema(true)
            .with_ballista_logical_extension_codec(codec.clone());

        let state = state_with_config(session_config);

        let ctx: SessionContext = SessionContext::standalone_with_state(state).await?;

        register_alltypes(&ctx, "test").await;

        let write_dir = tempfile::tempdir().expect("temporary directory to be created");
        let write_dir_path = write_dir
            .path()
            .to_str()
            .expect("path to be converted to str");

        let result = ctx
            .sql("select * from test")
            .await?
            .write_parquet(write_dir_path, Default::default(), Default::default())
            .await;

        // this codec should query fail
        assert!(result.is_err());
        assert!(codec.invoked.load(std::sync::atomic::Ordering::Relaxed));
        Ok(())
    }

    // tests if we can correctly set physical codec
    #[tokio::test]
    async fn should_set_physical_codec() -> datafusion::error::Result<()> {
        let physical_codec = Arc::new(MockPhysicalCodec::default());
        let session_config = SessionConfig::new_with_ballista()
            .with_information_schema(true)
            .with_ballista_physical_extension_codec(physical_codec.clone());

        let state = state_with_config(session_config);

        let ctx: SessionContext = SessionContext::standalone_with_state(state).await?;

        register_alltypes(&ctx, "test").await;

        let _result = ctx
            .sql("select string_col, timestamp_col from test where id > 4")
            .await?
            .collect()
            .await;

        assert!(
            physical_codec
                .invoked
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        Ok(())
    }

    // check
    #[tokio::test]
    async fn should_override_planner() -> datafusion::error::Result<()> {
        let session_config = SessionConfig::new_with_ballista()
            .with_information_schema(true)
            .with_ballista_query_planner(Arc::new(BadPlanner::default()));

        let state = state_with_config(session_config);

        let ctx: SessionContext = SessionContext::standalone_with_state(state).await?;

        let result = ctx.sql("SELECT 1").await?.collect().await;

        assert!(result.is_err());

        let session_config =
            SessionConfig::new_with_ballista().with_information_schema(true);

        let state = state_with_config(session_config);

        let ctx: SessionContext = SessionContext::standalone_with_state(state).await?;

        let result = ctx.sql("SELECT 1").await?.collect().await;

        assert!(result.is_ok());

        Ok(())
    }

    #[derive(Debug, Default)]
    struct BadLogicalCodec {
        invoked: AtomicBool,
    }

    impl LogicalExtensionCodec for BadLogicalCodec {
        fn try_decode(
            &self,
            _buf: &[u8],
            _inputs: &[datafusion::logical_expr::LogicalPlan],
            _ctx: &TaskContext,
        ) -> datafusion::error::Result<datafusion::logical_expr::Extension> {
            self.invoked
                .store(true, std::sync::atomic::Ordering::Relaxed);
            exec_err!("this codec does not work")
        }

        fn try_encode(
            &self,
            _node: &datafusion::logical_expr::Extension,
            _buf: &mut Vec<u8>,
        ) -> datafusion::error::Result<()> {
            self.invoked
                .store(true, std::sync::atomic::Ordering::Relaxed);
            exec_err!("this codec does not work")
        }

        fn try_decode_table_provider(
            &self,
            _buf: &[u8],
            _table_ref: &datafusion::common::TableReference,
            _schema: datafusion::arrow::datatypes::SchemaRef,
            _ctx: &TaskContext,
        ) -> datafusion::error::Result<
            std::sync::Arc<dyn datafusion::catalog::TableProvider>,
        > {
            self.invoked
                .store(true, std::sync::atomic::Ordering::Relaxed);
            exec_err!("this codec does not work")
        }

        fn try_encode_table_provider(
            &self,
            _table_ref: &datafusion::common::TableReference,
            _node: std::sync::Arc<dyn datafusion::catalog::TableProvider>,
            _buf: &mut Vec<u8>,
        ) -> datafusion::error::Result<()> {
            self.invoked
                .store(true, std::sync::atomic::Ordering::Relaxed);
            exec_err!("this codec does not work")
        }

        fn try_decode_file_format(
            &self,
            _buf: &[u8],
            _ctx: &TaskContext,
        ) -> datafusion::error::Result<
            Arc<dyn datafusion::datasource::file_format::FileFormatFactory>,
        > {
            self.invoked
                .store(true, std::sync::atomic::Ordering::Relaxed);
            exec_err!("this codec does not work")
        }

        fn try_encode_file_format(
            &self,
            _buf: &mut Vec<u8>,
            _node: Arc<dyn datafusion::datasource::file_format::FileFormatFactory>,
        ) -> datafusion::error::Result<()> {
            self.invoked
                .store(true, std::sync::atomic::Ordering::Relaxed);
            //Ok(())
            exec_err!("this codec does not work")
        }
    }

    #[derive(Debug)]
    struct MockPhysicalCodec {
        invoked: AtomicBool,
        codec: Arc<dyn PhysicalExtensionCodec>,
    }

    impl Default for MockPhysicalCodec {
        fn default() -> Self {
            Self {
                invoked: AtomicBool::new(false),
                codec: Arc::new(BallistaPhysicalExtensionCodec::default()),
            }
        }
    }

    impl PhysicalExtensionCodec for MockPhysicalCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            inputs: &[Arc<dyn datafusion::physical_plan::ExecutionPlan>],
            ctx: &TaskContext,
            proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>>
        {
            self.invoked
                .store(true, std::sync::atomic::Ordering::Relaxed);
            self.codec.try_decode(buf, inputs, ctx, proto_converter)
        }

        fn try_encode(
            &self,
            node: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
            buf: &mut Vec<u8>,
            proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> datafusion::error::Result<()> {
            self.invoked
                .store(true, std::sync::atomic::Ordering::Relaxed);
            self.codec.try_encode(node, buf, proto_converter)
        }
    }

    #[derive(Debug, Default)]
    struct BadPlanner {}

    #[async_trait::async_trait]
    impl QueryPlanner for BadPlanner {
        async fn create_physical_plan(
            &self,
            _logical_plan: &LogicalPlan,
            _session_state: &dyn Session,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            exec_err!("does not work")
        }
    }
}
