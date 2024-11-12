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

//! # Object Store Support
//!
//! Tests demonstrate how to setup object stores with ballista.
//!
//! Test depend on Minio testcontainer acting as S3 object
//! store.
//!
//! Tesctoncainers require docker to run.

mod common;

#[cfg(test)]
#[cfg(feature = "standalone")]
#[cfg(feature = "testcontainers")]
mod standalone {

    use ballista::extension::SessionContextExt;
    use datafusion::{assert_batches_eq, prelude::SessionContext};
    use datafusion::{
        error::DataFusionError,
        execution::{
            runtime_env::{RuntimeConfig, RuntimeEnv},
            SessionStateBuilder,
        },
    };
    use std::sync::Arc;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    #[tokio::test]
    async fn should_execute_sql_write() -> datafusion::error::Result<()> {
        let container = crate::common::create_minio_container();
        let node = container.start().await.unwrap();

        node.exec(crate::common::create_bucket_command())
            .await
            .unwrap();

        let port = node.get_host_port_ipv4(9000).await.unwrap();

        let object_store = crate::common::create_s3_store(port)
            .map_err(|e| DataFusionError::External(e.into()))?;

        let test_data = crate::common::example_test_data();
        let config = RuntimeConfig::new();
        let runtime_env = RuntimeEnv::new(config)?;

        runtime_env.register_object_store(
            &format!("s3://{}", crate::common::BUCKET)
                .as_str()
                .try_into()
                .unwrap(),
            Arc::new(object_store),
        );
        let state = SessionStateBuilder::new()
            .with_runtime_env(runtime_env.into())
            .build();

        let ctx: SessionContext = SessionContext::standalone_with_state(state).await?;
        ctx.register_parquet(
            "test",
            &format!("{test_data}/alltypes_plain.parquet"),
            Default::default(),
        )
        .await?;

        let write_dir_path =
            &format!("s3://{}/write_test.parquet", crate::common::BUCKET);

        ctx.sql("select * from test")
            .await?
            .write_parquet(write_dir_path, Default::default(), Default::default())
            .await?;

        ctx.register_parquet("written_table", write_dir_path, Default::default())
            .await?;

        let result = ctx
            .sql("select id, string_col, timestamp_col from written_table where id > 4")
            .await?
            .collect()
            .await?;
        let expected = [
            "+----+------------+---------------------+",
            "| id | string_col | timestamp_col       |",
            "+----+------------+---------------------+",
            "| 5  | 31         | 2009-03-01T00:01:00 |",
            "| 6  | 30         | 2009-04-01T00:00:00 |",
            "| 7  | 31         | 2009-04-01T00:01:00 |",
            "+----+------------+---------------------+",
        ];

        assert_batches_eq!(expected, &result);
        Ok(())
    }
}

#[cfg(test)]
#[cfg(feature = "testcontainers")]
mod remote {

    use ballista::extension::SessionContextExt;
    use datafusion::{assert_batches_eq, prelude::SessionContext};
    use datafusion::{
        error::DataFusionError,
        execution::{
            runtime_env::{RuntimeConfig, RuntimeEnv},
            SessionStateBuilder,
        },
    };
    use std::sync::Arc;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    #[tokio::test]
    async fn should_execute_sql_write() -> datafusion::error::Result<()> {
        let test_data = crate::common::example_test_data();

        let container = crate::common::create_minio_container();
        let node = container.start().await.unwrap();

        node.exec(crate::common::create_bucket_command())
            .await
            .unwrap();

        let port = node.get_host_port_ipv4(9000).await.unwrap();

        let object_store = crate::common::create_s3_store(port)
            .map_err(|e| DataFusionError::External(e.into()))?;

        let config = RuntimeConfig::new();
        let runtime_env = RuntimeEnv::new(config)?;

        runtime_env.register_object_store(
            &format!("s3://{}", crate::common::BUCKET)
                .as_str()
                .try_into()
                .unwrap(),
            Arc::new(object_store),
        );
        let state = SessionStateBuilder::new()
            .with_runtime_env(runtime_env.into())
            .build();

        let (host, port) =
            crate::common::setup_test_cluster_with_state(state.clone()).await;
        let url = format!("df://{host}:{port}");

        let ctx: SessionContext = SessionContext::remote_with_state(&url, state).await?;
        ctx.register_parquet(
            "test",
            &format!("{test_data}/alltypes_plain.parquet"),
            Default::default(),
        )
        .await?;

        let write_dir_path =
            &format!("s3://{}/write_test.parquet", crate::common::BUCKET);

        ctx.sql("select * from test")
            .await?
            .write_parquet(write_dir_path, Default::default(), Default::default())
            .await?;

        ctx.register_parquet("written_table", write_dir_path, Default::default())
            .await?;

        let result = ctx
            .sql("select id, string_col, timestamp_col from written_table where id > 4")
            .await?
            .collect()
            .await?;
        let expected = [
            "+----+------------+---------------------+",
            "| id | string_col | timestamp_col       |",
            "+----+------------+---------------------+",
            "| 5  | 31         | 2009-03-01T00:01:00 |",
            "| 6  | 30         | 2009-04-01T00:00:00 |",
            "| 7  | 31         | 2009-04-01T00:01:00 |",
            "+----+------------+---------------------+",
        ];

        assert_batches_eq!(expected, &result);
        Ok(())
    }
}
