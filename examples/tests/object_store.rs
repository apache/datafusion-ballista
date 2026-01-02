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
#[cfg(feature = "testcontainers")]
mod standalone {

    use ballista::extension::SessionContextExt;
    use ballista_examples::test_util::examples_test_data;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::{assert_batches_eq, prelude::SessionContext};
    use datafusion::{error::DataFusionError, execution::SessionStateBuilder};
    use std::sync::Arc;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    #[tokio::test]
    async fn should_execute_sql_write() -> datafusion::error::Result<()> {
        let container = crate::common::create_minio_container();
        let node = container.start().await.unwrap();

        node.exec(crate::common::create_bucket_command())
            .await
            .unwrap();

        let host = node.get_host().await.unwrap();
        let port = node.get_host_port_ipv4(9000).await.unwrap();

        let object_store = crate::common::create_s3_store(&host.to_string(), port)
            .map_err(|e| DataFusionError::External(e.into()))?;

        let test_data = examples_test_data();
        let runtime_env = RuntimeEnvBuilder::new().build()?;

        runtime_env.register_object_store(
            &format!("s3://{}", crate::common::BUCKET)
                .as_str()
                .try_into()
                .unwrap(),
            Arc::new(object_store),
        );
        let state = SessionStateBuilder::new()
            .with_runtime_env(runtime_env.into())
            .with_default_features()
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

        let result = ctx
            .sql("select max(id) as max, min(id) as min from written_table")
            .await?
            .collect()
            .await?;
        let expected = [
            "+-----+-----+",
            "| max | min |",
            "+-----+-----+",
            "| 7   | 0   |",
            "+-----+-----+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }
}

#[cfg(test)]
#[cfg(feature = "testcontainers")]
mod remote {

    use ballista::extension::SessionContextExt;
    use ballista_examples::test_util::examples_test_data;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::{assert_batches_eq, prelude::SessionContext};
    use datafusion::{error::DataFusionError, execution::SessionStateBuilder};
    use std::sync::Arc;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    #[tokio::test]
    async fn should_execute_sql_write() -> datafusion::error::Result<()> {
        let test_data = examples_test_data();

        let container = crate::common::create_minio_container();
        let node = container.start().await.unwrap();

        node.exec(crate::common::create_bucket_command())
            .await
            .unwrap();

        let host = node.get_host().await.unwrap();
        let port = node.get_host_port_ipv4(9000).await.unwrap();

        let object_store = crate::common::create_s3_store(&host.to_string(), port)
            .map_err(|e| DataFusionError::External(e.into()))?;

        let runtime_env = RuntimeEnvBuilder::new().build()?;

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

// this test shows how to register external ObjectStoreRegistry and configure it
// using infrastructure provided by ballista.
//
// it relies on ballista configuration integration with SessionConfig, and
// SessionConfig propagation across ballista cluster.

#[cfg(test)]
#[cfg(feature = "testcontainers")]
mod custom_s3_config {

    use crate::common::{ACCESS_KEY_ID, SECRET_KEY};
    use ballista::extension::SessionContextExt;
    use ballista::prelude::SessionConfigExt;
    use ballista_core::RuntimeProducer;
    use ballista_core::object_store::{CustomObjectStoreRegistry, S3Options};
    use ballista_examples::test_util::examples_test_data;
    use datafusion::execution::SessionState;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::SessionConfig;
    use datafusion::{assert_batches_eq, prelude::SessionContext};
    use datafusion::{error::DataFusionError, execution::SessionStateBuilder};
    use std::sync::Arc;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    #[tokio::test]
    async fn should_configure_s3_execute_sql_write_remote()
    -> datafusion::error::Result<()> {
        let test_data = examples_test_data();

        //
        // Minio cluster setup
        //
        let container = crate::common::create_minio_container();
        let node = container.start().await.unwrap();

        node.exec(crate::common::create_bucket_command())
            .await
            .unwrap();

        let endpoint_host = node.get_host().await.unwrap();
        let endpoint_port = node.get_host_port_ipv4(9000).await.unwrap();

        log::info!(
            "MINIO testcontainers host: {}, port: {}",
            endpoint_host,
            endpoint_port
        );

        //
        // Session Context and Ballista cluster setup
        //

        // Setting up configuration producer
        //
        // configuration producer registers user defined config extension
        // S3Option with relevant S3 configuration
        let config_producer =
            Arc::new(ballista_core::object_store::session_config_with_s3_support);
        // Setting up runtime producer
        //
        // Runtime producer creates object store registry
        // which can create object store connecter based on
        // S3Option configuration.
        let runtime_producer: RuntimeProducer =
            Arc::new(ballista_core::object_store::runtime_env_with_s3_support);

        // Session builder creates SessionState
        //
        // which is configured using runtime and configuration producer,
        // producing same runtime environment, and providing same
        // object store registry.

        let session_builder =
            Arc::new(ballista_core::object_store::session_state_with_s3_support);

        let state = session_builder(config_producer())?;

        // setting up ballista cluster with new runtime, configuration, and session state producers
        let (host, port) = crate::common::setup_test_cluster_with_builders(
            config_producer,
            runtime_producer,
            session_builder,
        )
        .await;
        let url = format!("df://{host}:{port}");

        // establishing cluster connection,
        let ctx: SessionContext = SessionContext::remote_with_state(&url, state).await?;

        // setting up relevant S3 options
        ctx.sql("SET s3.allow_http = true").await?.show().await?;
        ctx.sql(&format!("SET s3.access_key_id = '{}'", ACCESS_KEY_ID))
            .await?
            .show()
            .await?;
        ctx.sql(&format!("SET s3.secret_access_key = '{}'", SECRET_KEY))
            .await?
            .show()
            .await?;
        ctx.sql(&format!(
            "SET s3.endpoint = 'http://{}:{}'",
            endpoint_host, endpoint_port
        ))
        .await?
        .show()
        .await?;
        ctx.sql("SET s3.allow_http = true").await?.show().await?;

        // verifying that we have set S3Options
        ctx.sql("select name, value from information_schema.df_settings where name like 's3.%'").await?.show().await?;

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

        let result = ctx
            .sql("select max(id) as max, min(id) as min from written_table")
            .await?
            .collect()
            .await?;
        let expected = [
            "+-----+-----+",
            "| max | min |",
            "+-----+-----+",
            "| 7   | 0   |",
            "+-----+-----+",
        ];

        assert_batches_eq!(expected, &result);
        Ok(())
    }

    // this test shows how to register external ObjectStoreRegistry and configure it
    // using infrastructure provided by ballista standalone.
    //
    // it relies on ballista configuration integration with SessionConfig, and
    // SessionConfig propagation across ballista cluster.

    #[tokio::test]
    async fn should_configure_s3_execute_sql_write_standalone()
    -> datafusion::error::Result<()> {
        let test_data = examples_test_data();

        //
        // Minio cluster setup
        //
        let container = crate::common::create_minio_container();
        let node = container.start().await.unwrap();

        node.exec(crate::common::create_bucket_command())
            .await
            .unwrap();

        let endpoint_host = node.get_host().await.unwrap();
        let endpoint_port = node.get_host_port_ipv4(9000).await.unwrap();

        //
        // Session Context and Ballista cluster setup
        //

        // Setting up configuration producer
        //
        // configuration producer registers user defined config extension
        // S3Option with relevant S3 configuration
        let config_producer = Arc::new(|| {
            SessionConfig::new_with_ballista()
                .with_information_schema(true)
                .with_option_extension(S3Options::default())
        });

        // Session builder creates SessionState
        //
        // which is configured using runtime and configuration producer,
        // producing same runtime environment, and providing same
        // object store registry.

        let session_builder = Arc::new(produce_state);
        let state = session_builder(config_producer())?;

        // // establishing cluster connection,
        let ctx: SessionContext = SessionContext::standalone_with_state(state).await?;

        // setting up relevant S3 options
        ctx.sql("SET s3.allow_http = true").await?.show().await?;
        ctx.sql(&format!("SET s3.access_key_id = '{}'", ACCESS_KEY_ID))
            .await?
            .show()
            .await?;
        ctx.sql(&format!("SET s3.secret_access_key = '{}'", SECRET_KEY))
            .await?
            .show()
            .await?;
        ctx.sql(&format!(
            "SET s3.endpoint = 'http://{}:{}'",
            endpoint_host, endpoint_port
        ))
        .await?
        .show()
        .await?;
        ctx.sql("SET s3.allow_http = true").await?.show().await?;

        // verifying that we have set S3Options
        ctx.sql("select name, value from information_schema.df_settings where name like 's3.%'").await?.show().await?;

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

    fn produce_state(
        session_config: SessionConfig,
    ) -> datafusion::common::Result<SessionState> {
        let s3options = session_config
            .options()
            .extensions
            .get::<S3Options>()
            .ok_or(DataFusionError::Configuration(
                "S3 Options not set".to_string(),
            ))?;

        let runtime_env = RuntimeEnvBuilder::new()
            .with_object_store_registry(Arc::new(CustomObjectStoreRegistry::new(
                s3options.clone(),
            )))
            .build()?;

        Ok(SessionStateBuilder::new()
            .with_runtime_env(runtime_env.into())
            .with_config(session_config)
            .with_default_features()
            .build())
    }
}
