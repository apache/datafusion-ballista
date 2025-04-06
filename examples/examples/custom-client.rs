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

use ballista::extension::SessionContextExt;
use ballista_core::object_store::{
    session_config_with_s3_support, session_state_with_s3_support,
};
use datafusion::error::Result;
use datafusion::{assert_batches_eq, prelude::SessionContext};

/// bucket name to be used for this example
const S3_BUCKET: &str = "ballista";
/// S3 access key
const S3_ACCESS_KEY_ID: &str = "MINIO";
/// S3 secret key
const S3_SECRET_KEY: &str = "MINIOSECRET";
///
/// # Extending Ballista
///
/// This example demonstrates how to extend ballista scheduler and executor registering new object store registry.
/// It uses local [minio](https://min.io) to act as S3 object store.
///
/// Ballista will be extended providing custom session configuration, runtime environment and session state.
///
/// Minio can be started:
///
/// ```bash
/// docker run --rm -p 9000:9000  -p 9001:9001 -e "MINIO_ACCESS_KEY=MINIO"  -e "MINIO_SECRET_KEY=MINIOSECRET"   quay.io/minio/minio server /data --console-address ":9001"
/// ```
/// After minio, we need to start `custom-scheduler`
///
/// ```bash
/// cargo run --example custom-scheduler
/// ```
///
/// and `custom-executor`
///
/// ```bash
/// cargo run --example custom-executor
/// ```
///
/// ```bash
/// cargo run --example custom-client
/// ```
#[tokio::main]
async fn main() -> Result<()> {
    let test_data = ballista_examples::test_util::examples_test_data();

    // new sessions state with required custom session configuration and runtime environment
    let state = session_state_with_s3_support(session_config_with_s3_support())?;

    let ctx: SessionContext =
        SessionContext::remote_with_state("df://localhost:50050", state).await?;

    // session config has relevant S3 options registered and exposed.
    // S3 configuration options can be changed using `SET` statement
    ctx.sql("SET s3.allow_http = true").await?.show().await?;

    ctx.sql(&format!("SET s3.access_key_id = '{}'", S3_ACCESS_KEY_ID))
        .await?
        .show()
        .await?;
    ctx.sql(&format!("SET s3.secret_access_key = '{}'", S3_SECRET_KEY))
        .await?
        .show()
        .await?;
    ctx.sql("SET s3.endpoint = 'http://localhost:9000'")
        .await?
        .show()
        .await?;
    ctx.sql("SET s3.allow_http = true").await?.show().await?;

    ctx.register_parquet(
        "test",
        &format!("{test_data}/alltypes_plain.parquet"),
        Default::default(),
    )
    .await?;

    let write_dir_path = &format!("s3://{}/write_test.parquet", S3_BUCKET);

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
