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

//! Scans Iceberg tables in S3, through a REST catalog, with standalone
//! Ballista.
//!
//! Tests depend on RustFS and Iceberg REST catalog testcontainers, which
//! require docker to run.

mod common;

#[cfg(test)]
#[cfg(feature = "testcontainers")]
mod standalone {
    use crate::common::rustfs::RestCatalog;
    use crate::common::{assert_standalone_scans, create_table};

    #[tokio::test]
    async fn should_scan_s3_tables_through_a_rest_catalog()
    -> datafusion::error::Result<()> {
        let rest = RestCatalog::start().await;
        create_table(rest.catalog.as_ref()).await;
        assert_standalone_scans(rest.catalog.clone()).await
    }
}
