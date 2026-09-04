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

use ballista::prelude::{SessionConfigExt, SessionContextExt};
use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use hudi_ballista::{register_hudi_codec, register_hudi_table};
use hudi_test::SampleTable;

#[tokio::test]
async fn reads_projected_hudi_cow_rows_through_ballista() {
    let config = register_hudi_codec(
        SessionConfig::new_with_ballista()
            .with_target_partitions(2)
            .with_ballista_standalone_parallelism(2),
    );
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let ctx = SessionContext::standalone_with_state(state).await.unwrap();
    let table = SampleTable::V6Nonpartitioned.url_to_cow();
    register_hudi_table(
        &ctx,
        "users",
        table.as_str(),
        std::iter::empty::<(&str, &str)>(),
    )
    .await
    .unwrap();

    let batches = ctx
        .sql("SELECT id, name FROM users WHERE \"isActive\" ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let names = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(
        (0..ids.len())
            .map(|row| (ids.value(row), names.value(row)))
            .collect::<Vec<_>>(),
        vec![(3, "Carol"), (4, "Diana")]
    );
}
