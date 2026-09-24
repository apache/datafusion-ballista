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

//! Kubernetes (kind) backend scenarios.
//!
//! Gated behind the `k8s` feature and `CHAOS_BACKEND=kind`, so the default
//! `cargo test` never touches a cluster. Run with the kind runbook in the crate
//! README:
//!
//! ```sh
//! dev/build-chaos-docker.sh
//! kind create cluster --config chaos-testing/k8s/kind-config.yaml
//! kind load docker-image ballista-chaos:test
//! CHAOS_BACKEND=kind cargo test -p ballista-chaos --features k8s --test k8s -- --test-threads=1
//! ```
#![cfg(feature = "k8s")]

use ballista::prelude::{SessionConfigExt, SessionContextExt};
use chaos_testing::fixture::Fixture;
use chaos_testing::k8s::K8sCluster;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

/// The k8s scenarios need a running kind cluster with the chaos images loaded;
/// they are opt-in via `CHAOS_BACKEND=kind` so a plain `cargo test` skips them.
fn kind_backend_selected() -> bool {
    if std::env::var("CHAOS_BACKEND").as_deref() == Ok("kind") {
        true
    } else {
        eprintln!(
            "skipping k8s scenario: set CHAOS_BACKEND=kind and provide a kind cluster \
             with the chaos images loaded (see the crate README runbook)"
        );
        false
    }
}

/// The chaos-free baseline query, run on a fresh local DataFusion context. This
/// is the reference the cluster must reproduce exactly.
async fn local_baseline(fixture: &Fixture) -> String {
    let ctx = SessionContext::new();
    for stmt in fixture.register_sql() {
        ctx.sql(&stmt).await.unwrap().collect().await.unwrap();
    }
    let batches = ctx
        .sql(Fixture::baseline_query())
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    pretty_format_batches(&batches).unwrap().to_string()
}

/// Smoke test: a real query on a real kind cluster returns the same result as
/// plain local DataFusion. Exercises the whole path — client → scheduler → pods
/// → shuffle → result — with the fixture shared through the `hostPath` mount.
#[tokio::test]
async fn baseline_matches_local_datafusion_on_k8s() {
    if !kind_backend_selected() {
        return;
    }

    let cluster = K8sCluster::start(2).await.expect("kind cluster must start");

    // Written into the shared mount, so the scheduler and executor pods see it.
    let fixture = Fixture::write(cluster.shared_dir())
        .await
        .expect("fixture must be written to the shared mount");

    let expected = local_baseline(&fixture).await;

    let config = SessionConfig::new_with_ballista();
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let ctx = SessionContext::remote_with_state(&cluster.scheduler_url(), state)
        .await
        .expect("client must connect to the scheduler");

    for stmt in fixture.register_sql() {
        ctx.sql(&stmt).await.unwrap().collect().await.unwrap();
    }
    let batches = ctx
        .sql(Fixture::baseline_query())
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let actual = pretty_format_batches(&batches).unwrap().to_string();

    assert_eq!(
        actual, expected,
        "cluster result must match plain local DataFusion"
    );
}
