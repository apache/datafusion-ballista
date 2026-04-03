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

//! Riffle lifecycle manager for the scheduler.
//!
//! Manages the Riffle (Uniffle) shuffle service lifecycle:
//! - Registers the application with the coordinator on job start
//! - Sends periodic heartbeats to keep the app alive
//! - Unregisters the application on job completion/failure

use ballista_riffle::client::RiffleClient;
use ballista_riffle::config::RiffleConfig;
use dashmap::DashMap;
use log::{debug, info, warn};
use std::sync::Arc;
use tokio::task::JoinHandle;

/// Manages Riffle lifecycle for all jobs in the scheduler.
///
/// One instance lives for the scheduler's lifetime. It tracks per-job
/// heartbeat tasks and client instances for proper cleanup.
pub struct RiffleLifecycleManager {
    config: RiffleConfig,
    /// Per-job heartbeat task handles: job_id -> JoinHandle
    heartbeat_handles: DashMap<String, JoinHandle<()>>,
    /// Per-job client instances for cleanup: job_id -> RiffleClient
    /// Kept alive so unregister_application can find cached server connections.
    clients: DashMap<String, Arc<RiffleClient>>,
}

impl RiffleLifecycleManager {
    /// Create from Ballista config if Riffle is enabled.
    pub fn from_ballista_config(
        ballista_config: &ballista_core::config::BallistaConfig,
    ) -> Option<Arc<Self>> {
        if ballista_config.shuffle_backend() != "riffle" {
            return None;
        }

        let config = RiffleConfig {
            coordinator_host: ballista_config.riffle_coordinator_host(),
            coordinator_port: ballista_config.riffle_coordinator_port() as u16,
            app_id: String::new(), // set per-job
            ..Default::default()
        };

        Some(Arc::new(Self {
            config,
            heartbeat_handles: DashMap::new(),
            clients: DashMap::new(),
        }))
    }

    /// Called when a job is submitted. Registers the app and starts heartbeat.
    pub async fn on_job_start(&self, job_id: &str) {
        let config = RiffleConfig {
            app_id: job_id.to_string(),
            ..self.config.clone()
        };

        // Register application with coordinator
        let client = match RiffleClient::connect(config.clone()).await {
            Ok(client) => {
                if let Err(e) = client.register_application().await {
                    warn!("Failed to register Riffle app for job {job_id}: {e}");
                } else {
                    info!("Registered Riffle app for job {job_id}");
                }
                Arc::new(client)
            }
            Err(e) => {
                warn!("Failed to connect to Riffle coordinator for job {job_id}: {e}");
                return;
            }
        };

        // Store client for cleanup
        self.clients.insert(job_id.to_string(), client);

        // Start heartbeat background task
        let heartbeat_interval =
            std::time::Duration::from_secs(config.heartbeat_interval_secs);
        let job_id_owned = job_id.to_string();

        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(heartbeat_interval).await;

                match RiffleClient::connect(config.clone()).await {
                    Ok(client) => {
                        if let Err(e) = client.send_app_heartbeat().await {
                            debug!(
                                "Riffle heartbeat failed for job {job_id_owned}: {e}"
                            );
                        }
                    }
                    Err(e) => {
                        debug!(
                            "Riffle heartbeat connect failed for job {job_id_owned}: {e}"
                        );
                    }
                }
            }
        });

        self.heartbeat_handles
            .insert(job_id.to_string(), handle);
    }

    /// Called when a job completes (success or failure). Stops heartbeat
    /// and lets the coordinator expire the app data after the heartbeat timeout.
    ///
    /// Cleanup happens passively: when heartbeats stop, the coordinator marks
    /// the app as expired after `rss.coordinator.app.expired` (default 60s),
    /// and shuffle servers release the data. This matches Spark's behavior.
    pub async fn on_job_end(&self, job_id: &str) {
        // Stop heartbeat — coordinator will expire the app after timeout
        if let Some((_, handle)) = self.heartbeat_handles.remove(job_id) {
            handle.abort();
            info!("Stopped Riffle heartbeat for job {job_id}, data will expire after coordinator timeout");
        }

        // Remove stored client
        self.clients.remove(job_id);
    }
}

impl Drop for RiffleLifecycleManager {
    fn drop(&mut self) {
        // Abort all heartbeat tasks
        for entry in self.heartbeat_handles.iter() {
            entry.value().abort();
        }
    }
}
