use anyhow::Error;
use ballista_core::{
    circuit_breaker::model::CircuitBreakerStageKey,
    error::BallistaError,
    serde::protobuf::{self, CircuitBreakerUpdateRequest},
};
use dashmap::DashMap;
use lazy_static::lazy_static;
use prometheus::{
    register_histogram, register_int_counter, register_int_gauge, Histogram, IntCounter,
    IntGauge,
};
use std::{
    collections::{HashMap, HashSet},
    ops::Add,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
    time::Instant,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::{info, warn};

use crate::{
    circuit_breaker::stream::CircuitBreakerUpdate,
    scheduler_client_registry::SchedulerClientRegistry,
};

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct CircuitBreakerMetadataExtension {
    pub job_id: String,
    pub stage_id: u32,
    pub attempt_number: u32,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct CircuitBreakerClientConfig {
    pub send_interval: Duration,
    pub cache_cleanup_frequency: Duration,
    pub cache_ttl: Duration,
    pub channel_size: usize,
    pub max_batch_size: usize,
}

impl Default for CircuitBreakerClientConfig {
    fn default() -> Self {
        Self {
            send_interval: Duration::from_secs(1),
            cache_cleanup_frequency: Duration::from_secs(15),
            cache_ttl: Duration::from_secs(60),
            channel_size: 1000,
            max_batch_size: 1000,
        }
    }
}

pub struct CircuitBreakerClient {
    update_sender: Sender<ClientUpdate>,
    state_per_stage: Arc<DashMap<CircuitBreakerStageKey, CircuitBreakerStageState>>,
}

struct CircuitBreakerStageState {
    circuit_breaker: Arc<AtomicBool>,
    last_updated: Instant,
}

impl CircuitBreakerStageState {
    fn trip(&self) {
        self.circuit_breaker.store(true, Ordering::Release);
    }
}

#[derive(Debug)]
struct SchedulerRegistration {
    task_id: String,
    scheduler_id: String,
}

#[derive(Debug)]
struct SchedulerDeregistration {
    task_id: String,
}

#[derive(Debug)]
enum ClientUpdate {
    Update(CircuitBreakerUpdate),
    SchedulerRegistration(SchedulerRegistration),
    SchedulerDeregistration(SchedulerDeregistration),
}

lazy_static! {
    static ref BATCH_SIZE: Histogram = register_histogram!(
        "ballista_circuit_breaker_client_batch_size",
        "Number of updates in a batch sent to the scheduler (in percent of max batch size)",
        Vec::from_iter((0..=100).step_by(5).map(|x| x as f64 / 100.0))
    )
    .unwrap();
    static ref UPDATE_LATENCY_SECONDS: Histogram = register_histogram!(
        "ballista_circuit_breaker_client_update_latency",
        "Latency of sending updates to the schedulers in seconds",
        vec![0.01, 0.03, 0.05, 0.1, 0.3, 0.5, 1.0, 3.0]
    )
    .unwrap();
    static ref SENT_UPDATES: IntCounter = register_int_counter!(
        "ballista_circuit_breaker_client_sent_updates",
        "Number of updates sent to the schedulers"
    )
    .unwrap();
    static ref CACHE_SIZE: IntGauge = register_int_gauge!(
        "ballista_circuit_breaker_client_cache_size",
        "Number active stages in cache"
    )
    .unwrap();
    static ref SCHEDULER_LOOKUP_SIZE: IntGauge = register_int_gauge!(
        "ballista_circuit_breaker_client_scheduler_lookup_size",
        "Number of schedulers in lookup"
    )
    .unwrap();
}

impl CircuitBreakerClient {
    pub fn new(
        get_scheduler: Arc<dyn SchedulerClientRegistry>,
        config: CircuitBreakerClientConfig,
    ) -> Self {
        let (update_sender, update_receiver) = channel(config.channel_size);

        let executor_id = uuid::Uuid::new_v4().to_string();

        let state_per_stage = Arc::new(DashMap::new());

        tokio::spawn(Self::run_sender_daemon(
            update_receiver,
            config,
            get_scheduler,
            state_per_stage.clone(),
            executor_id,
        ));

        Self {
            update_sender,
            state_per_stage,
        }
    }

    pub fn register(
        &self,
        key: CircuitBreakerStageKey,
    ) -> Result<Arc<AtomicBool>, Error> {
        let state = self.state_per_stage.entry(key.clone()).or_insert_with(|| {
            CircuitBreakerStageState {
                circuit_breaker: Arc::new(AtomicBool::new(false)),
                last_updated: Instant::now(),
            }
        });

        Ok(state.circuit_breaker.clone())
    }

    pub fn send_update(&self, update: CircuitBreakerUpdate) -> Result<(), Error> {
        let update = ClientUpdate::Update(update);
        self.update_sender.try_send(update).map_err(|e| e.into())
    }

    pub fn register_scheduler(
        &self,
        task_id: String,
        scheduler_id: String,
    ) -> Result<(), BallistaError> {
        info!(
            "Registering scheduler {} for task {}",
            scheduler_id, task_id
        );

        let update = ClientUpdate::SchedulerRegistration(SchedulerRegistration {
            task_id,
            scheduler_id,
        });

        self.update_sender.try_send(update).map_err(|e| {
            BallistaError::Internal(format!(
                "Failed to send scheduler registration: {}",
                e
            ))
        })
    }

    pub fn deregister_scheduler(&self, task_id: String) -> Result<(), BallistaError> {
        info!("Deregistering scheduler for task {}", task_id);

        let update =
            ClientUpdate::SchedulerDeregistration(SchedulerDeregistration { task_id });

        self.update_sender.try_send(update).map_err(|e| {
            BallistaError::Internal(format!(
                "Failed to send scheduler deregistration: {}",
                e
            ))
        })
    }

    async fn run_sender_daemon(
        update_receiver: Receiver<ClientUpdate>,
        config: CircuitBreakerClientConfig,
        get_scheduler: Arc<dyn SchedulerClientRegistry>,
        state_per_stage: Arc<DashMap<CircuitBreakerStageKey, CircuitBreakerStageState>>,
        executor_id: String,
    ) {
        let mut scheduler_ids = HashMap::new();
        let mut last_cleanup = Instant::now();

        let updates_stream = ReceiverStream::new(update_receiver)
            .chunks_timeout(config.max_batch_size, config.send_interval);

        tokio::pin!(updates_stream);

        while let Some(combined_received) = updates_stream.next().await {
            BATCH_SIZE
                .observe(combined_received.len() as f64 / config.max_batch_size as f64);

            let mut updates = Vec::new();
            let mut scheduler_deregistrations = Vec::new();

            for update in combined_received {
                match update {
                    ClientUpdate::Update(update) => {
                        updates.push(update);
                    }
                    ClientUpdate::SchedulerRegistration(registration) => {
                        scheduler_ids
                            .insert(registration.task_id, registration.scheduler_id);
                    }

                    ClientUpdate::SchedulerDeregistration(deregistration) => {
                        scheduler_deregistrations.push(deregistration);
                    }
                }
            }

            let mut updates_per_scheduler = HashMap::new();
            let mut seen_task_keys = HashSet::new();
            let mut seen_stage_keys = HashSet::new();

            for update in updates.into_iter().rev() {
                // Per request only one update per task is sent
                // This is why we go from newest to oldest
                if seen_task_keys.insert(update.key.clone()) {
                    let scheduler_id: &String = match scheduler_ids
                        .get(&update.key.task_id)
                    {
                        Some(scheduler_id) => scheduler_id,
                        None => {
                            warn!("No scheduler found for task {}", update.key.task_id);
                            continue;
                        }
                    };

                    seen_stage_keys.insert(update.key.stage_key.clone());

                    updates_per_scheduler
                        .entry(scheduler_id.clone())
                        .or_insert_with(Vec::new)
                        .push(update);
                }
            }

            for stage_key in seen_stage_keys {
                if let Some(mut state) = state_per_stage.get_mut(&stage_key) {
                    state.last_updated = Instant::now();
                }
            }

            for (scheduler_id, updates) in updates_per_scheduler {
                let updates_len = updates.len();
                let mut request_updates = Vec::with_capacity(updates_len);

                for update in updates {
                    let key = update.key.into();

                    request_updates.push(protobuf::CircuitBreakerUpdate {
                        key: Some(key),
                        percent: update.percent,
                    })
                }

                let mut scheduler = match get_scheduler
                    .get_or_create_scheduler_client(&scheduler_id)
                    .await
                {
                    Ok(scheduler) => scheduler,
                    Err(e) => {
                        warn!("Failed to get scheduler {}: {}", scheduler_id, e);
                        continue;
                    }
                };

                let request = CircuitBreakerUpdateRequest {
                    updates: request_updates,
                    executor_id: executor_id.clone(),
                };

                let request_time = Instant::now();

                match scheduler.send_circuit_breaker_update(request).await {
                    Err(e) => warn!(
                        "Failed to send circuit breaker update to scheduler {}: {}",
                        scheduler_id, e
                    ),
                    Ok(response) => {
                        let request_latency = request_time.elapsed();

                        SENT_UPDATES.inc_by(updates_len as u64);
                        UPDATE_LATENCY_SECONDS.observe(request_latency.as_secs_f64());

                        let commands = response.into_inner().commands;

                        for command in commands {
                            if let Some(key_proto) = command.key {
                                let key = key_proto.into();

                                if let Some(state) = state_per_stage.get_mut(&key) {
                                    state.trip();
                                } else {
                                    warn!("No state found for task {:?}", key);
                                }
                            }
                        }
                    }
                };
            }

            // Remove scheduler registration after all updates have been processed,
            // to make sure we still send the last updates and deregister afterwards.
            for deregistration in scheduler_deregistrations {
                scheduler_ids.remove(&deregistration.task_id);
            }

            let now = Instant::now();

            if last_cleanup.add(config.cache_cleanup_frequency) < now {
                let mut keys_to_delete = Vec::new();

                for entry in state_per_stage.iter() {
                    if entry.value().last_updated.add(config.cache_ttl) < now {
                        keys_to_delete.push(entry.key().clone());
                    }
                }

                for key in keys_to_delete.iter() {
                    state_per_stage.remove(key);
                }

                let removed_count = keys_to_delete.len();

                if removed_count > 0 {
                    info!(
                        "Cleaned up {} inactive stages, {} left",
                        removed_count,
                        state_per_stage.len()
                    );
                }

                CACHE_SIZE.set(state_per_stage.len() as i64);

                // Not actually related to cleanup but good timing
                SCHEDULER_LOOKUP_SIZE.set(scheduler_ids.len() as i64);

                last_cleanup = now;
            }
        }
    }
}
