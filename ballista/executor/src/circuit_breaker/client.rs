use anyhow::Error;
use ballista_core::{
    circuit_breaker::model::{CircuitBreakerStageKey, CircuitBreakerTaskKey},
    error::BallistaError,
    serde::protobuf::{self, CircuitBreakerUpdateRequest, CircuitBreakerUpdateResponse},
};
use dashmap::DashMap;
use lazy_static::lazy_static;
use prometheus::{
    register_histogram, register_int_counter, register_int_gauge, Histogram, IntCounter,
    IntGauge,
};
use std::{
    collections::HashMap,
    ops::Add,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
    time::Instant,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};
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
        mut update_receiver: Receiver<ClientUpdate>,
        config: CircuitBreakerClientConfig,
        get_scheduler: Arc<dyn SchedulerClientRegistry>,
        state_per_stage: Arc<DashMap<CircuitBreakerStageKey, CircuitBreakerStageState>>,
        executor_id: String,
    ) {
        let mut last_cleanup = Instant::now();
        let mut task_scheduler_lookup = HashMap::new();
        let mut updates = HashMap::new();
        let mut scheduler_deregistrations = Vec::new();

        while let Some(update) = update_receiver.recv().await {
            Self::handle_update(
                update,
                &mut updates,
                &mut task_scheduler_lookup,
                &mut scheduler_deregistrations,
                &config,
                &executor_id,
                &state_per_stage,
                get_scheduler.as_ref(),
            )
            .await;

            let now = Instant::now();

            if last_cleanup.add(config.cache_cleanup_frequency) < now {
                Self::cleanup_stage_states(&config, &state_per_stage, now);
                Self::cleanup_scheduler_lookup(
                    &mut scheduler_deregistrations,
                    &mut task_scheduler_lookup,
                );

                last_cleanup = now;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_update(
        received: ClientUpdate,
        updates_per_scheduler: &mut HashMap<String, PerSchedulerState>,
        task_scheduler_lookup: &mut HashMap<String, String>,
        scheduler_deregistrations: &mut Vec<SchedulerDeregistration>,
        config: &CircuitBreakerClientConfig,
        executor_id: &str,
        state_per_stage: &DashMap<CircuitBreakerStageKey, CircuitBreakerStageState>,
        scheduler_lookup: &dyn SchedulerClientRegistry,
    ) {
        let now = Instant::now();

        match received {
            ClientUpdate::Update(update) => {
                if let Some(mut state) = state_per_stage.get_mut(&update.key.stage_key) {
                    state.last_updated = now;
                }

                if let Some(scheduler_id) = task_scheduler_lookup.get(&update.key.task_id)
                {
                    let mut per_scheduler_state = updates_per_scheduler
                        .remove(scheduler_id)
                        .unwrap_or_default();

                    per_scheduler_state
                        .updates
                        .entry(update.key)
                        .and_modify(|percent| {
                            *percent = update.percent.max(*percent);
                        })
                        .or_insert(update.percent);

                    if per_scheduler_state.updates.len() >= config.max_batch_size
                        || per_scheduler_state.last_sent.add(config.send_interval) < now
                    {
                        Self::process_batch(
                            scheduler_id,
                            &mut per_scheduler_state.updates,
                            state_per_stage,
                            scheduler_lookup,
                            executor_id,
                            config,
                        )
                        .await;
                    } else {
                        updates_per_scheduler
                            .insert(scheduler_id.clone(), per_scheduler_state);
                    }
                } else {
                    warn!("No scheduler found for task {}", update.key.task_id);
                }
            }
            ClientUpdate::SchedulerRegistration(registration) => {
                task_scheduler_lookup
                    .insert(registration.task_id, registration.scheduler_id);
            }

            ClientUpdate::SchedulerDeregistration(deregistration) => {
                scheduler_deregistrations.push(deregistration);
            }
        }
    }

    async fn process_batch(
        scheduler_id: &str,
        updates: &mut HashMap<CircuitBreakerTaskKey, f64>,
        state_per_stage: &DashMap<CircuitBreakerStageKey, CircuitBreakerStageState>,
        get_scheduler: &dyn SchedulerClientRegistry,
        executor_id: &str,
        config: &CircuitBreakerClientConfig,
    ) {
        let update_protos = updates
            .drain()
            .map(|(key, percent)| protobuf::CircuitBreakerUpdate {
                key: Some(key.into()),
                percent,
            })
            .collect::<Vec<_>>();

        Self::send_updates(
            executor_id,
            scheduler_id,
            update_protos,
            get_scheduler,
            state_per_stage,
            config,
        )
        .await;
    }

    async fn send_updates(
        executor_id: &str,
        scheduler_id: &str,
        updates: Vec<protobuf::CircuitBreakerUpdate>,
        get_scheduler: &dyn SchedulerClientRegistry,
        state_per_stage: &DashMap<CircuitBreakerStageKey, CircuitBreakerStageState>,
        config: &CircuitBreakerClientConfig,
    ) {
        let updates_len = updates.len();

        let mut scheduler = match get_scheduler
            .get_or_create_scheduler_client(scheduler_id)
            .await
        {
            Ok(scheduler) => scheduler,
            Err(e) => {
                warn!("Failed to get scheduler {}: {}", scheduler_id, e);
                return;
            }
        };

        let request = CircuitBreakerUpdateRequest {
            updates,
            executor_id: executor_id.to_owned(),
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
                BATCH_SIZE.observe(updates_len as f64 / config.max_batch_size as f64);
                UPDATE_LATENCY_SECONDS.observe(request_latency.as_secs_f64());

                Self::handle_response(response.into_inner(), state_per_stage);
            }
        };
    }

    fn handle_response(
        response: CircuitBreakerUpdateResponse,
        state_per_stage: &DashMap<CircuitBreakerStageKey, CircuitBreakerStageState>,
    ) {
        for command in response.commands {
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

    fn cleanup_stage_states(
        config: &CircuitBreakerClientConfig,
        state_per_stage: &DashMap<CircuitBreakerStageKey, CircuitBreakerStageState>,
        timestamp: Instant,
    ) {
        let mut keys_to_delete = Vec::new();

        for entry in state_per_stage.iter() {
            if entry.value().last_updated.add(config.cache_ttl) < timestamp {
                keys_to_delete.push(entry.key().clone());
            }
        }

        for key in keys_to_delete.iter() {
            state_per_stage.remove(key);
        }

        CACHE_SIZE.set(state_per_stage.len() as i64);
    }

    fn cleanup_scheduler_lookup(
        scheduler_deregistrations: &mut Vec<SchedulerDeregistration>,
        task_scheduler_lookup: &mut HashMap<String, String>,
    ) {
        for deregistration in scheduler_deregistrations.drain(..) {
            task_scheduler_lookup.remove(&deregistration.task_id);
        }

        SCHEDULER_LOOKUP_SIZE.set(task_scheduler_lookup.len() as i64);
    }
}

struct PerSchedulerState {
    updates: HashMap<CircuitBreakerTaskKey, f64>,
    last_sent: Instant,
}

impl Default for PerSchedulerState {
    fn default() -> Self {
        Self {
            updates: HashMap::new(),
            last_sent: Instant::now(),
        }
    }
}
