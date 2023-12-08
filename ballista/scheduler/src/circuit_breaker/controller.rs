use std::{
    cmp::max,
    collections::{HashMap, HashSet},
};

use ballista_core::circuit_breaker::model::{
    CircuitBreakerStageKey, CircuitBreakerTaskKey,
};
use itertools::Itertools;
use lazy_static::lazy_static;
use parking_lot::RwLock;
use prometheus::{register_int_counter, IntCounter};
use tracing::{debug, info};

pub struct CircuitBreakerController {
    job_states: RwLock<HashMap<String, JobState>>,
}

struct JobState {
    shared_states: HashMap<String, SharedState>,
}

// Multiple stages can share the same state,
// e.g. for per-query data scan limit.
struct SharedState {
    stage_states: HashMap<u32, StageState>,
    // This could also be calculated from the sum of the percentages of all latest attempts of each stage.
    percent: f64,
    executor_trip_state: HashMap<String, bool>,
    labels: HashSet<String>,
}

impl SharedState {
    fn new(executor_id: String) -> Self {
        let mut executor_trip_state = HashMap::new();
        executor_trip_state.insert(executor_id, false);

        Self {
            stage_states: HashMap::new(),
            percent: 0.0,
            executor_trip_state,
            labels: HashSet::new(),
        }
    }
}

struct StageState {
    latest_attempt_num: u32,
    attempt_states: HashMap<u32, AttemptState>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct PartitionKey {
    task_id: String,
    partition: u32,
}

struct AttemptState {
    partition_states: HashMap<PartitionKey, PartitionState>,
    // This could also be calculated from the sum of the percentages of all partitions.
    percent: f64,
}

struct PartitionState {
    percent: f64,
}

lazy_static! {
    static ref RECEIVED_UPDATES: IntCounter = register_int_counter!(
        "ballista_circuit_breaker_controller_received_updates_total",
        "Total number of updates received by the circuit breaker"
    )
    .unwrap();
}

impl Default for CircuitBreakerController {
    fn default() -> Self {
        let job_states = RwLock::new(HashMap::new());

        Self { job_states }
    }
}

impl CircuitBreakerController {
    pub fn create(&self, job_id: &str) {
        info!(job_id, "creating circuit breaker");

        let mut job_states = self.job_states.write();

        job_states.insert(
            job_id.to_owned(),
            JobState {
                shared_states: HashMap::new(),
            },
        );
    }

    pub fn delete(&self, job_id: &str) {
        let mut job_states = self.job_states.write();
        job_states.remove(job_id);

        info!(job_id, "deleted circuit breaker",);
    }

    pub fn register_labels(
        &self,
        key: CircuitBreakerStageKey,
        executor_id: String,
        labels: Vec<String>,
    ) {
        let mut job_states = self.job_states.write();

        let job_state = match job_states.get_mut(&key.job_id) {
            Some(state) => state,
            None => {
                debug!(
                    job_id = key.job_id,
                    "received circuit breaker update for unregistered job",
                );
                return;
            }
        };

        let shared_states = &mut job_state.shared_states;

        let entry = shared_states
            .entry(key.shared_state_id.clone())
            .or_insert_with(|| SharedState::new(executor_id));

        entry.labels.extend(labels);
    }

    pub fn update(
        &self,
        key: CircuitBreakerTaskKey,
        percent: f64,
        executor_id: String,
    ) -> Result<Option<Vec<String>>, String> {
        RECEIVED_UPDATES.inc();

        let mut job_states = self.job_states.write();

        let stage_key = key.stage_key.clone();

        let job_state = match job_states.get_mut(&stage_key.job_id) {
            Some(state) => state,
            None => {
                debug!(
                    job_id = stage_key.job_id,
                    "received circuit breaker update for unregistered job",
                );
                return Ok(None);
            }
        };

        let shared_states = &mut job_state.shared_states;

        let shared_state = shared_states
            .entry(key.stage_key.shared_state_id.clone())
            .or_insert_with(|| {
                let mut executor_trip_state = HashMap::new();
                executor_trip_state.insert(executor_id.clone(), false);
                SharedState {
                    stage_states: HashMap::new(),
                    executor_trip_state,
                    percent: 0.0,
                    labels: HashSet::new(),
                }
            });

        let stage_states = &mut shared_state.stage_states;

        let stage_state =
            &mut stage_states
                .entry(stage_key.stage_id)
                .or_insert_with(|| StageState {
                    latest_attempt_num: key.stage_key.attempt_num,
                    attempt_states: HashMap::new(),
                });

        let old_latest_attempt_num = stage_state.latest_attempt_num;

        let new_latest_attempt_num =
            max(old_latest_attempt_num, key.stage_key.attempt_num);

        stage_state.latest_attempt_num = new_latest_attempt_num;

        let attempt_states = &mut stage_state.attempt_states;

        let old_latest_attempt_percent = attempt_states
            .get(&old_latest_attempt_num)
            .map(|a| a.percent)
            .unwrap_or(0.0);

        let attempt_state = attempt_states
            .entry(key.stage_key.attempt_num)
            .or_insert_with(|| AttemptState {
                partition_states: HashMap::new(),
                percent: 0.0,
            });

        shared_state
            .executor_trip_state
            .entry(executor_id.clone())
            .or_insert_with(|| false);

        let partition_states = &mut attempt_state.partition_states;

        let old_sum_percentage = shared_state.percent;

        let partition_key = PartitionKey {
            task_id: key.task_id.clone(),
            partition: key.partition,
        };

        let partition_state = partition_states
            .entry(partition_key)
            .or_insert_with(|| PartitionState { percent: 0.0 });

        attempt_state.percent += percent - partition_state.percent;

        partition_state.percent = percent;

        if key.stage_key.attempt_num == new_latest_attempt_num {
            // No matter if the latest partition changed or remained the same,
            // this should update the shared percentage correctly.
            shared_state.percent += attempt_state.percent - old_latest_attempt_percent;
        };

        let should_trip = shared_state.percent >= 1.0 && old_sum_percentage < 1.0;

        let labels = if should_trip {
            Some(shared_state.labels.iter().cloned().collect_vec())
        } else {
            None
        };

        Ok(labels)
    }

    pub fn retrieve_tripped_stages(
        &self,
        executor_id: &str,
    ) -> Vec<CircuitBreakerStageKey> {
        let results = self
            .job_states
            .write()
            .iter_mut()
            .flat_map(|(job_id, job_state)| {
                job_state.shared_states.iter_mut().flat_map(
                    |(shared_state_id, shared_state)| {
                        if let Some(tripped) =
                            shared_state.executor_trip_state.get_mut(executor_id)
                        {
                            if !*tripped && shared_state.percent >= 1.0 {
                                *tripped = true;

                                shared_state
                                    .stage_states
                                    .iter()
                                    .flat_map(|(stage_num, stage_state)| {
                                        stage_state.attempt_states.keys().map(
                                            |attempt_num| CircuitBreakerStageKey {
                                                job_id: job_id.clone(),
                                                stage_id: *stage_num,
                                                shared_state_id: shared_state_id.clone(),
                                                attempt_num: *attempt_num,
                                            },
                                        )
                                    })
                                    .collect::<Vec<_>>()
                            } else {
                                Vec::new()
                            }
                        } else {
                            Vec::new()
                        }
                    },
                )
            })
            .collect::<Vec<_>>();

        results
    }
}
