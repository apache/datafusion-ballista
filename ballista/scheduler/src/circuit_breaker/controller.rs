use std::collections::HashMap;

use ballista_core::serde::protobuf::CircuitBreakerKey;
use parking_lot::RwLock;
use tracing::{debug, info};

pub struct CircuitBreakerController {
    job_states: RwLock<HashMap<String, JobState>>,
}

struct JobState {
    stage_states: HashMap<u32, StageState>,
}

struct StageState {
    attempt_states: HashMap<u32, AttemptState>,
}

struct AttemptState {
    node_states: HashMap<String, NodeState>,
}

struct NodeState {
    partition_states: HashMap<u32, PartitionState>,
}

struct PartitionState {
    percent: f64,
}

impl Default for CircuitBreakerController {
    fn default() -> Self {
        let job_states = RwLock::new(HashMap::new());
        Self { job_states }
    }
}

impl CircuitBreakerController {
    pub fn is_tripped_for(&self, job_id: &str) -> bool {
        let job_states = self.job_states.read();

        let stage_states = match job_states.get(job_id) {
            Some(state) => &state.stage_states,
            // If the registration hasn't happened yet
            None => {
                return false;
            }
        };

        let is_tripped = stage_states.values().any(|stage_state| {
            stage_state.attempt_states.values().any(|attempt_state| {
                attempt_state.node_states.values().any(|node_state| {
                    node_state
                        .partition_states
                        .values()
                        .fold(0.0, |a, b| a + b.percent)
                        >= 1.0
                })
            })
        });

        is_tripped
    }

    pub fn create(&self, job_id: &str) {
        info!(job_id, "creating circuit breaker");

        let mut job_states = self.job_states.write();

        job_states.insert(
            job_id.to_owned(),
            JobState {
                stage_states: HashMap::new(),
            },
        );
    }

    pub fn delete(&self, job_id: &str) {
        info!(job_id, "deleting circuit breaker");
        let mut job_states = self.job_states.write();
        job_states.remove(job_id);
    }

    pub fn update(&self, key: CircuitBreakerKey, percent: f64) -> Result<bool, String> {
        let mut job_states = self.job_states.write();

        let stage_states = match job_states.get_mut(&key.job_id) {
            Some(state) => &mut state.stage_states,
            None => {
                debug!(
                    job_id = key.job_id,
                    "received circuit breaker update for unregisterd job",
                );
                return Ok(false);
            }
        };

        let partition_states = &mut stage_states
            .entry(key.stage_id)
            .or_insert_with(|| StageState {
                attempt_states: HashMap::new(),
            })
            .attempt_states
            .entry(key.attempt_num)
            .or_insert_with(|| AttemptState {
                node_states: HashMap::new(),
            })
            .node_states
            .entry(key.node_id.to_owned())
            .or_insert_with(|| NodeState {
                partition_states: HashMap::new(),
            })
            .partition_states;

        partition_states
            .entry(key.partition)
            .or_insert_with(|| PartitionState { percent })
            .percent = percent;

        let should_trip =
            partition_states.values().map(|s| s.percent).sum::<f64>() >= 1.0;

        if should_trip {
            info!(
                job_id = key.job_id,
                task_id = key.task_id,
                "sending circuit breaker signal to task"
            );
        }

        Ok(should_trip)
    }
}
