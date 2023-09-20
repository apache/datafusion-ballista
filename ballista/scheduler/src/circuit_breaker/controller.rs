use std::collections::HashMap;

use ballista_core::circuit_breaker::model::{
    CircuitBreakerStageKey, CircuitBreakerTaskKey,
};
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

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct PartitionKey {
    task_id: String,
    partition: u32,
}

struct AttemptState {
    partition_states: HashMap<PartitionKey, PartitionState>,
    executor_trip_state: HashMap<String, bool>,
    percent: f64,
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
        let mut job_states = self.job_states.write();
        job_states.remove(job_id);

        info!(job_id, "deleted circuit breaker",);
    }

    pub fn update(
        &self,
        key: CircuitBreakerTaskKey,
        percent: f64,
        executor_id: String,
    ) -> Result<bool, String> {
        let mut job_states = self.job_states.write();

        let stage_key = key.stage_key.clone();

        let job_state = match job_states.get_mut(&stage_key.job_id) {
            Some(state) => state,
            None => {
                debug!(
                    job_id = stage_key.job_id,
                    "received circuit breaker update for unregistered job",
                );
                return Ok(false);
            }
        };

        let stage_states = &mut job_state.stage_states;

        let stage_state =
            &mut stage_states
                .entry(stage_key.stage_id)
                .or_insert_with(|| StageState {
                    attempt_states: HashMap::new(),
                });

        let attempt_states = &mut stage_state.attempt_states;

        let attempt_state = attempt_states
            .entry(key.stage_key.attempt_num)
            .or_insert_with(|| {
                let mut executor_trip_state = HashMap::new();
                executor_trip_state.insert(executor_id.clone(), false);
                AttemptState {
                    partition_states: HashMap::new(),
                    executor_trip_state,
                    percent: 0.0,
                }
            });

        attempt_state
            .executor_trip_state
            .entry(executor_id.clone())
            .or_insert_with(|| false);

        let partition_states = &mut attempt_state.partition_states;

        let old_sum_percentage = attempt_state.percent;

        let partition_key = PartitionKey {
            task_id: key.task_id.clone(),
            partition: key.partition,
        };

        partition_states
            .entry(partition_key)
            .or_insert_with(|| PartitionState { percent })
            .percent = percent;

        attempt_state.percent = partition_states.values().map(|s| s.percent).sum::<f64>();

        let should_trip = attempt_state.percent >= 1.0 && old_sum_percentage < 1.0;

        Ok(should_trip)
    }

    pub fn retrieve_tripped_stages(
        &self,
        executor_id: &str,
    ) -> Vec<CircuitBreakerStageKey> {
        self.job_states
            .write()
            .iter_mut()
            .flat_map(|(job_id, job_state)| {
                job_state
                    .stage_states
                    .iter_mut()
                    .flat_map(|(stage_num, stage_state)| {
                        stage_state.attempt_states.iter_mut().flat_map(
                            |(attempt_num, attempt_state)| {
                                if let Some(tripped) =
                                    attempt_state.executor_trip_state.get_mut(executor_id)
                                {
                                    if !*tripped && attempt_state.percent >= 1.0 {
                                        *tripped = true;

                                        Some(CircuitBreakerStageKey {
                                            job_id: job_id.clone(),
                                            stage_id: *stage_num,
                                            attempt_num: *attempt_num,
                                        })
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            },
                        )
                    })
            })
            .collect::<Vec<_>>()
    }
}
