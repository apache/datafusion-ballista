use crate::serde::protobuf;

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct CircuitBreakerStageKey {
    pub job_id: String,
    pub stage_id: u32,
    pub attempt_num: u32,
}

impl From<CircuitBreakerStageKey> for protobuf::CircuitBreakerStageKey {
    fn from(val: CircuitBreakerStageKey) -> Self {
        protobuf::CircuitBreakerStageKey {
            job_id: val.job_id,
            stage_id: val.stage_id,
            attempt_num: val.attempt_num,
        }
    }
}

impl From<protobuf::CircuitBreakerStageKey> for CircuitBreakerStageKey {
    fn from(key: protobuf::CircuitBreakerStageKey) -> Self {
        Self {
            job_id: key.job_id,
            stage_id: key.stage_id,
            attempt_num: key.attempt_num,
        }
    }
}

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct CircuitBreakerTaskKey {
    pub stage_key: CircuitBreakerStageKey,
    pub partition: u32,
    pub task_id: String,
}

impl From<CircuitBreakerTaskKey> for protobuf::CircuitBreakerTaskKey {
    fn from(val: CircuitBreakerTaskKey) -> Self {
        protobuf::CircuitBreakerTaskKey {
            stage_key: Some(val.stage_key.into()),
            partition: val.partition,
            task_id: val.task_id,
        }
    }
}

impl TryFrom<protobuf::CircuitBreakerTaskKey> for CircuitBreakerTaskKey {
    type Error = String;
    fn try_from(key: protobuf::CircuitBreakerTaskKey) -> Result<Self, String> {
        Ok(Self {
            stage_key: key
                .stage_key
                .ok_or_else(|| {
                    "Circuit breaker task key contains no stage key".to_owned()
                })?
                .into(),
            partition: key.partition,
            task_id: key.task_id,
        })
    }
}
