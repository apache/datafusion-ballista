use std::fmt::Debug;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Error;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::physical_plan::RecordBatchStream;
use futures::{Stream, StreamExt};
use tracing::{error, info, warn};

use super::client::{CircuitBreakerClient, CircuitBreakerKey};

pub struct CircuitBreakerStream {
    inner: Pin<Box<dyn RecordBatchStream + Send>>,
    calculate: Arc<dyn Fn(&RecordBatch) -> f64 + Sync + Send>,
    key: CircuitBreakerKey,
    percent: f64,
    is_lagging: bool,
    circuit_breaker: Arc<AtomicBool>,
    client: Arc<CircuitBreakerClient>,
}

impl CircuitBreakerStream {
    pub fn new(
        inner: Pin<Box<dyn RecordBatchStream + Send>>,
        calculate: Arc<dyn Fn(&RecordBatch) -> f64 + Sync + Send>,
        key: CircuitBreakerKey,
        client: Arc<CircuitBreakerClient>,
    ) -> Result<Self, Error> {
        let circuit_breaker = Arc::new(AtomicBool::new(false));
        client.register(key.clone(), circuit_breaker.clone())?;

        Ok(Self {
            inner,
            calculate,
            key,
            percent: 0.0,
            is_lagging: false,
            circuit_breaker,
            client,
        })
    }
}

impl Drop for CircuitBreakerStream {
    fn drop(&mut self) {
        if let Err(e) = self.client.deregister(self.key.clone()) {
            error!("Failed to deregister circuit breaker: {:?}", e);
        }
    }
}

#[derive(Debug)]
pub struct CircuitBreakerUpdate {
    pub key: CircuitBreakerKey,
    pub percent: f64,
}

impl RecordBatchStream for CircuitBreakerStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Stream for CircuitBreakerStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.circuit_breaker.load(Ordering::Acquire) {
            info!(key = ?self.key, "Stopping CircuitBreakerStream early (limit reached globally)");
            return Poll::Ready(None);
        }

        if self.percent >= 1.0 {
            warn!(key = ?self.key, "Stopping CircuitBreakerStream early (limit reached locally)");
            return Poll::Ready(None);
        }

        let poll = self.inner.poll_next_unpin(cx);

        if let Poll::Ready(Some(Ok(record_batch))) = &poll {
            let delta = (self.calculate)(record_batch);

            self.percent += delta;

            let status_update = CircuitBreakerUpdate {
                key: self.key.clone(),
                percent: self.percent,
            };

            if let Err(e) = self.client.send_update(status_update) {
                if !self.is_lagging {
                    self.is_lagging = true;
                    warn!("Stream could not send short circuit update to daemon, it might be running very fast! ({:?})", e);
                }
            }
        }

        poll
    }
}
