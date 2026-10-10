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

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use arrow_flight::{FlightData, Ticket};
use ballista_core::serde::scheduler::Action;
use ballista_core::serving::{BoxedFlightStream, ResultBackend};
use futures::Stream;
use tonic::Status;

use crate::metrics::{FetchOutcome, ResultMetricsCollector};

/// A [`ResultBackend`] that records each fetch of `inner` in `metrics`.
pub struct MeteredBackend<B> {
    inner: B,
    metrics: Arc<dyn ResultMetricsCollector>,
}

impl<B: ResultBackend> MeteredBackend<B> {
    pub fn new(inner: B, metrics: Arc<dyn ResultMetricsCollector>) -> Self {
        Self { inner, metrics }
    }
}

#[tonic::async_trait]
impl<B: ResultBackend> ResultBackend for MeteredBackend<B> {
    async fn fetch(
        &self,
        action: Action,
        ticket: Ticket,
    ) -> Result<BoxedFlightStream<FlightData>, Status> {
        let mut record = FetchRecord::start(self.metrics.clone());
        match self.inner.fetch(action, ticket).await {
            Ok(inner) => Ok(Box::pin(MeteredStream { inner, record })),
            Err(status) => {
                record.outcome = Some(FetchOutcome::Failed);
                Err(status)
            }
        }
    }
}

/// One fetch, recorded as finished when dropped. A fetch dropped before its
/// outcome is known was abandoned by the client.
struct FetchRecord {
    metrics: Arc<dyn ResultMetricsCollector>,
    started: Instant,
    bytes: u64,
    outcome: Option<FetchOutcome>,
}

impl FetchRecord {
    fn start(metrics: Arc<dyn ResultMetricsCollector>) -> Self {
        metrics.record_started();
        Self {
            metrics,
            started: Instant::now(),
            bytes: 0,
            outcome: None,
        }
    }
}

impl Drop for FetchRecord {
    fn drop(&mut self) {
        self.metrics.record_finished(
            self.outcome.unwrap_or(FetchOutcome::Cancelled),
            self.started.elapsed(),
            self.bytes,
        );
    }
}

struct MeteredStream {
    inner: BoxedFlightStream<FlightData>,
    record: FetchRecord,
}

impl Stream for MeteredStream {
    type Item = Result<FlightData, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let polled = self.inner.as_mut().poll_next(cx);
        let record = &mut self.record;
        match &polled {
            Poll::Ready(Some(Ok(data))) => {
                record.bytes += (data.data_header.len() + data.data_body.len()) as u64;
            }
            Poll::Ready(Some(Err(_))) => record.outcome = Some(FetchOutcome::Failed),
            Poll::Ready(None) => {
                record.outcome.get_or_insert(FetchOutcome::Completed);
            }
            Poll::Pending => {}
        }
        polled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::JobId;
    use ballista_core::error::Result as BallistaResult;
    use ballista_core::serde::scheduler::{ShuffleFileKind, ShuffleLayout};
    use futures::StreamExt;
    use std::sync::Mutex;
    use std::time::Duration;

    #[derive(Default)]
    struct RecordingCollector {
        started: Mutex<usize>,
        finished: Mutex<Vec<(FetchOutcome, u64)>>,
    }

    impl ResultMetricsCollector for RecordingCollector {
        fn record_started(&self) {
            *self.started.lock().unwrap() += 1;
        }

        fn record_finished(&self, outcome: FetchOutcome, _elapsed: Duration, bytes: u64) {
            self.finished.lock().unwrap().push((outcome, bytes));
        }

        fn gather_metrics(&self) -> BallistaResult<Option<(Vec<u8>, String)>> {
            Ok(None)
        }
    }

    /// Answers every fetch with `items`, or with `Unavailable` if `None`.
    struct FixedBackend(Option<Vec<Result<FlightData, Status>>>);

    #[tonic::async_trait]
    impl ResultBackend for FixedBackend {
        async fn fetch(
            &self,
            _action: Action,
            _ticket: Ticket,
        ) -> Result<BoxedFlightStream<FlightData>, Status> {
            match &self.0 {
                Some(items) => Ok(Box::pin(futures::stream::iter(items.clone()))),
                None => Err(Status::unavailable("executor unreachable")),
            }
        }
    }

    fn data(header: usize, body: usize) -> Result<FlightData, Status> {
        Ok(FlightData::new()
            .with_data_header(vec![0u8; header])
            .with_data_body(vec![0u8; body]))
    }

    fn metered(
        items: Option<Vec<Result<FlightData, Status>>>,
    ) -> (MeteredBackend<FixedBackend>, Arc<RecordingCollector>) {
        let collector = Arc::new(RecordingCollector::default());
        let backend = MeteredBackend::new(FixedBackend(items), collector.clone());
        (backend, collector)
    }

    async fn fetch(
        backend: &MeteredBackend<FixedBackend>,
    ) -> Result<BoxedFlightStream<FlightData>, Status> {
        let action = Action::FetchPartition {
            job_id: JobId::new("job"),
            stage_id: 1,
            partition_id: 0,
            host: "executor".to_owned(),
            port: 50051,
            file_id: None,
            layout: ShuffleLayout::default(),
            file_kind: ShuffleFileKind::default(),
            byte_ranges: vec![],
        };
        backend.fetch(action, Ticket::default()).await
    }

    fn finished(collector: &RecordingCollector) -> Vec<(FetchOutcome, u64)> {
        collector.finished.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn a_fully_read_stream_is_completed_with_its_bytes() {
        let (backend, collector) = metered(Some(vec![data(8, 100), data(8, 50)]));
        let stream = fetch(&backend).await.unwrap();
        assert_eq!(stream.collect::<Vec<_>>().await.len(), 2);

        assert_eq!(*collector.started.lock().unwrap(), 1);
        assert_eq!(finished(&collector), vec![(FetchOutcome::Completed, 166)]);
    }

    #[tokio::test]
    async fn a_stream_error_is_a_failure() {
        let (backend, collector) =
            metered(Some(vec![data(8, 100), Err(Status::internal("broken"))]));
        let stream = fetch(&backend).await.unwrap();
        drop(stream.collect::<Vec<_>>().await);

        assert_eq!(finished(&collector), vec![(FetchOutcome::Failed, 108)]);
    }

    #[tokio::test]
    async fn a_failed_fetch_is_a_failure() {
        let (backend, collector) = metered(None);
        assert!(fetch(&backend).await.is_err());

        assert_eq!(finished(&collector), vec![(FetchOutcome::Failed, 0)]);
    }

    #[tokio::test]
    async fn a_stream_dropped_before_its_end_is_cancelled() {
        let (backend, collector) = metered(Some(vec![data(8, 100), data(8, 50)]));
        let mut stream = fetch(&backend).await.unwrap();
        assert!(stream.next().await.is_some());
        assert!(finished(&collector).is_empty(), "still in flight");
        drop(stream);

        assert_eq!(finished(&collector), vec![(FetchOutcome::Cancelled, 108)]);
    }
}
