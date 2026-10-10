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

//! Metrics for result fetches.

use std::sync::Arc;
use std::time::Duration;

use ballista_core::error::Result;

/// How a result fetch ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FetchOutcome {
    /// The whole stream was delivered.
    Completed,
    /// The executor or the stream returned an error.
    Failed,
    /// The client went away before the stream ended.
    Cancelled,
}

/// Records result fetches.
pub trait ResultMetricsCollector: Send + Sync {
    /// A fetch started.
    fn record_started(&self);

    /// A fetch ended after `elapsed`, having relayed `bytes` of Flight data.
    fn record_finished(&self, outcome: FetchOutcome, elapsed: Duration, bytes: u64);

    /// The metrics and their content type, or `None` if this collector exports
    /// none.
    fn gather_metrics(&self) -> Result<Option<(Vec<u8>, String)>>;
}

/// A collector that records nothing.
#[cfg(any(test, not(feature = "prometheus-metrics")))]
#[derive(Default)]
pub struct NoopMetricsCollector;

#[cfg(any(test, not(feature = "prometheus-metrics")))]
impl ResultMetricsCollector for NoopMetricsCollector {
    fn record_started(&self) {}

    fn record_finished(&self, _outcome: FetchOutcome, _elapsed: Duration, _bytes: u64) {}

    fn gather_metrics(&self) -> Result<Option<(Vec<u8>, String)>> {
        Ok(None)
    }
}

/// The Prometheus collector with the `prometheus-metrics` feature, otherwise
/// one that records nothing.
pub fn default_metrics_collector() -> Result<Arc<dyn ResultMetricsCollector>> {
    #[cfg(feature = "prometheus-metrics")]
    {
        Ok(Arc::new(PrometheusMetricsCollector::new(
            ::prometheus::default_registry(),
        )?))
    }
    #[cfg(not(feature = "prometheus-metrics"))]
    {
        Ok(Arc::new(NoopMetricsCollector))
    }
}

#[cfg(feature = "prometheus-metrics")]
pub use prometheus_metrics::PrometheusMetricsCollector;

#[cfg(feature = "prometheus-metrics")]
mod prometheus_metrics {
    use super::{FetchOutcome, ResultMetricsCollector};
    use ballista_core::error::{BallistaError, Result};
    use prometheus::{
        Counter, Encoder, Gauge, Histogram, Registry, TextEncoder,
        register_counter_with_registry, register_gauge_with_registry,
        register_histogram_with_registry,
    };
    use std::time::Duration;

    /// A [`ResultMetricsCollector`] that exports Prometheus metrics:
    ///
    /// - `fetch_started_total`, `fetch_completed_total`, `fetch_failed_total`
    ///   and `fetch_cancelled_total` count fetches by how they ended
    /// - `fetch_in_flight` is the number of fetches currently streaming
    /// - `fetch_time_seconds` is a histogram of fetch durations
    /// - `fetch_bytes_total` counts the Flight data relayed to clients
    pub struct PrometheusMetricsCollector {
        registry: Registry,
        started: Counter,
        completed: Counter,
        failed: Counter,
        cancelled: Counter,
        in_flight: Gauge,
        time: Histogram,
        bytes: Counter,
    }

    fn registration_error(e: prometheus::Error) -> BallistaError {
        BallistaError::Internal(format!("Error registering metric: {e:?}"))
    }

    impl PrometheusMetricsCollector {
        /// Creates the collector, registering its metrics in `registry`.
        pub fn new(registry: &Registry) -> Result<Self> {
            let counter = |name: &str, help: &str| {
                register_counter_with_registry!(name, help, registry)
                    .map_err(registration_error)
            };
            Ok(Self {
                registry: registry.clone(),
                started: counter(
                    "fetch_started_total",
                    "Counter of result fetches started",
                )?,
                completed: counter(
                    "fetch_completed_total",
                    "Counter of result fetches that delivered the whole stream",
                )?,
                failed: counter(
                    "fetch_failed_total",
                    "Counter of failed result fetches",
                )?,
                cancelled: counter(
                    "fetch_cancelled_total",
                    "Counter of result fetches the client abandoned",
                )?,
                in_flight: register_gauge_with_registry!(
                    "fetch_in_flight",
                    "Number of result fetches in flight",
                    registry
                )
                .map_err(registration_error)?,
                time: register_histogram_with_registry!(
                    "fetch_time_seconds",
                    "Histogram of result fetch time in seconds",
                    vec![0.01_f64, 0.1_f64, 1_f64, 10_f64, 60_f64, 300_f64],
                    registry
                )
                .map_err(registration_error)?,
                bytes: counter(
                    "fetch_bytes_total",
                    "Counter of Flight data bytes relayed to clients",
                )?,
            })
        }
    }

    impl ResultMetricsCollector for PrometheusMetricsCollector {
        fn record_started(&self) {
            self.started.inc();
            self.in_flight.inc();
        }

        fn record_finished(&self, outcome: FetchOutcome, elapsed: Duration, bytes: u64) {
            self.in_flight.dec();
            self.time.observe(elapsed.as_secs_f64());
            self.bytes.inc_by(bytes as f64);
            match outcome {
                FetchOutcome::Completed => self.completed.inc(),
                FetchOutcome::Failed => self.failed.inc(),
                FetchOutcome::Cancelled => self.cancelled.inc(),
            }
        }

        fn gather_metrics(&self) -> Result<Option<(Vec<u8>, String)>> {
            let encoder = TextEncoder::new();
            let mut buffer = vec![];
            encoder
                .encode(&self.registry.gather(), &mut buffer)
                .map_err(|e| {
                    BallistaError::Internal(format!(
                        "Error encoding prometheus metrics: {e:?}"
                    ))
                })?;
            Ok(Some((buffer, encoder.format_type().to_owned())))
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn finished_fetches_are_counted_by_outcome() -> Result<()> {
            let collector = PrometheusMetricsCollector::new(&Registry::new())?;
            collector.record_started();
            collector.record_started();
            collector.record_finished(
                FetchOutcome::Completed,
                Duration::from_millis(5),
                1024,
            );

            let (text, _) = collector.gather_metrics()?.expect("prometheus exports");
            let text = String::from_utf8(text).expect("utf-8");
            for line in [
                "fetch_started_total 2",
                "fetch_completed_total 1",
                "fetch_failed_total 0",
                "fetch_in_flight 1",
                "fetch_bytes_total 1024",
                "fetch_time_seconds_count 1",
            ] {
                assert!(text.contains(line), "missing {line:?} in:\n{text}");
            }
            Ok(())
        }
    }
}
