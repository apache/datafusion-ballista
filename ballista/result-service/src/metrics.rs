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

    /// A fetch relayed `bytes` of Flight data to its client.
    fn record_bytes(&self, bytes: u64);

    /// A fetch ended after `elapsed`.
    fn record_finished(&self, outcome: FetchOutcome, elapsed: Duration);

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

    fn record_bytes(&self, _bytes: u64) {}

    fn record_finished(&self, _outcome: FetchOutcome, _elapsed: Duration) {}

    fn gather_metrics(&self) -> Result<Option<(Vec<u8>, String)>> {
        Ok(None)
    }
}

/// The Prometheus collector with the `prometheus-metrics` feature, otherwise
/// one that records nothing.
pub fn default_metrics_collector() -> Result<Arc<dyn ResultMetricsCollector>> {
    #[cfg(feature = "prometheus-metrics")]
    {
        PrometheusMetricsCollector::current()
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
    use once_cell::sync::OnceCell;
    use prometheus::{
        Encoder, Histogram, IntCounter, IntCounterVec, IntGauge, Registry, TextEncoder,
        register_histogram_with_registry, register_int_counter_vec_with_registry,
        register_int_counter_with_registry, register_int_gauge_with_registry,
    };
    use std::sync::Arc;
    use std::time::Duration;

    static COLLECTOR: OnceCell<Arc<dyn ResultMetricsCollector>> = OnceCell::new();

    const OUTCOMES: [FetchOutcome; 3] = [
        FetchOutcome::Completed,
        FetchOutcome::Failed,
        FetchOutcome::Cancelled,
    ];

    fn outcome_label(outcome: FetchOutcome) -> &'static str {
        match outcome {
            FetchOutcome::Completed => "completed",
            FetchOutcome::Failed => "failed",
            FetchOutcome::Cancelled => "cancelled",
        }
    }

    /// A [`ResultMetricsCollector`] that exports Prometheus metrics:
    ///
    /// - `ballista_result_service_fetches_total` counts finished fetches by
    ///   `outcome`: `completed`, `failed` or `cancelled`
    /// - `ballista_result_service_fetches_in_flight` is the number of fetches
    ///   currently streaming
    /// - `ballista_result_service_fetch_duration_seconds` is a histogram of
    ///   fetch durations
    /// - `ballista_result_service_relayed_bytes_total` counts the Flight data
    ///   relayed to clients
    pub struct PrometheusMetricsCollector {
        registry: Registry,
        fetches: IntCounterVec,
        in_flight: IntGauge,
        duration: Histogram,
        relayed_bytes: IntCounter,
    }

    fn registration_error(e: prometheus::Error) -> BallistaError {
        BallistaError::Internal(format!("Error registering metric: {e:?}"))
    }

    impl PrometheusMetricsCollector {
        /// Creates the collector, registering its metrics in `registry`.
        pub fn new(registry: &Registry) -> Result<Self> {
            let fetches = register_int_counter_vec_with_registry!(
                "ballista_result_service_fetches_total",
                "Counter of finished result fetches, by outcome",
                &["outcome"],
                registry
            )
            .map_err(registration_error)?;
            // Export every outcome from the start, so a rate over each is
            // defined before its first fetch.
            for outcome in OUTCOMES {
                fetches.with_label_values(&[outcome_label(outcome)]);
            }
            Ok(Self {
                registry: registry.clone(),
                fetches,
                in_flight: register_int_gauge_with_registry!(
                    "ballista_result_service_fetches_in_flight",
                    "Number of result fetches in flight",
                    registry
                )
                .map_err(registration_error)?,
                duration: register_histogram_with_registry!(
                    "ballista_result_service_fetch_duration_seconds",
                    "Histogram of result fetch durations in seconds",
                    vec![0.01_f64, 0.1_f64, 1_f64, 10_f64, 60_f64, 300_f64],
                    registry
                )
                .map_err(registration_error)?,
                relayed_bytes: register_int_counter_with_registry!(
                    "ballista_result_service_relayed_bytes_total",
                    "Counter of Flight data bytes relayed to clients",
                    registry
                )
                .map_err(registration_error)?,
            })
        }

        /// The process's collector, registered in the default registry on first
        /// use, so that `serve` can run more than once in a process.
        pub fn current() -> Result<Arc<dyn ResultMetricsCollector>> {
            COLLECTOR
                .get_or_try_init(|| {
                    let collector = Self::new(::prometheus::default_registry())?;
                    Ok(Arc::new(collector) as Arc<dyn ResultMetricsCollector>)
                })
                .cloned()
        }
    }

    impl ResultMetricsCollector for PrometheusMetricsCollector {
        fn record_started(&self) {
            self.in_flight.inc();
        }

        fn record_bytes(&self, bytes: u64) {
            self.relayed_bytes.inc_by(bytes);
        }

        fn record_finished(&self, outcome: FetchOutcome, elapsed: Duration) {
            self.in_flight.dec();
            self.duration.observe(elapsed.as_secs_f64());
            self.fetches
                .with_label_values(&[outcome_label(outcome)])
                .inc();
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
            collector.record_bytes(1024);
            collector.record_finished(FetchOutcome::Completed, Duration::from_millis(5));

            let (text, _) = collector.gather_metrics()?.expect("prometheus exports");
            let text = String::from_utf8(text).expect("utf-8");
            for line in [
                r#"ballista_result_service_fetches_total{outcome="completed"} 1"#,
                r#"ballista_result_service_fetches_total{outcome="failed"} 0"#,
                r#"ballista_result_service_fetches_total{outcome="cancelled"} 0"#,
                "ballista_result_service_fetches_in_flight 1",
                "ballista_result_service_relayed_bytes_total 1024",
                "ballista_result_service_fetch_duration_seconds_count 1",
            ] {
                assert!(text.contains(line), "missing {line:?} in:\n{text}");
            }
            Ok(())
        }

        #[test]
        fn the_default_collector_is_registered_once_per_process() -> Result<()> {
            let first = crate::metrics::default_metrics_collector()?;
            let second = crate::metrics::default_metrics_collector()?;
            assert!(Arc::ptr_eq(&first, &second));
            Ok(())
        }
    }
}
