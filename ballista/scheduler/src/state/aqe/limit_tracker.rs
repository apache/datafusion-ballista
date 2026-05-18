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

//! Per-job row-count tracker that fires once a global `LIMIT` is satisfied.
//!
//! Used by the AQE early-stop feature: the scheduler-side analyzer tags
//! the producer stages feeding an eligible `GlobalLimitExec`, then this
//! tracker observes per-task row counts as `TaskStatus` updates arrive.
//! When the sum crosses `limit * safety_factor`, the tracker returns
//! `CancelRemaining` exactly once and the scheduler dispatches an
//! `EarlyStopCancel` event for the job.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Decision returned by [`JobLimitTracker::observe`].
#[derive(Debug, PartialEq, Eq)]
pub enum EarlyStopDecision {
    /// Threshold has not been crossed, or another observer already won the
    /// race to fire the trigger.
    Continue,
    /// Threshold has just been crossed by this observation. Caller must
    /// dispatch the cancellation event exactly once.
    CancelRemaining,
}

/// Tracks the running sum of rows produced by a job's tagged producer
/// stages and fires a one-shot trigger when `sum >= threshold`.
///
/// The tracker is keyed by job ID externally (in the scheduler's
/// `DashMap<String, Arc<JobLimitTracker>>`); the tracker itself does
/// not hold the ID. `Send + Sync` so it can be shared across the
/// scheduler's async tasks.
#[derive(Debug)]
pub struct JobLimitTracker {
    limit: u64,
    /// Pre-computed `limit * safety_factor`, with the floating-point
    /// multiplication done once at construction time to keep `observe`
    /// branch-free of f64 arithmetic.
    threshold: u64,
    tagged_producer_stage_ids: HashSet<usize>,
    rows_so_far: AtomicU64,
    triggered: AtomicBool,
}

impl JobLimitTracker {
    /// Construct a tracker with the given `limit` (the LIMIT's fetch
    /// count) and `safety_factor` (typically 1.5).
    ///
    /// Panics if `limit == 0` (a zero-row LIMIT should be handled by the
    /// optimizer, not the tracker) or if `safety_factor < 1.0` (we must
    /// never trigger before the limit is reached).
    pub fn new(
        limit: u64,
        safety_factor: f64,
        tagged_producer_stage_ids: HashSet<usize>,
    ) -> Self {
        assert!(limit > 0, "JobLimitTracker requires a positive limit");
        assert!(
            safety_factor >= 1.0,
            "safety_factor must be >= 1.0 to preserve the asymmetric \
             correctness invariant (fire no earlier than rows >= limit)"
        );
        let threshold = compute_threshold(limit, safety_factor);
        Self {
            limit,
            threshold,
            tagged_producer_stage_ids,
            rows_so_far: AtomicU64::new(0),
            triggered: AtomicBool::new(false),
        }
    }

    /// Record that `partition_rows` rows were produced by `stage_id`.
    ///
    /// Returns `CancelRemaining` exactly once across all calls, when the
    /// running sum first crosses `threshold`.
    ///
    /// Asymmetric correctness invariant: we must fire no earlier than
    /// `sum >= limit` (otherwise the downstream LimitExec under-reports).
    /// Firing late is always safe; it only wastes I/O.
    pub fn observe(
        &self,
        stage_id: usize,
        partition_rows: u64,
    ) -> EarlyStopDecision {
        if !self.tagged_producer_stage_ids.contains(&stage_id) {
            return EarlyStopDecision::Continue;
        }
        let new_total =
            self.rows_so_far.fetch_add(partition_rows, Ordering::Relaxed)
                + partition_rows;
        if new_total >= self.threshold
            && !self.triggered.swap(true, Ordering::SeqCst)
        {
            EarlyStopDecision::CancelRemaining
        } else {
            EarlyStopDecision::Continue
        }
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }

    pub fn threshold(&self) -> u64 {
        self.threshold
    }

    pub fn rows_so_far(&self) -> u64 {
        self.rows_so_far.load(Ordering::Relaxed)
    }

    pub fn is_triggered(&self) -> bool {
        self.triggered.load(Ordering::Relaxed)
    }

    /// The producer stage IDs whose row counts this tracker observes.
    pub fn tagged_producer_stage_ids(&self) -> &HashSet<usize> {
        &self.tagged_producer_stage_ids
    }
}

/// Compute `limit * safety_factor` rounded up to the next integer, using
/// fixed-point arithmetic to avoid f64 imprecision at large limits.
fn compute_threshold(limit: u64, safety_factor: f64) -> u64 {
    const SCALE: u128 = 1_000_000;
    let scaled = (safety_factor * SCALE as f64).round() as u128;
    let product = (limit as u128) * scaled;
    let threshold = product.div_ceil(SCALE);
    threshold.min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::thread;

    fn tracker(limit: u64, stages: &[usize]) -> JobLimitTracker {
        JobLimitTracker::new(limit, 1.5, stages.iter().copied().collect())
    }

    #[test]
    fn threshold_rounding() {
        assert_eq!(compute_threshold(10, 1.5), 15);
        assert_eq!(compute_threshold(100, 1.5), 150);
        assert_eq!(compute_threshold(1, 1.5), 2); // 1.5 rounds up
        assert_eq!(compute_threshold(1_000_000, 1.0), 1_000_000);
    }

    #[test]
    fn observe_continues_below_threshold() {
        let t = tracker(100, &[0]);
        assert_eq!(t.observe(0, 50), EarlyStopDecision::Continue);
        assert_eq!(t.observe(0, 50), EarlyStopDecision::Continue);
        assert_eq!(t.rows_so_far(), 100);
        assert!(!t.is_triggered());
    }

    #[test]
    fn observe_fires_at_threshold() {
        // limit=10, sf=1.5 -> threshold=15
        let t = tracker(10, &[0]);
        assert_eq!(t.observe(0, 10), EarlyStopDecision::Continue);
        assert_eq!(t.observe(0, 5), EarlyStopDecision::CancelRemaining);
        assert!(t.is_triggered());
    }

    #[test]
    fn observe_fires_once_then_continue() {
        let t = tracker(10, &[0]);
        assert_eq!(t.observe(0, 20), EarlyStopDecision::CancelRemaining);
        // Subsequent observations after triggering must not re-fire.
        assert_eq!(t.observe(0, 5), EarlyStopDecision::Continue);
        assert_eq!(t.observe(0, 100), EarlyStopDecision::Continue);
    }

    #[test]
    fn observe_ignores_untagged_stage() {
        let t = tracker(10, &[0]);
        assert_eq!(t.observe(42, 1_000_000), EarlyStopDecision::Continue);
        assert_eq!(t.rows_so_far(), 0);
        assert!(!t.is_triggered());
    }

    #[test]
    fn observe_aggregates_multiple_tagged_stages() {
        let t = tracker(100, &[1, 2, 3]);
        assert_eq!(t.observe(1, 50), EarlyStopDecision::Continue);
        assert_eq!(t.observe(2, 50), EarlyStopDecision::Continue);
        assert_eq!(t.observe(3, 50), EarlyStopDecision::CancelRemaining);
    }

    #[test]
    fn concurrent_observers_trigger_once() {
        let t = Arc::new(tracker(1_000, &[0]));
        let fires = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];
        for _ in 0..32 {
            let t = t.clone();
            let fires = fires.clone();
            handles.push(thread::spawn(move || {
                // Each thread adds 100 rows -> total 3200 >> threshold(1500).
                if t.observe(0, 100) == EarlyStopDecision::CancelRemaining {
                    fires.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(fires.load(Ordering::Relaxed), 1);
        assert!(t.is_triggered());
        assert_eq!(t.rows_so_far(), 3200);
    }

    #[test]
    #[should_panic(expected = "positive limit")]
    fn zero_limit_panics() {
        JobLimitTracker::new(0, 1.5, HashSet::new());
    }

    #[test]
    #[should_panic(expected = "safety_factor")]
    fn safety_factor_below_one_panics() {
        JobLimitTracker::new(10, 0.9, HashSet::new());
    }
}
