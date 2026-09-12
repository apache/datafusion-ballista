//! Locality measurements and the observer hook that publishes them.

/// Locality the policy achieved, summed over its life and logged per round at `debug`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LocalityStats {
    /// Tasks bound.
    pub tasks: u64,
    /// Input partitions covered by those tasks.
    pub partitions: u64,
    /// Of those, the ones bound to an executor holding some of their input.
    pub local_partitions: u64,
    /// Input bytes those tasks will read from the executor running them.
    pub local_bytes: u64,
    /// Input bytes those tasks will read in total, counting shuffle input only.
    pub total_bytes: u64,
    /// Whether any count includes a placeholder for a producer that reported no
    /// size. Sticky once set.
    pub imputed_bytes: bool,
}

impl LocalityStats {
    /// Share of shuffle bytes read locally, in `0.0..=1.0`, or zero before any are
    /// bound. Counts locations rather than bytes while [`Self::imputed_bytes`] is set.
    pub fn local_byte_ratio(&self) -> f64 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        self.local_bytes as f64 / self.total_bytes as f64
    }
}

impl std::ops::AddAssign for LocalityStats {
    fn add_assign(&mut self, other: Self) {
        self.tasks += other.tasks;
        self.partitions += other.partitions;
        self.local_partitions += other.local_partitions;
        self.local_bytes += other.local_bytes;
        self.total_bytes += other.total_bytes;
        self.imputed_bytes |= other.imputed_bytes;
    }
}

/// Receives each binding round's measurement, so an embedder can publish it to its
/// own metrics. Any `Fn(&LocalityStats)` is an observer.
pub trait LocalityObserver: Send + Sync {
    /// One binding round, reported as a delta rather than a running total.
    fn observe(&self, round: &LocalityStats);
}

impl<F: Fn(&LocalityStats) + Send + Sync> LocalityObserver for F {
    fn observe(&self, round: &LocalityStats) {
        self(round)
    }
}
