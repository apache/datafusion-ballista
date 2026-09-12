//! Shuffle-affinity task distribution: place each task on the executor already
//! holding most of its shuffle input, so it reads locally instead of over Flight.

use std::sync::Mutex;

mod locality;
mod policy;
mod scheduler_internals;
mod stats;

#[cfg(test)]
mod tests;

pub use policy::ShuffleAffinityPolicy;
pub use stats::{LocalityObserver, LocalityStats};

/// Locks `mutex`, recovering from poisoning; the guarded memo and counters stay valid.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}
