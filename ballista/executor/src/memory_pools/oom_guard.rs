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

//! Real-usage accounting for the executor's global allocator.
//!
//! [`AccountingAllocator`] wraps the real global allocator and tracks the bytes it
//! hands out in a process-global balance. It performs **no enforcement**: unwinding
//! out of a `GlobalAlloc` is undefined behaviour, so the allocator only ever updates
//! a counter. Callers enforce at safe points by calling [`check_budget`] -- the
//! executor does so on every `poll_next` of a running stage, and on every memory-pool
//! growth.
//!
//! # The enforced quantity is *live allocator bytes*, not RSS
//!
//! Enforcement reads a counter of currently-live allocated bytes: every `alloc` adds
//! its layout size, every `dealloc` subtracts it. That is the only quantity that
//! failing a task can actually move. Resident set size (RSS) cannot: an allocator such
//! as mimalloc returns freed pages to the OS lazily (`MADV_FREE`), so RSS stays high
//! for a while after memory is logically free.
//!
//! Gating on RSS would make the guard **latch**: a query trips the limit, DataFusion
//! spills and frees several GB, but RSS has not moved, so every subsequent batch of
//! every task on the executor keeps failing -- and because those failures are
//! retriable, the scheduler re-lands the same tasks on the same executor, where they
//! die again. The guard's own successful spill would take the executor out. So RSS is
//! **advisory only** ([`observe_rss`], which logs and never writes the balance), and
//! the enforced counter decrements the instant memory is freed, which means the guard
//! un-trips within microseconds of a spill and can never latch.
//!
//! # The breaker debounces; the cooperative gate does not
//!
//! [`check_budget`] is the *circuit breaker*, and it only trips after the process has
//! been over budget for several consecutive checks **and** for a minimum wall-clock
//! interval -- see [`Breaker`]. `RealUsagePool::try_grow`, the *cooperative gate*, has
//! no such debounce and rejects immediately. That asymmetry is deliberate and is the
//! whole ordering of the two layers: the gate fires first and cheaply (DataFusion just
//! spills), the breaker is a last resort that fails tasks, so it must give the spill
//! time to land before it starts killing every task on the executor.

use datafusion::common::{DataFusionError, resources_datafusion_err};
use log::warn;
use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicU64, AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

// The `shard()` thread-local is read from inside `dealloc`, including during thread
// teardown. That is only sound because the key is `const`-initialized and drop-free, so
// std selects its eager, `#[thread_local]`-backed storage, which neither allocates nor
// panics. On a target *without* the `target_thread_local` feature, std falls back to
// lazy OS-key storage that allocates on first access -- from inside `alloc()`, that is
// unbounded re-entrancy into the global allocator.
//
// `cfg(target_thread_local)` is a nightly-only cfg (feature `cfg_target_thread_local`),
// so it cannot be tested on stable; on stable it would simply evaluate to false. Rather
// than invent a fragile probe, this is an allowlist of the targets Ballista ships on,
// all of which have `target_thread_local`. The build fails closed on anything else,
// which is the point: better a compile error than a silent re-entrancy bug.
#[cfg(not(all(
    any(target_os = "linux", target_os = "macos", target_os = "windows"),
    any(target_arch = "x86_64", target_arch = "aarch64")
)))]
compile_error!(
    "the `oom-guard` feature is only supported on linux/macos/windows with x86_64 or \
     aarch64. It requires std's eager `#[thread_local]` TLS storage (the target's \
     `target_thread_local` feature), because the tracking allocator reads a thread-local \
     from inside `dealloc`; on a target without it, std's lazy TLS allocates on first \
     access and re-enters the global allocator. `cfg(target_thread_local)` is unstable, \
     so this allowlist is the enforcement."
);

/// Number of counter shards. Threads are spread across these so that concurrent
/// allocations rarely contend on the same cache line.
///
/// Together with the one relaxed `fetch_add` per allocation in [`track`], this is the
/// knob to revisit if allocator overhead shows up in benchmarks. The count trades
/// contention (fewer shards) against the cost of summing them in [`current_balance`]
/// (more shards); the sum is only taken at enforcement points, not on the alloc path.
const SHARD_COUNT: usize = 64;

/// One shard of the balance, padded to its own cache line so that two threads writing
/// to different shards never false-share. 128 bytes covers the 64-byte lines of x86-64
/// and the 128-byte prefetch pairing of aarch64.
#[repr(align(128))]
struct Shard(AtomicIsize);

/// Process-wide outstanding bytes, sharded. The true balance is the sum of all shards.
///
/// Signed, because a thread may free memory another thread allocated: any individual
/// shard can go arbitrarily negative even though the sum cannot (much) -- and the sum
/// itself may dip below zero transiently while an `alloc`'s `fetch_add` on one shard is
/// in flight against a `dealloc`'s on another.
static SHARDS: [Shard; SHARD_COUNT] = [const { Shard(AtomicIsize::new(0)) }; SHARD_COUNT];

/// Hands out shard indices to threads, round-robin, on their first allocation.
static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);

/// Enforcement limit in bytes; 0 means unset (never gates).
static LIMIT: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// This thread's shard index, assigned on first use. `usize::MAX` means unassigned.
    ///
    /// A `const`-initialized `Cell` with no drop glue, so `LocalKey::with` cannot panic
    /// during thread-local teardown -- which matters, because this is reached from
    /// `dealloc` at thread exit. A thread-local that *did* carry a destructor would
    /// force std's lazy TLS storage, which allocates -- from inside the global
    /// allocator. That guarantee relies on std choosing its eager,
    /// `#[thread_local]`-backed storage for this key, which requires the
    /// `target_thread_local` feature of the target. That is not a universal guarantee of
    /// the language, so it is not left to documentation: the `compile_error!` at the top
    /// of this module refuses to build the `oom-guard` feature on any target outside the
    /// allowlist of ones known to have it.
    ///
    /// Because there is no per-thread residue -- every delta lands in a shard
    /// immediately -- a thread exiting leaves the balance exactly correct. That is what
    /// makes the counter exact, and what removes the need for any periodic correction.
    static SHARD_INDEX: Cell<usize> = const { Cell::new(usize::MAX) };
}

/// Rate-limits a log line to at most one emission per interval.
///
/// Uses [`Instant`], which is monotonic, rather than [`std::time::SystemTime`]: a
/// backwards wall-clock step (NTP correction, manual clock change) would otherwise
/// suppress the warning for the duration of the step, and a forward step would let an
/// extra line through immediately. `Instant` is not `const`-constructible, so the last
/// emission is held behind a `Mutex` rather than an atomic. That is fine here -- this is
/// reached only from the guard's trip path (`check_budget`, `observe_rss`), never from
/// `GlobalAlloc` -- but it does mean this type must never be used on the alloc/dealloc
/// path itself.
struct RateLimit {
    /// The instant of the last emission; `None` means "never yet".
    last_emit: Mutex<Option<Instant>>,
}

impl RateLimit {
    const fn new() -> Self {
        Self {
            last_emit: Mutex::new(None),
        }
    }

    /// Whether a log line may be emitted now. Two threads racing here may both emit
    /// within one window; that is harmless for an advisory warning.
    ///
    /// Never panics: a poisoned mutex (only possible if an earlier caller panicked
    /// while holding it, which nothing here does) is recovered from rather than
    /// propagated.
    fn allow(&self, min_interval: Duration) -> bool {
        let now = Instant::now();
        let mut last_emit = self.last_emit.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(last) = *last_emit
            && now.saturating_duration_since(last) < min_interval
        {
            return false;
        }
        *last_emit = Some(now);
        true
    }
}

/// How often the guard may log that it is over budget. [`check_budget`] runs once per
/// batch on every task, so an unthrottled log here would flood.
const TRIP_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// How often the advisory RSS-divergence warning may be logged.
const RSS_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Throttles the "over budget" warning emitted when the guard trips.
static TRIP_LOG: RateLimit = RateLimit::new();

/// Throttles the advisory "RSS diverges from tracked usage" warning.
static RSS_LOG: RateLimit = RateLimit::new();

/// Set once the "this platform does not report RSS" warning has been logged; that fact
/// never changes at runtime, so it is worth saying exactly once.
static RSS_UNAVAILABLE_LOGGED: AtomicBool = AtomicBool::new(false);

/// How many *consecutive* over-budget observations the breaker requires before it trips.
///
/// The trade-off. Too low (1, the original behaviour) and a single hungry task that
/// briefly overshoots takes down every other task on the executor with it: the
/// cooperative gate in `RealUsagePool` is at that same instant telling the hungry task
/// to spill, which will drop the balance within microseconds, but tasks B..H poll in
/// the meantime, see the same over-budget balance, and all fail. One greedy task
/// becomes an executor-wide stall. Too high, and a genuinely runaway allocation gets
/// more time to reach the OS OOM killer, which is the outcome the breaker exists to
/// prevent -- and the breaker is the *only* defence against memory the pool never sees.
/// Three is enough to ride out a spill without materially delaying a real trip.
const MIN_CONSECUTIVE_OVER_BUDGET_CHECKS: usize = 3;

/// How long the process must have been *continuously* over budget before the breaker
/// trips.
///
/// The count above is not sufficient on its own, because batches arrive at wildly
/// different rates: a stage emitting tiny batches burns three checks in microseconds --
/// far less time than a spill needs to complete -- so the count alone would reproduce
/// the very fan-out it is meant to prevent. Conversely time alone is not sufficient
/// either: a stage whose batches take seconds would let a single slow observation, taken
/// long ago, trip the breaker on its next poll with no evidence the condition persisted.
/// Requiring *both* means "over budget on every check we took, over a real interval",
/// which is the condition a completed spill would have cleared.
///
/// 100 ms is comfortably longer than a spill takes to start freeing memory, and short
/// enough that a true runaway is still caught long before the process is killed.
const MIN_TIME_OVER_BUDGET: Duration = Duration::from_millis(100);

/// The process-global breaker state.
///
/// In the crate's unit-test build the thresholds are relaxed to "trip on the first
/// over-budget check", so that the tests of *other* properties (`guard_exec`'s per-batch
/// checking, for instance) do not have to sleep. The debounce itself is not tested
/// through this static: [`Breaker`] takes its thresholds and its clock as arguments, and
/// the tests below drive a local instance with explicit values. The production thresholds
/// are exercised end to end in `tests/oom_guard_alloc.rs`, which links this library
/// without `cfg(test)`.
static BREAKER: Breaker = Breaker::new(
    if cfg!(test) {
        1
    } else {
        MIN_CONSECUTIVE_OVER_BUDGET_CHECKS
    },
    if cfg!(test) {
        Duration::ZERO
    } else {
        MIN_TIME_OVER_BUDGET
    },
);

/// The instant the process started tracking, used as the epoch for [`now_nanos`].
///
/// [`Instant`] is monotonic (unlike `SystemTime`, which an NTP correction can step
/// backwards, and which would then either suppress a trip indefinitely or trip it
/// early). It is not `const`-constructible, hence the `LazyLock`; initialising it does
/// not allocate, and nothing on this path is reachable from `GlobalAlloc` anyway.
static PROCESS_START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Monotonic nanoseconds since [`PROCESS_START`], never zero (zero is [`Breaker`]'s
/// "not currently over budget" sentinel). Allocation-free and panic-free.
fn now_nanos() -> u64 {
    let elapsed = PROCESS_START.elapsed().as_nanos();
    u64::try_from(elapsed).unwrap_or(u64::MAX).saturating_add(1)
}

/// Hysteresis for the circuit breaker: it trips only once the process has been over
/// budget for [`Breaker::min_consecutive`] checks in a row **and** for at least
/// [`Breaker::min_duration`].
///
/// The single most important property here is the **reset**: the moment any check
/// anywhere in the process observes the balance back under the limit, the streak is
/// discarded. A successful spill therefore un-arms the breaker, which is exactly the
/// hand-off between the two layers -- the cooperative gate spills, the balance drops,
/// and the breaker never fires.
///
/// The counter is process-global and shared by every task's stream: three checks may
/// come from three different threads. That is intended. The question the breaker asks is
/// "has the *process* stayed over budget", not "has this one task seen it three times".
struct Breaker {
    /// Consecutive over-budget observations required to trip.
    min_consecutive: usize,
    /// Minimum continuous time over budget required to trip.
    min_duration: Duration,
    /// Length of the current over-budget streak; reset to 0 by any under-budget check.
    consecutive: AtomicUsize,
    /// [`now_nanos`] of the first observation in the current streak; 0 means "no streak".
    over_since_nanos: AtomicU64,
}

impl Breaker {
    const fn new(min_consecutive: usize, min_duration: Duration) -> Self {
        Self {
            min_consecutive,
            min_duration,
            consecutive: AtomicUsize::new(0),
            over_since_nanos: AtomicU64::new(0),
        }
    }

    /// Feed one observation in; returns whether the breaker should trip *now*.
    ///
    /// Allocation-free: a handful of relaxed atomic operations, called once per batch.
    fn observe(&self, over_budget: bool, now_nanos: u64) -> bool {
        if !over_budget {
            // The spill worked (or the load simply passed). Forget the streak entirely.
            self.reset();
            return false;
        }

        let count = self
            .consecutive
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);

        // Stamp the start of the streak if this observation opened it. Racing threads
        // both see a start; whichever lands first wins, and the loser reads it back.
        let started = match self.over_since_nanos.compare_exchange(
            0,
            now_nanos,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => now_nanos,
            Err(existing) => existing,
        };
        let elapsed = u128::from(now_nanos.saturating_sub(started));

        count >= self.min_consecutive && elapsed >= self.min_duration.as_nanos()
    }

    /// Discard any in-progress streak.
    fn reset(&self) {
        self.consecutive.store(0, Ordering::Relaxed);
        self.over_since_nanos.store(0, Ordering::Relaxed);
    }

    /// The current streak length (test observability).
    #[cfg(test)]
    fn streak(&self) -> usize {
        self.consecutive.load(Ordering::Relaxed)
    }
}

/// Set the enforcement limit in bytes. `0` disables enforcement (tracking continues).
///
/// `pub` (rather than `pub(crate)`) because the `oom_guard_alloc` integration test binary
/// arms the guard directly.
pub fn arm(limit_bytes: usize) {
    LIMIT.store(limit_bytes, Ordering::Relaxed);
    // A new limit invalidates any streak accumulated against the old one.
    BREAKER.reset();
}

/// This thread's shard, assigned round-robin on first use and then stable.
///
/// Never allocates and never panics: the thread-local is a `const`-initialized, drop-free
/// `Cell`, and the index is masked into range.
#[inline]
fn shard() -> &'static AtomicIsize {
    let index = SHARD_INDEX.with(|cell| {
        let existing = cell.get();
        if existing != usize::MAX {
            return existing;
        }
        let assigned = NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % SHARD_COUNT;
        cell.set(assigned);
        assigned
    });
    &SHARDS[index % SHARD_COUNT].0
}

/// The exact signed balance: the sum of every shard.
fn raw_balance() -> isize {
    SHARDS.iter().fold(0isize, |acc, shard| {
        acc.wrapping_add(shard.0.load(Ordering::Relaxed))
    })
}

/// Current process-wide tracked usage in bytes (never reported negative).
///
/// This is the sum of the shards: bytes handed out by the global allocator and not yet
/// returned to it. It is *exact* -- every `alloc`/`dealloc` lands in a shard
/// immediately, so no thread carries un-flushed residue and no thread's exit leaves any
/// behind. Reading it is `SHARD_COUNT` relaxed loads, taken only at enforcement points.
///
/// Note what this is *not*: it is not RSS. It counts requested `Layout` bytes, so it
/// excludes allocator metadata and slack, and it drops the moment memory is freed even
/// though the pages may stay resident. Enforcing on it is deliberate -- see the module
/// docs -- because it is the quantity that failing or spilling a task can actually move.
///
/// `pub` (rather than `pub(crate)`) because `RealUsagePool::new` installs it as the
/// cooperative gate's balance source and the `oom_guard_alloc` integration test binary
/// reads it directly.
pub fn current_balance() -> usize {
    raw_balance().max(0) as usize
}

/// Fail with a retriable `ResourcesExhausted` if tracked live allocator usage has been
/// over the limit for long enough to trip the breaker. Callers invoke this at safe
/// points -- never from inside the allocator.
///
/// Two things keep this from taking out the executor when a single task overshoots:
///
/// - **Hysteresis.** One over-budget observation is not enough; see [`Breaker`]. The
///   cooperative gate (`RealUsagePool::try_grow`) is rejecting growth from the very first
///   over-budget byte, so by the time the breaker's threshold is met, a spill has had its
///   chance and failed to bring usage down.
/// - **No latching.** The balance decrements on every `dealloc`, so a spill that frees
///   memory both resets the streak and puts the next check back under the limit.
///
/// `pub` (rather than `pub(crate)`) because the `oom_guard_alloc` integration test binary
/// drives it directly.
pub fn check_budget() -> Result<(), DataFusionError> {
    let balance = raw_balance();
    let limit = LIMIT.load(Ordering::Relaxed);

    // Allocation-free on the hot path: relaxed atomic loads, one monotonic clock read,
    // and nothing allocated unless the breaker actually trips (the error string).
    if !BREAKER.observe(over_budget(balance, limit), now_nanos()) {
        return Ok(());
    }

    // Logged here, where the trip is *decided*, and rate-limited: `MemoryGuardExec` calls
    // this once per batch on every running task, so an unthrottled line would flood the
    // log exactly when the executor is under stress.
    if TRIP_LOG.allow(TRIP_LOG_INTERVAL) {
        warn!(
            "Ballista OOM guard is over budget: tracked live allocator usage {} bytes \
             exceeds the limit of {limit} bytes, and has done so continuously for long \
             enough that spilling has not rescued it. Tasks are being failed with a \
             retriable ResourcesExhausted. (Rate-limited to one line per {}s.)",
            balance.max(0),
            TRIP_LOG_INTERVAL.as_secs()
        );
    }
    Err(budget_error(balance, limit))
}

/// Record an advisory RSS sample; **never** writes the enforced balance.
///
/// `rss_bytes` is `None` when the platform cannot report process memory. The executor
/// calls this from a periodic ticker purely for observability: it is what lets an
/// operator staring at a storm of `ResourcesExhausted` tell "this query genuinely needs
/// more memory" (RSS tracks the balance) from "the allocator is hoarding freed pages"
/// (RSS stays high while the balance has dropped).
pub(crate) fn observe_rss(rss_bytes: Option<usize>) {
    let Some(rss) = rss_bytes else {
        if !RSS_UNAVAILABLE_LOGGED.swap(true, Ordering::Relaxed) {
            warn!(
                "Ballista OOM guard: this platform does not report process memory \
                 (memory_stats() returned None), so the advisory RSS check is disabled. \
                 Enforcement is unaffected -- it reads the allocator's tracked balance."
            );
        }
        return;
    };
    let tracked = current_balance();
    let limit = LIMIT.load(Ordering::Relaxed);
    if rss_diverges(rss, tracked, limit) && RSS_LOG.allow(RSS_LOG_INTERVAL) {
        warn!(
            "Ballista OOM guard: process RSS ({rss} bytes) has climbed above the \
             configured limit ({limit} bytes) even though tracked live allocator usage \
             ({tracked} bytes) has not. The allocator is most likely holding on to freed \
             pages (mimalloc releases them lazily) or otherwise reserving memory the \
             guard cannot see -- this is real memory that enforcement cannot reclaim by \
             failing tasks, because it gates on the tracked value, not RSS. \
             (Rate-limited to one line per {}s.)",
            RSS_LOG_INTERVAL.as_secs()
        );
    }
}

/// Only meaningful once a limit is set: the guard enforces on `tracked`, so the
/// dangerous state is RSS above the limit while `tracked` is below it -- real memory
/// the guard cannot see, and cannot reclaim by failing tasks. Comparing RSS to
/// `tracked` alone would warn on every healthy executor, because RSS legitimately
/// includes the binary's text and data, thread stacks, and allocator reserves, none
/// of which the tracking allocator ever sees.
///
/// Also deliberately silent when *both* `rss` and `tracked` are over `limit`: in that
/// case [`check_budget`] is already tripping on `tracked` and logging its own warning,
/// so a second line here would be redundant noise rather than new information.
fn rss_diverges(rss: usize, tracked: usize, limit: usize) -> bool {
    limit != 0 && rss > limit && tracked <= limit
}

/// The error a tripped breaker raises, as a pure function of `balance` and `limit`.
///
/// Split out from [`check_budget`] so that the exact variant -- `ResourcesExhausted`, on
/// which the whole retriability path in `ballista/core/src/error.rs` hinges -- can be
/// unit-tested directly with arbitrary inputs, rather than only through the
/// process-global atomics (which would require the tracking allocator to actually be
/// installed).
fn budget_error(balance: isize, limit: usize) -> DataFusionError {
    resources_datafusion_err!(
        "Ballista OOM guard: the executor's tracked native memory usage is {} bytes, \
         over the limit of {limit} bytes; failing this task",
        balance.max(0)
    )
}

/// Whether `balance` exceeds `limit`. `limit == 0` means unset, and a negative
/// balance never trips.
///
/// This is the *instantaneous* condition. It is not on its own sufficient to fail a
/// task -- [`Breaker`] debounces it -- but it is exactly what the cooperative gate acts
/// on with no debounce at all.
fn over_budget(balance: isize, limit: usize) -> bool {
    limit != 0 && balance > limit.try_into().unwrap_or(isize::MAX)
}

/// Record a change of `delta` bytes. Never panics, never allocates, never enforces.
///
/// One relaxed, usually-uncontended `fetch_add` per allocation. `fetch_add` on an
/// `AtomicIsize` wraps on overflow rather than panicking, which is what a `GlobalAlloc`
/// path requires.
#[inline]
fn track(delta: isize) {
    shard().fetch_add(delta, Ordering::Relaxed);
}

/// Wraps an inner global allocator, tracking the layout bytes it hands out.
///
/// Tracking only: this never enforces a limit and never unwinds. See [`check_budget`].
pub struct AccountingAllocator<A: GlobalAlloc> {
    inner: A,
}

impl<A: GlobalAlloc> AccountingAllocator<A> {
    /// Wrap `inner`, tracking every allocation it serves.
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

unsafe impl<A: GlobalAlloc> GlobalAlloc for AccountingAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc(layout) };
        if !ptr.is_null() {
            track(layout.size() as isize);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { self.inner.dealloc(ptr, layout) };
        track(-(layout.size() as isize));
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc_zeroed(layout) };
        if !ptr.is_null() {
            track(layout.size() as isize);
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { self.inner.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            // Only account for a realloc that actually happened. The casts and the
            // subtraction cannot overflow: a single allocation cannot exceed
            // isize::MAX on any real platform.
            track(new_size as isize - layout.size() as isize);
        }
        new_ptr
    }
}

/// Test-only helpers for driving the process-global limit and balance safely.
///
/// `LIMIT` and the shard array are process-global, so every test in the crate's unit test
/// binary that touches them -- in this module or in any other -- must serialize
/// against the *same* lock and restore what it changed. Hence a single shared guard
/// here rather than a private mutex per module.
#[cfg(test)]
pub(crate) mod test_support {
    use super::{BREAKER, LIMIT, Ordering, SHARDS, raw_balance};
    use crate::memory_pools::GLOBAL_STATE_LOCK;
    use std::sync::MutexGuard;

    /// Set the tracked balance directly, for tests that need a deterministic value
    /// without routing gigabytes through a real allocator.
    ///
    /// The production path has no such setter: the balance moves *only* via the
    /// allocator's deltas, which is precisely what stops the guard from latching. This
    /// collapses the whole balance into shard 0, which is equivalent for every reader
    /// (they all sum the shards).
    pub(crate) fn set_balance_for_test(bytes: isize) {
        for shard in SHARDS.iter() {
            shard.0.store(0, Ordering::Relaxed);
        }
        SHARDS[0].0.store(bytes, Ordering::Relaxed);
    }

    /// Holds [`GLOBAL_STATE_LOCK`] and restores [`LIMIT`], the balance, and the breaker's
    /// streak on drop -- including on an early return from a failed assertion. Without
    /// this, a panic partway through a test that armed the guard would leave the global
    /// limit set, the balance raised, and a stale over-budget streak in place for every
    /// test that runs afterwards in the same binary.
    pub(crate) struct ArmedGuard {
        #[allow(dead_code)]
        lock: MutexGuard<'static, ()>,
        balance: isize,
    }

    impl ArmedGuard {
        /// Acquire the shared lock, recovering from a poisoning left by an earlier panic.
        pub(crate) fn acquire() -> Self {
            let lock = GLOBAL_STATE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            BREAKER.reset();
            Self {
                lock,
                balance: raw_balance(),
            }
        }
    }

    impl Drop for ArmedGuard {
        fn drop(&mut self) {
            LIMIT.store(0, Ordering::Relaxed);
            set_balance_for_test(self.balance);
            BREAKER.reset();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pools::oom_guard::test_support::{
        ArmedGuard, set_balance_for_test,
    };

    #[test]
    fn over_budget_is_true_only_when_a_limit_is_set_and_exceeded() {
        assert!(!over_budget(100, 0), "an unset limit never trips");
        assert!(!over_budget(100, 200), "under the limit");
        assert!(
            !over_budget(200, 200),
            "at the limit: strictly greater is required"
        );
        assert!(over_budget(201, 200), "over the limit");
        assert!(!over_budget(-5, 200), "a negative balance never trips");
    }

    #[test]
    fn check_budget_is_ok_unless_a_limit_is_exceeded() {
        let _g = ArmedGuard::acquire();

        // An unset limit never gates, whatever the balance.
        arm(0);
        assert!(check_budget().is_ok(), "an unset limit must never gate");

        // Nor does a limit far above any plausible balance.
        arm(usize::MAX);
        assert!(check_budget().is_ok(), "a huge limit must not gate");
    }

    #[test]
    fn budget_error_is_a_resources_exhausted() {
        // The task-failure retriability logic in `ballista/core/src/error.rs` depends on
        // this exact variant, so assert the variant, not merely that some error came back.
        let err = budget_error(201, 200);
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "must be ResourcesExhausted, got: {err:?}"
        );
    }

    /// A local breaker with the production thresholds, driven by an explicit clock: no
    /// process globals, no sleeping, no dependence on how fast the test host runs.
    fn breaker() -> Breaker {
        Breaker::new(MIN_CONSECUTIVE_OVER_BUDGET_CHECKS, MIN_TIME_OVER_BUDGET)
    }

    /// Nanoseconds, `ms` milliseconds after the (arbitrary) start of a test's timeline.
    fn at_ms(ms: u64) -> u64 {
        ms * 1_000_000 + 1 // +1: `now_nanos` never returns 0, the "no streak" sentinel.
    }

    #[test]
    fn the_breaker_never_trips_while_under_budget() {
        let breaker = breaker();
        for ms in 0..1_000 {
            assert!(
                !breaker.observe(false, at_ms(ms)),
                "an under-budget process must never trip the breaker"
            );
        }
        assert_eq!(
            breaker.streak(),
            0,
            "no over-budget observation was made: the streak must stay at zero"
        );
    }

    #[test]
    fn the_breaker_does_not_trip_before_the_consecutive_threshold() {
        let breaker = breaker();
        // Fewer than MIN_CONSECUTIVE_OVER_BUDGET_CHECKS observations, spread over far
        // more than MIN_TIME_OVER_BUDGET: the time condition alone must not be enough.
        for i in 0..MIN_CONSECUTIVE_OVER_BUDGET_CHECKS - 1 {
            let now = at_ms(1_000 * i as u64);
            assert!(
                !breaker.observe(true, now),
                "trip at observation {i}: fewer than {MIN_CONSECUTIVE_OVER_BUDGET_CHECKS} \
                 consecutive checks must not trip the breaker, however long ago the first \
                 one was -- this is what stops one hungry task failing every other task \
                 on the executor while its own spill is still in flight"
            );
        }
    }

    #[test]
    fn the_breaker_does_not_trip_before_the_time_threshold() {
        let breaker = breaker();
        // Plenty of consecutive checks, but all inside MIN_TIME_OVER_BUDGET: a stage
        // emitting tiny batches burns the count in microseconds, which is far less time
        // than the cooperative gate's spill needs to land.
        for i in 0..50 {
            assert!(
                !breaker.observe(true, at_ms(0) + i * 1_000),
                "50 checks within a microsecond must not trip a 100ms debounce"
            );
        }
        // Once the interval has genuinely elapsed, the same streak trips.
        assert!(
            breaker.observe(true, at_ms(MIN_TIME_OVER_BUDGET.as_millis() as u64)),
            "over budget for the full interval, over many checks: the breaker must trip"
        );
    }

    #[test]
    fn the_breaker_trips_once_both_thresholds_are_met() {
        let breaker = breaker();
        let mut tripped = None;
        for i in 0..MIN_CONSECUTIVE_OVER_BUDGET_CHECKS {
            // One observation per 100 ms, so the time condition is met exactly when the
            // count condition is.
            let now = at_ms(MIN_TIME_OVER_BUDGET.as_millis() as u64 * i as u64);
            if breaker.observe(true, now) {
                tripped = Some(i);
                break;
            }
        }
        assert_eq!(
            tripped,
            Some(MIN_CONSECUTIVE_OVER_BUDGET_CHECKS - 1),
            "the breaker must trip on exactly the {MIN_CONSECUTIVE_OVER_BUDGET_CHECKS}th \
             consecutive over-budget check, no earlier and no later"
        );
    }

    /// The property the whole fix rests on: **a successful spill un-arms the breaker.**
    ///
    /// The cooperative gate rejects a grow, the consumer spills, memory is freed, and the
    /// very next check sees the balance back under the limit. That single under-budget
    /// observation must wipe the streak -- otherwise the breaker would keep counting
    /// across the spill and fail tasks for an over-budget condition that no longer holds.
    #[test]
    fn a_single_under_budget_observation_resets_the_streak() {
        let breaker = breaker();

        // Build a streak that is one observation short of tripping, over a long interval
        // so that the *time* condition is already satisfied.
        for i in 0..MIN_CONSECUTIVE_OVER_BUDGET_CHECKS - 1 {
            assert!(!breaker.observe(true, at_ms(100 * i as u64)));
        }
        assert_eq!(breaker.streak(), MIN_CONSECUTIVE_OVER_BUDGET_CHECKS - 1);

        // The spill lands: one check sees the process back under budget.
        assert!(!breaker.observe(false, at_ms(1_000)));
        assert_eq!(
            breaker.streak(),
            0,
            "an under-budget observation must reset the streak to zero"
        );

        // Usage climbs again. The streak restarts from scratch -- and, critically, so does
        // the clock, so the checks that made up the *old* streak cannot carry the new one
        // over the line.
        for i in 0..MIN_CONSECUTIVE_OVER_BUDGET_CHECKS - 1 {
            assert!(
                !breaker.observe(true, at_ms(2_000 + 100 * i as u64)),
                "the streak restarted: the breaker must not trip early on the strength of \
                 observations taken before the spill"
            );
        }
        // And only now, having been over budget for a full fresh streak, does it trip.
        assert!(
            breaker.observe(
                true,
                at_ms(2_000 + 100 * MIN_CONSECUTIVE_OVER_BUDGET_CHECKS as u64)
            ),
            "a genuinely sustained over-budget condition must still trip the breaker"
        );
    }

    #[test]
    fn a_tripped_breaker_keeps_tripping_until_it_sees_under_budget() {
        let breaker = breaker();
        for i in 0..MIN_CONSECUTIVE_OVER_BUDGET_CHECKS {
            breaker.observe(true, at_ms(100 * i as u64));
        }
        // Still over budget, so every subsequent check must keep failing its task: the
        // breaker is not a one-shot.
        for i in 0..10 {
            assert!(
                breaker.observe(true, at_ms(1_000 + i)),
                "while the process remains over budget the breaker must keep tripping"
            );
        }
        // ...and stops the instant it does not.
        assert!(!breaker.observe(false, at_ms(2_000)));
        assert!(
            !breaker.observe(true, at_ms(2_001)),
            "after a reset, a lone over-budget check must not trip"
        );
    }

    #[test]
    fn the_balance_is_the_sum_of_every_shard() {
        let _g = ArmedGuard::acquire();
        set_balance_for_test(0);

        // Deliberately write to shards other than 0: `current_balance` must sum them all,
        // not read a single counter.
        SHARDS[1].0.store(1_000, Ordering::Relaxed);
        SHARDS[SHARD_COUNT - 1].0.store(2_000, Ordering::Relaxed);
        assert_eq!(current_balance(), 3_000);

        // A negative shard cancels a positive one (a thread freeing memory another
        // thread allocated), and the reported balance is clamped at zero.
        SHARDS[1].0.store(-5_000, Ordering::Relaxed);
        assert_eq!(raw_balance(), -3_000);
        assert_eq!(current_balance(), 0, "a negative balance reports as zero");
    }

    /// The whole point of the fix: the guard must **un-trip by itself** once memory is
    /// freed, with no periodic resync, no RSS sample, and no tick of any kind.
    ///
    /// A guard that enforced on a sticky ground-truth measurement (process RSS, which an
    /// allocator like mimalloc keeps resident long after a `free`) would stay tripped
    /// here and keep failing every batch of every task on the executor -- a total outage
    /// triggered by its own successful spill. This test would fail against that design.
    #[test]
    fn a_tripped_guard_un_trips_as_soon_as_memory_is_freed() {
        let _g = ArmedGuard::acquire();
        set_balance_for_test(0);

        let allocator = AccountingAllocator::new(std::alloc::System);
        let layout = Layout::from_size_align(4 * 1024 * 1024, 8).unwrap();

        // A 1 MiB limit, and a task that allocates 4 MiB: over budget.
        arm(1024 * 1024);
        assert!(
            check_budget().is_ok(),
            "nothing allocated yet: under budget"
        );

        let ptr = unsafe { allocator.alloc(layout) };
        assert!(!ptr.is_null());
        let err = check_budget().expect_err("4 MiB against a 1 MiB limit must trip");
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "must be ResourcesExhausted so the task failure is retriable, got: {err}"
        );

        // The task spills: the memory is handed back to the allocator. No resync, no
        // ticker, no RSS sample -- the very next check must pass.
        unsafe { allocator.dealloc(ptr, layout) };
        assert!(
            check_budget().is_ok(),
            "freeing the memory must un-trip the guard immediately, with no resync: \
             enforcing on a quantity that a spill cannot move would latch the guard and \
             fail every task on this executor"
        );
        assert_eq!(
            current_balance(),
            0,
            "the freed bytes are gone from the balance"
        );
    }

    /// The sharded counter is exact across threads: N threads each allocate and free
    /// through the accounting allocator, and once they have all exited the balance is
    /// back exactly where it started.
    ///
    /// This is the test the old per-thread-drift design could not pass: it flushed into
    /// the shared balance only every 64 KiB and never flushed on thread exit, so every
    /// thread that died left up to +-64 KiB baked in permanently.
    #[test]
    fn the_balance_returns_to_zero_after_many_threads_alloc_and_free() {
        let _g = ArmedGuard::acquire();
        set_balance_for_test(0);

        const THREADS: usize = 16;
        const ROUNDS: usize = 8;
        // Three differently-sized buffers, churned in a rolling fashion: each is freed
        // only after the next has been allocated. Every byte is handed back, but the
        // *running* total wanders up and down instead of marching to a tidy zero.
        //
        // That is what makes this a discriminating test. A design that batched deltas in
        // a thread-local cell and only flushed past a threshold would leave each thread
        // holding an unflushed tail at exit -- here, 8_000 bytes a thread, 128_000 in
        // total -- baked into the shared balance forever, and growing without bound as
        // `spawn_blocking` threads churn. Exact per-allocation accounting has no tail.
        const A: usize = 96_000;
        const B: usize = 32_000;
        const C: usize = 8_000;

        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| {
                    let allocator = AccountingAllocator::new(std::alloc::System);
                    let a = Layout::from_size_align(A, 8).unwrap();
                    let b = Layout::from_size_align(B, 8).unwrap();
                    let c = Layout::from_size_align(C, 8).unwrap();
                    for _ in 0..ROUNDS {
                        let pa = unsafe { allocator.alloc(a) };
                        let pb = unsafe { allocator.alloc(b) };
                        assert!(!pa.is_null() && !pb.is_null());
                        unsafe { allocator.dealloc(pa, a) };
                        let pc = unsafe { allocator.alloc(c) };
                        assert!(!pc.is_null());
                        unsafe { allocator.dealloc(pb, b) };
                        unsafe { allocator.dealloc(pc, c) };
                    }
                });
            }
        });

        assert_eq!(
            raw_balance(),
            0,
            "every allocation was freed, and every thread has exited: an exact counter \
             must be back at zero, with no per-thread residue left behind"
        );
    }

    /// Threads that exit while still holding memory must leave the balance holding
    /// exactly those bytes -- no more, no less.
    #[test]
    fn a_thread_that_exits_leaves_no_residue_of_its_own() {
        let _g = ArmedGuard::acquire();
        set_balance_for_test(0);

        let allocator = AccountingAllocator::new(std::alloc::System);
        let layout = Layout::from_size_align(8 * 1024, 8).unwrap();

        // Allocate on a thread that then exits; free on this one.
        let ptr = std::thread::scope(|scope| {
            scope
                .spawn(|| unsafe { allocator.alloc(layout) } as usize)
                .join()
                .unwrap()
        });
        assert_ne!(ptr, 0);
        assert_eq!(
            raw_balance(),
            8 * 1024,
            "the exited thread's live allocation must be accounted exactly"
        );

        unsafe { allocator.dealloc(ptr as *mut u8, layout) };
        assert_eq!(
            raw_balance(),
            0,
            "freeing from a different thread than the one that allocated must balance out"
        );
    }

    #[test]
    fn rss_diverges_never_warns_with_no_limit_set() {
        // A healthy idle executor: no limit armed, RSS dwarfing a small tracked
        // balance because of the binary's own text/data, thread stacks, and allocator
        // reserves -- none of which `tracked` ever sees. This must never warn.
        let small_tracked = 20 * 1024 * 1024;
        let idle_rss = 250 * 1024 * 1024;
        assert!(
            !rss_diverges(idle_rss, small_tracked, 0),
            "an unset limit must never warn, however large the RSS/tracked gap"
        );
    }

    #[test]
    fn rss_diverges_never_warns_when_both_are_under_the_limit() {
        let limit = 1024 * 1024 * 1024;
        // Same healthy-idle-executor shape as above, but now with a limit armed: RSS
        // is nowhere near the limit, so there is nothing to warn about.
        assert!(!rss_diverges(250 * 1024 * 1024, 20 * 1024 * 1024, limit));
        // RSS closer to (but still under) the limit, tracked far under it: still fine.
        assert!(!rss_diverges(limit - 1, 1024, limit));
    }

    #[test]
    fn rss_diverges_warns_when_rss_exceeds_the_limit_but_tracked_does_not() {
        let limit = 1024 * 1024 * 1024;
        // This is the hoarding signal the check exists to catch: the guard enforces on
        // `tracked`, which is under the limit, so it sees nothing wrong -- but RSS has
        // climbed past the limit anyway, meaning real memory the guard cannot reclaim
        // by failing tasks.
        assert!(rss_diverges(limit + 1, limit, limit));
        assert!(rss_diverges(2 * limit, limit / 2, limit));
    }

    #[test]
    fn rss_diverges_does_not_warn_when_both_rss_and_tracked_are_over_the_limit() {
        let limit = 1024 * 1024 * 1024;
        // Here `check_budget` is already tripping on `tracked` and logging its own
        // warning; a second line here would be redundant, not new information.
        assert!(!rss_diverges(2 * limit, limit + 1, limit));
    }

    #[test]
    fn observe_rss_never_moves_the_enforced_balance() {
        let _g = ArmedGuard::acquire();
        set_balance_for_test(1024);
        arm(4096);

        // A wildly high RSS sample -- the exact situation that used to re-raise the
        // balance and latch the guard -- must be advisory only.
        observe_rss(Some(64 * 1024 * 1024 * 1024));
        assert_eq!(
            current_balance(),
            1024,
            "an RSS sample must never write the enforced balance"
        );
        assert!(
            check_budget().is_ok(),
            "an RSS sample must never be able to trip the guard"
        );

        // The unavailable-platform path must also be inert.
        observe_rss(None);
        assert_eq!(current_balance(), 1024);
    }

    #[test]
    fn rate_limit_allows_the_first_event_then_throttles() {
        let limiter = RateLimit::new();
        assert!(
            limiter.allow(Duration::from_secs(3600)),
            "the first event is always allowed"
        );
        assert!(
            !limiter.allow(Duration::from_secs(3600)),
            "a second event inside the window is suppressed"
        );
        // A zero-length window never suppresses.
        let unthrottled = RateLimit::new();
        assert!(unthrottled.allow(Duration::from_secs(0)));
        assert!(unthrottled.allow(Duration::from_secs(0)));
    }

    /// Drives `alloc` -> `realloc` (grow) -> `realloc` (shrink) -> `dealloc` directly
    /// through a standalone `AccountingAllocator` (not the globally-installed one), so
    /// the exact signed delta of every step can be asserted without racing the rest of
    /// the test binary's allocator traffic through an intervening `Vec`/`Box`.
    #[test]
    fn accounting_allocator_tracks_alloc_realloc_dealloc_deltas() {
        let _g = ArmedGuard::acquire();

        let allocator = AccountingAllocator::new(std::alloc::System);
        let initial_size = 1024 * 1024; // 1 MiB
        let grown_size = 3 * 1024 * 1024; // 3 MiB
        let shrunk_size = 256 * 1024; // 256 KiB

        let initial_layout = Layout::from_size_align(initial_size, 8).unwrap();
        let before = raw_balance();
        let ptr = unsafe { allocator.alloc(initial_layout) };
        assert!(!ptr.is_null());
        assert_eq!(
            raw_balance() - before,
            initial_size as isize,
            "alloc must track exactly the requested size"
        );

        // Grow.
        let after_alloc = raw_balance();
        let ptr = unsafe { allocator.realloc(ptr, initial_layout, grown_size) };
        assert!(!ptr.is_null());
        assert_eq!(
            raw_balance() - after_alloc,
            (grown_size - initial_size) as isize,
            "a growing realloc must track the positive delta"
        );

        // Shrink.
        let grown_layout = Layout::from_size_align(grown_size, 8).unwrap();
        let after_grow = raw_balance();
        let ptr = unsafe { allocator.realloc(ptr, grown_layout, shrunk_size) };
        assert!(!ptr.is_null());
        assert_eq!(
            raw_balance() - after_grow,
            shrunk_size as isize - grown_size as isize,
            "a shrinking realloc must track the negative delta"
        );

        // Dealloc, returning to the starting balance.
        let shrunk_layout = Layout::from_size_align(shrunk_size, 8).unwrap();
        unsafe { allocator.dealloc(ptr, shrunk_layout) };
        assert_eq!(
            raw_balance(),
            before,
            "dealloc must return the balance to its starting value"
        );
    }

    /// A stub inner allocator that always fails, for exercising the null-return paths
    /// of `alloc` and `realloc` -- the one place a wrong `if !ptr.is_null()` produces a
    /// permanent counter ratchet (tracking bytes for memory that was never handed out).
    struct AlwaysNull;

    unsafe impl GlobalAlloc for AlwaysNull {
        unsafe fn alloc(&self, _layout: Layout) -> *mut u8 {
            std::ptr::null_mut()
        }

        unsafe fn dealloc(&self, _ptr: *mut u8, _layout: Layout) {
            unreachable!("nothing should ever hold a pointer from this allocator");
        }

        unsafe fn realloc(
            &self,
            _ptr: *mut u8,
            _layout: Layout,
            _new_size: usize,
        ) -> *mut u8 {
            std::ptr::null_mut()
        }
    }

    #[test]
    fn a_failed_alloc_or_realloc_tracks_nothing() {
        let _g = ArmedGuard::acquire();

        let allocator = AccountingAllocator::new(AlwaysNull);
        let layout = Layout::from_size_align(4 * 1024 * 1024, 8).unwrap();
        let before = raw_balance();

        let ptr = unsafe { allocator.alloc(layout) };
        assert!(ptr.is_null());
        assert_eq!(
            raw_balance(),
            before,
            "a failed alloc must not move the balance"
        );

        let ptr =
            unsafe { allocator.realloc(std::ptr::null_mut(), layout, layout.size() * 2) };
        assert!(ptr.is_null());
        assert_eq!(
            raw_balance(),
            before,
            "a failed realloc must not move the balance -- this is the ratchet regression test"
        );
    }
}
