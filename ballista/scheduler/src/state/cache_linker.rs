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

//! Links the jobs behind a `DataFrame::cache()` query without blocking.
//!
//! A query reading a cache that is not built yet runs as a build (materialize)
//! job per missing cache plus the client's read job, which cannot be planned
//! until its builds finish. The read job is registered as queued, its id
//! returned at once, and it is held here until the jobs it waits on finish.
//!
//! [`CacheLinker`](crate::state::cache_linker::CacheLinker) is the bookkeeping;
//! the [`SchedulerState`] methods below drive it.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use ballista_core::error::{BallistaError, Result};
use ballista_core::event_loop::EventSender;
use ballista_core::extension::BallistaCacheNode;
use ballista_core::serde::protobuf::{FailedJob, JobStatus, job_status};
use ballista_core::{JobId, JobStatusSubscriber};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use futures::future::BoxFuture;
use log::{debug, error, warn};
use parking_lot::Mutex;

use crate::cache::{
    CACHE_DATA_LOST, OnMiss, ResolvedRead, all_locations_live, cache_node,
    resolve_cache_reads, shape_locations,
};
use crate::scheduler_server::event::{Admission, QueryStageSchedulerEvent, SubmitPlan};
use crate::scheduler_server::timestamp_millis;
use crate::state::SchedulerState;
use crate::state::cache_registry::{BeginOutcome, CacheKey, CompleteOutcome};

/// What a held job is for.
#[derive(Debug, Clone)]
pub(crate) enum PendingKind {
    /// The client's job, reading the caches.
    Read,
    /// Builds one cache, owning its registry claim on `key`.
    Materialize { key: CacheKey },
}

/// A job held until the build jobs it reads have finished.
pub(crate) struct PendingJob {
    pub(crate) job_id: JobId,
    pub(crate) job_name: String,
    pub(crate) ctx: Arc<SessionContext>,
    /// Still holds its `BallistaCacheNode`s; they become reads on release.
    pub(crate) plan: LogicalPlan,
    pub(crate) kind: PendingKind,
    /// Build jobs not finished yet.
    pub(crate) waiting_on: HashSet<JobId>,
    pub(crate) queued_at: u64,
    pub(crate) subscriber: Option<JobStatusSubscriber>,
    /// Times a cache it reads went stale while held; bounded like stage retries.
    pub(crate) relinks: usize,
    /// The submission's admission, so a scheduler shutting down waits for it.
    pub(crate) admission: Admission,
}

/// How a job ended, for [`SchedulerState::on_job_terminal`].
#[derive(Debug, Clone, Copy)]
pub(crate) enum JobEnd<'a> {
    Finished,
    Failed(&'a str),
    Cancelled,
}

/// A cache node's state when a submission is prepared.
enum CacheClaim {
    /// Built on live executors: read it.
    Hit,
    /// Claimed by this submission, for this build job.
    Claimed(JobId),
    /// Being built by another submission's job: wait on it.
    InFlight(JobId),
}

/// A concurrent submission can change a key between lookup and claim.
const CLAIM_ATTEMPTS: usize = 4;

/// What waits on a build job.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Waiter {
    /// A held job.
    Held(JobId),
    /// A running job's lost cache read, by external stage id.
    Input(JobId, usize),
}

/// What a build job's end releases, or fails.
#[derive(Default)]
pub(crate) struct Released {
    /// Held jobs.
    pub(crate) jobs: Vec<PendingJob>,
    /// Running jobs' lost reads, by external stage id.
    pub(crate) inputs: Vec<(JobId, usize)>,
}

/// A running job's cache reads, kept to rebuild a cache whose data it loses.
#[derive(Clone)]
pub(crate) struct JobReads {
    pub(crate) ctx: Arc<SessionContext>,
    pub(crate) queued_at: u64,
    /// The submission's admission, for rebuilds; dropped when the job ends.
    pub(crate) admission: Admission,
    pub(crate) reads: Vec<ResolvedRead>,
}

impl JobReads {
    fn read(&self, input: usize) -> Option<&ResolvedRead> {
        self.reads.iter().find(|read| read.input_id == input)
    }
}

#[derive(Default)]
struct Inner {
    /// Build jobs claimed and not yet ended, with their cache. A job missing
    /// here is done, or never was a build job.
    inflight: HashMap<JobId, CacheKey>,
    pending: HashMap<JobId, PendingJob>,
    /// Build job -> what waits on it.
    dependents: HashMap<JobId, HashSet<Waiter>>,
    /// Running job -> the caches it reads.
    reads: HashMap<JobId, JobReads>,
}

impl Inner {
    /// Whether `job` still runs and reads `input`.
    fn still_reads(&self, job: &JobId, input: usize) -> bool {
        self.reads
            .get(job)
            .is_some_and(|job_reads| job_reads.read(input).is_some())
    }
}

/// Tracks build jobs in flight, the jobs held for them, and running jobs' cache
/// reads. One lock guards it all; no operation awaits while holding it.
#[derive(Default)]
pub(crate) struct CacheLinker {
    inner: Mutex<Inner>,
}

impl CacheLinker {
    /// Marks `job` as building `key`. Called before the registry claim, so
    /// whoever sees the claim also finds the job in flight.
    pub(crate) fn track(&self, job: &JobId, key: CacheKey) {
        self.inner.lock().inflight.insert(job.clone(), key);
    }

    /// The cache `job` builds, if it is in flight.
    pub(crate) fn key_of(&self, job: &JobId) -> Option<CacheKey> {
        self.inner.lock().inflight.get(job).cloned()
    }

    /// Undoes [`track`](Self::track) for a job that did not get its claim.
    pub(crate) fn untrack(&self, job: &JobId) {
        self.inner.lock().inflight.remove(job);
    }

    /// Holds `jobs` until the build jobs they wait on finish, ignoring builds
    /// already finished. Returns the jobs with nothing to wait for.
    pub(crate) fn register(&self, jobs: Vec<PendingJob>) -> Vec<PendingJob> {
        let mut inner = self.inner.lock();
        let mut ready = Vec::new();
        for mut job in jobs {
            job.waiting_on.retain(|m| inner.inflight.contains_key(m));
            if job.waiting_on.is_empty() {
                ready.push(job);
                continue;
            }
            for m in &job.waiting_on {
                inner
                    .dependents
                    .entry(m.clone())
                    .or_default()
                    .insert(Waiter::Held(job.job_id.clone()));
            }
            inner.pending.insert(job.job_id.clone(), job);
        }
        ready
    }

    /// Makes `job`'s lost `input` wait for build job `on`. False if `on` has
    /// already ended.
    pub(crate) fn wait_for_input(&self, job: &JobId, input: usize, on: &JobId) -> bool {
        let mut inner = self.inner.lock();
        if !inner.inflight.contains_key(on) {
            return false;
        }
        inner
            .dependents
            .entry(on.clone())
            .or_default()
            .insert(Waiter::Input(job.clone(), input));
        true
    }

    /// `finished` built its cache: returns the held jobs now ready, and the
    /// lost reads it rebuilt.
    pub(crate) fn complete(&self, finished: &JobId) -> Released {
        let mut inner = self.inner.lock();
        inner.inflight.remove(finished);
        let mut released = Released::default();
        for waiter in inner.dependents.remove(finished).unwrap_or_default() {
            match waiter {
                Waiter::Held(id) => {
                    let done = inner.pending.get_mut(&id).map(|job| {
                        job.waiting_on.remove(finished);
                        job.waiting_on.is_empty()
                    });
                    if done == Some(true)
                        && let Some(job) = inner.pending.remove(&id)
                    {
                        released.jobs.push(job);
                    }
                }
                Waiter::Input(job, input) if inner.still_reads(&job, input) => {
                    released.inputs.push((job, input))
                }
                Waiter::Input(..) => {}
            }
        }
        released
    }

    /// `failed` will not build its cache: removes and returns every held job
    /// depending on it, directly or through another held build, and the lost
    /// reads waiting on any of them.
    pub(crate) fn fail(&self, failed: &JobId) -> Released {
        let mut inner = self.inner.lock();
        inner.inflight.remove(failed);
        let mut released = Released::default();
        let mut frontier = vec![failed.clone()];
        while let Some(id) = frontier.pop() {
            for waiter in inner.dependents.remove(&id).unwrap_or_default() {
                match waiter {
                    Waiter::Held(dependent) => {
                        let Some(job) = inner.pending.remove(&dependent) else {
                            continue;
                        };
                        if matches!(job.kind, PendingKind::Materialize { .. }) {
                            inner.inflight.remove(&dependent);
                            frontier.push(dependent);
                        }
                        released.jobs.push(job);
                    }
                    Waiter::Input(job, input) if inner.still_reads(&job, input) => {
                        released.inputs.push((job, input))
                    }
                    Waiter::Input(..) => {}
                }
            }
        }
        released
    }

    /// Removes a held job the client cancelled.
    pub(crate) fn cancel(&self, job: &JobId) -> Option<PendingJob> {
        self.inner.lock().pending.remove(job)
    }

    /// Records the cache reads of a job being submitted.
    pub(crate) fn record_reads(&self, job: &JobId, reads: JobReads) {
        if !reads.reads.is_empty() {
            self.inner.lock().reads.insert(job.clone(), reads);
        }
    }

    /// The cache reads of `job`, if it still runs.
    pub(crate) fn reads_of(&self, job: &JobId) -> Option<JobReads> {
        self.inner.lock().reads.get(job).cloned()
    }

    /// `job`'s `input` now reads the build made by `materializing_job`.
    pub(crate) fn input_rebuilt(
        &self,
        job: &JobId,
        input: usize,
        materializing_job: JobId,
    ) {
        if let Some(read) = self.inner.lock().reads.get_mut(job).and_then(|job_reads| {
            job_reads.reads.iter_mut().find(|r| r.input_id == input)
        }) {
            read.materializing_job = materializing_job;
        }
    }

    /// `job` ended: returns its cache reads, and whether it is still a build in
    /// flight, held, or waited on.
    pub(crate) fn job_ended(&self, job: &JobId) -> (Vec<ResolvedRead>, bool) {
        let mut inner = self.inner.lock();
        let reads = inner
            .reads
            .remove(job)
            .map(|job_reads| job_reads.reads)
            .unwrap_or_default();
        let linked = inner.inflight.contains_key(job)
            || inner.pending.contains_key(job)
            || inner.dependents.contains_key(job);
        (reads, linked)
    }
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerState<T, U> {
    /// Submits a query holding `DataFrame::cache()` nodes without waiting: claims
    /// a build job per missing cache, holds the read job (queued) until they
    /// finish, and returns its id.
    ///
    /// Runs on its own task: the client's request can be dropped at any await,
    /// and the claims taken here must still be submitted or given back.
    pub(crate) async fn submit_cache_plan(
        self: &Arc<Self>,
        sender: &EventSender<QueryStageSchedulerEvent>,
        job_name: &str,
        ctx: Arc<SessionContext>,
        plan: &LogicalPlan,
        subscriber: Option<JobStatusSubscriber>,
        admission: Admission,
    ) -> Result<JobId> {
        let state = Arc::clone(self);
        let sender = sender.clone();
        let job_name = job_name.to_owned();
        let plan = plan.clone();
        tokio::spawn(async move {
            state
                .claim_and_submit(&sender, &job_name, ctx, &plan, subscriber, admission)
                .await
        })
        .await
        .map_err(|e| BallistaError::Internal(format!("cache submission failed: {e}")))?
    }

    async fn claim_and_submit(
        self: &Arc<Self>,
        sender: &EventSender<QueryStageSchedulerEvent>,
        job_name: &str,
        ctx: Arc<SessionContext>,
        plan: &LogicalPlan,
        subscriber: Option<JobStatusSubscriber>,
        admission: Admission,
    ) -> Result<JobId> {
        // Named before anything is claimed, so a stall here holds no claim.
        let job_id: JobId = self.job_id_generator.next_id().await.into();
        let mut jobs = Vec::new();
        let alive_executors = self.executor_manager.get_alive_executors();
        let waiting_on = match self
            .prepare_caches(&ctx, plan, &alive_executors, &admission, &mut jobs)
            .await
        {
            Ok(waiting_on) => waiting_on,
            Err(e) => {
                self.roll_back_claims(&jobs, &e.to_string(), sender).await;
                return Err(e);
            }
        };

        let queued_at = timestamp_millis();
        if let Err(e) = self.task_manager.queue_job(&job_id, job_name, queued_at) {
            self.roll_back_claims(&jobs, &e.to_string(), sender).await;
            return Err(e);
        }
        jobs.push(PendingJob {
            job_id: job_id.clone(),
            job_name: job_name.to_owned(),
            ctx,
            plan: plan.clone(),
            kind: PendingKind::Read,
            waiting_on,
            queued_at,
            subscriber,
            relinks: 0,
            admission,
        });

        // Just prepared, so whatever need not wait is submitted as is.
        for job in self.cache_linker.register(jobs) {
            self.submit_released(job, sender).await;
        }
        Ok(job_id)
    }

    /// Claims a build job for each cache `plan` needs that is not built, without
    /// running or waiting on anything, and returns the jobs `plan` waits for.
    ///
    /// Pre-order, so a hit hides its subplan. A miss's subplan is walked for the
    /// caches its build job waits on. Claimed jobs are appended to `out` before
    /// recursing, so the caller can roll them back after a failure part-way.
    fn prepare_caches<'a>(
        &'a self,
        ctx: &'a Arc<SessionContext>,
        plan: &'a LogicalPlan,
        alive_executors: &'a HashSet<String>,
        admission: &'a Admission,
        out: &'a mut Vec<PendingJob>,
    ) -> BoxFuture<'a, Result<HashSet<JobId>>> {
        Box::pin(async move {
            let Some(node) = cache_node(plan) else {
                let mut deps = HashSet::new();
                for input in plan.inputs() {
                    deps.extend(
                        self.prepare_caches(ctx, input, alive_executors, admission, out)
                            .await?,
                    );
                }
                return Ok(deps);
            };
            let key = CacheKey::new(node.session_id(), node.cache_id());
            let job_id = match self.claim_or_hit(node, &key, alive_executors).await? {
                CacheClaim::Hit => return Ok(HashSet::new()),
                CacheClaim::InFlight(job_id) => return Ok(HashSet::from([job_id])),
                CacheClaim::Claimed(job_id) => job_id,
            };
            let input = node.inputs()[0];
            let idx = out.len();
            out.push(PendingJob {
                job_id: job_id.clone(),
                job_name: format!("cache {}", node.cache_id()),
                ctx: ctx.clone(),
                plan: input.clone(),
                kind: PendingKind::Materialize { key },
                waiting_on: HashSet::new(),
                queued_at: timestamp_millis(),
                subscriber: None,
                relinks: 0,
                admission: admission.clone(),
            });
            out[idx].waiting_on = self
                .prepare_caches(ctx, input, alive_executors, admission, out)
                .await?;
            Ok(HashSet::from([job_id]))
        })
    }

    /// Looks `node` up and, on a miss, claims a build job for it. A hit on a
    /// dead executor is dropped first (validate-on-read).
    async fn claim_or_hit(
        &self,
        node: &BallistaCacheNode,
        key: &CacheKey,
        alive_executors: &HashSet<String>,
    ) -> Result<CacheClaim> {
        for _ in 0..CLAIM_ATTEMPTS {
            if let Some(cache) = self.cache_state.lookup(key).await? {
                if all_locations_live(&cache.locations, alive_executors) {
                    return Ok(CacheClaim::Hit);
                }
                // Drop only the build seen here; another submission may have
                // replaced it already.
                self.invalidate_build(key, &cache.materializing_job).await?;
            }

            if let Some(in_flight) = self.cache_state.pending_job(key).await? {
                return Ok(CacheClaim::InFlight(in_flight));
            }

            // Tracked before claiming, so whoever sees the claim finds the job.
            let job_id: JobId = self.job_id_generator.next_id().await.into();
            self.cache_linker.track(&job_id, key.clone());
            let outcome = self
                .cache_state
                .begin_materialization(key.clone(), job_id.clone())
                .await;
            match outcome {
                Ok(BeginOutcome::Claimed) => return Ok(CacheClaim::Claimed(job_id)),
                // Changed by another submission since: look again.
                Ok(BeginOutcome::AlreadyPending | BeginOutcome::AlreadyMaterialized) => {
                    self.cache_linker.untrack(&job_id)
                }
                Err(e) => {
                    self.cache_linker.untrack(&job_id);
                    return Err(e);
                }
            }
        }
        Err(BallistaError::Internal(format!(
            "cache {} kept changing state while it was being claimed",
            node.cache_id()
        )))
    }

    /// Submits a released job, without waiting. A cache that went stale while
    /// it was held is claimed again and the job held for it, up to
    /// `stage_max_failures` times.
    pub(crate) fn dispatch_cache_job<'a>(
        self: &'a Arc<Self>,
        mut job: PendingJob,
        sender: &'a EventSender<QueryStageSchedulerEvent>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let mut claims = Vec::new();
            let alive_executors = self.executor_manager.get_alive_executors();
            let waiting_on = match self
                .prepare_caches(
                    &job.ctx,
                    &job.plan,
                    &alive_executors,
                    &job.admission,
                    &mut claims,
                )
                .await
            {
                Ok(waiting_on) => waiting_on,
                Err(e) => {
                    self.roll_back_claims(&claims, &e.to_string(), sender).await;
                    self.fail_pending(&job, &e.to_string(), sender).await;
                    return;
                }
            };
            if waiting_on.is_empty() {
                return self.submit_released(job, sender).await;
            }
            job.relinks += 1;
            if job.relinks > self.config.stage_max_failures {
                let reason = format!(
                    "the caches it reads went stale {} times while it waited",
                    job.relinks - 1
                );
                self.roll_back_claims(&claims, &reason, sender).await;
                return self.fail_pending(&job, &reason, sender).await;
            }
            job.waiting_on = waiting_on;
            claims.push(job);
            for ready in self.cache_linker.register(claims) {
                self.submit_released(ready, sender).await;
            }
        })
    }

    /// Rewrites a released job's caches into reads, records them, and submits it.
    async fn submit_released(
        &self,
        job: PendingJob,
        sender: &EventSender<QueryStageSchedulerEvent>,
    ) {
        let alive_executors = self.executor_manager.get_alive_executors();
        let mut reads = Vec::new();
        let plan = match resolve_cache_reads(
            &job.plan,
            self.cache_state.as_ref(),
            &alive_executors,
            &mut reads,
            OnMiss::Fail,
        )
        .await
        {
            Ok(plan) => plan,
            Err(e) => return self.fail_pending(&job, &e.to_string(), sender).await,
        };

        self.cache_linker.record_reads(
            &job.job_id,
            JobReads {
                ctx: job.ctx.clone(),
                queued_at: job.queued_at,
                admission: job.admission.clone(),
                reads,
            },
        );
        debug!("Submitting cache job {} ({})", job.job_id, job.job_name);
        let event = QueryStageSchedulerEvent::JobQueued {
            job_id: job.job_id.clone(),
            job_name: job.job_name.clone(),
            session_ctx: job.ctx.clone(),
            plan: Box::new(SubmitPlan::Logical(plan)),
            queued_at: job.queued_at,
            subscriber: job.subscriber.clone(),
            admission: job.admission.clone(),
        };
        if let Err(e) = sender.post_event(event).await {
            self.cache_linker.job_ended(&job.job_id);
            self.fail_pending(&job, &format!("failed to submit: {e}"), sender)
                .await;
        }
    }

    /// Dispatches a released job on its own task, off the event loop.
    fn spawn_dispatch(
        self: &Arc<Self>,
        job: PendingJob,
        sender: &EventSender<QueryStageSchedulerEvent>,
    ) {
        let state = Arc::clone(self);
        let sender = sender.clone();
        tokio::spawn(async move { state.dispatch_cache_job(job, &sender).await });
    }

    /// The plan to submit for an `EXPLAIN` of a query reading caches: built
    /// caches as reads, others as their subplan. Claims nothing.
    pub(crate) async fn explain_cache_plan(
        &self,
        plan: &LogicalPlan,
    ) -> Result<LogicalPlan> {
        let alive_executors = self.executor_manager.get_alive_executors();
        resolve_cache_reads(
            plan,
            self.cache_state.as_ref(),
            &alive_executors,
            &mut Vec::new(),
            OnMiss::Subplan,
        )
        .await
    }

    /// Cache bookkeeping for an ended job, called by every job end handler. A
    /// job that lost cache data drops the builds it read, so a retry rebuilds
    /// them. A build job's claim is recorded or released here, exactly once,
    /// and what waits on it released or failed. On success it runs before
    /// cleanup, so the cache's stage is pinned when cleanup looks.
    pub(crate) async fn on_job_terminal(
        self: &Arc<Self>,
        job_id: &JobId,
        end: JobEnd<'_>,
        sender: &EventSender<QueryStageSchedulerEvent>,
    ) {
        let (reads, linked) = self.cache_linker.job_ended(job_id);
        if let JobEnd::Failed(reason) = end
            && reason.contains(CACHE_DATA_LOST)
        {
            for read in reads {
                if let Err(e) = self
                    .invalidate_build(&read.key, &read.materializing_job)
                    .await
                {
                    warn!("failed to drop lost cache {:?}: {e:?}", read.key);
                }
            }
        }
        if !linked {
            return;
        }
        match end {
            JobEnd::Finished => {
                let Some(key) = self.cache_linker.key_of(job_id) else {
                    return;
                };
                match self.record_materialized_cache(job_id, &key).await {
                    Ok(()) => {
                        let released = self.cache_linker.complete(job_id);
                        for job in released.jobs {
                            self.spawn_dispatch(job, sender);
                        }
                        for (job, input) in released.inputs {
                            self.provide_input(&job, input, sender).await;
                        }
                    }
                    Err(reason) => {
                        self.fail_cache_job(job_id, Some(&key), &reason, sender)
                            .await
                    }
                }
            }
            JobEnd::Failed(reason) => {
                let key = self.cache_linker.key_of(job_id);
                self.fail_cache_job(job_id, key.as_ref(), reason, sender)
                    .await;
            }
            // A held job was never submitted, so `abort_job` had nothing to do.
            JobEnd::Cancelled => match self.cache_linker.cancel(job_id) {
                Some(held) => self.cancel_pending(held, sender).await,
                None => {
                    let key = self.cache_linker.key_of(job_id);
                    self.fail_cache_job(job_id, key.as_ref(), "Cancelled", sender)
                        .await;
                }
            },
        }
    }

    /// A running job lost the cache data it reads under `inputs`, and its stages
    /// wait for it: rebuilds those caches and provides them again, or fails the
    /// job. On its own task, off the event loop.
    pub(crate) fn on_external_input_lost(
        self: &Arc<Self>,
        job_id: JobId,
        inputs: Vec<usize>,
        sender: &EventSender<QueryStageSchedulerEvent>,
    ) {
        let state = Arc::clone(self);
        let sender = sender.clone();
        tokio::spawn(async move {
            let Some(job_reads) = state.cache_linker.reads_of(&job_id) else {
                return; // the job ended meanwhile
            };
            if let Err(reason) = state
                .rebuild_lost_inputs(&job_id, &job_reads, &inputs, &sender)
                .await
            {
                state
                    .post_running_failure(&job_id, job_reads.queued_at, &reason, &sender)
                    .await;
            }
        });
    }

    async fn rebuild_lost_inputs(
        &self,
        job_id: &JobId,
        job_reads: &JobReads,
        inputs: &[usize],
        sender: &EventSender<QueryStageSchedulerEvent>,
    ) -> std::result::Result<(), String> {
        // Every input is checked before any build is dropped or claimed, so a
        // job that cannot recover leaves the healthy ones as they are.
        let mut reads = Vec::with_capacity(inputs.len());
        for input in inputs {
            let Some(read) = job_reads.read(*input) else {
                return Err(format!(
                    "external input {input} is not a cache this scheduler can rebuild"
                ));
            };
            // A rebuild could give the query a second version of a cache it
            // already reads elsewhere.
            if shares_a_cache(&job_reads.reads, read) {
                return Err(format!(
                    "{CACHE_DATA_LOST} that shares a cache with another of its reads, so it cannot be rebuilt mid-query; retry the query"
                ));
            }
            reads.push(read);
        }
        for read in reads {
            let input = read.input_id;
            // Drop the lost build only if it is still the one read: another
            // reader may already have replaced it.
            self.invalidate_build(&read.key, &read.materializing_job)
                .await
                .map_err(|e| e.to_string())?;
            let mut claims = Vec::new();
            let alive_executors = self.executor_manager.get_alive_executors();
            let waiting_on = match self
                .prepare_caches(
                    &job_reads.ctx,
                    &read.cache,
                    &alive_executors,
                    &job_reads.admission,
                    &mut claims,
                )
                .await
            {
                Ok(waiting_on) => waiting_on,
                Err(e) => {
                    self.roll_back_claims(&claims, &e.to_string(), sender).await;
                    return Err(e.to_string());
                }
            };
            for ready in self.cache_linker.register(claims) {
                self.submit_released(ready, sender).await;
            }
            // A cache node waits on at most one job: its own build.
            let waits = waiting_on
                .iter()
                .next()
                .is_some_and(|on| self.cache_linker.wait_for_input(job_id, input, on));
            if !waits {
                self.provide_input(job_id, input, sender).await;
            }
        }
        Ok(())
    }

    /// Provides `job_id`'s lost `input` again, from its cache's current entry,
    /// or fails the job.
    async fn provide_input(
        &self,
        job_id: &JobId,
        input: usize,
        sender: &EventSender<QueryStageSchedulerEvent>,
    ) {
        let Some(job_reads) = self.cache_linker.reads_of(job_id) else {
            return; // the job ended meanwhile
        };
        let Some(read) = job_reads.read(input) else {
            return;
        };
        let reason = match self.cache_state.lookup(&read.key).await {
            Ok(Some(cache)) if cache.schema == read.written => {
                self.cache_linker.input_rebuilt(
                    job_id,
                    input,
                    cache.materializing_job.clone(),
                );
                let event = QueryStageSchedulerEvent::ExternalInputReady {
                    job_id: job_id.clone(),
                    input,
                    locations: cache.locations,
                    queued_at: job_reads.queued_at,
                };
                if let Err(e) = sender.post_event(event).await {
                    error!("failed to resume job {job_id}: {e}");
                }
                return;
            }
            Ok(Some(_)) => {
                "a cache it reads was rebuilt with a different schema".to_owned()
            }
            Ok(None) => {
                "a cache it reads was lost again before it could be read".to_owned()
            }
            Err(e) => format!("a cache it reads could not be looked up: {e}"),
        };
        self.post_running_failure(job_id, job_reads.queued_at, &reason, sender)
            .await;
    }

    /// Fails a running job whose lost cache cannot be provided again.
    async fn post_running_failure(
        &self,
        job_id: &JobId,
        queued_at: u64,
        reason: &str,
        sender: &EventSender<QueryStageSchedulerEvent>,
    ) {
        warn!("Job {job_id} could not recover a cache it reads: {reason}");
        let event = QueryStageSchedulerEvent::JobRunningFailed {
            job_id: job_id.clone(),
            fail_message: format!(
                "Job {job_id} could not recover a cache it reads: {reason}"
            ),
            queued_at,
            failed_at: timestamp_millis(),
        };
        if let Err(e) = sender.post_event(event).await {
            error!("failed to report job {job_id} as failed: {e}");
        }
    }

    /// Records `job_id`'s output as `key`'s entry, or says why it cannot.
    async fn record_materialized_cache(
        &self,
        job_id: &JobId,
        key: &CacheKey,
    ) -> std::result::Result<(), String> {
        let Some(graph) = self.task_manager.get_active_execution_graph(job_id) else {
            return Err("its output locations are unavailable".to_owned());
        };
        let (schema, locations) = {
            let graph = graph.read().await;
            // Cache builds run staged (see `SchedulerState::submit_job`); a
            // pipelined job streams its output and leaves nothing to record.
            if graph.as_pipelined().is_some() {
                return Err(
                    "it ran pipelined, which leaves no output to cache".to_owned()
                );
            }
            (
                graph.physical_plan().schema(),
                shape_locations(graph.output_locations()),
            )
        };
        match self
            .cache_state
            .complete_materialization(key.clone(), job_id.clone(), schema, locations)
            .await
        {
            Ok(CompleteOutcome::Completed) => Ok(()),
            Ok(_) => Err("it was invalidated while it was being built".to_owned()),
            Err(e) => Err(format!("its output could not be recorded: {e}")),
        }
    }

    /// `job_id` will not build a cache: releases its claim on `key`, if any, and
    /// fails the held jobs waiting on it.
    async fn fail_cache_job(
        &self,
        job_id: &JobId,
        key: Option<&CacheKey>,
        reason: &str,
        sender: &EventSender<QueryStageSchedulerEvent>,
    ) {
        if let Some(key) = key {
            self.release_claim(key, job_id).await;
        }
        let reason = format!("a cache this query reads could not be built: {reason}");
        let released = self.cache_linker.fail(job_id);
        for victim in released.jobs {
            match &victim.kind {
                // Its own dependents were failed with it.
                PendingKind::Materialize { key } => {
                    self.release_claim(key, &victim.job_id).await
                }
                PendingKind::Read => self.fail_held_read(&victim, &reason, sender).await,
            }
        }
        // A job fails once, however many of its lost reads waited here.
        let jobs: HashSet<JobId> =
            released.inputs.into_iter().map(|(job, _)| job).collect();
        for job in jobs {
            if let Some(job_reads) = self.cache_linker.reads_of(&job) {
                self.post_running_failure(&job, job_reads.queued_at, &reason, sender)
                    .await;
            }
        }
    }

    /// Fails a held job, and what waits on it.
    async fn fail_pending(
        &self,
        job: &PendingJob,
        reason: &str,
        sender: &EventSender<QueryStageSchedulerEvent>,
    ) {
        match &job.kind {
            PendingKind::Materialize { key } => {
                self.fail_cache_job(&job.job_id, Some(key), reason, sender)
                    .await
            }
            PendingKind::Read => self.fail_held_read(job, reason, sender).await,
        }
    }

    /// Reports a held read job failed to its client.
    async fn fail_held_read(
        &self,
        job: &PendingJob,
        reason: &str,
        sender: &EventSender<QueryStageSchedulerEvent>,
    ) {
        let failed_at = timestamp_millis();
        notify_failed(job, reason, failed_at);
        let event = QueryStageSchedulerEvent::JobPlanningFailed {
            job_id: job.job_id.clone(),
            fail_message: reason.to_owned(),
            queued_at: job.queued_at,
            failed_at,
        };
        if let Err(e) = sender.post_event(event).await {
            error!("failed to report job {} as failed: {e}", job.job_id);
        }
    }

    /// Ends a held job the client cancelled. The cancel is already counted, so
    /// a read job ends as an unscheduled job does, with no failure event.
    async fn cancel_pending(
        &self,
        job: PendingJob,
        sender: &EventSender<QueryStageSchedulerEvent>,
    ) {
        match &job.kind {
            PendingKind::Read => {
                notify_failed(&job, "Cancelled", timestamp_millis());
                if let Err(e) = self
                    .task_manager
                    .fail_unscheduled_job(&job.job_id, "Cancelled".to_owned())
                    .await
                {
                    error!("failed to end cancelled job {}: {e:?}", job.job_id);
                }
            }
            PendingKind::Materialize { .. } => {
                self.fail_pending(&job, "Cancelled", sender).await
            }
        }
    }

    /// Gives back the claims of a submission that failed part-way.
    async fn roll_back_claims(
        &self,
        jobs: &[PendingJob],
        reason: &str,
        sender: &EventSender<QueryStageSchedulerEvent>,
    ) {
        for job in jobs {
            self.fail_pending(job, reason, sender).await;
        }
    }

    /// Drops `key`'s entry while it is still `materializing_job`'s, and cleans up
    /// that build's data after the usual delay for a finished job's data, which
    /// lets a job still reading it finish.
    async fn invalidate_build(
        &self,
        key: &CacheKey,
        materializing_job: &JobId,
    ) -> Result<()> {
        if self
            .cache_state
            .invalidate_if(key, materializing_job)
            .await?
            .is_some()
        {
            self.executor_manager.clean_up_job_data_delayed(
                materializing_job.clone(),
                self.config.finished_job_data_clean_up_interval_seconds,
            );
        }
        Ok(())
    }

    async fn release_claim(&self, key: &CacheKey, job_id: &JobId) {
        if let Err(e) = self.cache_state.abort_materialization(key, job_id).await {
            warn!("failed to release cache claim of job {job_id}: {e:?}");
        }
    }
}

/// Whether `read` shares a cache with another of the job's `reads`: the same
/// one, one built from the other, or two built from a common one. A rebuild
/// of `read` may rebuild any cache it depends on, which the other read would
/// then have read in another version.
fn shares_a_cache(reads: &[ResolvedRead], read: &ResolvedRead) -> bool {
    let depends_on = read.depends_on();
    reads
        .iter()
        .filter(|other| other.input_id != read.input_id)
        .any(|other| !other.depends_on().is_disjoint(&depends_on))
}

/// Tells a held job's subscriber, if any, that it failed.
fn notify_failed(job: &PendingJob, reason: &str, failed_at: u64) {
    let Some(subscriber) = &job.subscriber else {
        return;
    };
    let status = JobStatus {
        job_id: job.job_id.clone().into(),
        job_name: job.job_name.clone(),
        status: Some(job_status::Status::Failed(FailedJob {
            error: reason.to_owned(),
            queued_at: job.queued_at,
            started_at: failed_at,
            ended_at: failed_at,
        })),
    };
    if subscriber.try_send(status).is_err() {
        error!(
            "could not tell the subscriber of job {} that it failed",
            job.job_id
        );
    }
}

#[cfg(test)]
mod tests {
    //! These drive real submissions through the scheduler's event loop with
    //! virtual executors. `SchedulerTest::submit` does not forward task
    //! statuses, so nothing completes until a test calls `tick`: a submission
    //! that blocked on its materialize jobs would hang inside `submit`.

    use super::*;
    use crate::cache::{CACHE_DATA_LOST, EXTERNAL_STAGE_ID_BASE};
    use crate::config::SchedulerConfig;
    use crate::scheduler_server::timestamp_millis;
    use crate::state::cache_registry::MaterializedCache;
    use crate::test_utils::{
        MetricEvent, SchedulerTest, TaskRunnerFn, TestMetricsCollector,
    };
    use ballista_core::config::{BALLISTA_EXECUTION_MODE, TaskSchedulingPolicy};
    use ballista_core::extension::BallistaCacheNode;
    use ballista_core::serde::protobuf::job_status::Status;
    use ballista_core::serde::protobuf::{
        ExecutionError, FailedTask, FetchPartitionError, MultiTaskDefinition, TaskId,
        TaskStatus, failed_task, task_status,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::functions_aggregate::sum::sum;
    use datafusion::logical_expr::{Extension, LogicalPlanBuilder, col};
    use datafusion::test_util::scan_empty_with_partitions;
    use std::time::Duration;

    fn scan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gmv", DataType::UInt64, false),
        ]);
        scan_empty_with_partitions(None, &schema, Some(vec![0, 1]), 2)
            .unwrap()
            .aggregate(vec![col("id")], vec![sum(col("gmv"))])
            .unwrap()
            .build()
            .unwrap()
    }

    fn cache_over(cache_id: &str, input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(BallistaCacheNode::new(
                cache_id.to_owned(),
                "session".to_owned(),
                input,
            )),
        })
    }

    fn union(a: LogicalPlan, b: LogicalPlan) -> LogicalPlan {
        LogicalPlanBuilder::from(a)
            .union(b)
            .unwrap()
            .build()
            .unwrap()
    }

    async fn scheduler_test(
        runner: Option<Arc<dyn crate::test_utils::TaskRunner>>,
    ) -> (SchedulerTest, Arc<TestMetricsCollector>) {
        scheduler_test_with(SchedulerConfig::default(), runner).await
    }

    async fn scheduler_test_with(
        config: SchedulerConfig,
        runner: Option<Arc<dyn crate::test_utils::TaskRunner>>,
    ) -> (SchedulerTest, Arc<TestMetricsCollector>) {
        let metrics = Arc::new(TestMetricsCollector::default());
        let config = config.with_scheduler_policy(TaskSchedulingPolicy::PushStaged);
        let test = SchedulerTest::new(config, metrics.clone(), 2, 2, runner)
            .await
            .unwrap();
        (test, metrics)
    }

    fn is_terminal(status: &JobStatus) -> bool {
        matches!(
            status.status,
            Some(Status::Successful(_)) | Some(Status::Failed(_))
        )
    }

    /// Processes task statuses until `job` reaches a terminal state, failing
    /// the test if it does not: a job must end, or recover, never hang.
    async fn finish(test: &mut SchedulerTest, job: &JobId) -> JobStatus {
        for _ in 0..100 {
            if let Some(status) = test.job_status(job).await.unwrap()
                && is_terminal(&status)
            {
                return status;
            }
            let _ = tokio::time::timeout(Duration::from_millis(100), test.tick()).await;
        }
        panic!("job {job} did not finish");
    }

    fn completed_jobs(metrics: &TestMetricsCollector) -> usize {
        metrics
            .events
            .lock()
            .iter()
            .filter(|e| matches!(e, MetricEvent::Completed(..)))
            .count()
    }

    fn is_successful(status: &JobStatus) -> bool {
        matches!(status.status, Some(Status::Successful(_)))
    }

    fn is_queued(status: &Option<JobStatus>) -> bool {
        matches!(
            status,
            Some(JobStatus {
                status: Some(Status::Queued(_)),
                ..
            })
        )
    }

    fn explain(plan: LogicalPlan) -> LogicalPlan {
        LogicalPlanBuilder::from(plan)
            .explain(false, false)
            .unwrap()
            .build()
            .unwrap()
    }

    /// EXPLAIN of a cached query runs as one job and materializes nothing, cold
    /// or warm.
    #[tokio::test]
    async fn explain_does_not_materialize() {
        let (mut test, metrics) = scheduler_test(None).await;
        let plan = cache_over("c1", scan());
        let key = CacheKey::new("session", "c1");

        // Cold: only the EXPLAIN job runs, and nothing is claimed.
        let job = test
            .submit("explain", &explain(plan.clone()))
            .await
            .unwrap();
        assert!(is_successful(&finish(&mut test, &job).await));
        assert_eq!(completed_jobs(&metrics), 1);
        assert!(
            test.cache_state()
                .pending_job(&key)
                .await
                .unwrap()
                .is_none()
        );
        assert!(test.cache_state().lookup(&key).await.unwrap().is_none());

        // Warm: build the cache, then EXPLAIN reads it without rebuilding.
        let read = test.submit("query", &plan).await.unwrap();
        finish(&mut test, &read).await;
        assert_eq!(completed_jobs(&metrics), 3);
        let job = test.submit("explain", &explain(plan)).await.unwrap();
        assert!(is_successful(&finish(&mut test, &job).await));
        assert_eq!(completed_jobs(&metrics), 4, "just the EXPLAIN job");
    }

    /// With caching disabled by the operator, a cached query runs as one ordinary
    /// job: nothing is materialized or recorded.
    #[tokio::test]
    async fn disabled_cache_runs_the_query_uncached() {
        let config = SchedulerConfig::default().with_disable_dataframe_cache(true);
        let (mut test, metrics) = scheduler_test_with(config, None).await;
        let plan = cache_over("outer", cache_over("inner", scan()));

        let job = test.submit("query", &plan).await.unwrap();
        assert!(is_successful(&finish(&mut test, &job).await));

        assert_eq!(completed_jobs(&metrics), 1, "no materialize jobs");
        let cache_state = test.cache_state();
        for id in ["inner", "outer"] {
            let key = CacheKey::new("session", id);
            assert!(cache_state.lookup(&key).await.unwrap().is_none());
            assert!(cache_state.pending_job(&key).await.unwrap().is_none());
        }
    }

    /// Cold, then warm, under both planners. The read job's id comes back before
    /// the cache is built (`submit` never ticks, so a staged submit would hang
    /// here), and it runs once the cache is built. A second query then reads the
    /// cache without building it again. The adaptive planner must accept a leaf
    /// `ShuffleReaderExec` whose stage id belongs to another job.
    #[rstest::rstest]
    #[tokio::test]
    async fn a_cache_is_built_once_then_read(#[values(false, true)] adaptive: bool) {
        let (mut test, metrics) = scheduler_test(None).await;
        test.set_adaptive_planner(adaptive);
        let plan = cache_over("c1", scan());

        let read = test.submit("query", &plan).await.unwrap();
        assert!(
            is_queued(&test.job_status(&read).await.unwrap()),
            "the read job waits, queued, for its cache"
        );

        assert!(is_successful(&finish(&mut test, &read).await));

        // The materialize job and the read job.
        assert_eq!(completed_jobs(&metrics), 2);
        let entry = test
            .cache_state()
            .lookup(&CacheKey::new("session", "c1"))
            .await
            .unwrap()
            .expect("the cache entry is recorded");
        assert_ne!(entry.materializing_job, read);

        let again = test.submit("again", &plan).await.unwrap();
        assert!(is_successful(&finish(&mut test, &again).await));
        assert_eq!(completed_jobs(&metrics), 3, "only the read job ran");
    }

    /// Two queries over one missing cache share a single build.
    #[tokio::test]
    async fn concurrent_queries_share_one_build() {
        let (mut test, metrics) = scheduler_test(None).await;
        let plan = cache_over("c1", scan());

        let first = test.submit("first", &plan).await.unwrap();
        let second = test.submit("second", &plan).await.unwrap();

        assert!(is_successful(&finish(&mut test, &first).await));
        assert!(is_successful(&finish(&mut test, &second).await));
        assert_eq!(completed_jobs(&metrics), 3, "one build, two reads");
    }

    fn failing_runner() -> Arc<dyn crate::test_utils::TaskRunner> {
        Arc::new(TaskRunnerFn::new(
            |_executor: String, task: MultiTaskDefinition| {
                task.task_ids
                    .into_iter()
                    .map(|TaskId { task_id, .. }| {
                        let now = timestamp_millis();
                        TaskStatus {
                            task_id,
                            job_id: task.job_id.clone(),
                            stage_id: task.stage_id,
                            stage_attempt_num: task.stage_attempt_num,
                            launch_time: now,
                            start_exec_time: now,
                            end_exec_time: now,
                            metrics: vec![],
                            status: Some(task_status::Status::Failed(FailedTask {
                                error: "boom".to_owned(),
                                retryable: false,
                                count_to_failures: false,
                                failed_reason: Some(
                                    failed_task::FailedReason::ExecutionError(
                                        ExecutionError {},
                                    ),
                                ),
                            })),
                        }
                    })
                    .collect()
            },
        ))
    }

    /// If a cache cannot be built, the cache built on it and the read job fail
    /// (none waits for ever), and both keys can be claimed again.
    #[tokio::test]
    async fn a_failed_build_fails_what_waits_on_it_and_frees_the_keys() {
        let (mut test, _metrics) = scheduler_test(Some(failing_runner())).await;
        let plan = cache_over("outer", cache_over("inner", scan()));

        let read = test.submit("query", &plan).await.unwrap();
        let status = finish(&mut test, &read).await;
        assert!(
            matches!(&status.status, Some(Status::Failed(f)) if f.error.contains("could not be built")),
            "{status:?}"
        );

        let cache_state = test.cache_state();
        for id in ["inner", "outer"] {
            let key = CacheKey::new("session", id);
            assert!(cache_state.lookup(&key).await.unwrap().is_none());
            assert!(
                cache_state.pending_job(&key).await.unwrap().is_none(),
                "{id}"
            );
        }
    }

    /// Cancelling a read job that is still waiting fails it, and it is never
    /// submitted once its cache finishes.
    #[tokio::test]
    async fn cancelling_a_waiting_read_job_fails_it() {
        let (mut test, metrics) = scheduler_test(None).await;
        let plan = cache_over("c1", scan());

        let read = test.submit("query", &plan).await.unwrap();
        test.cancel(&read).await.unwrap();

        let status = finish(&mut test, &read).await;
        assert!(
            matches!(&status.status, Some(Status::Failed(_))),
            "{status:?}"
        );
        // Counted once, as cancelled; it never ran, so it did not also fail.
        let events = metrics.job_events(&read);
        assert!(matches!(events.as_slice(), [MetricEvent::Cancelled(_)]));

        // The cache still gets built; only the cancelled read never runs.
        for _ in 0..40 {
            if completed_jobs(&metrics) == 1 {
                break;
            }
            let _ = tokio::time::timeout(Duration::from_millis(100), test.tick()).await;
        }
        assert_eq!(completed_jobs(&metrics), 1, "just the materialize job");
    }

    fn failure(status: &JobStatus) -> String {
        match &status.status {
            Some(Status::Failed(failed)) => failed.error.clone(),
            other => panic!("expected the job to fail, got {other:?}"),
        }
    }

    /// Waits, without processing any task status, until `job`'s graph is
    /// active: its tasks are launched and their statuses are waiting to be
    /// processed. Waiting for the `Running` status is not enough, as it is set
    /// just before the graph becomes active.
    async fn wait_until_running(test: &SchedulerTest, job: &JobId) {
        let running = crate::test_utils::await_condition(
            Duration::from_millis(20),
            100,
            || async { Ok(test.is_active(job)) },
        )
        .await
        .unwrap();
        assert!(running, "job {job} did not start");
    }

    /// An executor holding the cache's data is lost while the read job runs
    /// under the adaptive planner, which cannot resume a rolled-back stage: the
    /// job fails naming the loss, and nothing stays admitted.
    #[tokio::test]
    async fn losing_an_executor_mid_read_fails_an_adaptive_query() {
        let (mut test, _metrics) = scheduler_test(None).await;
        test.set_adaptive_planner(true);
        let plan = cache_over("c1", scan());
        let read = test.submit("query", &plan).await.unwrap();
        let entry = build(&mut test, "c1").await;
        wait_until_running(&test, &read).await;
        test.lose_executor(&holder_of(&entry)).await.unwrap();

        let error = failure(&finish(&mut test, &read).await);
        assert!(error.contains(CACHE_DATA_LOST), "{error}");
        assert_eq!(test.active_job_number(), 0, "nothing stays admitted");
    }

    /// Two running jobs lose the cache they read, with the executor holding it:
    /// one rebuild elsewhere serves both, both succeed, and nothing stays
    /// admitted.
    #[tokio::test]
    async fn two_readers_losing_one_cache_share_the_rebuild() {
        let (mut test, metrics) = scheduler_test(None).await;
        let plan = cache_over("c1", scan());
        let warm = test.submit("warm", &plan).await.unwrap();
        assert!(is_successful(&finish(&mut test, &warm).await));
        let key = CacheKey::new("session", "c1");
        let entry = test.cache_state().lookup(&key).await.unwrap().unwrap();

        let first = test.submit("first", &plan).await.unwrap();
        let second = test.submit("second", &plan).await.unwrap();
        wait_until_running(&test, &first).await;
        wait_until_running(&test, &second).await;
        let holder = holder_of(&entry);
        test.lose_executor(&holder).await.unwrap();

        assert!(is_successful(&finish(&mut test, &first).await));
        assert!(is_successful(&finish(&mut test, &second).await));
        // The first build and read, one rebuild, and the two reads.
        assert_eq!(completed_jobs(&metrics), 5);
        let rebuilt = test.cache_state().lookup(&key).await.unwrap().unwrap();
        assert_ne!(rebuilt.materializing_job, entry.materializing_job);
        assert!(
            rebuilt
                .locations
                .iter()
                .flatten()
                .all(|loc| loc.executor_meta.id != holder)
        );
        assert_eq!(test.active_job_number(), 0, "nothing stays admitted");
    }

    /// A job whose lost cache shares a cache with another of its reads (here the
    /// same cache twice, or one built from the other) fails cleanly rather than
    /// read two versions of it.
    #[rstest::rstest]
    #[case::twice(union(cache_over("c1", scan()), cache_over("c1", scan())), "c1")]
    #[case::through_another_cache(
        union(cache_over("c1", scan()), cache_over("c2", cache_over("c1", scan()))),
        "c2"
    )]
    #[tokio::test]
    async fn a_cache_shared_with_another_read_is_not_rebuilt_mid_query(
        #[case] plan: LogicalPlan,
        #[case] last_built: &str,
    ) {
        let (mut test, _metrics) = scheduler_test(None).await;
        let read = test.submit("query", &plan).await.unwrap();
        build(&mut test, last_built).await;
        wait_until_running(&test, &read).await;
        let c1 = test
            .cache_state()
            .lookup(&CacheKey::new("session", "c1"))
            .await
            .unwrap()
            .unwrap();
        test.lose_executor(&holder_of(&c1)).await.unwrap();

        let error = failure(&finish(&mut test, &read).await);
        assert!(error.contains("shares a cache"), "{error}");
    }

    /// A lost read is rebuilt mid-query only if it shares no cache with another
    /// read of the job, as its rebuild may rebuild any cache it depends on.
    #[rstest::rstest]
    #[case::once(vec![cache_over("c1", scan())], vec![false])]
    #[case::unrelated(vec![cache_over("c1", scan()), cache_over("c2", scan())], vec![false, false])]
    #[case::built_from_one_not_read(vec![cache_over("c2", cache_over("c1", scan()))], vec![false])]
    #[case::twice(vec![cache_over("c1", scan()), cache_over("c1", scan())], vec![true, true])]
    #[case::one_built_from_the_other(
        vec![cache_over("c1", scan()), cache_over("c2", cache_over("c1", scan()))],
        vec![true, true]
    )]
    #[case::built_from_a_common_one(
        vec![cache_over("c2", cache_over("c1", scan())), cache_over("c3", cache_over("c1", scan()))],
        vec![true, true]
    )]
    #[case::nested_and_its_innermost(
        vec![cache_over("c3", cache_over("c2", cache_over("c1", scan()))), cache_over("c1", scan())],
        vec![true, true]
    )]
    fn a_lost_read_is_rebuilt_only_if_it_shares_no_cache(
        #[case] caches: Vec<LogicalPlan>,
        #[case] shared: Vec<bool>,
    ) {
        let reads: Vec<ResolvedRead> = caches
            .into_iter()
            .enumerate()
            .map(|(n, cache)| {
                let node = cache_node(&cache).unwrap();
                ResolvedRead {
                    key: CacheKey::new(node.session_id(), node.cache_id()),
                    materializing_job: JobId::new("build"),
                    input_id: EXTERNAL_STAGE_ID_BASE + n,
                    written: Arc::new(Schema::empty()),
                    cache,
                }
            })
            .collect();
        let actual: Vec<bool> = reads
            .iter()
            .map(|read| shares_a_cache(&reads, read))
            .collect();
        assert_eq!(actual, shared);
    }

    /// Ticks until `cache_id`'s entry is recorded, without ticking past it: one
    /// tick too many would let the read job, dispatched once the entry is
    /// recorded, finish before the test acts.
    async fn build(test: &mut SchedulerTest, cache_id: &str) -> MaterializedCache {
        let key = CacheKey::new("session", cache_id);
        for _ in 0..60 {
            let _ = tokio::time::timeout(Duration::from_millis(100), test.tick()).await;
            for _ in 0..20 {
                if let Some(entry) = test.cache_state().lookup(&key).await.unwrap() {
                    return entry;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        panic!("cache {cache_id} was not built");
    }

    fn holder_of(entry: &MaterializedCache) -> String {
        entry
            .locations
            .iter()
            .flatten()
            .next()
            .unwrap()
            .executor_meta
            .id
            .clone()
    }

    /// A pipelined session builds its caches as staged jobs, which leave
    /// shuffle files to cache, and reads them as pipelined jobs.
    #[tokio::test]
    async fn a_pipelined_session_builds_its_cache_staged() {
        let (mut test, metrics) = scheduler_test(None).await;
        test.set_session_option(BALLISTA_EXECUTION_MODE, "pipelined")
            .unwrap();
        let plan = cache_over("c1", scan());

        let read = test.submit("query", &plan).await.unwrap();
        assert!(is_successful(&finish(&mut test, &read).await));
        let entry = test
            .cache_state()
            .lookup(&CacheKey::new("session", "c1"))
            .await
            .unwrap()
            .expect("the cache entry is recorded");
        assert!(entry.locations.iter().flatten().next().is_some());

        let again = test.submit("again", &plan).await.unwrap();
        assert!(is_successful(&finish(&mut test, &again).await));
        assert_eq!(completed_jobs(&metrics), 3, "one build, two reads");
    }

    /// An executor holding the cache's data is lost while a pipelined read job
    /// runs: the job fails naming the loss, and a retry rebuilds the cache.
    #[tokio::test]
    async fn losing_an_executor_with_cache_data_fails_a_pipelined_query() {
        let (mut test, _metrics) = scheduler_test(None).await;
        let plan = cache_over("c1", scan());
        // Built under the default (staged) mode, so the build can be
        // stepped while the read job is held.
        let read = test.submit("query", &plan).await.unwrap();
        let entry = build(&mut test, "c1").await;
        finish(&mut test, &read).await;

        test.set_session_option(BALLISTA_EXECUTION_MODE, "pipelined")
            .unwrap();
        let read = test.submit("pipelined", &plan).await.unwrap();
        wait_until_running(&test, &read).await;
        // Placed, so the loss lands once its tasks are bound: before that, the
        // job would wait for the cache to be rebuilt instead of failing.
        let placed = crate::test_utils::await_condition(
            Duration::from_millis(20),
            100,
            || async { Ok(test.is_pipelined_placed(&read).await) },
        )
        .await
        .unwrap();
        assert!(placed, "the pipelined job is placed");
        let holder = holder_of(&entry);
        test.lose_executor(&holder).await.unwrap();

        let error = failure(&finish(&mut test, &read).await);
        assert!(error.contains(CACHE_DATA_LOST), "{error}");

        let retry = test.submit("retry", &plan).await.unwrap();
        assert!(is_successful(&finish(&mut test, &retry).await));
        let rebuilt = test
            .cache_state()
            .lookup(&CacheKey::new("session", "c1"))
            .await
            .unwrap()
            .unwrap();
        assert_ne!(rebuilt.materializing_job, entry.materializing_job);
    }

    /// A pipelined read job fails to fetch cache data whose executor is still
    /// alive: the job fails naming the loss, so the build is dropped and a
    /// retry rebuilds it rather than failing the same way.
    #[tokio::test]
    async fn a_lost_cache_fetch_fails_a_pipelined_query_and_drops_the_cache() {
        let lost: Arc<
            std::sync::Mutex<Option<ballista_core::serde::scheduler::PartitionLocation>>,
        > = Arc::default();
        let runner = {
            let lost = lost.clone();
            let succeed = crate::test_utils::default_task_runner();
            TaskRunnerFn::new(move |executor: String, task: MultiTaskDefinition| {
                use crate::test_utils::TaskRunner;
                let Some(location) = lost.lock().unwrap().clone() else {
                    return succeed.run(executor, task);
                };
                let now = timestamp_millis();
                task.task_ids
                    .into_iter()
                    .map(|TaskId { task_id, .. }| TaskStatus {
                        task_id,
                        job_id: task.job_id.clone(),
                        stage_id: task.stage_id,
                        stage_attempt_num: task.stage_attempt_num,
                        launch_time: now,
                        start_exec_time: now,
                        end_exec_time: now,
                        metrics: vec![],
                        status: Some(task_status::Status::Failed(FailedTask {
                            error: "fetch failed".to_owned(),
                            retryable: false,
                            count_to_failures: false,
                            failed_reason: Some(
                                failed_task::FailedReason::FetchPartitionError(
                                    FetchPartitionError {
                                        executor_id: location.executor_meta.id.clone(),
                                        map_stage_id: location.partition_id.stage_id
                                            as u32,
                                        map_partition_id: location
                                            .partition_id
                                            .partition_id
                                            as u32,
                                    },
                                ),
                            ),
                        })),
                    })
                    .collect()
            })
        };
        let (mut test, _metrics) = scheduler_test(Some(Arc::new(runner))).await;
        test.set_session_option(BALLISTA_EXECUTION_MODE, "pipelined")
            .unwrap();
        let plan = cache_over("c1", scan());
        let read = test.submit("query", &plan).await.unwrap();
        assert!(is_successful(&finish(&mut test, &read).await));
        let key = CacheKey::new("session", "c1");
        let entry = test.cache_state().lookup(&key).await.unwrap().unwrap();

        *lost.lock().unwrap() = entry.locations.iter().flatten().next().cloned();
        let read = test.submit("lost", &plan).await.unwrap();
        let error = failure(&finish(&mut test, &read).await);
        assert!(error.contains(CACHE_DATA_LOST), "{error}");

        *lost.lock().unwrap() = None;
        let retry = test.submit("retry", &plan).await.unwrap();
        assert!(is_successful(&finish(&mut test, &retry).await));
        let rebuilt = test.cache_state().lookup(&key).await.unwrap().unwrap();
        assert_ne!(rebuilt.materializing_job, entry.materializing_job);
    }

    /// A cache a held job reads goes stale while it waits for another cache:
    /// when released, the job is linked again, the stale cache is rebuilt, and
    /// the query succeeds.
    #[tokio::test]
    async fn a_cache_gone_stale_while_held_is_rebuilt_before_release() {
        let (mut test, metrics) = scheduler_test(None).await;
        let warm = test
            .submit("warm", &cache_over("c1", scan()))
            .await
            .unwrap();
        assert!(is_successful(&finish(&mut test, &warm).await));
        assert_eq!(completed_jobs(&metrics), 2);
        let entry = test
            .cache_state()
            .lookup(&CacheKey::new("session", "c1"))
            .await
            .unwrap()
            .unwrap();

        // c1 is a hit, c2 is built: the query is held for c2. Meanwhile c1's
        // data is lost.
        let query = test
            .submit(
                "query",
                &union(cache_over("c1", scan()), cache_over("c2", scan())),
            )
            .await
            .unwrap();
        test.lose_executor(&holder_of(&entry)).await.unwrap();

        assert!(is_successful(&finish(&mut test, &query).await));
        // Before: 2. Then: c2, the c1 rebuild, and the query.
        assert_eq!(completed_jobs(&metrics), 5);
    }

    /// Hands out job ids, stalling on the third (`c2`'s build, after `c1` is
    /// claimed) until released.
    #[derive(Default)]
    struct StallingIds {
        calls: std::sync::atomic::AtomicUsize,
        released: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl crate::scheduler_server::JobIdGenerator for StallingIds {
        async fn next_id(&self) -> String {
            use std::sync::atomic::Ordering::SeqCst;
            let n = self.calls.fetch_add(1, SeqCst);
            while n == 2 && !self.released.load(SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            format!("job-{n}")
        }
    }

    /// The client gives up after `c1` is claimed: the claim is still submitted,
    /// so the next query over the same caches finishes.
    #[tokio::test]
    async fn a_dropped_submission_does_not_strand_its_caches() {
        let generator = Arc::new(StallingIds::default());
        let config = SchedulerConfig::default().with_job_id_generator(generator.clone());
        let (mut test, _metrics) = scheduler_test_with(config, None).await;
        let plan = union(cache_over("c1", scan()), cache_over("c2", scan()));

        let dropped = test.submit("dropped", &plan);
        let _ = tokio::time::timeout(Duration::from_millis(300), dropped).await;
        generator
            .released
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let again = test.submit("again", &plan).await.unwrap();
        assert!(is_successful(&finish(&mut test, &again).await));
    }
}
