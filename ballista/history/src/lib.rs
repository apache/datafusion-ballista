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

#![warn(missing_docs)]

//! Shared types for Ballista's scheduler REST API.
//!
//! This crate is the serialization boundary between the live scheduler and the
//! history server: the scheduler builds these types, writes them to an event
//! log, and the history server reads them back and serves them.
//!
//! # Write once, replay verbatim
//!
//! The history server does **not** re-derive responses from stored execution
//! state. When a job finishes, the scheduler runs its DTO builders once against
//! the live execution graph and writes the finished [`dto::JobResponse`] and
//! [`dto::QueryStagesResponse`] into the log's terminal record. Replay
//! deserializes those values and re-serializes them unchanged:
//!
//! ```text
//! scheduler ──> dto builders ──> DTO ──> event log ──> history server ──> JSON
//! ```
//!
//! So byte-identical output is a structural property, not a convention two
//! implementations have to keep agreeing on. There is one definition of each
//! type and one place that populates it. That is also why the builders
//! themselves stay in `ballista-scheduler`: they need the live execution graph,
//! and nothing on the replay path calls them.
//!
//! One consequence worth knowing: anything not captured when the record is
//! written cannot be recovered later. Plans, for instance, are rendered in a
//! single format at write time, so replay cannot re-render them differently.
//!
//! # What belongs here
//!
//! A type belongs in this crate when it is part of the `/api/*` wire contract
//! *and* is worth persisting in the event log. Types describing live scheduler
//! state (`SchedulerStateResponse`, `CancelJobResponse`) stay in
//! `ballista-scheduler`, because there is nothing to replay.
//!
//! `ExecutorResponse` also stays behind, for a different reason: it embeds
//! `ballista-core` types, and this crate is deliberately serde-only so it
//! stays cheap to depend on. Sharing it would mean either taking a
//! `ballista-core` dependency here or duplicating those structs.

pub mod dto;
