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

//! Wire types for Ballista's scheduler REST API.
//!
//! This crate is the single definition of what `/api/*` puts on the wire. It
//! exists because that contract has more than one party:
//!
//! - **`ballista-scheduler`** serves these responses for live jobs.
//! - **The web TUI** deserializes them to render the cluster.
//! - **A history server** ([#1923]) will serve them for completed jobs, replayed
//!   from a durable event log.
//!
//! Each of those independently re-declaring the same structs is how the shapes
//! drift apart, so they share one definition instead.
//!
//! The crate is deliberately serde-only. That keeps it cheap enough for anyone
//! to depend on, and it is what lets the TUI use it from a `wasm32` build.
//!
//! # What belongs here
//!
//! A type belongs here when it is part of the `/api/*` contract and more than
//! one party needs it. Types describing live scheduler internals
//! (`SchedulerStateResponse`, `CancelJobResponse`) stay in `ballista-scheduler`.
//!
//! `ExecutorResponse` stays behind for a different reason: it embeds
//! `ballista-core` types, so sharing it would mean either taking a
//! `ballista-core` dependency here or duplicating those structs. Neither is
//! worth it until something outside the scheduler needs it.
//!
//! Note that the types are shared but the *construction* is not: building a
//! response from a live execution graph needs scheduler internals, so those
//! builders live in `ballista-scheduler`. A history server does not re-derive
//! responses — it replays ones the scheduler already built and stored, so
//! byte-identical output is a structural property rather than two
//! implementations agreeing.
//!
//! [#1923]: https://github.com/apache/datafusion-ballista/issues/1923

pub mod dto;
