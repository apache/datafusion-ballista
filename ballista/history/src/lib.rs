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
//! These response types live outside `ballista-scheduler` so that a future
//! history server can serialize byte-identical JSON from stored state without
//! depending on the scheduler's live execution graph.
//!
//! # What belongs here
//!
//! A type belongs in this crate when it is part of the `/api/*` wire contract
//! *and* can be reconstructed from a stored event log. Types describing live
//! scheduler state (`SchedulerStateResponse`, `CancelJobResponse`) stay in
//! `ballista-scheduler`, because there is nothing to replay.
//!
//! `ExecutorResponse` also stays behind, for a different reason: it embeds
//! `ballista-core` types, and this crate is deliberately serde-only so it
//! stays cheap to depend on. Sharing it would mean either taking a
//! `ballista-core` dependency here or duplicating those structs.

pub mod dto;
