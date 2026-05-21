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

//! Adaptive query execution (AQE) execution plan wrappers used by the
//! scheduler.
//!
//! This module provides lightweight ExecutionPlan implementations used by the
//! scheduler's adaptive query execution logic. They do not perform actual
//! execution themselves; instead they act as placeholders/markers for
//! shuffle/exchange boundaries and carry metadata the scheduler uses to
//! resolve shuffle locations, stage ids, and finalization state.
//!
//! Types:
//!
//! - `ExchangeExec`: Represents an unresolved/resolved shuffle exchange. It
//!   stores the child plan, optional target partitioning, and (when
//!   available) the resolved `shuffle_partitions` describing where each
//!   partition's data lives.
//! - `AdaptiveDatafusionExec`: Wrapper used by AQE to mark a plan as
//!   adaptive and to carry mutable state such as `is_final` and resolved
//!   shuffle metadata.

mod adaptive;
mod exchange;

pub use adaptive::*;
pub use exchange::*;
