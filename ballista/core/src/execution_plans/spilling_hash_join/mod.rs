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

mod exec;
mod hash_table;
mod partitioner;
mod spill;
mod stream;

// `SpillingHashJoinExec` is the only public surface of this module; it is
// re-exported further up in `execution_plans::mod`.
pub use exec::SpillingHashJoinExec;

// `hash_table` and `partitioner` are private submodules. Their `pub` items
// (`ProbeTable`, `assemble_output`, `RowPartitioner`, `PartitionedBatch`) are
// internal helpers consumed only by `stream.rs` via `super::hash_table::` /
// `super::partitioner::` — they are not re-exported here, so they are not
// part of `ballista_core`'s public API.
