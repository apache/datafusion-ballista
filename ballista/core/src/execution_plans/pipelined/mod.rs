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

//! The exchange used by pipelined jobs (`ballista.execution.mode =
//! pipelined`).
//!
//! Every task of a pipelined job runs at once. A stage's root,
//! [`PipelinedShuffleWriterExec`], routes its output into bounded in-memory
//! queues held by the executor's [`PipelinedRegistry`], and the leaves of the
//! stages that consume it, [`PipelinedShuffleReaderExec`], stream those
//! queues while the producer runs: from the registry directly when the
//! producer shares the executor, over Arrow Flight otherwise. Nothing is
//! written to disk, and backpressure from a slow consumer stalls its
//! producer.

mod reader;
pub mod registry;
mod writer;

pub use reader::PipelinedShuffleReaderExec;
pub use registry::{OpenPartition, PipelinedRegistry, ProducerKey};
pub use writer::PipelinedShuffleWriterExec;

#[cfg(test)]
mod tests;
