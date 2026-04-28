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

//! [`TimedWrite`] wraps an underlying [`Write`] and records elapsed time on
//! every `write` / `flush` call into a DataFusion [`Time`] metric.
//!
//! Used by the shuffle writers to separate raw byte-pump cost from the surrounding
//! Arrow IPC encoding cost. Outer timer covers encode + write; inner [`Time`]
//! covers only the byte-pump.

use std::io::{self, Write};
use std::time::Instant;

use datafusion::physical_plan::metrics::Time;

/// `Write` adapter that records elapsed time of inner `write` / `flush` calls.
pub struct TimedWrite<W: Write> {
    inner: W,
    time: Time,
}

impl<W: Write> TimedWrite<W> {
    /// Wraps `inner`, accumulating elapsed write time into `time`.
    pub fn new(inner: W, time: Time) -> Self {
        Self { inner, time }
    }
}

impl<W: Write> Write for TimedWrite<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let start = Instant::now();
        let result = self.inner.write(buf);
        self.time.add_duration(start.elapsed());
        result
    }

    fn flush(&mut self) -> io::Result<()> {
        let start = Instant::now();
        let result = self.inner.flush();
        self.time.add_duration(start.elapsed());
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};

    fn time_metric() -> Time {
        let set = ExecutionPlanMetricsSet::new();
        MetricBuilder::new(&set).subset_time("disk_write_time", 0)
    }

    #[test]
    fn passes_bytes_through_to_inner() {
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut writer = TimedWrite::new(&mut sink, time_metric());
            writer.write_all(b"hello world").unwrap();
            writer.flush().unwrap();
        }
        assert_eq!(sink, b"hello world");
    }

    #[test]
    fn records_nonzero_elapsed_after_many_writes() {
        let time = time_metric();
        let mut writer = TimedWrite::new(Vec::new(), time.clone());

        // Many small writes ensure cumulative elapsed clears clock resolution
        // even on very fast machines.
        for _ in 0..10_000 {
            writer.write_all(b"abcdefghij").unwrap();
        }

        assert!(
            time.value() > 0,
            "expected non-zero recorded write time, got {}",
            time.value()
        );
    }
}
