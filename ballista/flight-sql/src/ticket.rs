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

//! Encoding for the opaque `statement_handle` the frontend puts in
//! `TicketStatementQuery`.
//!
//! Flight treats tickets as opaque bytes, so the framing is ours to choose: a
//! one-byte tag followed by a payload. Keeping the distributed case byte-identical
//! to the `ballista.protobuf.Action` the executors already understand means the
//! handle can be forwarded to
//! [`BallistaFlightProxyService`](ballista_core::flight_proxy_service::BallistaFlightProxyService)
//! without re-encoding.

use ballista_core::error::{BallistaError, Result};

const TAG_PARTITION: u8 = 0;
const TAG_LOCAL: u8 = 1;

/// What a `TicketStatementQuery` issued by this frontend points at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StatementHandle {
    /// One shuffle partition, addressed by an encoded
    /// `ballista.protobuf.Action::FetchPartition`.
    Partition(Vec<u8>),
    /// A result the frontend computed itself, held in the result cache under
    /// this handle.
    Local(String),
}

impl StatementHandle {
    pub(crate) fn encode(&self) -> Vec<u8> {
        match self {
            Self::Partition(action) => {
                let mut buf = Vec::with_capacity(action.len() + 1);
                buf.push(TAG_PARTITION);
                buf.extend_from_slice(action);
                buf
            }
            Self::Local(handle) => {
                let mut buf = Vec::with_capacity(handle.len() + 1);
                buf.push(TAG_LOCAL);
                buf.extend_from_slice(handle.as_bytes());
                buf
            }
        }
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let (tag, payload) = bytes.split_first().ok_or_else(|| {
            BallistaError::General("empty Flight SQL statement handle".to_string())
        })?;

        match *tag {
            TAG_PARTITION => Ok(Self::Partition(payload.to_vec())),
            TAG_LOCAL => std::str::from_utf8(payload)
                .map(|handle| Self::Local(handle.to_string()))
                .map_err(|e| {
                    BallistaError::General(format!(
                        "Flight SQL statement handle is not valid UTF-8: {e}"
                    ))
                }),
            other => Err(BallistaError::General(format!(
                "unrecognized Flight SQL statement handle tag {other}"
            ))),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn partition_handle_round_trips() {
        let handle = StatementHandle::Partition(vec![1, 2, 3]);
        assert_eq!(StatementHandle::decode(&handle.encode()).unwrap(), handle);
    }

    #[test]
    fn local_handle_round_trips() {
        let handle = StatementHandle::Local("abc-123".to_string());
        assert_eq!(StatementHandle::decode(&handle.encode()).unwrap(), handle);
    }

    #[test]
    fn empty_and_unknown_handles_are_rejected() {
        assert!(StatementHandle::decode(&[]).is_err());
        assert!(StatementHandle::decode(&[99, 1, 2]).is_err());
    }
}
