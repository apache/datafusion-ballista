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
//! one-byte tag followed by a payload. The distributed case carries an encoded
//! `ballista.protobuf.Action`, which is what
//! [`BallistaFlightProxyService`](ballista_core::flight_proxy_service::BallistaFlightProxyService)
//! already decodes, so redeeming a ticket needs no server-side lookup.

use ballista_core::error::{BallistaError, Result};

const TAG_PARTITION: u8 = 0;
const TAG_LOCAL: u8 = 1;
const TAG_PIPELINED: u8 = 2;

/// What a `TicketStatementQuery` issued by this frontend points at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StatementHandle {
    /// One shuffle partition, addressed by an encoded
    /// `ballista.protobuf.Action::FetchPartition`.
    Partition(Vec<u8>),
    /// A result the frontend computed itself, held in the result cache under
    /// this handle.
    Local(String),
    /// Every output partition of a pipelined job, read together and served
    /// as one stream. Handing them out as separate endpoints would let a
    /// client that reads endpoints one at a time stall the job, which cannot
    /// finish its first partition before it can write to the others.
    Pipelined {
        /// The result's schema, IPC-encoded, announced even when no batches
        /// follow.
        schema: Vec<u8>,
        /// One encoded `ballista.protobuf.Action::FetchPipelinedPartition`
        /// per partition.
        actions: Vec<Vec<u8>>,
    },
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
            Self::Pipelined { schema, actions } => {
                // Each part is prefixed with its length, as a little-endian u32.
                let mut buf = vec![TAG_PIPELINED];
                for part in std::iter::once(schema).chain(actions) {
                    buf.extend_from_slice(&(part.len() as u32).to_le_bytes());
                    buf.extend_from_slice(part);
                }
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
            TAG_PIPELINED => {
                let mut parts = vec![];
                let mut rest = payload;
                while !rest.is_empty() {
                    let truncated = || {
                        BallistaError::General(
                            "truncated pipelined Flight SQL statement handle".to_string(),
                        )
                    };
                    let (len, tail) =
                        rest.split_first_chunk::<4>().ok_or_else(truncated)?;
                    let len = u32::from_le_bytes(*len) as usize;
                    if tail.len() < len {
                        return Err(truncated());
                    }
                    let (part, tail) = tail.split_at(len);
                    parts.push(part.to_vec());
                    rest = tail;
                }
                let mut parts = parts.into_iter();
                let schema = parts.next().ok_or_else(|| {
                    BallistaError::General(
                        "pipelined Flight SQL statement handle has no schema".to_string(),
                    )
                })?;
                Ok(Self::Pipelined {
                    schema,
                    actions: parts.collect(),
                })
            }
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
    fn pipelined_handle_round_trips() {
        let handle = StatementHandle::Pipelined {
            schema: vec![9, 8, 7],
            actions: vec![vec![1, 2], vec![], vec![3; 300]],
        };
        assert_eq!(StatementHandle::decode(&handle.encode()).unwrap(), handle);

        let empty = StatementHandle::Pipelined {
            schema: vec![],
            actions: vec![],
        };
        assert_eq!(StatementHandle::decode(&empty.encode()).unwrap(), empty);
    }

    #[test]
    fn truncated_pipelined_handles_are_rejected() {
        let encoded = StatementHandle::Pipelined {
            schema: vec![9, 8, 7],
            actions: vec![vec![1, 2]],
        }
        .encode();
        for len in [2, 4, encoded.len() - 1] {
            assert!(StatementHandle::decode(&encoded[..len]).is_err(), "{len}");
        }
        // No parts at all: not even the schema.
        assert!(StatementHandle::decode(&[TAG_PIPELINED]).is_err());
    }

    #[test]
    fn empty_and_unknown_handles_are_rejected() {
        assert!(StatementHandle::decode(&[]).is_err());
        assert!(StatementHandle::decode(&[99, 1, 2]).is_err());
    }
}
