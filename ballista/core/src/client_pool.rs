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

//! Connection pool for `BallistaClient` instances.

use crate::client::BallistaClient;
use crate::error::Result;
use crate::extension::BallistaConfigGrpcEndpoint;
use crate::utils::GrpcClientConfig;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Manages a pool of reusable [BallistaClient] connections.
#[async_trait::async_trait]
pub trait BallistaClientPool: Send + Sync + Debug {
    /// Acquire an idle client for `(host, port, config)`, or create a new one if the
    /// pool is empty for that key. The returned [PooledClient] returns itself
    /// to the pool on drop.
    async fn acquire(
        &self,
        host: &str,
        port: u16,
        config: &GrpcClientConfig,
        customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    ) -> Result<PooledClient>;

    /// A client on the connection to `(host, port, config)` that every caller
    /// shares.
    ///
    /// Unlike [`acquire`](Self::acquire), nothing is checked out: any number
    /// of callers hold the connection at once and multiplex their streams
    /// over it, and it stays open until one of them
    /// [discards](SharedClient::discard) it. This is what the pipelined
    /// reader uses, so that an executor keeps one connection to each other
    /// executor instead of one per stream.
    ///
    /// The default opens a connection per call, closed when its last client
    /// is dropped. A pool that keeps one connection per key overrides it.
    async fn shared(
        &self,
        host: &str,
        port: u16,
        config: &GrpcClientConfig,
        customize_endpoint: Option<Arc<BallistaConfigGrpcEndpoint>>,
    ) -> Result<Arc<SharedClient>> {
        let pooled = self.acquire(host, port, config, customize_endpoint).await?;
        let client = (*pooled).clone();
        pooled.discard();
        Ok(Arc::new(SharedClient::new(client)))
    }

    /// Remove all idle clients that have been sitting unused longer than the
    /// configured idle timeout.
    async fn evict_idle(&self);
}

// ---------------------------------------------------------------------------
// SharedClient
// ---------------------------------------------------------------------------

/// A [BallistaClient] on a connection that any number of callers use at once.
///
/// Returned by [`BallistaClientPool::shared`]. Clients taken from it share
/// the connection, so dropping them does not close it; the pool keeps it
/// until it is [discarded](Self::discard).
pub struct SharedClient {
    client: BallistaClient,
    discarded: AtomicBool,
}

impl SharedClient {
    /// A shared client on `client`'s connection.
    pub fn new(client: BallistaClient) -> Self {
        Self {
            client,
            discarded: AtomicBool::new(false),
        }
    }

    /// A client on the shared connection.
    pub fn client(&self) -> BallistaClient {
        self.client.clone()
    }

    /// Marks the connection as gone, after a failure that suggests it is, so
    /// that the pool opens a new one for the next caller. Callers already on
    /// it keep it until they are done.
    pub fn discard(&self) {
        self.discarded.store(true, Ordering::Release);
    }

    /// Whether a caller has [discarded](Self::discard) the connection.
    pub fn is_discarded(&self) -> bool {
        self.discarded.load(Ordering::Acquire)
    }
}

impl Debug for SharedClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedClient")
            .field("discarded", &self.is_discarded())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// PooledClient guard
// ---------------------------------------------------------------------------

/// A [BallistaClient] checked out from a pool.
///
/// Implements [Deref] / [DerefMut] so it can be used exactly like a
/// [BallistaClient]. On drop, the inner client is returned to the pool
/// automatically. Call [PooledClient::discard] before dropping if the
/// connection should **not** be reused (e.g. after a transport error).
pub struct PooledClient {
    client: BallistaClient,
    /// Invoked in `Drop::drop` to push the client back into the idle deque.
    /// `None` after `discard()` is called.
    return_fn: Option<Box<dyn FnOnce(BallistaClient) + Send>>,
}

impl PooledClient {
    /// Creates new PooledClient
    pub fn new(
        client: BallistaClient,
        return_fn: Box<dyn FnOnce(BallistaClient) + Send>,
    ) -> Self {
        Self {
            client,
            return_fn: Some(return_fn),
        }
    }

    /// Close the connection instead of returning it to the pool.
    pub fn discard(mut self) {
        self.return_fn = None;
    }

    /// Keep this connection checked out until `stream`, opened on it, is
    /// dropped; if the stream fails, the connection is discarded instead of
    /// returned.
    ///
    /// Releasing the connection as soon as a stream opens would let the next
    /// caller multiplex onto it. A stream that is read lazily holds its unread
    /// data in the connection's HTTP/2 flow-control window, so enough waiting
    /// streams on one connection can starve the stream being read. Leasing
    /// keeps each pooled connection to one open stream.
    pub fn lease_stream(
        self,
        stream: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        Box::pin(LeasedStream {
            inner: stream,
            lease: Some(self),
        })
    }
}

/// A stream that holds the [PooledClient] it was opened on; see
/// [PooledClient::lease_stream].
struct LeasedStream {
    inner: SendableRecordBatchStream,
    lease: Option<PooledClient>,
}

impl Stream for LeasedStream {
    type Item = datafusion::error::Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let item = self.inner.poll_next_unpin(cx);
        if let Poll::Ready(Some(Err(_))) = &item
            && let Some(lease) = self.lease.take()
        {
            lease.discard();
        }
        item
    }
}

impl RecordBatchStream for LeasedStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Deref for PooledClient {
    type Target = BallistaClient;
    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for PooledClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

impl Drop for PooledClient {
    fn drop(&mut self) {
        if let Some(f) = self.return_fn.take() {
            let client = self.client.clone();
            f(client);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::error::DataFusionError;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A stream of `items` leased on a pooled connection, plus a count of how
    /// many times that connection was returned to the pool.
    fn leased_stream(
        items: Vec<datafusion::error::Result<RecordBatch>>,
    ) -> (SendableRecordBatchStream, Arc<AtomicUsize>) {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let returned = Arc::new(AtomicUsize::new(0));
        let lease = {
            let returned = returned.clone();
            PooledClient::new(
                BallistaClient::new_for_test("executor_1", 7070),
                Box::new(move |_| {
                    returned.fetch_add(1, Ordering::SeqCst);
                }),
            )
        };
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(items),
        ));
        (lease.lease_stream(stream), returned)
    }

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])
            .unwrap()
    }

    #[tokio::test]
    async fn leased_stream_returns_connection_only_when_dropped() {
        let (mut stream, returned) = leased_stream(vec![Ok(batch()), Ok(batch())]);

        stream.next().await.unwrap().unwrap();
        stream.next().await.unwrap().unwrap();
        assert!(stream.next().await.is_none());
        // Still checked out: no other fetch can share the connection yet.
        assert_eq!(returned.load(Ordering::SeqCst), 0);

        drop(stream);
        assert_eq!(returned.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn leased_stream_discards_connection_after_error() {
        let (mut stream, returned) = leased_stream(vec![Err(
            DataFusionError::Execution("connection reset".to_string()),
        )]);

        assert!(stream.next().await.unwrap().is_err());
        drop(stream);
        assert_eq!(returned.load(Ordering::SeqCst), 0);
    }
}
