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

//! Pluggable authentication for the Flight SQL frontend.
//!
//! Ballista ships no credentials of its own. The default
//! [`AnonymousAuthenticator`] accepts every handshake, which is only
//! appropriate on a trusted network; deployments that need real
//! authentication supply their own [`Authenticator`], or terminate auth in a
//! Tonic interceptor or proxy in front of the scheduler.

use async_trait::async_trait;
use tonic::Status;
use tonic::metadata::MetadataMap;

/// Who the client claims to be, as resolved from a handshake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// Username, or `None` for an unauthenticated client.
    pub user: Option<String>,
}

impl Identity {
    /// An unauthenticated identity.
    pub fn anonymous() -> Self {
        Self { user: None }
    }

    /// An identity for a named user.
    pub fn user(name: impl Into<String>) -> Self {
        Self {
            user: Some(name.into()),
        }
    }
}

/// Validates the credentials presented during `Handshake`.
///
/// The frontend calls this once per handshake and, on success, mints an opaque
/// bearer token bound to a fresh session. Per-request authorization is the
/// token check; implementations are not consulted again.
#[async_trait]
pub trait Authenticator: Send + Sync + 'static {
    /// Resolves the identity for a handshake, or returns
    /// [`Status::unauthenticated`] to reject it.
    ///
    /// `headers` are the request metadata; the Flight convention is HTTP Basic
    /// credentials in `authorization`.
    async fn authenticate(&self, headers: &MetadataMap) -> Result<Identity, Status>;

    /// Whether unauthenticated requests (those arriving with no bearer token)
    /// are allowed to fall back to the shared anonymous session.
    ///
    /// Real authenticators should leave this `false` so that a client which
    /// skips the handshake is rejected rather than silently sharing state with
    /// every other such client.
    fn allows_anonymous(&self) -> bool {
        false
    }
}

/// Accepts every handshake without checking anything.
///
/// **This performs no authentication.** It is the default so that the frontend
/// is usable out of the box on a laptop or inside a trusted cluster network;
/// the frontend logs a warning at startup when it is in use.
#[derive(Debug, Default, Clone, Copy)]
pub struct AnonymousAuthenticator;

#[async_trait]
impl Authenticator for AnonymousAuthenticator {
    async fn authenticate(&self, _headers: &MetadataMap) -> Result<Identity, Status> {
        Ok(Identity::anonymous())
    }

    fn allows_anonymous(&self) -> bool {
        true
    }
}
