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

//! Version checks between Ballista clients and schedulers.
//!
//! Each Ballista major version moves to a new DataFusion major version, and
//! DataFusion's plan encoding can change between majors in ways that still
//! decode but mean something different. A client and a scheduler with
//! different major versions could run a query and return wrong results, so
//! they refuse to work together.
//!
//! The client sends its [`BALLISTA_VERSION`](crate::BALLISTA_VERSION) in the
//! [`BALLISTA_VERSION_HEADER`](crate::version::BALLISTA_VERSION_HEADER) gRPC
//! header when it submits a job. The scheduler rejects the submission if the
//! major versions differ, and otherwise sends its own version back in the
//! response, so the client can also detect a scheduler that is too old to
//! check.
//!
//! The major version is the unit of compatibility. Any client works with any
//! scheduler of the same major version, whatever their minor and patch
//! versions, so a change that would break an older client of the same major
//! version has to wait for the next major release. That covers everything a
//! client relies on: job submission, job status, and the shuffle fetch it uses
//! to read results from executors.
//! [`BALLISTA_PROTOCOL_VERSION`](crate::BALLISTA_PROTOCOL_VERSION) is
//! different. Schedulers and executors are upgraded together, so it can change
//! in any release.

use tonic::metadata::{MetadataMap, MetadataValue};

use crate::BALLISTA_VERSION;

/// gRPC metadata key that carries a Ballista version between a client and a
/// scheduler.
pub const BALLISTA_VERSION_HEADER: &str = "ballista-version";

/// The first major version whose clients and schedulers send
/// [`BALLISTA_VERSION_HEADER`]. A peer that doesn't send it is older.
pub const FIRST_MAJOR_WITH_VERSION_HEADER: u64 = 55;

/// Checks that a client and a scheduler can work together.
///
/// Returns an error message naming both versions when their major versions
/// differ. `None` means that side didn't send [`BALLISTA_VERSION_HEADER`], so
/// it is older than [`FIRST_MAJOR_WITH_VERSION_HEADER`]. That is a mismatch
/// once the other side is on that major version or later.
pub fn check_compatibility(
    client: Option<&str>,
    scheduler: Option<&str>,
) -> Result<(), String> {
    let compatible = match (client, scheduler) {
        (Some(client), Some(scheduler)) => {
            let client_major = major_version(client);
            client_major.is_some() && client_major == major_version(scheduler)
        }
        (Some(version), None) | (None, Some(version)) => major_version(version)
            .is_some_and(|major| major < FIRST_MAJOR_WITH_VERSION_HEADER),
        (None, None) => true,
    };
    if compatible {
        Ok(())
    } else {
        Err(format!(
            "Ballista {} cannot be used with {}. The client and the scheduler must \
             have the same major version.",
            describe("client", client),
            describe("scheduler", scheduler),
        ))
    }
}

/// Adds this build's [`BALLISTA_VERSION`] to `metadata` as
/// [`BALLISTA_VERSION_HEADER`].
pub fn insert_version_header(metadata: &mut MetadataMap) {
    metadata.insert(
        BALLISTA_VERSION_HEADER,
        MetadataValue::from_static(BALLISTA_VERSION),
    );
}

/// Returns the version a peer sent in [`BALLISTA_VERSION_HEADER`], if any.
pub fn version_from_metadata(metadata: &MetadataMap) -> Option<&str> {
    metadata
        .get(BALLISTA_VERSION_HEADER)
        .and_then(|value| value.to_str().ok())
}

fn major_version(version: &str) -> Option<u64> {
    version.split('.').next()?.parse().ok()
}

fn describe(role: &str, version: Option<&str>) -> String {
    match version {
        Some(version) => format!("{role} {version}"),
        None => format!(
            "{role} older than {FIRST_MAJOR_WITH_VERSION_HEADER}.0.0 \
             (no `{BALLISTA_VERSION_HEADER}` header)"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::check_compatibility;

    #[test]
    fn compares_major_versions() {
        // (client, scheduler, compatible)
        let cases = [
            (Some("55.0.0"), Some("55.0.0"), true),
            (Some("55.0.0"), Some("55.2.1"), true),
            (Some("54.1.0"), Some("55.0.0"), false),
            (Some("56.0.0"), Some("55.3.0"), false),
            // A side that sends no version is older than 55.0.0.
            (None, Some("55.0.0"), false),
            (None, Some("56.1.0"), false),
            (Some("55.0.0"), None, false),
            // Before 55.0.0 a missing version can't be told apart from a
            // matching one.
            (None, Some("54.0.0"), true),
            (Some("54.0.0"), None, true),
            (Some("banana"), Some("55.0.0"), false),
            (Some("55.0.0"), Some(""), false),
            (Some("dev"), Some("dev"), false),
        ];
        for (client, scheduler, compatible) in cases {
            assert_eq!(
                check_compatibility(client, scheduler).is_ok(),
                compatible,
                "client={client:?} scheduler={scheduler:?}"
            );
        }
    }

    #[test]
    fn mismatch_names_both_versions() {
        let err = check_compatibility(Some("54.1.0"), Some("55.0.0")).unwrap_err();
        assert!(err.contains("client 54.1.0"), "{err}");
        assert!(err.contains("scheduler 55.0.0"), "{err}");
    }

    #[test]
    fn missing_version_names_the_header() {
        let err = check_compatibility(None, Some("55.0.0")).unwrap_err();
        assert!(err.contains("ballista-version"), "{err}");
    }
}
