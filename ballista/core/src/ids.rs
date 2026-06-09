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

//! Strongly-typed string identifiers used throughout Ballista.
//!
//! Many entities in Ballista (jobs, executors, sessions, ...) are identified by
//! strings. Using a bare `String`/`&str` for all of them means the compiler
//! cannot tell, for example, a job id from a job name even though confusing the
//! two is an easy mistake. The newtypes in this module make those
//! values distinct at the type level while remaining as cheap as the `String`
//! they wrap.
//!
//! Each id derives the trait set Ballista relies on:
//! - hashing/equality/ordering so the ids work as map and set keys,
//! - [`std::fmt::Display`] so they format identically to the inner string in logs,
//! - [`From`] conversions and [`AsRef<str>`] for cheap construction and access at
//!   the protobuf and DataFusion boundaries,
//! - [`std::borrow::Borrow<str>`] so a `HashMap<Id, V>` can be looked up with a
//!   plain `&str` without allocating a temporary id.
//!
//! The newtypes are deliberately incompatible, so confusing one for another is
//! a compile error rather than a silent bug. This is the whole point of the
//! module, and it is enforced by the type system. The example below is
//! expected to *fail* to compile:
//!
//! ```compile_fail
//! use ballista_core::{JobId, JobName};
//!
//! fn requires_job_id(_: JobId) {}
//!
//! // A `JobName` cannot be passed where a `JobId` is expected.
//! requires_job_id(JobName::new("oops"));
//! ```

/// Defines a transparent newtype wrapping a [`String`], with the full set of
/// conversions and trait impls Ballista needs for an identifier.
macro_rules! string_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord,
            serde::Serialize, serde::Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Creates a new `", stringify!($name), "` from anything convertible into a `String`.")]
            pub fn new(s: impl Into<String>) -> Self {
                Self(s.into())
            }

            #[doc = concat!("Returns the underlying string of this `", stringify!($name), "`.")]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            #[doc = concat!("Consumes the `", stringify!($name), "`, returning the owned inner `String`.")]
            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self(s)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(s.to_owned())
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl std::borrow::Borrow<str> for $name {
            fn borrow(&self) -> &str {
                &self.0
            }
        }
    };
}

string_id! {
    /// Unique identifier for a job, created by the scheduler when a query is accepted.
    JobId
}

string_id! {
    /// Human-supplied, non-unique display name for a job.
    ///
    /// Unlike [`JobId`] this is not guaranteed to be unique and carries no
    /// scheduler semantics; it exists purely for display and diagnostics.
    JobName
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn borrow_enables_str_keyed_lookup() {
        // The reason `Borrow<str>` exists: a `JobId`-keyed map can be queried
        // with a plain `&str`. This only compiles with the `Borrow<str>` impl,
        // and only succeeds if the derived `Hash`/`Eq` agree with the `&str`.
        let mut map: HashMap<JobId, u32> = HashMap::new();
        map.insert(JobId::new("job-123"), 7);
        assert_eq!(map.get("job-123"), Some(&7));
    }
}
