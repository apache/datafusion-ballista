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

use crate::udf::{chaos_delay_udf, chaos_fail_udf};
use ballista_core::registry::BallistaFunctionRegistry;
use datafusion::execution::FunctionRegistry;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::prelude::SessionConfig;
use std::sync::Arc;

/// The executor's function registry, extended with the chaos UDFs.
///
/// Starts from `BallistaFunctionRegistry::default()` (the DataFusion built-ins)
/// so overriding the registry does not silently drop the standard functions.
pub fn chaos_function_registry() -> Arc<BallistaFunctionRegistry> {
    let mut registry = BallistaFunctionRegistry::default();
    for udf in [chaos_fail_udf(), chaos_delay_udf()] {
        registry
            .scalar_functions
            .insert(udf.name().to_string(), udf);
    }
    Arc::new(registry)
}

/// The scheduler's session state, extended with the chaos UDFs.
///
/// The scheduler needs them to plan a logical plan that references them; the
/// executor needs them to decode the physical plan. Both must agree.
///
/// Note: `SessionStateBuilder::with_scalar_functions` *replaces* the builder's
/// scalar function list rather than appending to it, so calling it after
/// `with_default_features()` would silently drop every built-in (including
/// `abs`). To avoid that footgun, the chaos UDFs are registered on the already
/// built `SessionState` via `FunctionRegistry::register_udf`, which inserts
/// into the existing map instead of replacing it.
pub fn chaos_session_state(
    config: SessionConfig,
) -> datafusion::error::Result<SessionState> {
    let mut state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    for udf in [chaos_fail_udf(), chaos_delay_udf()] {
        state.register_udf(udf)?;
    }

    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_resolves_the_chaos_udfs_by_name() {
        // The executor resolves UDFs by name at plan-decode time. If these names
        // are absent, every chaos query fails to deserialize on the executor.
        let registry = chaos_function_registry();
        assert!(registry.udf("chaos_fail").is_ok());
        assert!(registry.udf("chaos_delay").is_ok());
    }

    #[test]
    fn registry_retains_the_standard_functions() {
        // Regression guard: overriding the registry must not drop the built-ins,
        // or ordinary query operators stop working on the executor.
        let registry = chaos_function_registry();
        assert!(registry.udf("abs").is_ok());
    }

    #[test]
    fn session_state_resolves_the_chaos_udfs_by_name() {
        // Mirrors registry_resolves_the_chaos_udfs_by_name, but for the
        // scheduler's session state rather than the executor's registry.
        let state = chaos_session_state(SessionConfig::new()).unwrap();
        assert!(state.udf("chaos_fail").is_ok());
        assert!(state.udf("chaos_delay").is_ok());
    }

    #[test]
    fn session_state_retains_the_standard_functions() {
        // Regression guard for the with_scalar_functions replace-vs-append
        // footgun described on chaos_session_state: the scheduler must still be
        // able to plan ordinary queries that use built-in functions.
        let state = chaos_session_state(SessionConfig::new()).unwrap();
        assert!(state.udf("abs").is_ok());
    }
}
