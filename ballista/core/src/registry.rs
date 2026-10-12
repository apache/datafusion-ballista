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

use datafusion::common::DataFusionError;
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::functions::all_default_functions;
use datafusion::functions_aggregate::all_default_aggregate_functions;
use datafusion::functions_nested::all_default_higher_order_functions;
use datafusion::functions_window::all_default_window_functions;
use datafusion::logical_expr::HigherOrderUDF;
use datafusion::logical_expr::planner::ExprPlanner;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// A function registry containing scalar, aggregate, window, and table functions for Ballista.
#[derive(Debug)]
pub struct BallistaFunctionRegistry {
    /// Scalar user-defined functions.
    pub scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    /// Aggregate user-defined functions.
    pub aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    /// Window user-defined functions.
    pub window_functions: HashMap<String, Arc<WindowUDF>>,
    /// Higher-order (lambda) user-defined functions.
    pub higher_order_functions: HashMap<String, Arc<HigherOrderUDF>>,
}

impl Default for BallistaFunctionRegistry {
    fn default() -> Self {
        let scalar_functions: HashMap<String, Arc<ScalarUDF>> = all_default_functions()
            .into_iter()
            .map(|f| (f.name().to_string(), f))
            .collect();

        let aggregate_functions: HashMap<String, Arc<AggregateUDF>> =
            all_default_aggregate_functions()
                .into_iter()
                .map(|f| (f.name().to_string(), f))
                .collect();

        let window_functions: HashMap<String, Arc<WindowUDF>> =
            all_default_window_functions()
                .into_iter()
                .map(|f| (f.name().to_string(), f))
                .collect();

        let higher_order_functions: HashMap<String, Arc<HigherOrderUDF>> =
            all_default_higher_order_functions()
                .into_iter()
                .map(|f| (f.name().to_string(), f))
                .collect();

        Self {
            scalar_functions,
            aggregate_functions,
            window_functions,
            higher_order_functions,
        }
    }
}

impl FunctionRegistry for BallistaFunctionRegistry {
    fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
        vec![]
    }

    fn udfs(&self) -> HashSet<String> {
        self.scalar_functions.keys().cloned().collect()
    }

    fn udafs(&self) -> HashSet<String> {
        self.aggregate_functions.keys().cloned().collect()
    }

    fn udwfs(&self) -> HashSet<String> {
        self.window_functions.keys().cloned().collect()
    }

    fn higher_order_function_names(&self) -> HashSet<String> {
        self.higher_order_functions.keys().cloned().collect()
    }

    fn higher_order_function(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Arc<HigherOrderUDF>> {
        self.higher_order_functions.get(name).cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "There is no higher-order function named \"{name}\" in the TaskContext"
            ))
        })
    }

    fn udf(&self, name: &str) -> datafusion::common::Result<Arc<ScalarUDF>> {
        let result = self.scalar_functions.get(name);

        result.cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "There is no UDF named \"{name}\" in the TaskContext"
            ))
        })
    }

    fn udaf(&self, name: &str) -> datafusion::common::Result<Arc<AggregateUDF>> {
        let result = self.aggregate_functions.get(name);

        result.cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "There is no UDAF named \"{name}\" in the TaskContext"
            ))
        })
    }

    fn udwf(&self, name: &str) -> datafusion::common::Result<Arc<WindowUDF>> {
        let result = self.window_functions.get(name);

        result.cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "There is no UDWF named \"{name}\" in the TaskContext"
            ))
        })
    }
}

impl From<&SessionState> for BallistaFunctionRegistry {
    fn from(state: &SessionState) -> Self {
        let scalar_functions = state.scalar_functions().clone();
        let aggregate_functions = state.aggregate_functions().clone();
        let window_functions = state.window_functions().clone();
        let higher_order_functions = state.higher_order_functions().clone();

        Self {
            scalar_functions,
            aggregate_functions,
            window_functions,
            higher_order_functions,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datafusion_functions_available() {
        let registry = BallistaFunctionRegistry::default();

        // DataFusion's `abs` function should always be available
        assert!(registry.udf("abs").is_ok());
        assert!(registry.udfs().contains("abs"));
    }

    #[test]
    fn test_higher_order_functions_available() {
        let registry = BallistaFunctionRegistry::default();

        // DataFusion ships built-in higher-order functions (e.g. array_filter);
        // the registry the executor runs with must carry them or those queries
        // fail to plan on the executor.
        let names = registry.higher_order_function_names();
        assert!(
            !names.is_empty(),
            "registry must expose DataFusion's built-in higher-order functions"
        );
        for name in &names {
            assert!(
                registry.higher_order_function(name).is_ok(),
                "named higher-order function {name} must be retrievable"
            );
        }
    }
}
