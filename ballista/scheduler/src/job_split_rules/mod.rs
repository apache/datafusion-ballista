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

//! Job Split Rules
//!
//! This module provides a trait-based system for splitting jobs into
//! dependent sub-jobs. Rules can be registered and applied automatically
//! when processing queries.

use ballista_core::error::Result;
use datafusion::logical_expr::LogicalPlan;
use std::sync::Arc;

/// Represents a split job with upstream and downstream components
#[derive(Debug, Clone)]
pub struct SplitJobPlan {
    /// The upstream job plan (executes first)
    pub upstream_plan: Arc<LogicalPlan>,
    /// The downstream job plan (waits for upstream to complete)
    pub downstream_plan: Arc<LogicalPlan>,
}

/// Trait for job splitting rules
pub trait JobSplitRule: Send + Sync {
    /// Returns the name of this rule (for logging)
    fn name(&self) -> &str;

    /// Checks if this rule can split the given logical plan
    fn can_split(&self, plan: &LogicalPlan) -> Result<bool>;

    /// Applies the split rule to the logical plan
    fn apply(&self, plan: &LogicalPlan) -> Result<SplitJobPlan>;
}

/// Registry for job split rules
///
/// Manages a collection of rules and provides methods to apply them to plans.
#[derive(Clone)]
pub struct JobSplitRuleRegistry {
    rules: Vec<Arc<dyn JobSplitRule>>,
}

impl JobSplitRuleRegistry {
    /// Creates a new empty rule registry
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Creates a registry with default rules
    pub fn with_defaults() -> Self {
        // Default rules can be registered here
        // registry.register(Arc::new(SomeRule::new()));
        Self::new()
    }

    /// Registers a new rule
    pub fn register(&mut self, rule: Arc<dyn JobSplitRule>) {
        self.rules.push(rule);
    }

    /// Attempts to split a plan by trying all registered rules
    pub fn try_split(&self, plan: &LogicalPlan) -> Result<Option<SplitJobPlan>> {
        for rule in &self.rules {
            if rule.can_split(plan)? {
                log::info!("Rule '{}' matched, applying split", rule.name());
                return Ok(Some(rule.apply(plan)?));
            }
        }
        Ok(None)
    }

    /// Returns the number of registered rules
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }

    /// Returns the names of all registered rules
    pub fn rule_names(&self) -> Vec<&str> {
        self.rules.iter().map(|r| r.name()).collect()
    }
}

impl Default for JobSplitRuleRegistry {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyRule {
        name: String,
        should_match: bool,
    }

    impl JobSplitRule for DummyRule {
        fn name(&self) -> &str {
            &self.name
        }

        fn can_split(&self, _plan: &LogicalPlan) -> Result<bool> {
            Ok(self.should_match)
        }

        fn apply(&self, plan: &LogicalPlan) -> Result<SplitJobPlan> {
            Ok(SplitJobPlan {
                upstream_plan: Arc::new(plan.clone()),
                downstream_plan: Arc::new(plan.clone()),
            })
        }
    }

    #[test]
    fn test_registry_registration() {
        let mut registry = JobSplitRuleRegistry::new();
        assert_eq!(registry.rule_count(), 0);

        registry.register(Arc::new(DummyRule {
            name: "Rule1".to_string(),
            should_match: false,
        }));

        assert_eq!(registry.rule_count(), 1);
        assert_eq!(registry.rule_names(), vec!["Rule1"]);
    }

    #[test]
    fn test_default_registry() {
        let registry = JobSplitRuleRegistry::with_defaults();
        assert!(registry.rule_count() == 0);
    }
}
