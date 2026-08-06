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

use std::path::{Path, PathBuf};

/// A filesystem-backed budget of injectable faults, shared across processes.
///
/// A budget of `n` is a directory containing `n` token files. Consuming a token
/// is `fs::remove_file`, which is atomic across processes: exactly one caller
/// can succeed for a given token, whichever executor it runs in. The directory
/// outlives task retries and executor restarts, so the budget bounds the total
/// number of injected faults for the whole run rather than per attempt.
#[derive(Debug, Clone)]
pub struct FaultBudget {
    dir: PathBuf,
}

impl FaultBudget {
    /// Create the budget directory with `tokens` tokens, replacing any existing one.
    pub fn create(dir: &Path, tokens: usize) -> std::io::Result<Self> {
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir)?;
        for i in 0..tokens {
            std::fs::write(dir.join(format!("token-{i}")), b"")?;
        }
        Ok(Self {
            dir: dir.to_path_buf(),
        })
    }

    /// Open an existing budget directory. Used by executor processes, which only
    /// ever consume; a missing directory simply means no faults are available.
    pub fn open(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn remaining(&self) -> usize {
        std::fs::read_dir(&self.dir)
            .map(|entries| entries.flatten().count())
            .unwrap_or(0)
    }

    /// Attempt to consume one token. Returns true iff this caller won the token.
    ///
    /// `remove_file` is the atomicity primitive: if two executors race for the
    /// last token, exactly one `remove_file` returns Ok and the other errors.
    pub fn try_consume(&self) -> bool {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return false;
        };
        for entry in entries.flatten() {
            if std::fs::remove_file(entry.path()).is_ok() {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use crate::budget::FaultBudget;
    use std::path::PathBuf;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("ballista-chaos-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn consumes_exactly_the_budget() {
        let dir = temp_dir("budget-exact");
        let budget = FaultBudget::create(&dir, 2).unwrap();

        assert_eq!(budget.remaining(), 2);
        assert!(budget.try_consume());
        assert!(budget.try_consume());
        assert!(
            !budget.try_consume(),
            "third consume must fail: budget was 2"
        );
        assert_eq!(budget.remaining(), 0);
    }

    #[test]
    fn zero_budget_never_consumes() {
        let dir = temp_dir("budget-zero");
        let budget = FaultBudget::create(&dir, 0).unwrap();
        assert!(!budget.try_consume());
    }

    #[test]
    fn open_sees_tokens_created_by_another_handle() {
        // This models a separate executor process reading the same budget dir.
        let dir = temp_dir("budget-shared");
        let creator = FaultBudget::create(&dir, 1).unwrap();
        let other = FaultBudget::open(&dir);

        assert!(other.try_consume(), "second handle must see the token");
        assert!(
            !creator.try_consume(),
            "token already consumed by the other handle"
        );
    }
}
