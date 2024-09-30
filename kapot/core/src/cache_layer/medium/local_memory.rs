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

use crate::cache_layer::medium::CacheMedium;
use crate::cache_layer::object_store::ObjectStoreWithKey;
use object_store::memory::InMemory;
use object_store::path::{Path, DELIMITER};
use object_store::ObjectStore;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct LocalMemoryMedium {
    cache_object_store: Arc<InMemory>,
}

impl LocalMemoryMedium {
    pub fn new() -> Self {
        Self {
            cache_object_store: Arc::new(InMemory::new()),
        }
    }
}

impl Default for LocalMemoryMedium {
    fn default() -> Self {
        Self::new()
    }
}

impl Display for LocalMemoryMedium {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cache medium with local memory")
    }
}

impl CacheMedium for LocalMemoryMedium {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_object_store(&self) -> Arc<dyn ObjectStore> {
        self.cache_object_store.clone()
    }

    fn get_mapping_location(
        &self,
        source_location: &Path,
        source_object_store: &ObjectStoreWithKey,
    ) -> Path {
        let cache_location = format!(
            "{}{DELIMITER}{}{DELIMITER}{source_location}",
            "memory",
            source_object_store.key(),
        );
        Path::from(cache_location)
    }
}
