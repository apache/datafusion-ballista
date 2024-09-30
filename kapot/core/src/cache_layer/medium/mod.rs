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

use crate::cache_layer::object_store::ObjectStoreWithKey;
use object_store::path::Path;
use object_store::ObjectStore;
use std::any::Any;
use std::fmt::{Debug, Display};
use std::sync::Arc;

pub mod local_disk;
pub mod local_memory;

pub trait CacheMedium: Debug + Send + Sync + Display + 'static {
    /// Returns the cache layer policy as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Get the ObjectStore for the cache storage
    fn get_object_store(&self) -> Arc<dyn ObjectStore>;

    /// Get the mapping location on the cache ObjectStore for the source location on the source ObjectStore
    fn get_mapping_location(
        &self,
        source_location: &Path,
        source_object_store: &ObjectStoreWithKey,
    ) -> Path;
}
