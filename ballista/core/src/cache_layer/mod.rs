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

use crate::cache_layer::medium::local_disk::LocalDiskMedium;
use crate::cache_layer::medium::local_memory::LocalMemoryMedium;
use crate::cache_layer::policy::file::FileCacheLayer;
use std::sync::Arc;

pub mod medium;
pub mod object_store;
pub mod policy;

#[derive(Debug, Clone)]
pub enum CacheLayer {
    /// The local disk will be used as the cache layer medium
    /// and the cache level will be the whole file.
    LocalDiskFile(Arc<FileCacheLayer<LocalDiskMedium>>),

    /// The local memory will be used as the cache layer medium
    /// and the cache level will be the whole file.
    LocalMemoryFile(Arc<FileCacheLayer<LocalMemoryMedium>>),
}

#[cfg(test)]
mod tests {
    use ballista_cache::loading_cache::LoadingCache;
    use futures::TryStreamExt;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use object_store::{GetResultPayload, ObjectStore};
    use std::io::Write;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    use crate::cache_layer::medium::local_memory::LocalMemoryMedium;
    use crate::cache_layer::object_store::file::FileCacheObjectStore;
    use crate::cache_layer::object_store::ObjectStoreWithKey;
    use crate::cache_layer::policy::file::FileCacheLayer;
    use crate::error::{BallistaError, Result};

    #[tokio::test]
    async fn test_cache_file_to_memory() -> Result<()> {
        let test_data = "test_cache_file_to_memory";
        let test_bytes = test_data.as_bytes();

        let mut test_file = NamedTempFile::new()?;
        let source_location = Path::from(test_file.as_ref().to_str().unwrap());

        let test_size = test_file.write(test_bytes)?;
        assert_eq!(test_bytes.len(), test_size);

        // Check the testing data on the source object store
        let source_object_store = Arc::new(LocalFileSystem::new());
        let source_key = "file";
        let source_object_store_with_key = Arc::new(ObjectStoreWithKey::new(
            source_key.to_string(),
            source_object_store.clone(),
        ));
        let actual_source = source_object_store.get(&source_location).await.unwrap();
        match actual_source.payload {
            GetResultPayload::File(file, _) => {
                assert_eq!(test_bytes.len(), file.metadata()?.len() as usize);
            }
            _ => {
                return Err(BallistaError::General(
                    "File instead of data stream should be returned".to_string(),
                ))
            }
        }

        // Check the testing data on the cache object store
        let cache_medium = LocalMemoryMedium::new();
        let cache_layer = FileCacheLayer::new(1000, 1, cache_medium);
        let cache_meta = cache_layer
            .cache()
            .get(
                source_location.clone(),
                source_object_store_with_key.clone(),
            )
            .await;
        assert_eq!(test_bytes.len(), cache_meta.size);

        let cache_object_store = FileCacheObjectStore::new(
            Arc::new(cache_layer),
            source_object_store_with_key.clone(),
        );
        let actual_cache = cache_object_store.get(&source_location).await.unwrap();
        match actual_cache.payload {
            GetResultPayload::File(_, _) => {
                return Err(BallistaError::General(
                    "Data stream instead of file should be returned".to_string(),
                ))
            }
            GetResultPayload::Stream(s) => {
                let mut buf: Vec<u8> = vec![];
                s.try_fold(&mut buf, |acc, part| async move {
                    let mut part: Vec<u8> = part.into();
                    acc.append(&mut part);
                    Ok(acc)
                })
                .await
                .unwrap();
                let actual_cache_data = String::from_utf8(buf).unwrap();
                assert_eq!(test_data, actual_cache_data);
            }
        }

        test_file.close()?;

        std::mem::forget(cache_object_store);

        Ok(())
    }
}
