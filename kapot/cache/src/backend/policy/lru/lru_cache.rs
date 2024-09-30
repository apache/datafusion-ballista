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

use crate::backend::policy::lru::ResourceCounter;
use crate::backend::policy::{lru::LruCachePolicy, CachePolicy, CachePolicyPutResult};
use hashbrown::hash_map::DefaultHashBuilder;
use hashlink::linked_hash_map::{self, IntoIter, Iter, IterMut, LinkedHashMap};
use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash};

pub struct LruCache<K, V, H = DefaultHashBuilder>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    map: LinkedHashMap<K, V, H>,
    resource_counter: Box<dyn ResourceCounter<K = K, V = V>>,
}

impl<K, V> LruCache<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    pub fn with_resource_counter<R>(resource_counter: R) -> Self
    where
        R: ResourceCounter<K = K, V = V>,
    {
        LruCache {
            map: LinkedHashMap::new(),
            resource_counter: Box::new(resource_counter),
        }
    }
}

impl<K, V, H> LruCache<K, V, H>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    pub fn with_resource_counter_and_hasher<R>(
        resource_counter: R,
        hash_builder: H,
    ) -> Self
    where
        R: ResourceCounter<K = K, V = V>,
    {
        LruCache {
            map: LinkedHashMap::with_hasher(hash_builder),
            resource_counter: Box::new(resource_counter),
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn iter(&self) -> Iter<K, V> {
        self.map.iter()
    }

    pub fn iter_mut(&mut self) -> IterMut<K, V> {
        self.map.iter_mut()
    }
}

impl<K, V, H> LruCachePolicy for LruCache<K, V, H>
where
    K: 'static + Clone + Debug + Eq + Hash + Ord + Send,
    H: 'static + BuildHasher + Debug + Send,
    V: 'static + Clone + Debug + Send,
{
    fn get_lru(&mut self, k: &Self::K) -> Option<Self::V> {
        match self.map.raw_entry_mut().from_key(k) {
            linked_hash_map::RawEntryMut::Occupied(mut occupied) => {
                occupied.to_back();
                Some(occupied.into_mut().clone())
            }
            linked_hash_map::RawEntryMut::Vacant(_) => None,
        }
    }

    fn put_lru(
        &mut self,
        k: Self::K,
        v: Self::V,
    ) -> CachePolicyPutResult<Self::K, Self::V> {
        let old_val = self.map.insert(k.clone(), v.clone());
        // Consume resources for (k, v)
        self.resource_counter.consume(&k, &v);
        // Restore resources for old (k, old_val)
        if let Some(old_val) = &old_val {
            self.resource_counter.restore(&k, old_val);
        }

        let mut popped_entries = vec![];
        while self.resource_counter.exceed_capacity() {
            if let Some(entry) = self.pop_lru() {
                popped_entries.push(entry);
            }
        }
        (old_val, popped_entries)
    }

    fn pop_lru(&mut self) -> Option<(Self::K, Self::V)> {
        if let Some(entry) = self.map.pop_front() {
            self.resource_counter.restore(&entry.0, &entry.1);
            Some(entry)
        } else {
            None
        }
    }
}

impl<K, V, H> CachePolicy for LruCache<K, V, H>
where
    K: 'static + Clone + Debug + Eq + Hash + Ord + Send,
    H: 'static + BuildHasher + Debug + Send,
    V: 'static + Clone + Debug + Send,
{
    type K = K;
    type V = V;

    fn get(&mut self, k: &Self::K) -> Option<Self::V> {
        self.get_lru(k)
    }

    fn peek(&mut self, k: &Self::K) -> Option<Self::V> {
        self.map.get(k).cloned()
    }

    fn put(&mut self, k: Self::K, v: Self::V) -> CachePolicyPutResult<Self::K, Self::V> {
        self.put_lru(k, v)
    }

    fn remove(&mut self, k: &Self::K) -> Option<Self::V> {
        if let Some(v) = self.map.remove(k) {
            self.resource_counter.restore(k, &v);
            Some(v)
        } else {
            None
        }
    }

    fn pop(&mut self) -> Option<(Self::K, Self::V)> {
        self.pop_lru()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<K, V, H> IntoIterator for LruCache<K, V, H>
where
    K: 'static + Clone + Debug + Eq + Hash + Ord + Send,
    V: 'static + Clone + Debug + Send,
{
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> IntoIter<K, V> {
        self.map.into_iter()
    }
}

impl<'a, K, V, H> IntoIterator for &'a LruCache<K, V, H>
where
    K: 'static + Clone + Debug + Eq + Hash + Ord + Send,
    V: 'static + Clone + Debug + Send,
{
    type Item = (&'a K, &'a V);
    type IntoIter = Iter<'a, K, V>;

    fn into_iter(self) -> Iter<'a, K, V> {
        self.iter()
    }
}

impl<'a, K, V, H> IntoIterator for &'a mut LruCache<K, V, H>
where
    K: 'static + Clone + Debug + Eq + Hash + Ord + Send,
    V: 'static + Clone + Debug + Send,
{
    type Item = (&'a K, &'a mut V);
    type IntoIter = IterMut<'a, K, V>;

    fn into_iter(self) -> IterMut<'a, K, V> {
        self.iter_mut()
    }
}

impl<K, V, H> Debug for LruCache<K, V, H>
where
    K: 'static + Clone + Debug + Eq + Hash + Ord + Send,
    V: 'static + Clone + Debug + Send,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_map().entries(self.iter().rev()).finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::policy::lru::lru_cache::LruCache;
    use crate::backend::policy::lru::{DefaultResourceCounter, ResourceCounter};
    use crate::backend::policy::CachePolicy;
    use hashbrown::HashMap;

    #[test]
    fn test_cache_with_lru_policy() {
        let mut cache = LruCache::with_resource_counter(DefaultResourceCounter::new(3));

        cache.put("1".to_string(), "file1".to_string());
        cache.put("2".to_string(), "file2".to_string());
        cache.put("3".to_string(), "file3".to_string());
        assert_eq!(3, cache.len());

        cache.put("4".to_string(), "file4".to_string());
        assert_eq!(3, cache.len());
        assert!(cache.peek(&"1".to_string()).is_none());

        assert!(cache.peek(&"2".to_string()).is_some());
        let mut iter = cache.iter();
        assert_eq!("2", iter.next().unwrap().0);
        assert_eq!("3", iter.next().unwrap().0);
        assert_eq!("4", iter.next().unwrap().0);

        assert!(cache.get(&"2".to_string()).is_some());
        let mut iter = cache.iter();
        assert_eq!("3", iter.next().unwrap().0);
        assert_eq!("4", iter.next().unwrap().0);
        assert_eq!("2", iter.next().unwrap().0);

        assert_eq!(Some("file4".to_string()), cache.remove(&"4".to_string()));

        assert_eq!("3".to_string(), cache.pop().unwrap().0);
        assert_eq!("2".to_string(), cache.pop().unwrap().0);
        assert!(cache.pop().is_none());
    }

    #[test]
    fn test_cache_with_size_resource_counter() {
        let mut cache =
            LruCache::with_resource_counter(get_test_size_resource_counter(50));

        cache.put("1".to_string(), "file1".to_string());
        cache.put("2".to_string(), "file2".to_string());
        cache.put("3".to_string(), "file3".to_string());
        assert_eq!(3, cache.len());

        cache.put("4".to_string(), "file4".to_string());
        assert_eq!(2, cache.len());
        assert!(cache.peek(&"1".to_string()).is_none());
        assert!(cache.peek(&"2".to_string()).is_none());

        assert!(cache.peek(&"3".to_string()).is_some());
        let mut iter = cache.iter();
        assert_eq!("3", iter.next().unwrap().0);
        assert_eq!("4", iter.next().unwrap().0);

        assert!(cache.get(&"3".to_string()).is_some());
        let mut iter = cache.iter();
        assert_eq!("4", iter.next().unwrap().0);
        assert_eq!("3", iter.next().unwrap().0);

        cache.put("5".to_string(), "file5".to_string());
        cache.put("3".to_string(), "file3-bak".to_string());
        cache.put("1".to_string(), "file1".to_string());
        let mut iter = cache.iter();
        assert_eq!("3", iter.next().unwrap().0);
        assert_eq!("1", iter.next().unwrap().0);
        assert!(iter.next().is_none());
    }

    fn get_test_size_resource_counter(max_size: usize) -> TestSizeResourceCounter {
        let mut size_map = HashMap::new();
        size_map.insert(("1".to_string(), "file1".to_string()), 10);
        size_map.insert(("2".to_string(), "file2".to_string()), 20);
        size_map.insert(("3".to_string(), "file3".to_string()), 15);
        size_map.insert(("3".to_string(), "file3-bak".to_string()), 30);
        size_map.insert(("4".to_string(), "file4".to_string()), 35);
        size_map.insert(("5".to_string(), "file5".to_string()), 25);

        TestSizeResourceCounter {
            size_map,
            max_size,
            current_size: 0,
        }
    }

    #[derive(Debug)]
    struct TestSizeResourceCounter {
        size_map: HashMap<(String, String), usize>,
        max_size: usize,
        current_size: usize,
    }

    impl ResourceCounter for TestSizeResourceCounter {
        type K = String;
        type V = String;

        fn consume(&mut self, k: &Self::K, v: &Self::V) {
            let s = self.size_map.get(&(k.clone(), v.clone())).unwrap();
            self.current_size += s;
        }

        fn restore(&mut self, k: &Self::K, v: &Self::V) {
            let s = self.size_map.get(&(k.clone(), v.clone())).unwrap();
            self.current_size -= s;
        }

        fn exceed_capacity(&self) -> bool {
            self.current_size > self.max_size
        }
    }
}
