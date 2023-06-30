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

use crate::consistent_hash::node::Node;
use md5::{Digest, Md5};
use std::collections::{BTreeMap, HashMap};

pub mod node;

pub type HashFunction = fn(&[u8]) -> Vec<u8>;

pub struct ConsistentHash<N>
where
    N: Node,
{
    virtual_nodes: BTreeMap<Vec<u8>, String>,
    node_replicas: HashMap<String, (N, usize)>,
    hash_func: HashFunction,
}

impl<N> ConsistentHash<N>
where
    N: Node,
{
    pub fn new(node_replicas: Vec<(N, usize)>) -> Self {
        let consistent_hash = Self {
            virtual_nodes: BTreeMap::new(),
            node_replicas: HashMap::new(),
            hash_func: md5_hash,
        };
        consistent_hash.init(node_replicas)
    }

    pub fn new_with_hash(
        node_replicas: Vec<(N, usize)>,
        hash_func: HashFunction,
    ) -> Self {
        let consistent_hash = Self {
            virtual_nodes: BTreeMap::new(),
            node_replicas: HashMap::new(),
            hash_func,
        };
        consistent_hash.init(node_replicas)
    }

    fn init(mut self, node_replicas: Vec<(N, usize)>) -> Self {
        node_replicas.into_iter().for_each(|(node, num_replicas)| {
            self.add(node, num_replicas);
        });
        self
    }

    pub fn nodes(&self) -> Vec<&N> {
        self.node_replicas
            .values()
            .map(|(node, _)| node)
            .collect::<Vec<_>>()
    }

    pub fn nodes_mut(&mut self) -> Vec<&mut N> {
        self.node_replicas
            .values_mut()
            .map(|(node, _)| node)
            .collect::<Vec<_>>()
    }

    pub fn add(&mut self, node: N, num_replicas: usize) {
        // Remove existing ones
        self.remove(node.name());

        for i in 0..num_replicas {
            let vnode_id = format!("{}:{i}", node.name());
            let vnode_key = (self.hash_func)(vnode_id.as_bytes());
            self.virtual_nodes
                .insert(vnode_key, node.name().to_string());
        }
        self.node_replicas
            .insert(node.name().to_string(), (node, num_replicas));
    }

    pub fn remove(&mut self, node_name: &str) -> Option<(N, usize)> {
        if let Some((node, num_replicas)) = self.node_replicas.remove(node_name) {
            for i in 0..num_replicas {
                let vnode_id = format!("{}:{i}", node_name);
                let vnode_key = (self.hash_func)(vnode_id.as_bytes());
                self.virtual_nodes.remove(vnode_key.as_slice());
            }
            Some((node, num_replicas))
        } else {
            None
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&N> {
        self.get_with_tolerance(key, 0)
    }

    pub fn get_with_tolerance(&self, key: &[u8], tolerance: usize) -> Option<&N> {
        self.get_position_key(key, tolerance)
            .and_then(move |position_key| {
                self.virtual_nodes
                    .get(&position_key)
                    .map(|node_name| &(self.node_replicas.get(node_name).unwrap().0))
            })
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut N> {
        self.get_mut_with_tolerance(key, 0)
    }

    pub fn get_mut_with_tolerance(
        &mut self,
        key: &[u8],
        tolerance: usize,
    ) -> Option<&mut N> {
        self.get_position_key(key, tolerance)
            .and_then(move |position_key| {
                if let Some(node_name) = self.virtual_nodes.get(&position_key) {
                    Some(&mut (self.node_replicas.get_mut(node_name).unwrap().0))
                } else {
                    None
                }
            })
    }

    fn get_position_key(&self, key: &[u8], tolerance: usize) -> Option<Vec<u8>> {
        if self.node_replicas.is_empty() {
            return None;
        };

        let mut tolerance = if tolerance >= self.virtual_nodes.len() {
            self.virtual_nodes.len() - 1
        } else {
            tolerance
        };
        let hashed_key = (self.hash_func)(key);
        for (position_key, node_name) in self
            .virtual_nodes
            .range(hashed_key..)
            .chain(self.virtual_nodes.iter())
        {
            if let Some((node, _)) = self.node_replicas.get(node_name) {
                if node.is_valid() {
                    return Some(position_key.clone());
                }
            }
            if tolerance == 0 {
                return None;
            } else {
                tolerance -= 1;
            }
        }

        None
    }
}

pub fn md5_hash(data: &[u8]) -> Vec<u8> {
    let mut digest = Md5::default();
    digest.update(data);
    digest.finalize().to_vec()
}

#[cfg(test)]
mod test {
    use crate::consistent_hash::node::Node;
    use crate::consistent_hash::ConsistentHash;

    #[test]
    fn test_topology() {
        let (mut consistent_hash, nodes, keys) = prepare_consistent_hash();

        // Test removal case
        let (node, num_replicas) = consistent_hash.remove(nodes[3].name()).unwrap();
        for (i, key) in keys.iter().enumerate() {
            if i == 3 {
                assert_ne!(
                    consistent_hash.get(key.as_bytes()).unwrap().name(),
                    nodes[i].name()
                );
            } else {
                assert_eq!(
                    consistent_hash.get(key.as_bytes()).unwrap().name(),
                    nodes[i].name()
                );
            }
        }

        // Test adding case
        consistent_hash.add(node, num_replicas);
        for (i, key) in keys.iter().enumerate() {
            assert_eq!(
                consistent_hash.get(key.as_bytes()).unwrap().name(),
                nodes[i].name()
            );
        }
    }

    #[test]
    fn test_tolerance() {
        let (mut consistent_hash, nodes, keys) = prepare_consistent_hash();
        let (mut node, num_replicas) = consistent_hash.remove(nodes[2].name()).unwrap();
        node.available = false;
        consistent_hash.add(node, num_replicas);
        for (i, key) in keys.iter().enumerate() {
            if i == 2 {
                assert!(consistent_hash.get(key.as_bytes()).is_none());
                assert!(consistent_hash
                    .get_with_tolerance(key.as_bytes(), 1)
                    .is_some());
            } else {
                assert_eq!(
                    consistent_hash.get(key.as_bytes()).unwrap().name(),
                    nodes[i].name()
                );
            }
        }

        for (i, node) in nodes.iter().enumerate() {
            if i != 2 && i != 1 {
                let (mut node, num_replicas) =
                    consistent_hash.remove(node.name()).unwrap();
                node.available = false;
                consistent_hash.add(node, num_replicas);
            }
        }
        for (i, key) in keys.iter().enumerate() {
            if i == 1 {
                assert_eq!(
                    consistent_hash.get(key.as_bytes()).unwrap().name(),
                    nodes[i].name()
                );
            } else {
                assert!(consistent_hash.get(key.as_bytes()).is_none());
            }
            assert_eq!(
                consistent_hash
                    .get_with_tolerance(key.as_bytes(), usize::MAX)
                    .unwrap()
                    .name(),
                nodes[1].name()
            );
        }
    }

    #[derive(Clone)]
    struct ServerNode {
        name: String,
        available: bool,
    }

    impl ServerNode {
        fn new(host: &str, port: u16) -> Self {
            Self::new_with_available(host, port, true)
        }

        fn new_with_available(host: &str, port: u16, available: bool) -> Self {
            Self {
                name: format!("{host}:{port}"),
                available,
            }
        }
    }

    impl Node for ServerNode {
        fn name(&self) -> &str {
            &self.name
        }

        fn is_valid(&self) -> bool {
            self.available
        }
    }

    fn prepare_consistent_hash() -> (
        ConsistentHash<ServerNode>,
        Vec<ServerNode>,
        Vec<&'static str>,
    ) {
        let num_replicas = 20usize;

        let nodes = vec![
            ServerNode::new("localhost", 10000),
            ServerNode::new("localhost", 10001),
            ServerNode::new("localhost", 10002),
            ServerNode::new("localhost", 10003),
            ServerNode::new("localhost", 10004),
        ];

        let node_replicas = nodes
            .iter()
            .map(|node| (node.clone(), num_replicas))
            .collect::<Vec<_>>();
        let consistent_hash = ConsistentHash::new(node_replicas);

        let keys = vec!["1", "4", "5", "3", "2"];
        for (i, key) in keys.iter().enumerate() {
            // println!("{}", consistent_hash.get(key.as_bytes()).unwrap().name());
            assert_eq!(
                consistent_hash.get(key.as_bytes()).unwrap().name(),
                nodes[i].name()
            );
        }

        (consistent_hash, nodes, keys)
    }
}
