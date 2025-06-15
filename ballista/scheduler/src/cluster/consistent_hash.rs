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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashSet;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use log::debug;

use ballista_core::error::Result;
use ballista_core::serde::protobuf::{job_status, AvailableTaskSlots};
use ballista_core::serde::scheduler::PartitionId;
use md5::{Digest, Md5};

use crate::cluster::{
    bind_task_round_robin, BoundTask, DistributionPolicy, GetScanFilesFunc,
};

use crate::state::execution_graph::{create_task_info, TaskDescription};
use crate::state::task_manager::JobInfoCache;

//
// Custom consistent hash scheduler policy
//

#[derive(Clone)]
pub struct TopologyNode {
    pub id: String,
    //pub name: String,
    //pub last_seen_ts: u64,
    pub available_slots: u32,
}

impl TopologyNode {
    fn new(id: &str, available_slots: u32) -> Self {
        Self {
            id: id.to_string(),
            available_slots,
        }
    }
}

impl Node for TopologyNode {
    fn name(&self) -> &str {
        &self.id
    }

    fn is_valid(&self) -> bool {
        self.available_slots > 0
    }
}

///
/// ConsistentHashPolicy uses cluster and file scan information
/// to decide where to schedule tasks.
///
/// IMPORTANT: In order for policy to be functional it has to be subscribed to ClusterState
/// notifications, in order to track cluster state.
///
/// Policy will inspect stage physical plan and decide if tasks needed to be
/// scheduled using consistent hash policy. Tasks not suitable for consistent hash
/// policy will be scheduled using round robbin policy.
///
/// Word of caution:
///
/// - current implementation is very experimental
/// - it prioritizes tasks which can't be scheduled using consistent hash policy,
///   leading to possible starvation of consistent hash tasks
///
#[derive(Debug)]
pub struct ConsistentHashPolicy {
    num_replicas: usize,
    tolerance: usize,
    executors: DashSet<String>,
}

impl ConsistentHashPolicy {
    pub fn new(num_replicas: usize, tolerance: usize) -> Self {
        Self {
            num_replicas,
            tolerance,
            executors: Default::default(),
        }
    }
    /// Hash to called every time new executor has been added.
    /// This method should be called when cluster state notification
    /// has been triggered.
    pub fn add_executor(&self, executor_id: String) {
        self.executors.insert(executor_id);
    }
    /// Hash to called every time new executor has been removed
    /// This method should be called when cluster state notification
    /// has been triggered.
    pub fn remove_executor(&self, executor_in: &str) {
        self.executors.remove(executor_in);
    }

    fn get_topology_nodes(
        &self,
        slots: &Vec<&mut AvailableTaskSlots>,
    ) -> HashMap<String, TopologyNode> {
        let mut nodes: HashMap<String, TopologyNode> = HashMap::new();
        for executor_id in self.executors.iter() {
            let slots_available = slots
                .iter()
                .find(|s| s.executor_id == *executor_id)
                .map(|s| s.slots)
                .unwrap_or(0);
            let node = TopologyNode::new(&executor_id, slots_available);
            nodes.insert(node.name().to_string(), node);
        }

        nodes
    }

    pub(crate) async fn bind_task_consistent_hash(
        topology_nodes: HashMap<String, TopologyNode>,
        num_replicas: usize,
        tolerance: usize,
        running_jobs: Arc<HashMap<String, JobInfoCache>>,
        get_scan_files: GetScanFilesFunc,
    ) -> Result<(Vec<BoundTask>, Option<ConsistentHash<TopologyNode>>)> {
        let mut total_slots = 0usize;
        for (_, node) in topology_nodes.iter() {
            total_slots += node.available_slots as usize;
        }
        if total_slots == 0 {
            debug!("Not enough available executor slots for binding tasks with consistent hashing policy");
            return Ok((vec![], None));
        }
        debug!(
            "Total slot number for consistent hash binding is {}",
            total_slots
        );

        let node_replicas = topology_nodes
            .into_values()
            .map(|node| (node, num_replicas))
            .collect::<Vec<_>>();
        let mut ch_topology: ConsistentHash<TopologyNode> =
            ConsistentHash::new(node_replicas);

        let mut schedulable_tasks: Vec<BoundTask> = vec![];
        for (job_id, job_info) in running_jobs.iter() {
            if !matches!(job_info.status, Some(job_status::Status::Running(_))) {
                debug!(
                    "Job {} is not in running status and will be skipped",
                    job_id
                );
                continue;
            }
            let mut graph = job_info.execution_graph.write().await;
            let session_id = graph.session_id().to_string();
            let mut black_list = vec![];
            while let Some((running_stage, task_id_gen)) =
                graph.fetch_running_stage(&black_list)
            {
                let scan_files = get_scan_files(job_id, running_stage.plan.clone())?;
                if Self::is_skip_consistent_hash(&scan_files) {
                    debug!(
                        "Will skip stage {}/{} for consistent hashing task binding",
                        job_id, running_stage.stage_id
                    );
                    black_list.push(running_stage.stage_id);
                    continue;
                }
                debug!(
                    "Will select stage {}/{} for consistent hashing task binding",
                    job_id, running_stage.stage_id
                );
                let pre_total_slots = total_slots;
                let scan_files = &scan_files[0];
                let tolerance_list = vec![0, tolerance];
                // First round with 0 tolerance consistent hashing policy
                // Second round with [`tolerance`] tolerance consistent hashing policy
                for tolerance in tolerance_list {
                    let runnable_tasks = running_stage
                        .task_infos
                        .iter_mut()
                        .enumerate()
                        .filter(|(_partition, info)| info.is_none())
                        .take(total_slots)
                        .collect::<Vec<_>>();
                    for (partition_id, task_info) in runnable_tasks {
                        let partition_files = &scan_files[partition_id];
                        assert!(!partition_files.is_empty());
                        // Currently we choose the first file for a task for consistent hash.
                        // Later when splitting files for tasks in datafusion, it's better to
                        // introduce this hash based policy besides the file number policy or file size policy.
                        let file_for_hash = &partition_files[0];
                        if let Some(node) = ch_topology.get_mut_with_tolerance(
                            file_for_hash.object_meta.location.as_ref().as_bytes(),
                            tolerance,
                        ) {
                            let executor_id = node.id.clone();
                            let task_id = *task_id_gen;
                            *task_id_gen += 1;
                            *task_info =
                                Some(create_task_info(executor_id.clone(), task_id));

                            let partition = PartitionId {
                                job_id: job_id.clone(),
                                stage_id: running_stage.stage_id,
                                partition_id,
                            };
                            let task_desc = TaskDescription {
                                session_id: session_id.clone(),
                                partition,
                                stage_attempt_num: running_stage.stage_attempt_num,
                                task_id,
                                task_attempt: running_stage.task_failure_numbers
                                    [partition_id],
                                plan: running_stage.plan.clone(),
                                session_config: running_stage.session_config.clone(),
                            };
                            schedulable_tasks.push((executor_id, task_desc));

                            node.available_slots -= 1;
                            total_slots -= 1;
                            if total_slots == 0 {
                                return Ok((schedulable_tasks, Some(ch_topology)));
                            }
                        }
                    }
                }
                // Since there's no more tasks from this stage which can be bound,
                // we should skip this stage at the next round.
                if pre_total_slots == total_slots {
                    black_list.push(running_stage.stage_id);
                }
            }
        }

        Ok((schedulable_tasks, Some(ch_topology)))
    }

    // If if there's no plan which needs to scan files, skip it.
    // Or there are multiple plans which need to scan files for a stage, skip it.
    pub(crate) fn is_skip_consistent_hash(
        scan_files: &[Vec<Vec<PartitionedFile>>],
    ) -> bool {
        scan_files.is_empty() || scan_files.len() > 1
    }

    /// Get all of the [`PartitionedFile`] to be scanned for an [`ExecutionPlan`]
    pub(crate) fn get_scan_files(
        plan: Arc<dyn ExecutionPlan>,
    ) -> std::result::Result<Vec<Vec<Vec<PartitionedFile>>>, DataFusionError> {
        let mut collector: Vec<Vec<Vec<PartitionedFile>>> = vec![];
        plan.apply(&mut |plan: &Arc<dyn ExecutionPlan>| {
            let plan_any = plan.as_any();

            if let Some(config) = plan_any
                .downcast_ref::<DataSourceExec>()
                .and_then(|c| c.data_source().as_any().downcast_ref::<FileScanConfig>())
            {
                collector.push(
                    config
                        .file_groups
                        .iter()
                        .map(|f| f.clone().into_inner())
                        .collect(),
                );
                Ok(TreeNodeRecursion::Jump)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })?;
        Ok(collector)
    }
}

#[async_trait::async_trait]
impl DistributionPolicy for ConsistentHashPolicy {
    /// 1. Firstly, try to bind tasks without scanning source files by [`RoundRobin`] policy.
    /// 2. Then for a task for scanning source files, firstly calculate a hash value based on input files.
    ///    And then bind it with an execute according to consistent hashing policy.
    /// 3. If needed, work stealing can be enabled based on the tolerance of the consistent hashing.
    async fn bind_tasks(
        &self,
        mut slots: Vec<&mut AvailableTaskSlots>,
        running_jobs: Arc<HashMap<String, JobInfoCache>>,
    ) -> datafusion::error::Result<Vec<BoundTask>> {
        // TODO this is wrong
        let topology_nodes = self.get_topology_nodes(&slots);

        let mut bound_tasks = bind_task_round_robin(
            &mut slots,
            running_jobs.clone(),
            |stage_plan: Arc<dyn ExecutionPlan>| {
                if let Ok(scan_files) = Self::get_scan_files(stage_plan) {
                    // Should be opposite to consistent hash ones.
                    !Self::is_skip_consistent_hash(&scan_files)
                } else {
                    false
                }
            },
        )
        .await;

        debug!("{} tasks bound by round robin policy", bound_tasks.len());
        let (bound_tasks_consistent_hash, ch_topology) = Self::bind_task_consistent_hash(
            topology_nodes,
            self.num_replicas,
            self.tolerance,
            running_jobs,
            |_, plan| Self::get_scan_files(plan),
        )
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        debug!(
            "{} tasks bound by consistent hashing policy",
            bound_tasks_consistent_hash.len()
        );
        if !bound_tasks_consistent_hash.is_empty() {
            bound_tasks.extend(bound_tasks_consistent_hash);
            // Update the available slots after consistent hash
            // allocated tasks
            let ch_topology = ch_topology.unwrap();
            for node in ch_topology.nodes() {
                if let Some(data) = slots.iter_mut().find(|n| n.executor_id == node.id) {
                    data.slots = node.available_slots;
                } else {
                    log::warn!("Fail to find executor data for {}", &node.id);
                }
            }
        }
        Ok(bound_tasks)
    }

    fn name(&self) -> &str {
        "ConsistentHashPolicy"
    }
}

pub trait Node {
    fn name(&self) -> &str;

    fn is_valid(&self) -> bool;
}

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
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::datasource::listing::PartitionedFile;
    use object_store::path::Path;
    use object_store::ObjectMeta;

    use ballista_core::error::Result;

    use crate::cluster::consistent_hash::{
        ConsistentHash, ConsistentHashPolicy, Node, TopologyNode,
    };
    use crate::cluster::test::{get_result, mock_active_jobs};

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
            assert_eq!(
                consistent_hash.get(key.as_bytes()).unwrap().name(),
                nodes[i].name()
            );
        }

        (consistent_hash, nodes, keys)
    }

    #[tokio::test]
    async fn test_bind_task_consistent_hash() -> Result<()> {
        let num_partition = 8usize;
        let active_jobs = mock_active_jobs(num_partition).await?;
        let active_jobs = Arc::new(active_jobs);
        let topology_nodes = mock_topology_nodes();
        let num_replicas = 31;
        let tolerance = 0;

        // Check none scan files case
        {
            let (bound_tasks, _) = ConsistentHashPolicy::bind_task_consistent_hash(
                topology_nodes.clone(),
                num_replicas,
                tolerance,
                active_jobs.clone(),
                |_, _| Ok(vec![]),
            )
            .await?;
            assert_eq!(0, bound_tasks.len());
        }
        log::info!("consistent hash ...");
        // Check job_b with scan files
        {
            let (bound_tasks, _) = ConsistentHashPolicy::bind_task_consistent_hash(
                topology_nodes,
                num_replicas,
                tolerance,
                active_jobs,
                |job_id, _| mock_get_scan_files("job_b", job_id, 8),
            )
            .await?;
            assert_eq!(6, bound_tasks.len());

            let result = get_result(bound_tasks);

            let mut expected = HashMap::new();
            {
                let mut entry_b = HashMap::new();
                entry_b.insert("executor_3".to_string(), 2);
                entry_b.insert("executor_2".to_string(), 3);
                entry_b.insert("executor_1".to_string(), 1);

                expected.insert("job_b".to_string(), entry_b);
            }
            assert!(
                expected.eq(&result),
                "The result {:?} is not as expected {:?}",
                result,
                expected
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_bind_task_consistent_hash_with_tolerance() -> Result<()> {
        let num_partition = 8usize;
        let active_jobs = mock_active_jobs(num_partition).await?;
        let active_jobs = Arc::new(active_jobs);
        let topology_nodes = mock_topology_nodes();
        let num_replicas = 31;
        let tolerance = 1;

        {
            let (bound_tasks, _) = ConsistentHashPolicy::bind_task_consistent_hash(
                topology_nodes,
                num_replicas,
                tolerance,
                active_jobs,
                |job_id, _| mock_get_scan_files("job_b", job_id, 8),
            )
            .await?;
            assert_eq!(7, bound_tasks.len());

            let result = get_result(bound_tasks);

            let mut expected = HashMap::new();
            {
                let mut entry_b = HashMap::new();
                entry_b.insert("executor_3".to_string(), 3);
                entry_b.insert("executor_2".to_string(), 3);
                entry_b.insert("executor_1".to_string(), 1);

                expected.insert("job_b".to_string(), entry_b);
            }
            assert!(
                expected.eq(&result),
                "The result {:?} is not as expected {:?}",
                result,
                expected
            );
        }

        Ok(())
    }

    fn mock_topology_nodes() -> HashMap<String, TopologyNode> {
        let mut topology_nodes = HashMap::new();
        topology_nodes
            .insert("executor_1".to_string(), TopologyNode::new("executor_1", 1));
        topology_nodes
            .insert("executor_2".to_string(), TopologyNode::new("executor_2", 3));
        topology_nodes
            .insert("executor_3".to_string(), TopologyNode::new("executor_3", 5));
        topology_nodes
    }

    fn mock_get_scan_files(
        expected_job_id: &str,
        job_id: &str,
        num_partition: usize,
    ) -> datafusion::common::Result<Vec<Vec<Vec<PartitionedFile>>>> {
        Ok(if expected_job_id.eq(job_id) {
            mock_scan_files(num_partition)
        } else {
            vec![]
        })
    }

    fn mock_scan_files(num_partition: usize) -> Vec<Vec<Vec<PartitionedFile>>> {
        let mut scan_files = vec![];
        for i in 0..num_partition {
            scan_files.push(vec![PartitionedFile {
                object_meta: ObjectMeta {
                    location: Path::from(format!("file--{}", i)),
                    last_modified: Default::default(),
                    size: 1,
                    e_tag: None,
                    version: None,
                },
                partition_values: vec![],
                range: None,
                extensions: None,
                statistics: None,
                metadata_size_hint: None,
            }]);
        }
        vec![scan_files]
    }
}
