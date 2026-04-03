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

//! Riffle (Uniffle) shuffle service client.
//!
//! Provides a high-level client for interacting with a Riffle/Uniffle
//! remote shuffle service cluster (coordinator + shuffle servers).

use crate::config::RiffleConfig;
use crate::error::{Result, RiffleError};
use crate::proto::{
    coordinator_server_client::CoordinatorServerClient,
    shuffle_server_client::ShuffleServerClient, AppHeartBeatRequest,
    ApplicationInfoRequest, FinishShuffleRequest, GetLocalShuffleDataRequest,
    GetLocalShuffleIndexRequest, GetMemoryShuffleDataRequest, GetShuffleServerRequest,
    PartitionRangeAssignment, PartitionToBlockIds, ReportShuffleResultRequest,
    RequireBufferRequest, SendShuffleDataRequest, ShuffleBlock, ShuffleCommitRequest,
    ShuffleData, ShufflePartitionRange, ShuffleRegisterRequest,
};
use dashmap::DashMap;
use log::debug;
use std::sync::atomic::{AtomicI64, Ordering};
use tonic::transport::Channel;

/// Metadata for a single block within a partition's concatenated data.
///
/// When multiple mappers write to the same partition, Riffle stores each
/// mapper's data as a separate block. The reader receives all blocks
/// concatenated with segments describing the boundaries.
#[derive(Debug, Clone)]
pub struct BlockSegment {
    /// Block identifier.
    pub block_id: i64,
    /// Byte offset within the concatenated data blob.
    pub offset: i64,
    /// Compressed length in bytes.
    pub length: i32,
    /// Uncompressed length in bytes.
    pub uncompress_length: i32,
    /// CRC32 checksum.
    pub crc: i64,
    /// Which mapper task produced this block.
    pub task_attempt_id: i64,
}

/// Result of reading shuffle data for a partition.
///
/// Contains the raw concatenated bytes and segment metadata describing
/// where each block (from each mapper) starts and ends.
#[derive(Debug)]
pub struct ShuffleReadResult {
    /// Concatenated bytes from all blocks for this partition.
    pub data: Vec<u8>,
    /// Segment metadata for each block within `data`.
    /// Each segment corresponds to one mapper's contribution.
    pub segments: Vec<BlockSegment>,
}

/// High-level client for the Riffle (Uniffle) remote shuffle service.
///
/// Wraps the coordinator and shuffle server gRPC clients, providing
/// connection pooling and the full shuffle lifecycle.
pub struct RiffleClient {
    config: RiffleConfig,
    coordinator: CoordinatorServerClient<Channel>,
    /// Cached connections to shuffle servers: "host:port" -> client
    server_cache: DashMap<String, ShuffleServerClient<Channel>>,
    /// Monotonic block ID counter
    next_block_id: AtomicI64,
}

impl RiffleClient {
    /// Connect to a Riffle coordinator.
    pub async fn connect(config: RiffleConfig) -> Result<Self> {
        let endpoint = config.coordinator_endpoint();
        debug!("Connecting to Riffle coordinator at {endpoint}");
        let coordinator = CoordinatorServerClient::connect(endpoint).await?;

        Ok(Self {
            config,
            coordinator,
            server_cache: DashMap::new(),
            next_block_id: AtomicI64::new(0),
        })
    }

    /// Get or create a shuffle server client for the given host:port.
    async fn get_server_client(
        &self,
        host: &str,
        port: i32,
    ) -> Result<ShuffleServerClient<Channel>> {
        let key = format!("{host}:{port}");
        if let Some(client) = self.server_cache.get(&key) {
            return Ok(client.clone());
        }

        let endpoint = self.config.server_endpoint(host, port);
        debug!("Connecting to Riffle shuffle server at {endpoint}");
        let client = ShuffleServerClient::connect(endpoint).await?;
        self.server_cache.insert(key, client.clone());
        Ok(client)
    }

    /// Register a shuffle with the shuffle server.
    pub async fn register_shuffle(
        &self,
        shuffle_id: i32,
        num_partitions: i32,
        server_host: &str,
        server_port: i32,
    ) -> Result<()> {
        let mut client = self.get_server_client(server_host, server_port).await?;
        let request = ShuffleRegisterRequest {
            app_id: self.config.app_id.clone(),
            shuffle_id,
            partition_ranges: vec![ShufflePartitionRange {
                start: 0,
                end: num_partitions - 1,
            }],
            remote_storage: None,
            user: String::new(),
            shuffle_data_distribution: 0, // NORMAL
            max_concurrency_per_partition_to_write: 1,
            merge_context: None,
            properties: Default::default(),
        };

        let response = client.register_shuffle(request).await?.into_inner();
        check_status(response.status, &response.ret_msg)?;
        Ok(())
    }

    /// Request buffer space on a shuffle server before sending data.
    /// Retries on transient NO_BUFFER errors with exponential backoff.
    pub async fn require_buffer(
        &self,
        shuffle_id: i32,
        require_size: i32,
        partition_ids: Vec<i32>,
        server_host: &str,
        server_port: i32,
    ) -> Result<i64> {
        let mut delay = std::time::Duration::from_millis(100);
        let max_retries = 5;

        for attempt in 0..=max_retries {
            let mut client = self.get_server_client(server_host, server_port).await?;
            let request = RequireBufferRequest {
                require_size,
                app_id: self.config.app_id.clone(),
                shuffle_id,
                partition_ids: partition_ids.clone(),
                partition_require_sizes: vec![],
            };

            let response = client.require_buffer(request).await?.into_inner();

            // NO_BUFFER (2) and NO_BUFFER_FOR_HUGE_PARTITION (10) are retryable
            if response.status == 2 || response.status == 10 {
                if attempt < max_retries {
                    debug!(
                        "Riffle require_buffer returned NO_BUFFER (attempt {}/{}), retrying in {:?}",
                        attempt + 1, max_retries, delay
                    );
                    tokio::time::sleep(delay).await;
                    delay = std::cmp::min(delay * 2, std::time::Duration::from_secs(5));
                    continue;
                }
            }

            check_status(response.status, &response.ret_msg)?;
            return Ok(response.require_buffer_id);
        }

        Err(RiffleError::General(
            "require_buffer: max retries exceeded".to_string(),
        ))
    }

    /// Send shuffle data (blocks) to a shuffle server.
    /// Returns the block_id used for this data.
    /// Retries on transient gRPC errors with exponential backoff.
    pub async fn send_shuffle_data(
        &self,
        shuffle_id: i32,
        require_buffer_id: i64,
        partition_id: i32,
        data: Vec<u8>,
        task_attempt_id: i64,
        server_host: &str,
        server_port: i32,
    ) -> Result<i64> {
        let sequence_no = self.next_block_id.fetch_add(1, Ordering::Relaxed);
        let block_id = encode_block_id(sequence_no, partition_id, task_attempt_id);
        let uncompress_length = data.len() as i32;
        let length = data.len() as i32;
        let crc = crc32_of(&data);

        let max_retries = 3;
        let mut delay = std::time::Duration::from_millis(200);

        for attempt in 0..=max_retries {
            let block = ShuffleBlock {
                block_id,
                length,
                uncompress_length,
                crc,
                data: data.clone(),
                task_attempt_id,
            };

            let shuffle_data = ShuffleData {
                partition_id,
                block: vec![block],
            };

            let mut client = self.get_server_client(server_host, server_port).await?;
            let request = SendShuffleDataRequest {
                app_id: self.config.app_id.clone(),
                shuffle_id,
                require_buffer_id,
                shuffle_data: vec![shuffle_data],
                timestamp: current_timestamp_ms(),
                stage_attempt_number: 0,
            };

            match client.send_shuffle_data(request).await {
                Ok(resp) => {
                    let response = resp.into_inner();
                    check_status(response.status, &response.ret_msg)?;
                    return Ok(block_id);
                }
                Err(status) if is_retryable_grpc_error(&status) && attempt < max_retries => {
                    debug!(
                        "Riffle send_shuffle_data transient error (attempt {}/{}): {}, retrying in {:?}",
                        attempt + 1, max_retries, status, delay
                    );
                    tokio::time::sleep(delay).await;
                    delay = std::cmp::min(delay * 2, std::time::Duration::from_secs(5));
                }
                Err(status) => return Err(status.into()),
            }
        }

        Err(RiffleError::General(
            "send_shuffle_data: max retries exceeded".to_string(),
        ))
    }

    /// Report shuffle result to the server (tells it which blocks are available).
    pub async fn report_shuffle_result(
        &self,
        shuffle_id: i32,
        partition_block_ids: Vec<(i32, Vec<i64>)>,
        server_host: &str,
        server_port: i32,
    ) -> Result<()> {
        let mut client = self.get_server_client(server_host, server_port).await?;

        let partition_to_block_ids = partition_block_ids
            .into_iter()
            .map(|(part_id, block_ids)| PartitionToBlockIds {
                partition_id: part_id,
                block_ids,
            })
            .collect();

        let request = ReportShuffleResultRequest {
            app_id: self.config.app_id.clone(),
            shuffle_id,
            task_attempt_id: 0,
            bitmap_num: 1,
            partition_to_block_ids,
            partition_stats: vec![],
        };

        let response = client.report_shuffle_result(request).await?.into_inner();
        check_status(response.status, &response.ret_msg)?;
        Ok(())
    }

    /// Commit a shuffle task after all data has been sent.
    pub async fn commit_shuffle_task(
        &self,
        shuffle_id: i32,
        server_host: &str,
        server_port: i32,
    ) -> Result<()> {
        let mut client = self.get_server_client(server_host, server_port).await?;
        let request = ShuffleCommitRequest {
            app_id: self.config.app_id.clone(),
            shuffle_id,
        };

        let response = client.commit_shuffle_task(request).await?.into_inner();
        check_status(response.status, &response.ret_msg)?;
        Ok(())
    }

    /// Get shuffle server assignments for a shuffle from the coordinator.
    pub async fn get_shuffle_assignments(
        &self,
        shuffle_id: i32,
        num_partitions: i32,
    ) -> Result<Vec<PartitionRangeAssignment>> {
        let mut coordinator = self.coordinator.clone();
        let request = GetShuffleServerRequest {
            client_host: String::new(),
            client_port: String::new(),
            client_property: String::new(),
            application_id: self.config.app_id.clone(),
            shuffle_id,
            partition_num: num_partitions,
            partition_num_per_range: 1,
            data_replica: self.config.data_replica_write,
            require_tags: vec![],
            assignment_shuffle_server_number: 0,
            estimate_task_concurrency: 0,
            faulty_server_ids: vec![],
        };

        let response = coordinator
            .get_shuffle_assignments(request)
            .await?
            .into_inner();
        check_status(response.status, &response.ret_msg)?;
        Ok(response.assignments)
    }

    /// Register this application with the coordinator.
    /// Should be called once per job before any shuffle operations.
    pub async fn register_application(&self) -> Result<()> {
        let mut coordinator = self.coordinator.clone();
        let request = ApplicationInfoRequest {
            app_id: self.config.app_id.clone(),
            user: String::new(),
            version: None,
            git_commit_id: None,
        };
        let response = coordinator
            .register_application_info(request)
            .await?
            .into_inner();
        check_status(response.status, &response.ret_msg)?;
        Ok(())
    }

    /// Send a heartbeat to the coordinator to keep the application alive.
    pub async fn send_app_heartbeat(&self) -> Result<()> {
        let mut coordinator = self.coordinator.clone();
        let request = AppHeartBeatRequest {
            app_id: self.config.app_id.clone(),
        };
        let response = coordinator.app_heartbeat(request).await?.into_inner();
        check_status(response.status, &response.ret_msg)?;
        Ok(())
    }

    /// Send a heartbeat to a shuffle server to keep the app data alive.
    pub async fn send_server_heartbeat(
        &self,
        server_host: &str,
        server_port: i32,
    ) -> Result<()> {
        let mut client = self.get_server_client(server_host, server_port).await?;
        let request = AppHeartBeatRequest {
            app_id: self.config.app_id.clone(),
        };
        let response = client.app_heartbeat(request).await?.into_inner();
        check_status(response.status, &response.ret_msg)?;
        Ok(())
    }

    /// Mark a shuffle as finished on a shuffle server.
    /// Called after all mappers have pushed data for a stage.
    pub async fn finish_shuffle(
        &self,
        shuffle_id: i32,
        server_host: &str,
        server_port: i32,
    ) -> Result<()> {
        let mut client = self.get_server_client(server_host, server_port).await?;
        let request = FinishShuffleRequest {
            app_id: self.config.app_id.clone(),
            shuffle_id,
        };
        let response = client.finish_shuffle(request).await?.into_inner();
        check_status(response.status, &response.ret_msg)?;
        Ok(())
    }

    /// Unregister the application and release all shuffle data on all known servers.
    /// Read shuffle data from a shuffle server for a specific partition.
    ///
    /// Returns the concatenated data bytes and segment metadata describing
    /// where each block (from each mapper) starts and ends within the data.
    /// Tries memory storage first, then falls back to local file storage.
    pub async fn get_shuffle_data(
        &self,
        shuffle_id: i32,
        partition_id: i32,
        server_host: &str,
        server_port: i32,
    ) -> Result<ShuffleReadResult> {
        let mut client = self.get_server_client(server_host, server_port).await?;

        // Try memory first, with pagination for large partitions
        let mut all_data = Vec::new();
        let mut all_segments = Vec::new();
        let mut last_block_id: i64 = -1;

        loop {
            let mem_request = GetMemoryShuffleDataRequest {
                app_id: self.config.app_id.clone(),
                shuffle_id,
                partition_id,
                last_block_id,
                read_buffer_size: 64 * 1024 * 1024, // 64MB per page
                timestamp: current_timestamp_ms(),
                serialized_expected_task_ids_bitmap: None,
            };

            let mem_response = client
                .get_memory_shuffle_data(mem_request)
                .await?
                .into_inner();
            check_status(mem_response.status, &mem_response.ret_msg)?;

            if mem_response.data.is_empty()
                && mem_response.shuffle_data_block_segments.is_empty()
            {
                break;
            }

            // Adjust segment offsets relative to our accumulated data
            let data_offset = all_data.len() as i64;
            for seg in &mem_response.shuffle_data_block_segments {
                last_block_id = last_block_id.max(seg.block_id);
                all_segments.push(BlockSegment {
                    block_id: seg.block_id,
                    offset: seg.offset + data_offset,
                    length: seg.length,
                    uncompress_length: seg.uncompress_length,
                    crc: seg.crc,
                    task_attempt_id: seg.task_attempt_id,
                });
            }
            all_data.extend_from_slice(&mem_response.data);

            // Check if this is the last page
            let is_end = mem_response.is_end.map(|v| v).unwrap_or(true);
            if is_end {
                break;
            }
        }

        if !all_data.is_empty() {
            return Ok(ShuffleReadResult {
                data: all_data,
                segments: all_segments,
            });
        }

        // Fall back to local file storage
        let index_request = GetLocalShuffleIndexRequest {
            app_id: self.config.app_id.clone(),
            shuffle_id,
            partition_id,
            partition_num_per_range: 1,
            partition_num: 0,
        };

        let index_response = client
            .get_local_shuffle_index(index_request)
            .await?
            .into_inner();
        check_status(index_response.status, &index_response.ret_msg)?;

        let data_file_len = index_response.data_file_len;
        if data_file_len == 0 {
            return Ok(ShuffleReadResult {
                data: Vec::new(),
                segments: Vec::new(),
            });
        }

        // Parse index data into segments (40 bytes per entry)
        let segments = parse_local_index(&index_response.index_data);

        let data_request = GetLocalShuffleDataRequest {
            app_id: self.config.app_id.clone(),
            shuffle_id,
            partition_id,
            partition_num_per_range: 1,
            partition_num: 0,
            offset: 0,
            length: data_file_len as i32,
            timestamp: current_timestamp_ms(),
            storage_id: 0,
            next_read_segments: vec![],
        };

        let data_response = client
            .get_local_shuffle_data(data_request)
            .await?
            .into_inner();
        check_status(data_response.status, &data_response.ret_msg)?;

        Ok(ShuffleReadResult {
            data: data_response.data,
            segments,
        })
    }

    /// Returns the app ID for this client.
    pub fn app_id(&self) -> &str {
        &self.config.app_id
    }

    /// Returns a reference to the config.
    pub fn config(&self) -> &RiffleConfig {
        &self.config
    }
}

impl std::fmt::Debug for RiffleClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RiffleClient")
            .field("config", &self.config)
            .field("cached_servers", &self.server_cache.len())
            .finish()
    }
}

/// Check the Uniffle status code and return an error if not SUCCESS.
fn check_status(status: i32, message: &str) -> Result<()> {
    // StatusCode::SUCCESS = 0
    if status != 0 {
        return Err(RiffleError::ServerError {
            status,
            message: message.to_string(),
        });
    }
    Ok(())
}

/// Returns true for gRPC status codes that indicate transient errors worth retrying.
fn is_retryable_grpc_error(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::ResourceExhausted
            | tonic::Code::Aborted
    )
}

/// Parse the local index binary data into BlockSegments.
///
/// Each index entry is 40 bytes (Riffle's index_codec format):
///   offset: i64 (8 bytes) — byte offset in the .data file
///   length: i32 (4 bytes) — compressed block size
///   uncompress_length: i32 (4 bytes) — uncompressed block size
///   crc: i64 (8 bytes) — CRC32 checksum
///   block_id: i64 (8 bytes) — block identifier
///   task_attempt_id: i64 (8 bytes) — mapper task ID
const INDEX_ENTRY_SIZE: usize = 40;

fn parse_local_index(index_data: &[u8]) -> Vec<BlockSegment> {
    let entry_count = index_data.len() / INDEX_ENTRY_SIZE;
    let mut segments = Vec::with_capacity(entry_count);

    for i in 0..entry_count {
        let base = i * INDEX_ENTRY_SIZE;
        let offset = i64::from_be_bytes(index_data[base..base + 8].try_into().unwrap());
        let length =
            i32::from_be_bytes(index_data[base + 8..base + 12].try_into().unwrap());
        let uncompress_length =
            i32::from_be_bytes(index_data[base + 12..base + 16].try_into().unwrap());
        let crc =
            i64::from_be_bytes(index_data[base + 16..base + 24].try_into().unwrap());
        let block_id =
            i64::from_be_bytes(index_data[base + 24..base + 32].try_into().unwrap());
        let task_attempt_id =
            i64::from_be_bytes(index_data[base + 32..base + 40].try_into().unwrap());

        segments.push(BlockSegment {
            block_id,
            offset,
            length,
            uncompress_length,
            crc,
            task_attempt_id,
        });
    }
    segments
}

/// Simple CRC32 checksum for data integrity.
fn crc32_of(data: &[u8]) -> i64 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    (crc ^ 0xFFFF_FFFF) as i64
}

/// Current timestamp in milliseconds.
fn current_timestamp_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Encode a block ID using Uniffle's layout: sequence_no(18) | partition_id(24) | task_attempt_id(21).
/// Total: 63 bits (MSB is sign bit, always 0 for positive values).
const PARTITION_ID_BITS: u32 = 24;
const TASK_ATTEMPT_ID_BITS: u32 = 21;

fn encode_block_id(sequence_no: i64, partition_id: i32, task_attempt_id: i64) -> i64 {
    let seq = sequence_no & ((1 << 18) - 1);
    let part = (partition_id as i64) & ((1 << PARTITION_ID_BITS) - 1);
    let task = task_attempt_id & ((1 << TASK_ATTEMPT_ID_BITS) - 1);
    (seq << (PARTITION_ID_BITS + TASK_ATTEMPT_ID_BITS))
        | (part << TASK_ATTEMPT_ID_BITS)
        | task
}
