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

//! End-to-end integration tests for sort-based shuffle.
//!
//! These tests verify that the sort-based shuffle implementation produces
//! correct results for various query patterns that involve shuffling.
//!
//! Tests are parameterized to run with both local reads and remote reads
//! (via the flight service) to ensure both paths work correctly.

mod common;

#[cfg(test)]
#[cfg(feature = "standalone")]
mod sort_shuffle_tests {
    use ballista::prelude::{SessionConfigExt, SessionContextExt};
    use ballista_core::config::{
        BALLISTA_SHUFFLE_READER_FORCE_REMOTE_READ,
        BALLISTA_SHUFFLE_READER_REMOTE_PREFER_FLIGHT,
        BALLISTA_SHUFFLE_SORT_BASED_BUFFER_SIZE, BALLISTA_SHUFFLE_SORT_BASED_ENABLED,
        BALLISTA_SHUFFLE_SORT_BASED_MEMORY_LIMIT,
    };
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
    use rstest::rstest;
    use std::collections::HashSet;

    /// Read mode for shuffle data
    #[derive(Debug, Clone, Copy)]
    enum ReadMode {
        /// Read shuffle data locally (default)
        Local,
        /// Read shuffle data via flight service (remote read)
        RemoteFlight,
    }

    /// Creates a standalone session context with sort-based shuffle enabled.
    async fn create_sort_shuffle_context(read_mode: ReadMode) -> SessionContext {
        let mut config = SessionConfig::new_with_ballista()
            .set_str(BALLISTA_SHUFFLE_SORT_BASED_ENABLED, "true")
            .set_str(BALLISTA_SHUFFLE_SORT_BASED_BUFFER_SIZE, "1048576") // 1MB
            .set_str(BALLISTA_SHUFFLE_SORT_BASED_MEMORY_LIMIT, "268435456"); // 256MB

        // Configure read mode
        match read_mode {
            ReadMode::Local => {}
            ReadMode::RemoteFlight => {
                config = config
                    .set_str(BALLISTA_SHUFFLE_READER_FORCE_REMOTE_READ, "true")
                    .set_str(BALLISTA_SHUFFLE_READER_REMOTE_PREFER_FLIGHT, "true");
            }
        }

        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build();

        SessionContext::standalone_with_state(state).await.unwrap()
    }

    /// Creates a standalone session context with hash-based shuffle (default).
    async fn create_hash_shuffle_context() -> SessionContext {
        SessionContext::standalone().await.unwrap()
    }

    /// Registers test data in the context.
    async fn register_test_data(ctx: &SessionContext) {
        ctx.register_parquet(
            "test",
            "testdata/alltypes_plain.parquet",
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();
    }

    fn assert_result_eq(
        expected: Vec<&str>,
        results: &[datafusion::arrow::record_batch::RecordBatch],
    ) {
        assert_eq!(
            expected,
            pretty_format_batches(results)
                .unwrap()
                .to_string()
                .trim()
                .lines()
                .collect::<Vec<&str>>()
        );
    }

    /// Extracts values from a result set, ignoring order.
    fn extract_values_unordered(
        results: &[datafusion::arrow::record_batch::RecordBatch],
    ) -> HashSet<String> {
        pretty_format_batches(results)
            .unwrap()
            .to_string()
            .trim()
            .lines()
            .skip(3) // Skip header lines
            .filter(|line| !line.starts_with('+'))
            .map(|s| s.to_string())
            .collect()
    }

    // ==================== Basic Aggregation Tests ====================

    #[rstest]
    #[case::local(ReadMode::Local)]
    #[case::remote_flight(ReadMode::RemoteFlight)]
    #[tokio::test]
    async fn test_sort_shuffle_group_by_single_column(
        #[case] read_mode: ReadMode,
    ) -> Result<()> {
        let ctx = create_sort_shuffle_context(read_mode).await;
        register_test_data(&ctx).await;

        let df = ctx
            .sql("SELECT bool_col, COUNT(*) as cnt FROM test GROUP BY bool_col ORDER BY bool_col")
            .await?;
        let results = df.collect().await?;

        let expected = vec![
            "+----------+-----+",
            "| bool_col | cnt |",
            "+----------+-----+",
            "| false    | 4   |",
            "| true     | 4   |",
            "+----------+-----+",
        ];
        assert_result_eq(expected, &results);
        Ok(())
    }

    #[rstest]
    #[case::local(ReadMode::Local)]
    #[case::remote_flight(ReadMode::RemoteFlight)]
    #[tokio::test]
    async fn test_sort_shuffle_group_by_multiple_columns(
        #[case] read_mode: ReadMode,
    ) -> Result<()> {
        let ctx = create_sort_shuffle_context(read_mode).await;
        register_test_data(&ctx).await;

        let df = ctx
            .sql(
                "SELECT bool_col, tinyint_col, COUNT(*) as cnt
                 FROM test
                 GROUP BY bool_col, tinyint_col
                 ORDER BY bool_col, tinyint_col",
            )
            .await?;
        let results = df.collect().await?;

        // Verify we got results with correct grouping
        assert!(!results.is_empty());
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0);
        Ok(())
    }

    #[rstest]
    #[case::local(ReadMode::Local)]
    #[case::remote_flight(ReadMode::RemoteFlight)]
    #[tokio::test]
    async fn test_sort_shuffle_aggregate_sum(#[case] read_mode: ReadMode) -> Result<()> {
        let ctx = create_sort_shuffle_context(read_mode).await;
        register_test_data(&ctx).await;

        let df = ctx.sql("SELECT SUM(id) FROM test").await?;
        let results = df.collect().await?;

        let expected = vec![
            "+--------------+",
            "| sum(test.id) |",
            "+--------------+",
            "| 28           |",
            "+--------------+",
        ];
        assert_result_eq(expected, &results);
        Ok(())
    }

    #[rstest]
    #[case::local(ReadMode::Local)]
    #[case::remote_flight(ReadMode::RemoteFlight)]
    #[tokio::test]
    async fn test_sort_shuffle_aggregate_avg(#[case] read_mode: ReadMode) -> Result<()> {
        let ctx = create_sort_shuffle_context(read_mode).await;
        register_test_data(&ctx).await;

        let df = ctx.sql("SELECT AVG(id) FROM test").await?;
        let results = df.collect().await?;

        let expected = vec![
            "+--------------+",
            "| avg(test.id) |",
            "+--------------+",
            "| 3.5          |",
            "+--------------+",
        ];
        assert_result_eq(expected, &results);
        Ok(())
    }

    #[rstest]
    #[case::local(ReadMode::Local)]
    #[case::remote_flight(ReadMode::RemoteFlight)]
    #[tokio::test]
    async fn test_sort_shuffle_aggregate_count(
        #[case] read_mode: ReadMode,
    ) -> Result<()> {
        let ctx = create_sort_shuffle_context(read_mode).await;
        register_test_data(&ctx).await;

        let df = ctx.sql("SELECT COUNT(*) FROM test").await?;
        let results = df.collect().await?;

        let expected = vec![
            "+----------+",
            "| count(*) |",
            "+----------+",
            "| 8        |",
            "+----------+",
        ];
        assert_result_eq(expected, &results);
        Ok(())
    }

    #[rstest]
    #[case::local(ReadMode::Local)]
    #[case::remote_flight(ReadMode::RemoteFlight)]
    #[tokio::test]
    async fn test_sort_shuffle_aggregate_min_max(
        #[case] read_mode: ReadMode,
    ) -> Result<()> {
        let ctx = create_sort_shuffle_context(read_mode).await;
        register_test_data(&ctx).await;

        let df = ctx.sql("SELECT MIN(id), MAX(id) FROM test").await?;
        let results = df.collect().await?;

        let expected = vec![
            "+--------------+--------------+",
            "| min(test.id) | max(test.id) |",
            "+--------------+--------------+",
            "| 0            | 7            |",
            "+--------------+--------------+",
        ];
        assert_result_eq(expected, &results);
        Ok(())
    }

    // ==================== Comparison with Hash Shuffle ====================

    #[tokio::test]
    async fn test_sort_vs_hash_shuffle_group_by() -> Result<()> {
        // Test with sort shuffle (local read is sufficient for comparison)
        let sort_ctx = create_sort_shuffle_context(ReadMode::Local).await;
        register_test_data(&sort_ctx).await;
        let sort_results = sort_ctx
            .sql("SELECT bool_col, SUM(id) as total FROM test GROUP BY bool_col")
            .await?
            .collect()
            .await?;

        // Test with hash shuffle
        let hash_ctx = create_hash_shuffle_context().await;
        register_test_data(&hash_ctx).await;
        let hash_results = hash_ctx
            .sql("SELECT bool_col, SUM(id) as total FROM test GROUP BY bool_col")
            .await?
            .collect()
            .await?;

        // Results should be equivalent (order may differ)
        let sort_values = extract_values_unordered(&sort_results);
        let hash_values = extract_values_unordered(&hash_results);
        assert_eq!(sort_values, hash_values);
        Ok(())
    }

    #[tokio::test]
    async fn test_sort_vs_hash_shuffle_distinct() -> Result<()> {
        // Test with sort shuffle (local read is sufficient for comparison)
        let sort_ctx = create_sort_shuffle_context(ReadMode::Local).await;
        register_test_data(&sort_ctx).await;
        let sort_results = sort_ctx
            .sql("SELECT DISTINCT bool_col FROM test")
            .await?
            .collect()
            .await?;

        // Test with hash shuffle
        let hash_ctx = create_hash_shuffle_context().await;
        register_test_data(&hash_ctx).await;
        let hash_results = hash_ctx
            .sql("SELECT DISTINCT bool_col FROM test")
            .await?
            .collect()
            .await?;

        // Results should be equivalent
        let sort_values = extract_values_unordered(&sort_results);
        let hash_values = extract_values_unordered(&hash_results);
        assert_eq!(sort_values, hash_values);
        Ok(())
    }

    // ==================== Edge Cases ====================

    #[rstest]
    #[case::local(ReadMode::Local)]
    #[case::remote_flight(ReadMode::RemoteFlight)]
    #[tokio::test]
    async fn test_sort_shuffle_empty_result(#[case] read_mode: ReadMode) -> Result<()> {
        let ctx = create_sort_shuffle_context(read_mode).await;
        register_test_data(&ctx).await;

        let df = ctx.sql("SELECT id FROM test WHERE id > 100").await?;
        let results = df.collect().await?;

        // Should return empty result without error
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
        Ok(())
    }

    #[rstest]
    #[case::local(ReadMode::Local)]
    #[case::remote_flight(ReadMode::RemoteFlight)]
    #[tokio::test]
    async fn test_sort_shuffle_single_partition(
        #[case] read_mode: ReadMode,
    ) -> Result<()> {
        let ctx = create_sort_shuffle_context(read_mode).await;
        register_test_data(&ctx).await;

        // Query that results in single partition output
        let df = ctx.sql("SELECT COUNT(*) FROM test").await?;
        let results = df.collect().await?;

        assert!(!results.is_empty());
        Ok(())
    }

    #[rstest]
    #[case::local(ReadMode::Local)]
    #[case::remote_flight(ReadMode::RemoteFlight)]
    #[tokio::test]
    async fn test_sort_shuffle_multiple_aggregates(
        #[case] read_mode: ReadMode,
    ) -> Result<()> {
        let ctx = create_sort_shuffle_context(read_mode).await;
        register_test_data(&ctx).await;

        let df = ctx
            .sql(
                "SELECT
                    bool_col,
                    COUNT(*) as cnt,
                    SUM(id) as sum_id,
                    AVG(id) as avg_id,
                    MIN(id) as min_id,
                    MAX(id) as max_id
                 FROM test
                 GROUP BY bool_col
                 ORDER BY bool_col",
            )
            .await?;
        let results = df.collect().await?;

        assert!(!results.is_empty());
        // Verify we have 2 groups (true and false)
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
        Ok(())
    }

    #[rstest]
    #[case::local(ReadMode::Local)]
    #[case::remote_flight(ReadMode::RemoteFlight)]
    #[tokio::test]
    async fn test_sort_shuffle_having_clause(#[case] read_mode: ReadMode) -> Result<()> {
        let ctx = create_sort_shuffle_context(read_mode).await;
        register_test_data(&ctx).await;

        let df = ctx
            .sql(
                "SELECT bool_col, COUNT(*) as cnt
                 FROM test
                 GROUP BY bool_col
                 HAVING COUNT(*) > 2
                 ORDER BY bool_col",
            )
            .await?;
        let results = df.collect().await?;

        // Both groups should have count > 2
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
        Ok(())
    }

    // ==================== Subquery and Complex Queries ====================

    #[rstest]
    #[case::local(ReadMode::Local)]
    #[case::remote_flight(ReadMode::RemoteFlight)]
    #[tokio::test]
    async fn test_sort_shuffle_subquery(#[case] read_mode: ReadMode) -> Result<()> {
        let ctx = create_sort_shuffle_context(read_mode).await;
        register_test_data(&ctx).await;

        let df = ctx
            .sql(
                "SELECT * FROM (
                    SELECT bool_col, COUNT(*) as cnt
                    FROM test
                    GROUP BY bool_col
                ) sub
                WHERE cnt > 0",
            )
            .await?;
        let results = df.collect().await?;

        assert!(!results.is_empty());
        Ok(())
    }

    #[rstest]
    #[case::local(ReadMode::Local)]
    #[case::remote_flight(ReadMode::RemoteFlight)]
    #[tokio::test]
    async fn test_sort_shuffle_union(#[case] read_mode: ReadMode) -> Result<()> {
        let ctx = create_sort_shuffle_context(read_mode).await;
        register_test_data(&ctx).await;

        let df = ctx
            .sql(
                "SELECT bool_col, COUNT(*) as cnt FROM test WHERE id < 4 GROUP BY bool_col
                 UNION ALL
                 SELECT bool_col, COUNT(*) as cnt FROM test WHERE id >= 4 GROUP BY bool_col",
            )
            .await?;
        let results = df.collect().await?;

        // Should have results from both parts of the union
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows >= 2);
        Ok(())
    }

    // ==================== Order By with Shuffle ====================

    #[rstest]
    #[case::local(ReadMode::Local)]
    #[case::remote_flight(ReadMode::RemoteFlight)]
    #[tokio::test]
    async fn test_sort_shuffle_order_by(#[case] read_mode: ReadMode) -> Result<()> {
        let ctx = create_sort_shuffle_context(read_mode).await;
        register_test_data(&ctx).await;

        let df = ctx.sql("SELECT id FROM test ORDER BY id").await?;
        let results = df.collect().await?;

        // Verify ordering is correct
        let expected = vec![
            "+----+", "| id |", "+----+", "| 0  |", "| 1  |", "| 2  |", "| 3  |",
            "| 4  |", "| 5  |", "| 6  |", "| 7  |", "+----+",
        ];
        assert_result_eq(expected, &results);
        Ok(())
    }

    #[rstest]
    #[case::local(ReadMode::Local)]
    #[case::remote_flight(ReadMode::RemoteFlight)]
    #[tokio::test]
    async fn test_sort_shuffle_order_by_desc(#[case] read_mode: ReadMode) -> Result<()> {
        let ctx = create_sort_shuffle_context(read_mode).await;
        register_test_data(&ctx).await;

        let df = ctx.sql("SELECT id FROM test ORDER BY id DESC").await?;
        let results = df.collect().await?;

        // Verify descending order
        let expected = vec![
            "+----+", "| id |", "+----+", "| 7  |", "| 6  |", "| 5  |", "| 4  |",
            "| 3  |", "| 2  |", "| 1  |", "| 0  |", "+----+",
        ];
        assert_result_eq(expected, &results);
        Ok(())
    }

    #[rstest]
    #[case::local(ReadMode::Local)]
    #[case::remote_flight(ReadMode::RemoteFlight)]
    #[tokio::test]
    async fn test_sort_shuffle_limit(#[case] read_mode: ReadMode) -> Result<()> {
        let ctx = create_sort_shuffle_context(read_mode).await;
        register_test_data(&ctx).await;

        let df = ctx.sql("SELECT id FROM test ORDER BY id LIMIT 3").await?;
        let results = df.collect().await?;

        let expected = vec![
            "+----+", "| id |", "+----+", "| 0  |", "| 1  |", "| 2  |", "+----+",
        ];
        assert_result_eq(expected, &results);
        Ok(())
    }
}
