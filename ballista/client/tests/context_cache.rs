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

mod common;

/// End-to-end coverage for `DataFrame::cache()`, enabled per session via
/// `SET ballista.cache.noop = false` (the global default is still a no-op until
/// cache invalidation/eviction land). On a miss the cached input is
/// materialized as its own job and pinned; the cached frame is read back from
/// that shuffle output, and collecting it again hits the registry.
#[cfg(test)]
mod cache {
    use crate::common::{remote_context, standalone_context};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::assert_batches_eq;
    use datafusion::functions::math::expr_fn::random;
    use datafusion::prelude::*;
    use rstest::*;

    #[rstest::fixture]
    fn test_data() -> String {
        crate::common::example_test_data()
    }

    /// A cached DataFrame is materialized once and read back from the pinned
    /// shuffle output. Collecting it again, or a different query over it, reads
    /// that output rather than recomputing it, so even a volatile column keeps
    /// its values.
    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn caches_and_reads_back_data_frame(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
        test_data: String,
    ) {
        enable_cache(&ctx).await;

        let cached = alltypes(&ctx, &test_data)
            .await
            .select(vec![col("id"), random().alias("r")])
            .unwrap()
            .filter(col("id").gt(lit(5)))
            .unwrap()
            .cache()
            .await
            .unwrap();
        let collect_sorted = |df: DataFrame| async move {
            let batches = df
                .sort(vec![col("id").sort(true, false)])
                .unwrap()
                .collect()
                .await
                .unwrap();
            pretty_format_batches(&batches).unwrap().to_string()
        };

        // The first action materializes the cache; the second reads it back.
        let first = collect_sorted(cached.clone()).await;
        assert_eq!(first.lines().count(), 6, "two rows: {first}");
        let second = collect_sorted(cached.clone()).await;
        assert_eq!(first, second, "read back, not recomputed");

        // A different query over the same frame reads it too.
        let derived = cached
            .filter(col("id").gt(lit(6)))
            .unwrap()
            .count()
            .await
            .unwrap();
        assert_eq!(derived, 1);
    }

    async fn enable_cache(ctx: &SessionContext) {
        ctx.sql("SET ballista.cache.noop = false")
            .await
            .unwrap()
            .show()
            .await
            .unwrap();
    }

    async fn alltypes(ctx: &SessionContext, test_data: &str) -> DataFrame {
        ctx.read_parquet(
            &format!("{test_data}/alltypes_plain.parquet"),
            Default::default(),
        )
        .await
        .unwrap()
    }

    /// A cache built on another cache: the inner cache is materialized first, then
    /// the outer one reads it, then the query reads the outer. Results are
    /// unchanged, and a second collect hits both.
    #[rstest]
    #[case::standalone(standalone_context())]
    #[case::remote(remote_context())]
    #[tokio::test]
    async fn nested_caches_read_back(
        #[future(awt)]
        #[case]
        ctx: SessionContext,
        test_data: String,
    ) {
        enable_cache(&ctx).await;

        let inner = alltypes(&ctx, &test_data)
            .await
            .select_columns(&["id", "bool_col"])
            .unwrap()
            .cache()
            .await
            .unwrap();
        let outer = inner
            .filter(col("id").gt(lit(5)))
            .unwrap()
            .cache()
            .await
            .unwrap();

        let expected = [
            "+----+----------+",
            "| id | bool_col |",
            "+----+----------+",
            "| 6  | true     |",
            "| 7  | false    |",
            "+----+----------+",
        ];
        assert_batches_eq!(expected, &outer.clone().collect().await.unwrap());
        assert_batches_eq!(expected, &outer.collect().await.unwrap());
    }
}
