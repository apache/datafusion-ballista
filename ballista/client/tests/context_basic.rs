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

//
// The tests are extracted from context.rs where `BallistaContext` lives.
// to be checked if `SessionContextExt` has same functionality like `BallistaContext`
//
#[cfg(test)]
#[cfg(feature = "standalone")]
mod basic {
    use ballista::prelude::SessionContextExt;
    use datafusion::arrow;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion::config::TableParquetOptions;
    use datafusion::dataframe::DataFrameWriteOptions;
    use datafusion::prelude::ParquetReadOptions;
    use datafusion::prelude::SessionContext;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_standalone_mode() {
        let context = SessionContext::standalone().await.unwrap();
        let df = context.sql("SELECT 1;").await.unwrap();
        df.collect().await.unwrap();
    }

    #[tokio::test]
    async fn test_write_parquet() -> Result<()> {
        let context = SessionContext::standalone().await?;
        let df = context.sql("SELECT 1;").await?;
        let tmp_dir = TempDir::new().unwrap();
        let file_path = format!(
            "{}",
            tmp_dir.path().join("test_write_parquet.parquet").display()
        );
        df.write_parquet(
            &file_path,
            DataFrameWriteOptions::default(),
            Some(TableParquetOptions::default()),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_write_csv() -> Result<()> {
        let context = SessionContext::standalone().await?;
        let df = context.sql("SELECT 1;").await?;
        let tmp_dir = TempDir::new().unwrap();
        let file_path =
            format!("{}", tmp_dir.path().join("test_write_csv.csv").display());
        df.write_csv(&file_path, DataFrameWriteOptions::default(), None)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_ballista_show_tables() {
        let context = SessionContext::standalone().await.unwrap();

        let data = "Jorge,2018-12-13T12:12:10.011Z\n\
                    Andrew,2018-11-13T17:11:10.011Z";

        let tmp_dir = TempDir::new().unwrap();
        let file_path = tmp_dir.path().join("timestamps.csv");

        // scope to ensure the file is closed and written
        {
            File::create(&file_path)
                .expect("creating temp file")
                .write_all(data.as_bytes())
                .expect("writing data");
        }

        let sql = format!(
            "CREATE EXTERNAL TABLE csv_with_timestamps (
                  name VARCHAR,
                  ts TIMESTAMP
              )
              STORED AS CSV
              LOCATION '{}'
              OPTIONS ('has_header' 'false', 'delimiter' ',')
              ",
            file_path.to_str().expect("path is utf8")
        );

        context.sql(sql.as_str()).await.unwrap();

        let df = context.sql("show columns from csv_with_timestamps;").await;

        // used to fail with ballista context
        // assert!(df.is_err());
        assert!(df.is_ok());

        let result = df.unwrap().collect().await.unwrap();

        let expected = [
            "+---------------+--------------+---------------------+-------------+---------------+-------------+",
            "| table_catalog | table_schema | table_name          | column_name | data_type     | is_nullable |",
            "+---------------+--------------+---------------------+-------------+---------------+-------------+",
            "| datafusion    | public       | csv_with_timestamps | name        | Utf8          | YES         |",
            "| datafusion    | public       | csv_with_timestamps | ts          | Timestamp(ns) | YES         |",
            "+---------------+--------------+---------------------+-------------+---------------+-------------+",
        ];
        datafusion::assert_batches_eq!(expected, &result);
    }

    #[tokio::test]
    async fn test_show_tables_not_with_information_schema() {
        let context = SessionContext::standalone().await.unwrap();

        let data = "Jorge,2018-12-13T12:12:10.011Z\n\
                    Andrew,2018-11-13T17:11:10.011Z";

        let tmp_dir = TempDir::new().unwrap();
        let file_path = tmp_dir.path().join("timestamps.csv");

        // scope to ensure the file is closed and written
        {
            File::create(&file_path)
                .expect("creating temp file")
                .write_all(data.as_bytes())
                .expect("writing data");
        }

        let sql = format!(
            "CREATE EXTERNAL TABLE csv_with_timestamps (
                  name VARCHAR,
                  ts TIMESTAMP
              )
              STORED AS CSV
              LOCATION '{}'
              ",
            file_path.to_str().expect("path is utf8")
        );

        context.sql(sql.as_str()).await.unwrap();
        let df = context.sql("show tables;").await;
        assert!(df.is_ok());
    }
    #[tokio::test]
    async fn test_empty_exec_with_one_row() {
        let context = SessionContext::standalone().await.unwrap();

        let sql = "select EXTRACT(year FROM to_timestamp('2020-09-08T12:13:14+00:00'));";

        let df = context.sql(sql).await.unwrap();
        assert!(!df.collect().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_union_and_union_all() {
        let context = SessionContext::standalone().await.unwrap();

        let df = context
            .sql("SELECT 1 as NUMBER union SELECT 1 as NUMBER;")
            .await
            .unwrap();
        let res1 = df.collect().await.unwrap();
        let expected1 = vec![
            "+--------+",
            "| number |",
            "+--------+",
            "| 1      |",
            "+--------+",
        ];
        assert_eq!(
            expected1,
            pretty_format_batches(&res1)
                .unwrap()
                .to_string()
                .trim()
                .lines()
                .collect::<Vec<&str>>()
        );
        let expected2 = vec![
            "+--------+",
            "| number |",
            "+--------+",
            "| 1      |",
            "| 1      |",
            "+--------+",
        ];
        let df = context
            .sql("SELECT 1 as NUMBER union all SELECT 1 as NUMBER;")
            .await
            .unwrap();
        let res2 = df.collect().await.unwrap();
        assert_eq!(
            expected2,
            pretty_format_batches(&res2)
                .unwrap()
                .to_string()
                .trim()
                .lines()
                .collect::<Vec<&str>>()
        );
    }

    #[tokio::test]
    async fn test_aggregate_min_max() {
        let context = create_test_context().await;

        let df = context.sql("select min(\"id\") from test").await.unwrap();
        let res = df.collect().await.unwrap();
        let expected = vec![
            "+--------------+",
            "| min(test.id) |",
            "+--------------+",
            "| 0            |",
            "+--------------+",
        ];
        assert_result_eq(expected, &res);

        let df = context.sql("select max(\"id\") from test").await.unwrap();
        let res = df.collect().await.unwrap();
        let expected = vec![
            "+--------------+",
            "| max(test.id) |",
            "+--------------+",
            "| 7            |",
            "+--------------+",
        ];
        assert_result_eq(expected, &res);
    }

    #[tokio::test]
    async fn test_aggregate_sum() {
        let context = create_test_context().await;

        let df = context.sql("select SUM(\"id\") from test").await.unwrap();
        let res = df.collect().await.unwrap();
        let expected = vec![
            "+--------------+",
            "| sum(test.id) |",
            "+--------------+",
            "| 28           |",
            "+--------------+",
        ];
        assert_result_eq(expected, &res);
    }
    #[tokio::test]
    async fn test_aggregate_avg() {
        let context = create_test_context().await;

        let df = context.sql("select AVG(\"id\") from test").await.unwrap();
        let res = df.collect().await.unwrap();
        let expected = vec![
            "+--------------+",
            "| avg(test.id) |",
            "+--------------+",
            "| 3.5          |",
            "+--------------+",
        ];
        assert_result_eq(expected, &res);
    }

    #[tokio::test]
    async fn test_aggregate_count() {
        let context = create_test_context().await;

        let df = context.sql("select COUNT(\"id\") from test").await.unwrap();
        let res = df.collect().await.unwrap();
        let expected = vec![
            "+----------------+",
            "| count(test.id) |",
            "+----------------+",
            "| 8              |",
            "+----------------+",
        ];
        assert_result_eq(expected, &res);
    }
    #[tokio::test]
    async fn test_aggregate_approx_distinct() {
        let context = create_test_context().await;

        let df = context
            .sql("select approx_distinct(\"id\") from test")
            .await
            .unwrap();
        let res = df.collect().await.unwrap();
        let expected = vec![
            "+--------------------------+",
            "| approx_distinct(test.id) |",
            "+--------------------------+",
            "| 8                        |",
            "+--------------------------+",
        ];
        assert_result_eq(expected, &res);
    }
    #[tokio::test]
    async fn test_aggregate_array_agg() {
        let context = create_test_context().await;

        let df = context
            .sql("select ARRAY_AGG(\"id\") from test")
            .await
            .unwrap();
        let res = df.collect().await.unwrap();
        let expected = vec![
            "+--------------------------+",
            "| array_agg(test.id)       |",
            "+--------------------------+",
            "| [4, 5, 6, 7, 2, 3, 0, 1] |",
            "+--------------------------+",
        ];
        assert_result_eq(expected, &res);
    }
    #[tokio::test]
    async fn test_aggregate_var() {
        let context = create_test_context().await;

        let df = context.sql("select VAR(\"id\") from test").await.unwrap();
        let res = df.collect().await.unwrap();
        let expected = vec![
            "+-------------------+",
            "| var(test.id)      |",
            "+-------------------+",
            "| 6.000000000000001 |",
            "+-------------------+",
        ];
        assert_result_eq(expected, &res);

        let df = context
            .sql("select VAR_POP(\"id\") from test")
            .await
            .unwrap();
        let res = df.collect().await.unwrap();
        let expected = vec![
            "+-------------------+",
            "| var_pop(test.id)  |",
            "+-------------------+",
            "| 5.250000000000001 |",
            "+-------------------+",
        ];
        assert_result_eq(expected, &res);

        let df = context
            .sql("select VAR_SAMP(\"id\") from test")
            .await
            .unwrap();
        let res = df.collect().await.unwrap();
        let expected = vec![
            "+-------------------+",
            "| var_samp(test.id) |",
            "+-------------------+",
            "| 6.000000000000001 |",
            "+-------------------+",
        ];
        assert_result_eq(expected, &res);
    }
    #[tokio::test]
    async fn test_aggregate_stddev() {
        let context = create_test_context().await;

        let df = context
            .sql("select STDDEV(\"id\") from test")
            .await
            .unwrap();
        let res = df.collect().await.unwrap();
        let expected = vec![
            "+--------------------+",
            "| stddev(test.id)    |",
            "+--------------------+",
            "| 2.4494897427831783 |",
            "+--------------------+",
        ];
        assert_result_eq(expected, &res);

        let df = context
            .sql("select STDDEV_SAMP(\"id\") from test")
            .await
            .unwrap();
        let res = df.collect().await.unwrap();
        let expected = vec![
            "+----------------------+",
            "| stddev_samp(test.id) |",
            "+----------------------+",
            "| 2.4494897427831783   |",
            "+----------------------+",
        ];
        assert_result_eq(expected, &res);
    }
    #[tokio::test]
    async fn test_aggregate_covar() {
        let context = create_test_context().await;

        let df = context
            .sql("select COVAR(id, tinyint_col) from test")
            .await
            .unwrap();
        let res = df.collect().await.unwrap();
        let expected = vec![
            "+---------------------------------+",
            "| covar(test.id,test.tinyint_col) |",
            "+---------------------------------+",
            "| 0.28571428571428586             |",
            "+---------------------------------+",
        ];
        assert_result_eq(expected, &res);
    }
    #[tokio::test]
    async fn test_aggregate_correlation() {
        let context = create_test_context().await;

        let df = context
            .sql("select CORR(id, tinyint_col) from test")
            .await
            .unwrap();
        let res = df.collect().await.unwrap();
        let expected = vec![
            "+--------------------------------+",
            "| corr(test.id,test.tinyint_col) |",
            "+--------------------------------+",
            "| 0.21821789023599245            |",
            "+--------------------------------+",
        ];
        assert_result_eq(expected, &res);
    }
    // enable when upgrading Datafusion to > 42
    #[ignore]
    #[tokio::test]
    async fn test_aggregate_approx_percentile() {
        let context = create_test_context().await;

        let df = context
            .sql("select approx_percentile_cont_with_weight(id, 2, 0.5) from test")
            .await
            .unwrap();
        let res = df.collect().await.unwrap();
        let expected = vec![
            "+-------------------------------------------------------------------+",
            "| approx_percentile_cont_with_weight(test.id,Int64(2),Float64(0.5)) |",
            "+-------------------------------------------------------------------+",
            "| 1                                                                 |",
            "+-------------------------------------------------------------------+",
        ];
        assert_result_eq(expected, &res);

        let df = context
            .sql("select approx_percentile_cont(\"double_col\", 0.5) from test")
            .await
            .unwrap();
        let res = df.collect().await.unwrap();
        let expected = vec![
            "+------------------------------------------------------+",
            "| approx_percentile_cont(test.double_col,Float64(0.5)) |",
            "+------------------------------------------------------+",
            "| 7.574999999999999                                    |",
            "+------------------------------------------------------+",
        ];

        assert_result_eq(expected, &res);
    }

    fn assert_result_eq(
        expected: Vec<&str>,
        results: &[arrow::record_batch::RecordBatch],
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
    async fn create_test_context() -> SessionContext {
        let context = SessionContext::standalone().await.unwrap();

        context
            .register_parquet(
                "test",
                "testdata/alltypes_plain.parquet",
                ParquetReadOptions::default(),
            )
            .await
            .unwrap();
        context
    }
}
