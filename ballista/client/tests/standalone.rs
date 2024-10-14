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

#[cfg(test)]
#[cfg(feature = "standalone")]
mod standalone {
    use ballista::{extension::SessionContextExt, prelude::*};
    use datafusion::prelude::*;
    use datafusion::{
        assert_batches_eq, error::DataFusionError, prelude::SessionContext,
    };

    #[tokio::test]
    async fn should_execute_sql_show() -> datafusion::error::Result<()> {
        let test_data = crate::common::example_test_data();

        let config = BallistaConfig::new()
            .map_err(|e| DataFusionError::Configuration(e.to_string()))?;

        let ctx: SessionContext = SessionContext::standalone(&config).await?;
        ctx.register_parquet(
            "test",
            &format!("{test_data}/alltypes_plain.parquet"),
            Default::default(),
        )
        .await?;

        let result = ctx
            .sql("select string_col, timestamp_col from test where id > 4")
            .await?
            .collect()
            .await?;
        let expected = vec![
            "+------------+---------------------+",
            "| string_col | timestamp_col       |",
            "+------------+---------------------+",
            "| 31         | 2009-03-01T00:01:00 |",
            "| 30         | 2009-04-01T00:00:00 |",
            "| 31         | 2009-04-01T00:01:00 |",
            "+------------+---------------------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    async fn should_execute_sql_create_table() -> datafusion::error::Result<()> {
        let test_data = crate::common::example_test_data();

        let config = BallistaConfig::new()
            .map_err(|e| DataFusionError::Configuration(e.to_string()))?;

        let ctx: SessionContext = SessionContext::standalone(&config).await?;
        ctx.sql(&format!("CREATE EXTERNAL TABLE tbl_test STORED AS PARQUET LOCATION '{}/alltypes_plain.parquet'", test_data, )).await?.show().await?;

        let result = ctx
            .sql("select id, string_col, timestamp_col from tbl_test where id > 4")
            .await?
            .collect()
            .await?;
        let expected = vec![
            "+----+------------+---------------------+",
            "| id | string_col | timestamp_col       |",
            "+----+------------+---------------------+",
            "| 5  | 31         | 2009-03-01T00:01:00 |",
            "| 6  | 30         | 2009-04-01T00:00:00 |",
            "| 7  | 31         | 2009-04-01T00:01:00 |",
            "+----+------------+---------------------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    async fn should_execute_dataframe() -> datafusion::error::Result<()> {
        let test_data = crate::common::example_test_data();

        let config = BallistaConfig::new()
            .map_err(|e| DataFusionError::Configuration(e.to_string()))?;

        let ctx: SessionContext = SessionContext::standalone(&config).await?;

        let df = ctx
            .read_parquet(
                &format!("{test_data}/alltypes_plain.parquet"),
                Default::default(),
            )
            .await?
            .select_columns(&["id", "bool_col", "timestamp_col"])?
            .filter(col("id").gt(lit(5)))?;

        let result = df.collect().await?;

        let expected = vec![
            "+----+----------+---------------------+",
            "| id | bool_col | timestamp_col       |",
            "+----+----------+---------------------+",
            "| 6  | true     | 2009-04-01T00:00:00 |",
            "| 7  | false    | 2009-04-01T00:01:00 |",
            "+----+----------+---------------------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    async fn should_execute_sql_write() -> datafusion::error::Result<()> {
        let test_data = crate::common::example_test_data();

        let config = BallistaConfig::new()
            .map_err(|e| DataFusionError::Configuration(e.to_string()))?;

        let ctx: SessionContext = SessionContext::standalone(&config).await?;
        ctx.register_parquet(
            "test",
            &format!("{test_data}/alltypes_plain.parquet"),
            Default::default(),
        )
        .await?;
        let write_dir = tempfile::tempdir().expect("temporary directory to be created");
        let write_dir_path = write_dir
            .path()
            .to_str()
            .expect("path to be converted to str");

        ctx.sql("select * from test")
            .await?
            .write_parquet(&write_dir_path, Default::default(), Default::default())
            .await?;
        // there is discrepancy between logical plan encoded and decoded
        // for some reason decoded format is csv instead of parquet.
        //
        // client encoded:
        // CopyTo: format=parquet output_url=/var/folders/82/9qj_ms4d4cx01xzxjcdf1_f80000gn/T/.tmpNl0TDp options: ()
        //   TableScan: test projection=[id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col]
        //
        // scheduler decoded:
        // CopyTo: format=csv output_url=/var/folders/82/9qj_ms4d4cx01xzxjcdf1_f80000gn/T/.tmpNl0TDp options: ()
        //   TableScan: test projection=[id, bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col]
        //
        // on scheduler side file type is decoded as:
        // file_type Some(DefaultFileType { file_format_factory: CsvFormatFactory { options: Some(CsvOptions { has_header: None, delimiter: 44, quote: 34, terminator: None, escape: None, double_quote: None, newlines_in_values: None, compression: GZIP, schema_infer_max_rec: 0, date_format: None, datetime_format: None, timestamp_format: None, timestamp_tz_format: None, time_format: None, null_value: None, comment: None }) } })
        ctx.register_parquet("written_table", &write_dir_path, Default::default())
            .await?;

        let result = ctx
            .sql("select id, string_col, timestamp_col from written_table where id > 4")
            .await?
            .collect()
            .await?;
        let expected = vec![
            "+----+------------+---------------------+",
            "| id | string_col | timestamp_col       |",
            "+----+------------+---------------------+",
            "| 5  | 31         | 2009-03-01T00:01:00 |",
            "| 6  | 30         | 2009-04-01T00:00:00 |",
            "| 7  | 31         | 2009-04-01T00:01:00 |",
            "+----+------------+---------------------+",
        ];

        assert_batches_eq!(expected, &result);
        Ok(())
    }
}
