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

use ballista_scheduler::planner::DistributedPlanner;
use datafusion::common::Result;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_tpch_q1() -> Result<()> {
    test_expected_tpch_plan("q1").await
}

#[tokio::test]
async fn test_tpch_q2() -> Result<()> {
    test_expected_tpch_plan("q2").await
}

#[tokio::test]
async fn test_tpch_q3() -> Result<()> {
    test_expected_tpch_plan("q3").await
}

#[tokio::test]
async fn test_tpch_q4() -> Result<()> {
    test_expected_tpch_plan("q4").await
}

#[tokio::test]
async fn test_tpch_q5() -> Result<()> {
    test_expected_tpch_plan("q5").await
}

#[tokio::test]
async fn test_tpch_q6() -> Result<()> {
    test_expected_tpch_plan("q6").await
}

#[tokio::test]
#[ignore] // test is not deterministic due to https://github.com/apache/datafusion/issues/12473
async fn test_tpch_q7() -> Result<()> {
    test_expected_tpch_plan("q7").await
}

#[tokio::test]
async fn test_tpch_q8() -> Result<()> {
    test_expected_tpch_plan("q8").await
}

#[tokio::test]
#[ignore] // test is not deterministic due to https://github.com/apache/datafusion/issues/12473
async fn test_tpch_q9() -> Result<()> {
    test_expected_tpch_plan("q9").await
}

#[tokio::test]
async fn test_tpch_q10() -> Result<()> {
    test_expected_tpch_plan("q10").await
}

#[tokio::test]
async fn test_tpch_q11() -> Result<()> {
    test_expected_tpch_plan("q11").await
}

#[tokio::test]
#[ignore] // test is not deterministic due to https://github.com/apache/datafusion/issues/12473
async fn test_tpch_q12() -> Result<()> {
    test_expected_tpch_plan("q12").await
}

#[tokio::test]
async fn test_tpch_q13() -> Result<()> {
    test_expected_tpch_plan("q13").await
}

#[tokio::test]
async fn test_tpch_q14() -> Result<()> {
    test_expected_tpch_plan("q14").await
}

#[tokio::test]
async fn test_tpch_q15() -> Result<()> {
    test_expected_tpch_plan("q15").await
}

#[tokio::test]
#[ignore] // test is not deterministic due to https://github.com/apache/datafusion/issues/12473
async fn test_tpch_q16() -> Result<()> {
    test_expected_tpch_plan("q16").await
}

#[tokio::test]
async fn test_tpch_q17() -> Result<()> {
    test_expected_tpch_plan("q17").await
}

#[tokio::test]
async fn test_tpch_q18() -> Result<()> {
    test_expected_tpch_plan("q18").await
}

#[tokio::test]
#[ignore] // test is not deterministic due to https://github.com/apache/datafusion/issues/12473
async fn test_tpch_q19() -> Result<()> {
    test_expected_tpch_plan("q19").await
}

#[tokio::test]
async fn test_tpch_q20() -> Result<()> {
    test_expected_tpch_plan("q20").await
}

#[tokio::test]
async fn test_tpch_q21() -> Result<()> {
    test_expected_tpch_plan("q21").await
}

#[tokio::test]
async fn test_tpch_q22() -> Result<()> {
    test_expected_tpch_plan("q22").await
}

async fn test_expected_tpch_plan(name: &str) -> Result<()> {
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(8));
    for table in [
        "customer", "nation", "part", "region", "lineitem", "orders", "partsupp",
        "supplier",
    ] {
        let path = format!("/mnt/bigdata/tpch/sf100/{}.parquet", table);
        ctx.register_parquet(table, &path, ParquetReadOptions::default())
            .await?;
    }

    let query_file = format!("../../benchmarks/queries/{}.sql", name);
    println!("Reading: {}", query_file);
    let path = Path::new(&query_file);
    let file = File::open(&path)?;
    let reader = BufReader::new(file);
    let mut sqls = Vec::new();
    let mut sql = String::new();
    for line in reader.lines() {
        let line = line?;
        if line.starts_with("--") {
            continue;
        }
        sql.push_str(line.as_str());
        sql.push('\n');

        if line.trim().ends_with(";") {
            sqls.push(sql.clone());
            sql.clear();
        }
    }

    let expected_plan_path = format!("tpch/expected-plans/{}.txt", name);
    if fs::exists(&expected_plan_path)? {
        let temp_file = NamedTempFile::new()?;
        write_plans(ctx, &mut sqls, temp_file.path()).await?;
        let expected = fs::read_to_string(Path::new(&expected_plan_path))?;
        let actual = fs::read_to_string(temp_file)?;
        assert_eq!(expected, actual);
    } else {
        println!("Writing: {}", expected_plan_path);
        write_plans(ctx, &mut sqls, Path::new(&expected_plan_path)).await?;
    }

    Ok(())
}

async fn write_plans(
    ctx: SessionContext,
    sqls: &mut Vec<String>,
    expected_plan_path: &Path,
) -> Result<()> {
    let file = File::create(expected_plan_path)?;
    let mut w = BufWriter::new(file);

    for sql in sqls {
        write!(w, "{}\n\n", sql)?;
        let df = ctx.sql(&sql).await?;
        let physical_plan = ctx.state().create_physical_plan(df.logical_plan()).await?;
        let mut planner = DistributedPlanner::new();
        let query_stages = planner.plan_query_stages("test", physical_plan).unwrap();
        query_stages.iter().enumerate().for_each(|(idx, stage)| {
            let displayable = displayable(stage.as_ref()).indent(true);
            write!(w, "Query Stage {}:\n{}\n", idx, displayable).unwrap();
        });
    }
    Ok(())
}
