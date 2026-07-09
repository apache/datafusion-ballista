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

//! TPC-H schema/fixture helpers and the distributed staged-plan text helper.
//!
//! `staged_plan_text` loads a TPC-H query's SQL, registers the 8 TPC-H tables
//! (via [`TpchStatsTable`]) with SF100 row-count statistics, builds the
//! physical plan with the static (Ballista) planner, breaks it into
//! distributed query stages with [`DefaultDistributedPlanner`], and renders
//! the stages to a normalized text representation suitable for snapshotting.

use std::sync::Arc;

use ballista_core::JobId;
use ballista_core::execution_plans::ShuffleWriter;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::BallistaPhysicalExtensionCodec;
use ballista_scheduler::physical_optimizer::reuse_exchange::{
    protobuf_canonical_key, reuse_shuffle_stages,
};
use ballista_scheduler::planner::{DefaultDistributedPlanner, DistributedPlanner};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::PhysicalPlanNode;

use crate::stats_table::{StatsExec, TpchStatsTable};

const TARGET_PARTITIONS: usize = 16;
const JOB_ID: &str = "plan_stability";

/// The 8 TPC-H table names and their SF100 row counts.
pub const SF100_ROWS: &[(&str, usize)] = &[
    ("region", 5),
    ("nation", 25),
    ("supplier", 1_000_000),
    ("customer", 15_000_000),
    ("part", 20_000_000),
    ("partsupp", 80_000_000),
    ("orders", 150_000_000),
    ("lineitem", 600_037_902),
];

/// Returns the schema for a TPC-H table, copied verbatim from
/// `benchmarks/src/bin/tpch.rs::get_schema` (that function lives in a binary
/// and cannot be imported directly).
pub fn tpch_schema(table: &str) -> Schema {
    match table {
        "part" => Schema::new(vec![
            Field::new("p_partkey", DataType::Int64, false),
            Field::new("p_name", DataType::Utf8, false),
            Field::new("p_mfgr", DataType::Utf8, false),
            Field::new("p_brand", DataType::Utf8, false),
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_size", DataType::Int32, false),
            Field::new("p_container", DataType::Utf8, false),
            Field::new("p_retailprice", DataType::Decimal128(15, 2), false),
            Field::new("p_comment", DataType::Utf8, false),
        ]),

        "supplier" => Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_name", DataType::Utf8, false),
            Field::new("s_address", DataType::Utf8, false),
            Field::new("s_nationkey", DataType::Int64, false),
            Field::new("s_phone", DataType::Utf8, false),
            Field::new("s_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("s_comment", DataType::Utf8, false),
        ]),

        "partsupp" => Schema::new(vec![
            Field::new("ps_partkey", DataType::Int64, false),
            Field::new("ps_suppkey", DataType::Int64, false),
            Field::new("ps_availqty", DataType::Int32, false),
            Field::new("ps_supplycost", DataType::Decimal128(15, 2), false),
            Field::new("ps_comment", DataType::Utf8, false),
        ]),

        "customer" => Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::Int64, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, false),
        ]),

        "orders" => Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
            Field::new("o_orderdate", DataType::Date32, false),
            Field::new("o_orderpriority", DataType::Utf8, false),
            Field::new("o_clerk", DataType::Utf8, false),
            Field::new("o_shippriority", DataType::Int32, false),
            Field::new("o_comment", DataType::Utf8, false),
        ]),

        "lineitem" => Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Decimal128(15, 2), false),
            Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
            Field::new("l_discount", DataType::Decimal128(15, 2), false),
            Field::new("l_tax", DataType::Decimal128(15, 2), false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Date32, false),
            Field::new("l_commitdate", DataType::Date32, false),
            Field::new("l_receiptdate", DataType::Date32, false),
            Field::new("l_shipinstruct", DataType::Utf8, false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_comment", DataType::Utf8, false),
        ]),

        "nation" => Schema::new(vec![
            Field::new("n_nationkey", DataType::Int64, false),
            Field::new("n_name", DataType::Utf8, false),
            Field::new("n_regionkey", DataType::Int64, false),
            Field::new("n_comment", DataType::Utf8, false),
        ]),

        "region" => Schema::new(vec![
            Field::new("r_regionkey", DataType::Int64, false),
            Field::new("r_name", DataType::Utf8, false),
            Field::new("r_comment", DataType::Utf8, false),
        ]),

        other => panic!("unknown tpch table {other}"),
    }
}

fn make_ctx() -> SessionContext {
    let config =
        SessionConfig::new_with_ballista().with_target_partitions(TARGET_PARTITIONS);
    let ctx = SessionContext::new_with_config(config);
    for (table, rows) in SF100_ROWS {
        let schema = Arc::new(tpch_schema(table));
        ctx.register_table(*table, Arc::new(TpchStatsTable::new(schema, *rows)))
            .unwrap();
    }
    ctx
}

fn is_query_stmt(stmt: &str) -> bool {
    let u = stmt.trim_start().to_uppercase();
    u.starts_with("SELECT") || u.starts_with("WITH")
}

/// Build the pre-reuse distributed stages for a TPC-H query, plus the
/// `SessionContext` used to build them (its `SessionState` carries the
/// `ConfigOptions` needed to apply the reuse pass afterward). Shared by
/// [`staged_plan_text`] and [`staged_stages_len`] so the ctx/plan/stages
/// construction lives in exactly one place.
async fn plan_stages(query_name: &str) -> (SessionContext, Vec<Arc<dyn ShuffleWriter>>) {
    // Read the query SQL directly from the canonical benchmark location rather
    // than a copy, so a change to a benchmark query surfaces as a golden diff.
    let sql_path = format!(
        "{}/../../benchmarks/queries/{query_name}.sql",
        env!("CARGO_MANIFEST_DIR")
    );
    let sql = std::fs::read_to_string(&sql_path)
        .unwrap_or_else(|e| panic!("read {sql_path}: {e}"));

    let ctx = make_ctx();

    // Split into statements; execute DDL, capture the physical plan of the answer
    // (last SELECT/WITH) statement. Single-statement queries take the one statement.
    let stmts: Vec<&str> = sql
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    let answer_idx = stmts
        .iter()
        .rposition(|s| is_query_stmt(s))
        .expect("no SELECT/WITH statement in query");

    let mut physical = None;
    for (i, stmt) in stmts.iter().enumerate() {
        if i == answer_idx {
            physical = Some(
                ctx.sql(stmt)
                    .await
                    .unwrap()
                    .create_physical_plan()
                    .await
                    .unwrap(),
            );
        } else {
            // DDL such as CREATE VIEW / DROP VIEW (q15) — apply it.
            ctx.sql(stmt).await.unwrap().collect().await.unwrap();
        }
    }
    let physical = physical.unwrap();

    let mut planner = DefaultDistributedPlanner::new();
    let state = ctx.state();
    let job_id: JobId = JOB_ID.into();
    let stages = planner
        .plan_query_stages(&job_id, physical, state.config().options())
        .unwrap();

    (ctx, stages)
}

/// Extension codec used only by this fixture's exchange-reuse canonicalizer.
/// Delegates everything to the production `BallistaPhysicalExtensionCodec`
/// (the same codec `ExecutionGraph::new_with_reuse` uses) except the
/// test-only [`StatsExec`] scan leaf.
///
/// `StatsExec` stands in for a real table scan so the fixture can plan at
/// SF100 without data on disk. In production that scan is a
/// `DataSourceExec`/`ParquetExec`, a type `datafusion-proto` recognizes
/// natively — no extension codec involvement at all. `StatsExec` has no such
/// built-in or Ballista-side support, so without this wrapper *any* stage
/// whose subtree touches it fails to encode, `protobuf_canonical_key` returns
/// `None`, and — because the failure sits at the leaves — it cascades upward
/// through every dependent stage's canonical key and defeats reuse for the
/// whole query, even though the corresponding production plan (with a real,
/// encodable scan) would dedup normally. This was confirmed empirically: q11
/// stayed at 14 stages before *and* after `reuse_shuffle_stages` until this
/// codec was added.
///
/// The key only needs to distinguish or equate scans, never decode them, so
/// `try_encode` is a cheap schema + row-count fingerprint and `try_decode` is
/// unreachable for `StatsExec` in this test's usage (delegated to `inner`
/// unconditionally, which is fine since it is never asked to decode one).
#[derive(Debug)]
struct FixtureReuseCodec {
    inner: BallistaPhysicalExtensionCodec,
}

impl PhysicalExtensionCodec for FixtureReuseCodec {
    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        if let Some(stats) = node.downcast_ref::<StatsExec>() {
            let rows = stats.partition_statistics(None)?.num_rows;
            buf.extend_from_slice(
                format!("StatsExec|schema={:?}|rows={rows:?}", stats.schema()).as_bytes(),
            );
            Ok(())
        } else {
            self.inner.try_encode(node, buf)
        }
    }

    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner.try_decode(buf, inputs, ctx)
    }
}

/// Apply the same exchange-reuse pass the scheduler applies in
/// `ExecutionGraph::new_with_reuse` (`reuse_shuffle_stages` keyed by
/// `protobuf_canonical_key` over the Ballista codec), so the fixture reflects
/// the plan actually executed rather than the pre-reuse planner output.
fn apply_reuse(
    ctx: &SessionContext,
    stages: Vec<Arc<dyn ShuffleWriter>>,
) -> Vec<Arc<dyn ShuffleWriter>> {
    let codec = FixtureReuseCodec {
        inner: BallistaPhysicalExtensionCodec::default(),
    };
    let canonical = |plan: &Arc<dyn ExecutionPlan>| {
        protobuf_canonical_key::<PhysicalPlanNode>(plan, &codec)
    };
    reuse_shuffle_stages(stages, ctx.state().config().options(), &canonical).unwrap()
}

/// Produce the normalized distributed staged-plan text for a TPC-H query,
/// after applying exchange reuse (mirrors what the scheduler actually runs).
pub async fn staged_plan_text(query_name: &str) -> String {
    let (ctx, stages) = plan_stages(query_name).await;
    let stages = apply_reuse(&ctx, stages);

    let mut out = String::new();
    for stage in &stages {
        out.push_str(&format!("=== Stage {} ===\n", stage.stage_id()));
        let ep: &dyn datafusion::physical_plan::ExecutionPlan = stage.as_ref();
        out.push_str(&DisplayableExecutionPlan::new(ep).indent(false).to_string());
        out.push('\n');
    }
    normalize(&out)
}

/// Build the distributed stages for a query, optionally applying exchange
/// reuse, and return the stage count. Used by the liveness test to prove
/// reuse actually fires in the fixture (before > after for q11).
pub async fn staged_stages_len(query_name: &str, apply_reuse_pass: bool) -> usize {
    let (ctx, stages) = plan_stages(query_name).await;
    if apply_reuse_pass {
        apply_reuse(&ctx, stages).len()
    } else {
        stages.len()
    }
}

fn normalize(plan: &str) -> String {
    // Strip the fixed job id and any hex addresses so output is byte-stable.
    let s = plan.replace(JOB_ID, "<job_id>");
    // remove 0x… addresses if any appear
    let re = regex::Regex::new(r"0x[0-9a-fA-F]+").unwrap();
    re.replace_all(&s, "0x<addr>").into_owned()
}
