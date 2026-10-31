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
use ballista_core::test_utils::tpch::tpch_schema;
use ballista_scheduler::planner::{DefaultDistributedPlanner, DistributedPlanner};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};

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

/// Build the distributed stages for a TPC-H query, with or without exchange
/// reuse (the scheduler enables it by default).
async fn plan_stages(query_name: &str, reuse: bool) -> Vec<Arc<dyn ShuffleWriter>> {
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
    if reuse {
        planner = planner.with_exchange_reuse(Arc::new(FixtureReuseCodec {
            inner: BallistaPhysicalExtensionCodec::default(),
        }));
    }
    let state = ctx.state();
    let job_id: JobId = JOB_ID.into();
    planner
        .plan_query_stages(&job_id, physical, state.config().options())
        .unwrap()
}

/// Codec for this fixture's exchange-reuse key. The test-only [`StatsExec`]
/// scan has no protobuf form, so it is fingerprinted by schema and row count.
/// Without this every stage would fail to encode and reuse would never fire.
/// Keys are never decoded. Everything else goes to the production
/// `BallistaPhysicalExtensionCodec`.
#[derive(Debug)]
struct FixtureReuseCodec {
    inner: BallistaPhysicalExtensionCodec,
}

impl PhysicalExtensionCodec for FixtureReuseCodec {
    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> datafusion::error::Result<()> {
        if let Some(stats) = node.downcast_ref::<StatsExec>() {
            let rows = stats.num_rows();
            buf.extend_from_slice(
                format!("StatsExec|schema={:?}|rows={rows}", stats.schema()).as_bytes(),
            );
            Ok(())
        } else {
            self.inner.try_encode(node, buf, proto_converter)
        }
    }

    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner.try_decode(buf, inputs, ctx, proto_converter)
    }
}

/// Produce the normalized distributed staged-plan text for a TPC-H query,
/// with exchange reuse applied as the scheduler does by default.
pub async fn staged_plan_text(query_name: &str) -> String {
    let stages = plan_stages(query_name, true).await;

    let mut out = String::new();
    for stage in &stages {
        out.push_str(&format!("=== Stage {} ===\n", stage.stage_id()));
        let ep: &dyn datafusion::physical_plan::ExecutionPlan = stage.as_ref();
        out.push_str(&DisplayableExecutionPlan::new(ep).indent(false).to_string());
        out.push('\n');
    }
    normalize(&out)
}

/// Stage counts for a query planned without and with exchange reuse.
pub async fn stage_counts(query_name: &str) -> (usize, usize) {
    let before = plan_stages(query_name, false).await.len();
    let after = plan_stages(query_name, true).await.len();
    (before, after)
}

fn normalize(plan: &str) -> String {
    // Strip the fixed job id and any hex addresses so output is byte-stable.
    let s = plan.replace(JOB_ID, "<job_id>");
    // remove 0x… addresses if any appear
    let re = regex::Regex::new(r"0x[0-9a-fA-F]+").unwrap();
    re.replace_all(&s, "0x<addr>").into_owned()
}
