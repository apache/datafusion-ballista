#!/usr/bin/env bash
# Regenerate the approved TPC-H distributed plans for the plan-stability suite.
# Run after an intended planner/plan-shape change, then review the diff.
set -euo pipefail
cd "$(dirname "$0")/.."
BALLISTA_GENERATE_GOLDEN=1 cargo test -p ballista-scheduler --test tpch_plan_stability
echo "Regenerated. Review changes under ballista/scheduler/tests/tpch_plan_stability/approved/"
