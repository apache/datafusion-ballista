#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


#
# SHOW PLAN HELPER
#
# showplan.sh is a helper command line utility for Ballista.
# which prints command line utility 
#
# Ballista exposes REST API at `http://localhost:50050/` by default 
# where address and port can be configurable. 
#
# `curl http://localhost:50050/api/jobs | jq` 
# returns list of running jobs in format 
#
#```json
# [
#  {
#    "job_id": "LOZNWx3",
#    "job_name": "test job ",
#    "job_status": "Completed. Produced 1 partition containing 1 row. Elapsed time: 20 ms.",
#    "status": "Completed",
#    "num_stages": 1,
#    "completed_stages": 1,
#    "percent_complete": 100,
#    "start_time": 1779600307389,
#    "end_time": 1779600307409
#  }
# ]
# ```
#
# jobs are not sorted.
#
# `curl http://localhost:50050/api/job/LOZNWx3 | jq` 
# returns the specific job `LOZNWx3` information
#
# ```json
# {
#   "job_id": "LOZNWx3",
#   "job_name": "",
#   "job_status": "Completed. Produced 1 partition containing 1 row. Elapsed time: 20 ms.",
#   "status": "Completed",
#   "num_stages": 1,
#   "completed_stages": 1,
#   "percent_complete": 100,
#   "start_time": 1779600307389,
#   "end_time": 1779600307409,
#   "logical_plan": "Projection: Int64(1)\n  EmptyRelation: rows=1",
#   "physical_plan": "ProjectionExec: expr=[1 as Int64(1)]\n  PlaceholderRowExec\n",
#   "stage_plan": "AdaptiveExecutionGraph { TRUNCATED }"
# }
# ```
#
# `curl http://localhost:50050/api/job/LOZNWx3/stages | jq`
# returns stage information for a given job
# ```
# {
#   "stages": [
#     {
#       "stage_id": "0",
#       "stage_status": "Successful",
#       "input_rows": 1,
#       "output_rows": 2,
#       "elapsed_compute": "6.00ms",
#       "stage_plan": "ShuffleWriterExec: partitioning: None\n  ProjectionExec: expr=[1 as Int64(1)]\n    PlaceholderRowExec\n",
#       "task_duration_percentiles": {
#         "min": 6,
#         "p25": 6,
#         "median": 6,
#         "p75": 6,
#         "max": 6
#       },
#       "task_input_percentiles": {
#         "min": 1,
#         "p25": 1,
#         "median": 1,
#         "p75": 1,
#         "max": 1
#       },
#       "tasks": [
#         {
#           "id": 0,
#           "status": "Successful",
#           "partition_id": 0,
#           "scheduled_time": 1779600307391,
#           "launch_time": 1779600307391,
#           "start_exec_time": 1779600307400,
#           "end_exec_time": 1779600307406,
#           "exec_duration": 6,
#           "finish_time": 1779600307407,
#           "input_rows": 1,
#           "output_rows": 2
#         }
#       ]
#     }
#   ]
# }
# ```
#
# Job and stages endpoint accept `render_tree=true` parameter which 
# renders plans as tree.
#
# Command actions and options
#
# showplan.sh <job_id> [options]
# 
# <job_id> is optional parameter specifying, if not provided
#          uses the last job (filtered by max start time)
#
# -a specify the Scheduler API address in format `http://executor-host:12345/`
#    it should contain host and port
# -p display `physical_plan` (default, if not specified)
# -l display `logical_plan`
# -e display `stage_plan` (from job info)
# -s [<stage_id>] display `physical_plan` for specified stage, or all stages if omitted (from stages info)
# -w  displays result with no word wrap, horizontal scroll)
# -t  display plans as tree render (where applicable)

set -euo pipefail

# Defaults
API_BASE="http://localhost:50050/"
JOB_ID=""
DISPLAY_MODE="physical"
STAGE_ID=""
USE_PAGER=false
RENDER_TREE=false

usage() {
    echo "Usage: $(basename "$0") [job_id] [-a address] [-p] [-l] [-e] [-s stage_id]"
    echo "  job_id         optional; defaults to the most recent job"
    echo "  -a ADDR        API base URL (default: http://localhost:50050/)"
    echo "  -p             display physical_plan (default)"
    echo "  -l             display logical_plan"
    echo "  -e             display stage_plan (from job info)"
    echo "  -s [STAGE_ID]  display stage_plan for the given stage, or all stages if omitted (from stages info)"
    echo "  -w             displays result with no word wrap, horizontal scroll)"
    echo "  -t             display plans as tree render (where applicable)"
    exit 1
}

# First positional argument may be a job_id (does not start with -)
if [[ $# -gt 0 && "$1" != -* ]]; then
    JOB_ID="$1"
    shift
fi

# Parse flags
while [[ $# -gt 0 ]]; do
    case "$1" in
        -a)
            [[ $# -lt 2 ]] && { echo "Error: -a requires an address argument"; usage; }
            API_BASE="$2"
            shift 2
            ;;
        -p)
            DISPLAY_MODE="physical"
            shift
            ;;
        -l)
            DISPLAY_MODE="logical"
            shift
            ;;
        -s)
            DISPLAY_MODE="stage"
            # stage_id is optional; consume next arg only if it is not a flag
            if [[ $# -ge 2 && "$2" != -* ]]; then
                STAGE_ID="$2"
                shift 2
            else
                STAGE_ID=""
                shift
            fi
            ;;
        -e)
            DISPLAY_MODE="execution"
            shift
            ;;
        -w)
            USE_PAGER=true
            shift
            ;;
        -t)
            RENDER_TREE=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Check dependencies
for cmd in curl jq tput; do
    if ! command -v "$cmd" &> /dev/null; then
        echo "Error: $cmd is not installed." >&2
        exit 1
    fi
done

# Normalize API base URL (ensure exactly one trailing slash)
API_BASE="${API_BASE%/}/"

# Resolve job_id if not provided
if [[ -z "$JOB_ID" ]]; then
    JOB_ID=$(curl -sSf "${API_BASE}api/jobs" | jq -r 'max_by(.start_time) | .job_id')
    if [[ -z "$JOB_ID" || "$JOB_ID" == "null" ]]; then
        echo "Error: no jobs found at ${API_BASE}api/jobs" >&2
        exit 1
    fi
fi

# Build job info and stages URLs (optionally with render_tree query param)
if [[ "$RENDER_TREE" == "true" ]]; then
    JOB_URL="${API_BASE}api/job/${JOB_ID}?render_tree=true"
    STAGES_URL="${API_BASE}api/job/${JOB_ID}/stages?render_tree=true"
else
    JOB_URL="${API_BASE}api/job/${JOB_ID}"
    STAGES_URL="${API_BASE}api/job/${JOB_ID}/stages"
fi

COLS=$(tput cols 2>/dev/tty || echo 80)
SEP=$(printf '%*s' "$COLS" ' ' | tr ' ' '-')

print_plan() {
    case "$DISPLAY_MODE" in
        physical)
            echo "${SEP}"
            echo "Job ${JOB_ID} physical plan:"
            echo "${SEP}"
            curl -sSf "${JOB_URL}" | jq -r '.physical_plan'
            ;;
        logical)
            echo "${SEP}"
            echo "Job ${JOB_ID} logical plan:"
            echo "${SEP}"
            curl -sSf "${JOB_URL}" | jq -r '.logical_plan'
            ;;
        execution)
            echo "${SEP}"
            echo "Job ${JOB_ID} stage plan:"
            echo "${SEP}"
            curl -sSf "${JOB_URL}" | jq -r '.stage_plan'
            ;;
        stage)
            STAGES_JSON=$(curl -sSf "${STAGES_URL}")
            if [[ -n "$STAGE_ID" ]]; then
                echo "${SEP}"
                echo "Job ${JOB_ID}/${STAGE_ID} physical plan:"
                echo "${SEP}"
                echo "${STAGES_JSON}" \
                    | jq -r --arg sid "$STAGE_ID" '.stages[] | select(.stage_id == $sid) | .stage_plan'
            else
                STAGE_COUNT=$(echo "${STAGES_JSON}" | jq '.stages | length')
                for ((i = 0; i < STAGE_COUNT; i++)); do
                    SID=$(echo "${STAGES_JSON}" | jq -r ".stages[$i].stage_id")
                    echo "${SEP}"
                    echo "Job ${JOB_ID}/${SID} physical plan:"
                    echo "${SEP}"
                    echo "${STAGES_JSON}" | jq -r ".stages[$i].stage_plan"
                done
            fi
            ;;
    esac
}

if [[ "$USE_PAGER" == "true" ]]; then
    trap 'printf "\033[?7h"' EXIT   # restore auto-wrap even on error/Ctrl-C
    printf '\033[?7l'               # disable auto-wrap
    print_plan
else
    print_plan
fi
