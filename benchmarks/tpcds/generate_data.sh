#!/usr/bin/env bash
# Generate TPC-DS SF1 data as Parquet for Ballista benchmarks.
# Requires dsdgen from TPC-DS tools (https://www.tpc.org/tpcds/).
#
# Usage:
#   ./generate_data.sh [SCALE_FACTOR] [OUTPUT_DIR]
# Defaults: SCALE_FACTOR=1, OUTPUT_DIR=./data
#
# Environment:
#   DSDGEN_DIR  - path to built TPC-DS tools (must contain dsdgen)
#   DSDGEN_URL  - optional tarball URL if tools not present
#
# ponytail: SF1 only; SF10 via input when CI budget allows.

set -euo pipefail

SCALE_FACTOR="${1:-1}"
OUTPUT_DIR="${2:-./data}"
DSDGEN_DIR="${DSDGEN_DIR:-./tpcds-kit/tools}"
# Pin known public mirror of TPC-DS tools (v2.13.0 / 3.2.0 kit)
DSDGEN_URL="${DSDGEN_URL:-https://github.com/databricks/tpcds-kit/archive/refs/heads/master.tar.gz}"

TABLES=(
  call_center catalog_page catalog_returns catalog_sales
  customer customer_address customer_demographics date_dim
  household_demographics income_band inventory item
  promotion reason ship_mode store store_returns store_sales
  time_dim warehouse web_page web_returns web_sales web_site
)

log() { echo "[tpcds-gen] $*"; }

ensure_dsdgen() {
  if [[ -x "${DSDGEN_DIR}/dsdgen" ]]; then
    return 0
  fi
  log "dsdgen not found at ${DSDGEN_DIR}; fetching tools"
  local tmp
  tmp="$(mktemp -d)"
  curl -fsSL "${DSDGEN_URL}" | tar -xz -C "${tmp}" --strip-components=1
  # kit layout: tools/ under extracted root
  if [[ -d "${tmp}/tools" ]]; then
    make -C "${tmp}/tools" OS=LINUX
    DSDGEN_DIR="${tmp}/tools"
  else
    make -C "${tmp}" OS=LINUX
    DSDGEN_DIR="${tmp}"
  fi
  if [[ ! -x "${DSDGEN_DIR}/dsdgen" ]]; then
    echo "error: failed to build dsdgen" >&2
    exit 1
  fi
}

csv_to_parquet() {
  local csv="$1"
  local parquet="$2"
  # Prefer datafusion-cli if available; else leave CSV for caller conversion
  if command -v datafusion-cli >/dev/null 2>&1; then
    datafusion-cli -c "COPY (SELECT * FROM read_csv('${csv}', delimiter='|', header=false, file_extension='.dat')) TO '${parquet}' (FORMAT PARQUET);" \
      || python3 - "${csv}" "${parquet}" <<'PY'
import sys
try:
    import pyarrow.csv as pcsv
    import pyarrow.parquet as pq
except ImportError:
    sys.exit(2)
csv_path, pq_path = sys.argv[1], sys.argv[2]
table = pcsv.read_csv(csv_path, parse_options=pcsv.ParseOptions(delimiter="|"))
pq.write_table(table, pq_path)
print(f"wrote {pq_path}")
PY
  elif command -v python3 >/dev/null 2>&1; then
    python3 - "${csv}" "${parquet}" <<'PY'
import sys
try:
    import pyarrow.csv as pcsv
    import pyarrow.parquet as pq
except ImportError:
    print("pyarrow not installed; leaving CSV", file=sys.stderr)
    sys.exit(0)
csv_path, pq_path = sys.argv[1], sys.argv[2]
table = pcsv.read_csv(csv_path, parse_options=pcsv.ParseOptions(delimiter="|"))
pq.write_table(table, pq_path)
print(f"wrote {pq_path}")
PY
  else
    log "no converter found; CSV left at ${csv}"
  fi
}

main() {
  ensure_dsdgen
  mkdir -p "${OUTPUT_DIR}/csv" "${OUTPUT_DIR}/parquet"

  log "generating TPC-DS SF=${SCALE_FACTOR} into ${OUTPUT_DIR}"
  (
    cd "${DSDGEN_DIR}"
    ./dsdgen -SCALE "${SCALE_FACTOR}" -DIR "$(cd "${OUTPUT_DIR}/csv" && pwd)" -FORCE -VERBOSE
  )

  for t in "${TABLES[@]}"; do
    local dat="${OUTPUT_DIR}/csv/${t}.dat"
    if [[ ! -f "${dat}" ]]; then
      # some kits emit .csv
      dat="${OUTPUT_DIR}/csv/${t}.csv"
    fi
    if [[ -f "${dat}" ]]; then
      csv_to_parquet "${dat}" "${OUTPUT_DIR}/parquet/${t}.parquet"
    else
      log "warn: missing ${t} data file"
    fi
  done

  log "done. parquet under ${OUTPUT_DIR}/parquet (csv under ${OUTPUT_DIR}/csv)"
  log "stable path for CI mount: ${OUTPUT_DIR}/parquet"
}

main "$@"
