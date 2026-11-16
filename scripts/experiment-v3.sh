#!/usr/bin/env bash
# Assessment 3 experiment driver: SSE versus long polling.
#
# Three conditions, two methods, three repetitions each (3 x 2 x 3 = 18 runs):
#
#   A  Quiet interval     30 s of active work with no client-visible progress
#   B  Real progress      the real thumbnail, preview, and display commits
#   C  Frequent updates   a labelled 10 s phase with updates every 200 ms
#
# Each condition runs against a fresh server process so its configuration is
# exact. Method order alternates between conditions. The script writes raw
# logs, per-run rows, a summary, and three charts under experiments/v3/.
#
# Requires POOLING_DSN in the environment (the Makefile variable) or
# POOLING_DB_DSN, and a free port (default 4000).
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

PORT="${PORT:-4000}"
BASE="http://localhost:${PORT}"
REPS="${REPS:-3}"
QUIET="${QUIET:-30s}"
SIM_DURATION="${SIM_DURATION:-10s}"
SIM_INTERVAL="${SIM_INTERVAL:-200ms}"

OUT_DIR="experiments/v3"
RAW_DIR="$OUT_DIR/raw"
LOG_DIR="$OUT_DIR/logs"
CSV="$OUT_DIR/runs.csv"
ASSETS_DIR="/tmp/imagelab-v3-assets"
IMAGE="$ASSETS_DIR/photo.png"

DSN="${POOLING_DB_DSN:-${POOLING_DSN:-}}"
if [[ -z "$DSN" ]]; then
  echo "error: POOLING_DB_DSN is not set"
  exit 1
fi

echo "cleaning previous Version 3 results"
rm -rf "$OUT_DIR"
mkdir -p "$RAW_DIR" "$LOG_DIR"

echo "building server, measurement client, and summary tool"
go build -o /tmp/imagelab-api-v3 ./cmd/api
go build -o /tmp/imagelab-exp-v3 ./cmd/experiment
go build -o /tmp/imagelab-sse-summary ./cmd/sse-summary

echo "generating test image"
mkdir -p "$ASSETS_DIR"
go run ./scripts/gen_assets.go "$ASSETS_DIR" >/dev/null

SERVER_PID=""
stop_server() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null
    wait "$SERVER_PID" 2>/dev/null
    SERVER_PID=""
  fi
}

wait_ready() {
  for _ in $(seq 1 50); do
    code="$(curl -s -o /dev/null -w '%{http_code}' "$BASE/" 2>/dev/null || true)"
    if [[ "$code" == "200" ]]; then return 0; fi
    sleep 0.2
  done
  return 1
}

# run_condition <id> <label> <server flags...>
# Methods alternate order so neither method is always measured first.
RUN_INDEX=0
run_condition() {
  local id="$1" label="$2"; shift 2
  local server_flags=("$@")
  local log="$LOG_DIR/$id.log"

  RUN_INDEX=$((RUN_INDEX + 1))
  local methods
  if (( RUN_INDEX % 2 == 1 )); then
    methods=(long sse)
  else
    methods=(sse long)
  fi

  echo
  echo "=== condition $id: $label ==="
  echo "server flags: ${server_flags[*]:-(none)}"
  echo "method order: ${methods[*]}"

  /tmp/imagelab-api-v3 -port "$PORT" -db-dsn="$DSN" "${server_flags[@]}" > "$log" 2>&1 &
  SERVER_PID=$!
  if ! wait_ready; then
    echo "error: server for condition $id did not become ready"
    cat "$log"
    stop_server
    return 1
  fi

  local method
  for method in "${methods[@]}"; do
    /tmp/imagelab-exp-v3 \
      -base "$BASE" \
      -image "$IMAGE" \
      -condition "$id" \
      -label "$label" \
      -mode "$method" \
      -reps "$REPS" \
      -quiet-delay "$(quiet_ms "$id")" \
      -simulate-duration "$(sim_ms "$id")" \
      -simulate-interval "$(interval_ms "$id")" \
      -request-timeout 120000 \
      -server-log "$log" \
      -out "$CSV" \
      -rawdir "$RAW_DIR"
  done

  stop_server
}

quiet_ms() {
  case "$1" in
    A) echo 30000 ;;
    *) echo 0 ;;
  esac
}
sim_ms() {
  case "$1" in
    C) echo 10000 ;;
    *) echo 0 ;;
  esac
}
interval_ms() {
  case "$1" in
    C) echo 200 ;;
    *) echo 0 ;;
  esac
}

# Condition A: quiet interval, 30 s with no client-visible progress.
run_condition A "Quiet interval (30s, no updates)" -quiet-delay "$QUIET"

# Condition B: real progress only (default worker behaviour).
run_condition B "Real progress"

# Condition C: labelled 10 s simulated phase, updates every 200 ms.
run_condition C "Frequent updates (10s @ 200ms)" -simulate-duration "$SIM_DURATION" -simulate-interval "$SIM_INTERVAL"

echo
echo "summarising and drawing charts"
/tmp/imagelab-sse-summary -csv "$CSV" -outdir "$OUT_DIR"

echo
echo "done. results in $OUT_DIR"
echo "  runs:    $CSV ($(( $(wc -l < "$CSV") - 1 )) data rows)"
echo "  summary: $OUT_DIR/summary.csv"
echo "  charts:  $OUT_DIR/chart-gets.svg, $OUT_DIR/chart-detection.svg, $OUT_DIR/chart-coverage.svg"
echo "  raw:     $RAW_DIR"
echo "  logs:    $LOG_DIR"
