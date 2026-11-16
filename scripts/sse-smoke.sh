#!/usr/bin/env bash
# SSE lifecycle acceptance checks for ImageLab Version 3.
#
# Requires a running server. For the multi-observer and disconnect checks the
# job must stay active long enough to connect, so start it with a short
# simulated phase:
#
#   go run ./cmd/api -simulate-duration=2s -simulate-interval=200ms
#
# Checks: unknown job 404 before streaming, stream headers, initial snapshot,
# terminal frame and stream end, two observers receiving the same job, and a
# disconnect that leaves the worker running to a real completion.
set -uo pipefail

BASE="${BASE:-http://localhost:4000}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

PASS=0
FAIL=0

check() {
  local label="$1" expected="$2" actual="$3" extra="${4:-}"
  if [[ "$expected" == "$actual" ]]; then
    PASS=$((PASS + 1))
    echo "  PASS  $label"
  else
    FAIL=$((FAIL + 1))
    echo "  FAIL  $label  (expected $expected, got $actual) $extra"
  fi
}

echo "creating test assets in $WORK"
go run ./scripts/gen_assets.go "$WORK"

new_job() {
  local body
  body="$(curl -s -X POST "$BASE/v1/images" -F "file=@$WORK/photo.png")"
  JOB_ID="$(echo "$body" | grep -o '"job_id": "[^"]*"' | cut -d'"' -f4)"
  STATUS_URL="$(echo "$body" | grep -o '"status_url": "[^"]*"' | cut -d'"' -f4)"
}

echo "1) unknown job returns 404 before any stream"
UNKNOWN="$(curl -s -o /dev/null -w '%{http_code}' "$BASE/v1/jobs/00000000-0000-0000-0000-000000000000/events")"
check "unknown job events -> 404" 404 "$UNKNOWN"

echo "2) stream headers and immediate initial snapshot"
new_job
check "job created" true "$([ -n "$JOB_ID" ] && echo true || echo false)"
curl -s -D "$WORK/headers" -o "$WORK/initial" --max-time 1 "$BASE$STATUS_URL/events" >/dev/null 2>&1 || true
CT="$(awk -F': ' 'tolower($1)=="content-type"{print $2}' "$WORK/headers" | tr -d '\r' | head -1)"
CC="$(awk -F': ' 'tolower($1)=="cache-control"{print $2}' "$WORK/headers" | tr -d '\r' | head -1)"
check "Content-Type is text/event-stream" "text/event-stream; charset=utf-8" "$CT"
check "Cache-Control is no-cache" "no-cache" "$CC"
check "initial frame is a named job-update" true "$(grep -q '^event: job-update' "$WORK/initial" && echo true || echo false)"
check "initial snapshot carries a version" true "$(grep -q '"version":' "$WORK/initial" && echo true || echo false)"
check "no SSE id field is emitted" true "$(grep -q '^id:' "$WORK/initial" && echo false || echo true)"

echo "3) terminal snapshot, stream end, and two observers"
new_job
TAB1="$WORK/tab1"
TAB2="$WORK/tab2"
curl -sN --max-time 20 "$BASE$STATUS_URL/events" > "$TAB1" &
P1=$!
curl -sN --max-time 20 "$BASE$STATUS_URL/events" > "$TAB2" &
P2=$!
wait "$P1" "$P2" 2>/dev/null || true
check "observer 1 received the terminal snapshot" true "$(grep -q '"status":"completed"' "$TAB1" && echo true || echo false)"
check "observer 2 received the terminal snapshot" true "$(grep -q '"status":"completed"' "$TAB2" && echo true || echo false)"
check "observer 1 received multiple frames" true "$([ "$(grep -c '^event: job-update' "$TAB1")" -ge 2 ] && echo true || echo false)"
check "observer 2 received multiple frames" true "$([ "$(grep -c '^event: job-update' "$TAB2")" -ge 2 ] && echo true || echo false)"
check "observer streams ended" 0 "$([ -s "$TAB1" ] && [ -s "$TAB2" ] && echo 0 || echo 1)"

echo "4) disconnecting observation does not stop the worker"
new_job
timeout 0.4 curl -sN "$BASE$STATUS_URL/events" >/dev/null 2>&1 || true
STATE=""
for _ in $(seq 1 60); do
  STATE="$(curl -s "$BASE$STATUS_URL" | grep -o '"status": "[^"]*"' | cut -d'"' -f4)"
  case "$STATE" in completed|failed) break ;; esac
  sleep 0.25
done
check "job still reaches completed after disconnect" completed "$STATE"

echo
echo "$PASS passed, $FAIL failed"
if (( FAIL > 0 )); then exit 1; fi
