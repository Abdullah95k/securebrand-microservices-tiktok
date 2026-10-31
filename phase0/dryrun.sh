#!/usr/bin/env bash
# Rehearses Phase 0 end to end against a local fakefb: no proxies, no
# Facebook, about a minute. fakefb serves rotated doc_ids, so the run only
# passes if every step works: preflight, doc_id discovery, capture and the
# fixture check, smoke, the measurement run, the report and the bundle.
#
#   phase0/dryrun.sh [extra fbs-spike phase0 flags]
set -euo pipefail
cd "$(dirname "$0")/.."

GO=${GO:-go}
OUT=${OUT:-phase0/out-dryrun}
ADDR=${ADDR:-127.0.0.1:8090}

"$GO" build -o bin/ ./cmd/fakefb ./cmd/fbs-spike
rm -rf "$OUT"
mkdir -p "$OUT"
bin/fakefb --addr "$ADDR" --docids rotated --pages 5 >"$OUT/fakefb.log" 2>&1 &
pid=$!
trap 'kill "$pid" 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do
  if (exec 3<>"/dev/tcp/${ADDR%:*}/${ADDR#*:}") 2>/dev/null; then break; fi
  sleep 0.2
done

bin/fbs-spike phase0 --config phase0/dryrun.yaml --pages phase0/dryrun-pages.csv --tiers direct \
  --out-dir "$OUT" --rps 20 --ip-echo-url "" "$@"
