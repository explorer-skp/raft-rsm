#!/usr/bin/env bash
# Phase 7 before/after sweep: runs phase7_bench across runtime/wait modes and
# client counts, REPEATS times each, and prints per-config medians (this
# laptop's hybrid cores + frequency scaling make single runs noisy).
#
#   ./bench/phase7_sweep.sh [seconds] [repeats] [extra phase7_bench args...]
#
# The rigorous open-loop harness with thread pinning is Phase 8; this is the
# relative-comparison instrument the Phase 7 gate requires.
set -euo pipefail
cd "$(dirname "$0")/.."

BENCH=./build/bench/phase7_bench
SECONDS_ARG="${1:-6}"
REPEATS="${2:-3}"
shift $(( $# >= 2 ? 2 : $# )) || true
EXTRA=("$@")

configs=(
  "--runtime legacy"
  "--runtime threaded --wait block"
  "--runtime threaded --wait spin"
)

printf "%-38s %8s %10s %10s %10s\n" config clients "ops/s" p50 p99
for cfg in "${configs[@]}"; do
  for clients in 1 4 8; do
    tps=(); p50s=(); p99s=()
    for ((r=0; r<REPEATS; r++)); do
      out=$($BENCH $cfg --clients "$clients" --seconds "$SECONDS_ARG" \
            --warmup 2 "${EXTRA[@]}" 2>/dev/null)
      tps+=("$(awk '/^throughput/ {print $2}' <<<"$out")")
      p50s+=("$(grep -o 'p50=[^ ]*' <<<"$out" | cut -d= -f2)")
      p99s+=("$(grep -o ' p99=[^ ]*' <<<"$out" | cut -d= -f2)")
    done
    # median = middle of sorted list (REPEATS assumed odd)
    tp=$(printf '%s\n' "${tps[@]}" | sort -n | sed -n "$(( (REPEATS+1)/2 ))p")
    p50=$(printf '%s\n' "${p50s[@]}" | sort -n | sed -n "$(( (REPEATS+1)/2 ))p")
    p99=$(printf '%s\n' "${p99s[@]}" | sort -n | sed -n "$(( (REPEATS+1)/2 ))p")
    printf "%-38s %8s %10s %10s %10s\n" "$cfg ${EXTRA[*]}" "$clients" "$tp" "$p50" "$p99"
  done
done
