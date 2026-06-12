#!/usr/bin/env bash
# Phase 9: regenerates every order-book (matching engine) number in the
# README from one invocation, reusing the Phase 8 harness, tooling, and
# discipline unchanged — open-loop fixed-rate load measured from INTENDED
# send time (coordinated-omission-safe), leadership-stability validity
# gates, per-cell repeats with printed seeds, machine state recorded in
# every JSON, plots + summary regenerated from the saved files only.
#
#   bench/run_orderbook_bench.sh                       # full set (~20 min)
#   REPEATS=1 SECONDS_MAIN=5 bench/run_orderbook_bench.sh   # quick smoke
#
# The workload is NEW limit orders (side uniform, price uniform in a ±10
# band around 100, qty 1-10) against the replicated matching engine — the
# band keeps matching continuous, so the run measures the engine matching,
# not just appending. Same stated-load criterion as Phase 8
# (pick_rate.py): rep-robust rate fidelity + tail bounds, headline = 70%.
set -euo pipefail
cd "$(dirname "$0")/.."

SEED="${SEED:-1}"
REPEATS="${REPEATS:-3}"
SECONDS_MAIN="${SECONDS_MAIN:-10}"
SECONDS_HEADLINE="${SECONDS_HEADLINE:-20}"
WARMUP="${WARMUP:-3}"
TRIALS="${TRIALS:-30}"
OPEN_THREADS="${OPEN_THREADS:-48}"
PIN="${PIN:---pin}"
OUT="${OUT:-bench/results/phase9}"
BENCH=./build/bench/rsm_bench

mkdir -p "$OUT/plots"

# ---------- host control + capture (same as run_benchmarks.sh) ------------
if command -v cpupower >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
    sudo cpupower frequency-set -g performance >/dev/null 2>&1 || true
fi
GOV="$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)"
if [ "$GOV" != "performance" ]; then
    echo "WARNING: cpu governor is '$GOV', not 'performance'." >&2
    echo "         Run: sudo cpupower frequency-set -g performance" >&2
    echo "         Proceeding anyway; the governor is recorded with every result." >&2
fi
{
    date -Is
    echo "git: $(git rev-parse HEAD 2>/dev/null || echo n/a)"
    echo "governor: $GOV"
    echo "no_turbo: $(cat /sys/devices/system/cpu/intel_pstate/no_turbo 2>/dev/null || echo n/a)"
    echo "seed: $SEED repeats: $REPEATS"
    uname -a
    grep -m1 'model name' /proc/cpuinfo
    nproc
    free -h
    cat /proc/loadavg
    grep 'CMAKE_CXX_FLAGS_RELEASE\|CMAKE_BUILD_TYPE' build/CMakeCache.txt 2>/dev/null || true
} > "$OUT/machine.txt"

cmake --build build -j >/dev/null

run() { # run LABEL extra-args...
    local label="$1"; shift
    for r in $(seq 1 "$REPEATS"); do
        local seed=$((SEED + r - 1))
        echo "=== $label (rep $r, seed $seed)"
        "$BENCH" --sm orderbook --seed "$seed" --warmup "$WARMUP" $PIN \
                 --label "$label" --out "$OUT/$label.r$r.json" "$@"
    done
}

# ---------- closed-loop saturation (matching-engine throughput ceiling) ----
run "closed.c16.block.b16" --mode closed --seconds "$SECONDS_MAIN" \
    --threads 16 --wait block --batch 16
run "closed.c1.spin.b1" --mode closed --seconds "$SECONDS_MAIN" \
    --threads 1 --wait spin --batch 1

# ---------- open-loop rate sweeps (the latency-vs-throughput knee) ---------
for rate in 5000 10000 15000 20000 25000 30000; do
    run "open.base.rate$rate" --mode open --seconds "$SECONDS_MAIN" \
        --threads "$OPEN_THREADS" --rate "$rate" --wait block --batch 1
done
for rate in 10000 20000 30000 40000 50000 60000 70000; do
    run "open.best.rate$rate" --mode open --seconds "$SECONDS_MAIN" \
        --threads "$OPEN_THREADS" --rate "$rate" --wait block --batch 16
done

# ---------- stated-load headline runs (rates picked FROM the saved sweep) --
BASE_RATE=$(./bench/pick_rate.py "$OUT" open.base)
BEST_RATE=$(./bench/pick_rate.py "$OUT" open.best)
echo "picked stated loads: base=$BASE_RATE best=$BEST_RATE"
run "headline.base" --mode open --seconds "$SECONDS_HEADLINE" \
    --threads "$OPEN_THREADS" --rate "$BASE_RATE" --wait block --batch 1
run "headline.best" --mode open --seconds "$SECONDS_HEADLINE" \
    --threads "$OPEN_THREADS" --rate "$BEST_RATE" --wait block --batch 16

# ---------- failover under order load --------------------------------------
"$BENCH" --sm orderbook --mode failover --seed "$SEED" --trials "$TRIALS" \
         --threads 8 --wait block --batch 1 $PIN \
         --label "failover" --out "$OUT/failover.json"

# ---------- plots + summary from the saved JSON only -----------------------
./bench/plot_results.py "$OUT"
echo "done: raw data, plots, and summary in $OUT"
