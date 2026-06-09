#!/usr/bin/env bash
# Phase 8: the single script that regenerates EVERY number and plot in the
# README Results section. Each rsm_bench invocation prints its full config
# and seed and writes a raw JSON file; bench/plot_results.py turns the saved
# files into the plots and summary tables without re-running anything.
#
#   bench/run_benchmarks.sh                  # full suite (~45 min)
#   REPEATS=1 SECONDS_MAIN=5 bench/run_benchmarks.sh   # quick smoke
#
# Host control: the script tries to set the CPU frequency governor to
# `performance` (needs sudo). If it cannot, it WARNS and records whatever
# the governor actually is — numbers from an uncontrolled host carry that
# record with them. Run on an idle, AC-powered machine.
#
# Any clean-load run that records an unexpected election exits 3 (rsm_bench
# refuses to report numbers from a quietly degraded cluster) and this
# script aborts: that is a bug to investigate, not noise to average over.
set -euo pipefail
cd "$(dirname "$0")/.."

SEED="${SEED:-1}"
REPEATS="${REPEATS:-3}"
SECONDS_MAIN="${SECONDS_MAIN:-10}"     # per-run measure window (sweeps)
SECONDS_HEADLINE="${SECONDS_HEADLINE:-20}"
SECONDS_STRESS="${SECONDS_STRESS:-30}"
WARMUP="${WARMUP:-3}"
TRIALS="${TRIALS:-60}"
OPEN_THREADS="${OPEN_THREADS:-48}"
PIN="${PIN:---pin}"                    # set PIN="" to disable raft pinning
DISK_BASE="${DISK_BASE:-$HOME/.rsm_bench_disk}"
STAMP="$(date +%Y%m%d_%H%M%S)"
OUT="${OUT:-bench/results/$STAMP}"
BENCH=./build/bench/rsm_bench

mkdir -p "$OUT/plots" "$DISK_BASE"

# ---------- host control + capture ----------------------------------------
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

cmake -S . -B build -DCMAKE_BUILD_TYPE=Release >/dev/null
cmake --build build -j >/dev/null

# run NAME [args...] — REPEATS runs, seeds SEED..SEED+REPEATS-1.
run() {
    local name="$1"; shift
    local r
    for r in $(seq 1 "$REPEATS"); do
        echo "== $name (rep $r/$REPEATS) =="
        "$BENCH" "$@" --seed $((SEED + r - 1)) --label "$name" \
                 --out "$OUT/$name.r$r.json"
    done
}
# run1 NAME [args...] — single run (stress, failover, faults).
run1() {
    local name="$1"; shift
    echo "== $name =="
    "$BENCH" "$@" --seed "$SEED" --label "$name" --out "$OUT/$name.json"
}

# ---------- 0. pin A/B (the thread-pinning decision, quantified) ----------
# Interleaved pin/no-pin so neither side systematically benefits from the
# package's burst-then-sustained power behavior (a back-to-back A/B on this
# host is order-confounded; learned the hard way, see DESIGN.md).
for r in $(seq 1 "$REPEATS"); do
    for pin in pin1 pin0; do
        PINFLAG=""; [ "$pin" = pin1 ] && PINFLAG="--pin"
        echo "== pin_ab2.$pin (rep $r/$REPEATS) =="
        "$BENCH" --mode closed --threads 16 --seconds "$SECONDS_MAIN" \
                 --warmup "$WARMUP" $PINFLAG --seed $((SEED + r - 1)) \
                 --label "pin_ab2.$pin" --out "$OUT/pin_ab2.$pin.r$r.json"
    done
done

# ---------- 1. closed-loop concurrency sweep (block vs spin) --------------
for wait in block spin; do
    for c in 1 2 4 8 16 32; do
        run "closed.c$c.$wait.b1" --mode closed --threads "$c" \
            --wait "$wait" --batch 1 --seconds "$SECONDS_MAIN" \
            --warmup "$WARMUP" $PIN
    done
done

# ---------- 2. batch-size sweep at 16 clients (group commit knob) ---------
for wait in block spin; do
    for b in 2 4 8 16 32; do
        run "closed.c16.$wait.b$b" --mode closed --threads 16 \
            --wait "$wait" --batch "$b" --seconds "$SECONDS_MAIN" \
            --warmup "$WARMUP" $PIN
    done
done

# ---------- 3. fsync policy on a real disk (per-entry vs group commit) ----
# On tmpfs fsync is nearly free; the durability knob only shows its teeth on
# a real filesystem. batch=1 is fsync-per-entry; batch=N amortizes one fsync
# over N entries (group commit). fsync=every vs =group at batch=1 is the
# documented no-op equivalence — measured once to SHOW it.
for b in 1 8 32; do
    run "disk.c16.block.b$b" --mode closed --threads 16 --batch "$b" \
        --seconds "$SECONDS_MAIN" --warmup "$WARMUP" \
        --data-base "$DISK_BASE" $PIN
done
run "disk.c16.block.b1.fsgroup" --mode closed --threads 16 --batch 1 \
    --fsync group --seconds "$SECONDS_MAIN" --warmup "$WARMUP" \
    --data-base "$DISK_BASE" $PIN

# ---------- 4. open-loop rate sweep: the latency-vs-throughput curve ------
# base: no batching, block waits. perf: group commit + spin (the Phase 7
# throughput configuration). Oversaturated points are expected to report
# abandoned schedules / huge tails — that IS the knee being mapped.
for rate in 2000 5000 10000 15000 20000 25000 30000 40000; do
    run "open.base.rate$rate" --mode open --rate "$rate" \
        --threads "$OPEN_THREADS" --seconds "$SECONDS_MAIN" \
        --warmup "$WARMUP" --drain-cap-s 5 $PIN
done
for rate in 5000 10000 20000 30000 40000 50000 60000 70000; do
    run "open.perf.rate$rate" --mode open --rate "$rate" \
        --threads "$OPEN_THREADS" --batch 8 --wait spin \
        --seconds "$SECONDS_MAIN" --warmup "$WARMUP" --drain-cap-s 5 $PIN
done
# best: the configuration the closed-loop batch sweep crowns (block waits +
# batch 16 on this host) — the knee of the best configuration is an
# explicit deliverable.
for rate in 10000 20000 30000 40000 50000 60000 70000; do
    run "open.best.rate$rate" --mode open --rate "$rate" \
        --threads "$OPEN_THREADS" --batch 16 --wait block \
        --seconds "$SECONDS_MAIN" --warmup "$WARMUP" --drain-cap-s 5 $PIN
done

# ---------- 5. headline latency tables at a stated offered load -----------
# The stated load is ~70% of the highest rate the curve sustained cleanly
# (achieved >= 99% of offered, nothing abandoned); pick_rate.py reads the
# saved sweep so the choice itself is reproducible from the raw data.
HEADLINE_BASE=$(python3 bench/pick_rate.py "$OUT" open.base)
HEADLINE_PERF=$(python3 bench/pick_rate.py "$OUT" open.perf)
HEADLINE_BEST=$(python3 bench/pick_rate.py "$OUT" open.best)
echo "headline rates: base=$HEADLINE_BASE perf=$HEADLINE_PERF best=$HEADLINE_BEST"
run "headline.base" --mode open --rate "$HEADLINE_BASE" \
    --threads "$OPEN_THREADS" --seconds "$SECONDS_HEADLINE" \
    --warmup "$WARMUP" $PIN
run "headline.perf" --mode open --rate "$HEADLINE_PERF" \
    --threads "$OPEN_THREADS" --batch 8 --wait spin \
    --seconds "$SECONDS_HEADLINE" --warmup "$WARMUP" $PIN
run "headline.best" --mode open --rate "$HEADLINE_BEST" \
    --threads "$OPEN_THREADS" --batch 16 --wait block \
    --seconds "$SECONDS_HEADLINE" --warmup "$WARMUP" $PIN

# ---------- 6. stress regime: leadership must hold -------------------------
# High concurrency + group commit + sustained duration — the interaction
# class that exposed the Phase 7 apply-backpressure bug. rsm_bench exits 3
# (and this script aborts) if the term moves under this clean load.
run1 "stress" --mode closed --threads 32 --batch 8 --wait spin \
    --seconds "$SECONDS_STRESS" --warmup "$WARMUP" $PIN

# ---------- 7. failover-time distribution under load ----------------------
run1 "failover" --mode failover --trials "$TRIALS" --threads 8 \
    --warmup 2 $PIN

# ---------- 8. sustained faults vs clean baseline -------------------------
FAULT_RATE=$HEADLINE_BASE
for loss in 1 5 10; do
    run1 "fault.open.loss$loss" --mode open --rate "$FAULT_RATE" \
        --threads "$OPEN_THREADS" --seconds 15 --warmup "$WARMUP" \
        --loss-pct "$loss" $PIN
    run1 "fault.closed.loss$loss" --mode closed --threads 16 \
        --seconds 15 --warmup "$WARMUP" --loss-pct "$loss" $PIN
done
run1 "fault.open.partition" --mode open --rate "$FAULT_RATE" \
    --threads "$OPEN_THREADS" --seconds 30 --warmup "$WARMUP" \
    --partition-period-ms 5000 --partition-len-ms 1000 $PIN
run1 "fault.closed.partition" --mode closed --threads 16 --seconds 30 \
    --warmup "$WARMUP" --partition-period-ms 5000 --partition-len-ms 1000 \
    $PIN
# Clean references at the same windows for honest comparison.
run1 "fault.open.clean" --mode open --rate "$FAULT_RATE" \
    --threads "$OPEN_THREADS" --seconds 15 --warmup "$WARMUP" $PIN
run1 "fault.closed.clean" --mode closed --threads 16 --seconds 15 \
    --warmup "$WARMUP" $PIN

# ---------- plots + summary -------------------------------------------------
python3 bench/plot_results.py "$OUT"
echo
echo "results: $OUT  (raw JSON + machine.txt + plots/ + summary.txt)"
