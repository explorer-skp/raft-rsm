# raft-rsm — a low-latency replicated state machine on from-scratch Raft

A 3-node replicated key-value store over a Raft consensus core implemented
from scratch in C++20 — no consensus/RPC/queue libraries — with a
lock-free-pipeline performance layer and a rigorously measured benchmark
suite. Correctness is enforced by a deterministic simulator with seeded
chaos, five continuously-checked Raft safety invariants (each with a
must-flag self-test), and a per-key linearizability checker.

Full design record: [`DESIGN.md`](DESIGN.md). Specification:
`../raft_rsm_build_spec.md`. The order-book state machine and the full
architecture writeup land in Phase 9.

## Build and test

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure

./build_and_test.sh   # Release + ASan/UBSan + TSan, all suites
```

## Run a 3-node cluster

```bash
cat > /tmp/peers.conf <<EOF
1 127.0.0.1 5001
2 127.0.0.1 5002
3 127.0.0.1 5003
EOF
for i in 1 2 3; do
  ./build/src/runtime/raft_node --id $i --config /tmp/peers.conf \
      --data-dir /tmp/rsm-node$i &
done
./build/src/runtime/kv_cli --config /tmp/peers.conf put hello world
./build/src/runtime/kv_cli --config /tmp/peers.conf get hello
```

`raft_node` knobs: `--fsync every|group`, `--wait block|spin`,
`--batch N --linger-us N` (group commit).

## Results

Everything below regenerates from **one script** — raw per-run JSON,
plots, and summary tables included:

```bash
sudo cpupower frequency-set -g performance   # host control (recorded)
bench/run_benchmarks.sh                      # ~45 min full suite
```

Raw data for the reported numbers: `bench/results/phase8/` (per-run JSON
with the full config, seed, and machine state captured at start and end of
every run; `machine.txt`; `plots/`; `summary.txt`).

### Methodology (the part that makes the numbers mean something)

- **Open-loop load for latency.** Fixed-rate arrivals; each request's
  latency is measured from its *intended* (scheduled) send time, so
  queueing behind a slowdown is charged to the requests that suffered it —
  the coordinated-omission correction. The harness proves the correction
  works: a self-test injects a 500 ms stall into the state machine and the
  corrected tail must report it (measured: intended-time p99 = 470 ms vs
  actual-send-time p99 = 1.9 ms — the uncorrected view hides the stall
  ~250×). Closed-loop generators are used for saturation throughput only,
  because closed loops self-throttle and understate tails.
- **The observer doesn't perturb the observed.** The load-generator hot
  loop and latency capture are allocation- and lock-free in steady state
  (asserted by a global-operator-new test, same discipline as the Phase 7
  pipeline); internal commit latency is measured on the Raft thread itself
  through existing seams.
- **Leadership stability is a health signal, not noise.** Every run records
  every term/role transition. A clean-load run with an election inside the
  measurement window is *invalid*: the harness refuses its numbers and
  exits nonzero. The sweep includes the stress regime (high concurrency +
  group commit + sustained duration) that historically exposed a
  threaded-runtime bug (DESIGN.md Phase 7), asserting the term stays
  constant.
- **Reproducibility.** Per-cell repeats with printed seeds (medians
  reported); same seed ⇒ same workload (unit-tested); plots regenerate
  from saved JSON without re-running.
- **Host control.** Each JSON embeds CPU model, governor, turbo state,
  load average, and hottest-thermal-zone temperature at run start and end.
  The three Raft threads are pinned to dedicated physical cores (measured
  via interleaved A/B: throughput parity; kept as scheduling-variance
  control on the election-critical thread — DESIGN.md); everything else
  floats — on a 12-thread host running 12 pipeline threads there are no
  exclusive cores to hand out, and that is recorded rather than hidden.

**Test bed.** Intel i5-1235U (2 P-cores + 8 E-cores, 12 threads), 7.4 GiB
RAM, Fedora (Linux 6.19), governor `performance` on all CPUs for every run
(asserted from the per-run machine records), turbo on, otherwise idle, max
thermal-zone temperature 82 °C across the suite. 3-node cluster over
loopback TCP, one process **by design** (equivalent contention to the
spec's 3-processes-on-one-host topology — same threads, same cores, same
sockets; see DESIGN.md): real network RTT is excluded and would add a
roughly constant term. The cross-process wiring is validated separately —
a real three-`raft_node`-process cluster serving `kv_cli` ops is on record
at `bench/results/phase8/three_process_smoke.txt`. 16-byte values, 64-key
PUT workload, election timeout
150–300 ms, heartbeat 50 ms. Raft threads pinned (one per physical core);
3 repeats per sweep cell, medians reported, seeds 1–3.

**Honest host caveat.** This is a U-series laptop, not a fixed-frequency
server: under the suite's continuous load the package settles to its
sustained power limit, and isolated burst runs on an idle box measure tens
of percent higher than mid-suite cells (measured during development;
DESIGN.md). All reported numbers are from the *sustained* state —
mutually comparable and conservative. The methodology carries no such
caveat; rerun the same script on a dedicated box for better absolute
numbers.

### Throughput (committed ops/s)

Group commit is the dominant knob. The wait-mode knob lands exactly on the
Phase 7 trade-off: busy-spin wins at low concurrency, where hand-off wake
latency dominates (1 client: spin 13,891 ops/s at p50 72 µs vs block
10,001 at 98 µs); blocking wins once many client threads compete with the
12 pipeline threads for 12 hardware threads (spinning steals the cores the
clients need), including at the throughput-optimal point.

| 16 clients, tmpfs | batch 1 | batch 4 | batch 8 | batch 16 | batch 32 |
|---|---|---|---|---|---|
| block | 26,498 | 52,299 | 55,486 | **69,122** | 38,900 |
| spin | 18,133 | 52,710 | 58,700 | 52,560 | 41,188 |

Batch 32 regresses because 32 > the 16 in-flight requests, so every batch
waits out the 200 µs linger — the documented batch ≤ concurrency rule.

Concurrency (batch off, block): 1 client → 10,001, 2 → 15,337, 4 → 21,218,
8 → 25,208, 16 → 26,498, 32 → 27,112 — a ~26 k plateau from 8 clients up.
The single-client point is the honest pipeline-hop cost (p50 72 µs spin /
98 µs block per committed write, fsync on tmpfs included).

**fsync policy (real btrfs NVMe disk, 16 clients):** per-entry fsync
(batch 1) → 446 ops/s; group commit batch 8 → 3,216 ops/s (7.2×); batch 32
→ 2,914. The `--fsync every|group` flag at batch 1 measures 446 vs 444
ops/s — the documented by-construction equivalence; the *real* fsync knob
is the batch size (one fsync per batch).

### Latency at a stated offered load (open-loop, CO-corrected)

20-second windows, 48 generator threads, latencies in µs from the
*intended* send time (plus the leader-internal commit latency: request
enqueued at leader → entry committed, measured on the Raft thread).

**No batching (base), 14,000 req/s offered — 280k samples/run:**

| series | p50 | p99 | p99.9 | p99.99 | max |
|---|---|---|---|---|---|
| end-to-end (intended send) | 211 | 426 | 4,981 | 10,486 | 11,785 |
| end-to-end (actual send) | 154 | 369 | 2,097 | 10,093 | 11,728 |
| commit (leader internal) | 76 | 199 | 508 | 5,243 | 11,338 |

**Best config (batch 16 + block), 35,000 req/s offered — 700k samples/run:**

| series | p50 | p99 | p99.9 | p99.99 | max |
|---|---|---|---|---|---|
| end-to-end (intended send) | 360 | 655 | 10,486 | 19,923 | 20,556 |
| end-to-end (actual send) | 307 | 590 | 893 | 10,879 | 20,499 |
| commit (leader internal) | 182 | 381 | 479 | 5,046 | 20,360 |

(The Phase 7 throughput config, batch 8 + spin, sustains a lower stated
load — 28 k/s, p50 328 / p99 598 / p99.9 11,928 — and is in the raw data
as `headline.perf`.) The intended-vs-actual gap at p99.9 — 10,486 µs vs
893 µs in the best-config table — is the coordinated-omission correction
doing its job on production data: transient backlogs that late dispatch
would hide are charged to the requests that waited through them. Stated
loads are picked from the saved sweep by `pick_rate.py` (70 % of the
highest rate every repeat sustained with ≥ 99 % rate fidelity, p99 ≤ 50 ms
and p99.9 ≤ 20 ms).

### Latency vs throughput

![latency vs throughput](bench/results/phase8/plots/latency_vs_throughput.png)

The knee, per configuration (10 s windows): **base** (no batching) holds
p99 ≤ 1.4 ms through 20 k/s and collapses at 25 k/s; **best**
(batch 16 + block) holds **sub-millisecond p50 with p99 ≤ 2.8 ms through
60 k/s** and hits a hard wall at 70 k/s (p99 = 491 ms, schedule still
fully served). The p99.9 tail starts growing past 40 k/s, and one 60 k/s
repeat recorded a 258 ms p99.9 backlog episode — which is exactly why the
stated-load criterion is rep-robust (every repeat must hold the tail
bounds) and the headline tables sit at 70 % of the sustainable rate
(35 k/s), not at the cliff edge.

![latency percentiles](bench/results/phase8/plots/latency_percentiles.png)

### Failover-time distribution

![failover distribution](bench/results/phase8/plots/failover_distribution.png)

60 trials under 8-client load: the current leader is killed at a known
instant (its restart between trials replays from disk — the Phase 4 path);
a probe client with 100 ms attempts measures time to the *next committed
write*, leader discovery included. **p50 = p90 = 277 ms,
p99 = max = 629 ms.** The distribution is bimodal exactly as Raft predicts
with a 150–300 ms randomized election timeout: the main mass is one
election timeout plus a round trip; the 400–630 ms cluster is trials where
the first candidate lost the race (split vote / stale-log candidate) and a
second timeout fired. This is why failover is reported as a distribution,
never a single number — and why these *intentional* kills are distinct
from clean-load elections, which the harness treats as bugs.

### Behavior under sustained faults

![faults](bench/results/phase8/plots/faults.png)

Faults are measured **open-loop at a fixed offered load** — that is the
only honest way to measure a system under faults, because the load does
not politely back off when the cluster degrades. 14 k req/s offered,
medians of 3:

| condition | achieved/s | p50 µs | p99 µs | elections |
|---|---|---|---|---|
| clean | 14,000 | 209 | 418 | 0 |
| 1 % message loss | 14,000 | 209 | 414 | 0 |
| 5 % loss | 14,000 | 211 | 393 | 0 |
| 10 % loss | 14,000 | 211 | 381 | 0 |
| partition 1 s every 5 s | 14,000 | 217 | 1,308,623 | 5 |

Sustained symmetric message loss up to 10 % is absorbed at this load with
the offered rate fully served, the tail essentially unmoved, and zero
elections (loopback RTTs make retransmission cheap; commit needs one of
two followers per entry, heartbeats re-drive the rest). Periodic
partitions read exactly as the protocol dictates, by design: isolating the
leader stops commits for ~1 s of partition plus an election, the five
elections in the window are the expected majority-side re-elections (one
per leader isolation), and the CO-corrected p99 of ~1.3 s honestly charges
each outage to the requests scheduled during it — while the p50 of 217 µs
shows full recovery between partitions and the offered rate is still
served in aggregate.

Closed-loop fault runs exist in the raw data (`fault.closed.*`) but
**closed-loop throughput-under-loss is not a meaningful metric and is not
presented as a finding**: on an oversubscribed host, dropping inter-node
messages frees contended CPU that the closed-loop clients immediately
convert into more requests, so measured throughput can *rise* with loss
(it does here: 26.1 k clean → 30.5 k at 10 % loss). That artifact is
precisely why the fault story above is open-loop — the offered load must
be independent of the system's condition.

**Stress regime** (the Phase 7 lesson, now a standing benchmark gate):
32 clients + group commit (batch 8, spin) + 30 s sustained =
**75,140 ops/s with zero elections and the term constant** — 2.25 M
committed entries, 500 MB of logs, no compaction needed (the snapshotting
deferral evidence, recorded as `data_bytes` in every run).

Leadership stability held across the entire suite: **zero invalid runs in
194** — no clean-load run ever recorded an election inside its measurement
window.

### Plots

| plot | file |
|---|---|
| Latency vs throughput (knee) | `bench/results/phase8/plots/latency_vs_throughput.png` |
| Latency percentile curves | `bench/results/phase8/plots/latency_percentiles.png` |
| Throughput vs batch size | `bench/results/phase8/plots/throughput_vs_batch.png` |
| Throughput vs concurrency | `bench/results/phase8/plots/throughput_vs_clients.png` |
| Failover distribution | `bench/results/phase8/plots/failover_distribution.png` |
| Sustained faults | `bench/results/phase8/plots/faults.png` |

## Future work

- **Snapshotting / log compaction + `InstallSnapshot`** — deferred with
  evidence: log growth never constrained a benchmark run (`data_bytes`
  recorded per run); the state machine (including the exactly-once session
  table) has been snapshot-serializable since Phase 5.
- Dynamic membership (joint consensus), multi-host/WAN deployment, TLS and
  auth, Byzantine fault tolerance, a query layer — explicit non-goals per
  the spec.
- ReadIndex / lease reads (reads currently go through the log for
  by-construction linearizability).
