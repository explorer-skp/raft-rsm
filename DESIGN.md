# DESIGN.md — Raft Replicated State Machine

Running log of non-obvious decisions, optimizations (with measured before/after),
and how each safety invariant is tested. Authoritative spec: `../raft_rsm_build_spec.md`.

## Safety invariants (from spec §4.3) and how each is tested

| # | Invariant | Test (filled in as phases land) |
|---|-----------|--------------------------------|
| 1 | Election Safety — at most one leader per term | Asserted over the **whole history** of every Phase 2 integration run: a transition observer records each `won-election` as (term → node); the assertion is that no term maps to two nodes. Checked after every simulated millisecond in `raft_sim_test.cpp` (cold start, leader failure, split vote, 20-seed chaotic timing, partition/rejoin) and across the real-TCP run in `raft_cluster_test.cpp`. |
| 2 | Leader Append-Only — a leader never overwrites/deletes its own log entries | Enforced by construction: `truncateSuffixFrom` has exactly one call site, inside the AppendEntries *receiver* after the leader role is excluded (a leader ignores a valid AE at its own term and steps down to follower before processing a higher-term one), and `propose()` only appends. Exercised by every Phase 3 leadership-change test. |
| 3 | Log Matching — same index+term ⇒ identical logs up to that point | `SimCluster::checkLogMatching()`: for every pair of live logs, at every index where both hold the same term the full prefixes are compared entry-by-entry. Run at the end of every Phase 3 sim test (normal replication, catch-up, divergent repair, duplicate-AE, Figure 8). |
| 4 | Leader Completeness — a committed entry is present in all future leaders' logs | Exercised by the Figure 8 sim test and the real-TCP failover test (new leader holds and re-commits every committed entry), and since Phase 4 across **crashes**: committed entries survive follower/leader crash+restart and full-cluster restart (durable sim + real-TCP restart tests), resting on the durable-commit invariant below. *(Seeded chaos schedule lands in Phase 6.)* |
| 5 | State Machine Safety — no two nodes apply different commands at the same log index | `SimCluster::checkStateMachineSafety()`: pairwise position-by-position comparison of the recorded applied sequences, run in every Phase 3 sim test; the real-TCP test asserts byte-identical applied sequences across nodes and across a failover. *(Chaos-schedule coverage lands with the Phase 6 harness.)* |

## Decisions

- **Test framework: doctest v2.4.12, vendored single header** (`third_party/doctest/doctest.h`).
  On the spec's allowed list. Vendored rather than fetched at configure time so the
  build is reproducible offline; no system package was available.
- **Sanitizer configs are CMake options**: `ENABLE_SANITIZERS` (ASan+UBSan) and
  `ENABLE_TSAN`, mutually exclusive (ASan and TSan cannot be combined in one binary).
  TSan builds become a phase-gate requirement once threading/ring-buffer code exists (Phase 7).
- **Release flags pinned to `-O2 -march=native -DNDEBUG`** (overriding CMake's default
  `-O3`) because the spec mandates exactly `-O2 -march=native` for benchmark builds —
  benchmark numbers should come from the stated flags.

## Wire format and transport (Phase 1)

### Envelope byte layout

Every message starts with a fixed 12-byte envelope. All envelope and payload
fields are **little-endian**.

| Offset | Size | Field | Notes |
|-------:|-----:|-------|-------|
| 0 | 1 | `version` | must equal 1; anything else is rejected |
| 1 | 1 | `messageType` | 1=RequestVote 2=RequestVoteReply 3=AppendEntries 4=AppendEntriesReply 5=ClientRequest 6=ClientReply |
| 2 | 2 | `fromNodeId` | u16 |
| 4 | 2 | `toNodeId` | u16 |
| 6 | 2 | reserved | must be zero; rejected otherwise |
| 8 | 4 | `payloadLength` | u32; must equal exactly the bytes that follow |

Payload layouts (u64 terms/indices, u16 node ids, bools as one byte 0/1 —
any other value rejected): variable-length byte strings are a u32 length
followed by the bytes; `AppendEntries.entries[]` is a u32 entry count followed
by entries of (u64 term, u32 commandLength, command bytes). The decoder is
strict: truncation, over-long inner lengths, counts the input cannot back
(checked before any allocation), out-of-range enum/bool bytes, and trailing
bytes all return decode failure — never a crash or over-read.

### Framing

Frames on the TCP stream are `4-byte big-endian body length + body`
(network byte order for the prefix is conventional; everything inside the
body is little-endian as above). Body length 0 or > 16 MiB + envelope is a
protocol error that poisons the connection. `FrameAssembler` reassembles
frames across arbitrary fragmentation/coalescing boundaries; it buffers with
a `std::vector` and an O(n) compaction per feed — deliberately simple, the
performance layer is Phase 7.

### Peer config format

One line per node: `nodeId host port`, whitespace-separated; blank lines and
`#` comments ignored; `host` is numeric IPv4 (this is a loopback-only
project). Duplicate ids, out-of-range values, or trailing tokens are errors.

### Transport decisions

- **Connection model:** each node dials its peers; a node pair uses two
  simplex TCP connections (outbound = my sends, inbound = my receives).
  Nothing has to pair connections to identities; costs one extra socket per
  pair, irrelevant on loopback.
- **Unreachable peer ⇒ drop, log, return false.** Messages are never queued
  on failure: Raft is built to tolerate message loss, and its timers retry
  (elections, heartbeats). A queue would add unbounded memory and reordering
  questions for no correctness gain. One reconnect is attempted per send to
  handle a peer that restarted since the last send.
- **Threading:** one I/O thread per transport (poll over listen + inbound
  fds, 100 ms tick to observe stop()); the receive handler runs on that
  thread. send() runs on the caller's thread under a peers mutex; the I/O
  thread never touches the peer table.
- Inbound reads are processed **before** accepting new connections in each
  poll iteration so pollfd results stay index-aligned with the connection
  list (accepting first shifted indices and could close a fresh connection —
  caught by the pre-start-send integration test).

## Leader election (Phase 2)

### Design seams

- **`Clock`** (`src/raft/clock.h`): all time in Raft logic flows through a
  `Clock` virtual interface — `SteadyClock` in production, `ManualClock`
  (explicitly advanced) in tests. No `steady_clock::now()` calls inside Raft
  code.
- **Seedable RNG:** election timeouts are drawn from a `std::mt19937_64`
  seeded via the `RaftCore` constructor (production seeds from
  `std::random_device` in `node_main` and prints the seed; tests pass fixed
  seeds). Draw: `uniform_int_distribution` over `[electionTimeoutMin,
  electionTimeoutMax]`, redrawn on every (re)arm.
- **Single-threaded event loop** (`src/raft/event_loop.h`): inbound RPCs and
  replies are enqueued by the transport I/O thread; one loop thread drains
  the queue into `RaftCore::handle()` and drives `RaftCore::tick()`, sleeping
  until `RaftCore::nextDeadline()`. `RaftCore` is single-threaded by
  contract and lock-free; tests with a `ManualClock` bypass the loop and
  drive the core directly, which is what makes the sim tests deterministic.
- **`PersistentState`** (`src/raft/persistent_state.h`): `currentTerm` +
  `votedFor` behind an interface with a single `save(term, votedFor)` call
  (so Phase 4's durable impl can write both atomically before any reply is
  sent). Memory-backed this phase; the core already saves *before* sending
  any message that depends on the saved state.

### Role transitions as implemented

```
Follower  --election timeout-->                          Candidate
Candidate --election timeout (no majority)-->            Candidate (term+1)
Candidate --majority of RequestVoteReply{term==cur}-->   Leader
Candidate --AppendEntries with term >= cur-->            Follower
any role  --any RPC/reply with term > currentTerm-->     Follower (universal
                                                          term rule: term=T,
                                                          votedFor=null)
```

Exact rules, matching the phase prompt literally:
- **Universal term rule** runs before any other handling of all four Raft
  message kinds (requests *and* replies).
- **Election timer resets on exactly three events:** (1) `AppendEntries`
  with `term >= currentTerm` (valid leader), (2) *granting* a vote,
  (3) starting an election. Stale RPCs and denied `RequestVote`s do not
  reset; higher-term step-down by itself does not reset.
- **One deliberate addition:** a *leader* runs no election timer
  (`TimePoint::max()`), so on leader→follower step-down the timer is
  re-armed. That is arming a timer that did not exist — without it a deposed
  leader could never campaign again (liveness); it is not one of the three
  reset events, which govern an already-armed follower/candidate timer.
- **Vote counting:** only `RequestVoteReply{term == currentTerm,
  voteGranted}` counts; granted votes are a `std::set<NodeId>` so duplicate
  replies count once; self-vote included; majority = N/2 + 1.
- **Up-to-date check** is a pure function (`candidateLogAtLeastAsUpToDate`),
  unit-tested by table; the log is 0/0 this phase.
- A leader receiving a valid `AppendEntries` for **its own term** would be a
  protocol violation (two leaders, one term); it logs at Error and ignores
  rather than stepping down.

### How Election Safety is tested

Every `RaftCore` exposes a transition observer. Integration harnesses record
each `won-election` into `term → set<winners>` and assert every set has size
≤ 1 — over the run's entire history, not a point-in-time sample. The sim
harness (`raft_sim_test.cpp`: ManualClock, in-memory wire with seeded
per-message delays, isolation and kill switches) checks this after **every
simulated millisecond**, including a 20-seed chaotic-timing variant with
delays up to 8× the election timeout, a forced split vote (two candidates
collide in term 1 via colliding RNG seeds, with the third node isolated),
and a partition test that deliberately produces two concurrent leaders in
*different* terms and then steps the stale one down on rejoin. The real-TCP
test (`raft_cluster_test.cpp`, 3 transports + 3 event loops in-process)
applies the same whole-history assertion and is the TSan workload.

## Log replication, commit, and apply (Phase 3)

### Indexing convention

Log indices are **1-based** everywhere. Index 0 is the empty sentinel: an
empty log has `lastIndex() == 0`, `lastTerm() == 0`, `termAt(0) == 0`, and a
`prevLogIndex` of 0 passes the consistency check trivially. Entry terms are
always ≥ 1, which is what lets `conflictTerm == 0` encode "none" on the wire.

### Seams added

- **`RaftLog`** (`src/storage/log.h`): `append / termAt / entryAt /
  entriesFrom / truncateSuffixFrom / lastIndex / lastTerm`, in-memory now,
  durable in Phase 4 behind the same interface.
- **`StateMachine`** (`src/statemachine/state_machine.h`): spec §4.5
  interface; Phase 3 ships only `RecordingStateMachine` (ordered applied-
  command capture for tests). The apply path is deterministic by
  construction: no clock, no RNG, index-ordered.
- **`RaftCore::propose(command)`** (and a thread-safe
  `RaftEventLoop::propose` that runs it on the loop thread): leader appends
  `{currentTerm, command}` and triggers replication, returning the index;
  non-leader returns nullopt. The Phase 5 client layer sits on this.

### AppendEntries receiver (exact order)

1. `term < currentTerm` → `{currentTerm, false}`, no timer reset.
2. Universal term rule already ran; record `leaderId`, Candidate→Follower,
   **reset election timer** — even if step 3 fails: the leader is live and
   its retry will repair us.
3. Consistency check: `prevLogIndex > lastIndex()` → false with
   `{conflictTerm = none, conflictIndex = lastIndex()+1}`;
   `termAt(prevLogIndex) != prevLogTerm` → false with
   `{conflictTerm = termAt(prevLogIndex), conflictIndex = first index of
   that term}`.
4. Walk incoming entries by index. Skip entries already present (same index
   **and** term). At the first term conflict: `truncateSuffixFrom(there)`
   and append the rest. **No-spurious-truncation:** an RPC that is a prefix
   or duplicate of the local log walks off the end of this loop having
   truncated nothing and appended nothing — a delayed/duplicate AE must
   never delete entries, because entries past the RPC's window may be
   committed. Truncating "because the RPC carries fewer entries" is the
   classic bug this rule kills.
5. `commitIndex = min(leaderCommit, prevLogIndex + entries.size())` if it
   grows — capped at what *this RPC* vouches for, since our log may extend
   beyond what this (possibly old) leader has confirmed.
6. Reply success.

### Success-ack reply semantics (deviation from "reply is just a bool")

The leader must update `matchIndex` from the request that produced a success
reply, but replies carry no correlation id and the transport may drop or
deliver out of order; leader-side in-flight bookkeeping desyncs on loss. So
the follower echoes `request.prevLogIndex + request.entries.size()` in the
reply's `conflictIndex` field (meaningless on success otherwise — no wire
change). Leader: `matchIndex = max(matchIndex, ack)`, `nextIndex = ack + 1`.
Acks are monotone within a term (a leader's log only grows in its term), so
the max() guard makes stale replies harmless.

### Conflict-hint backtracking (one RPC per conflicting term)

Follower: too-short → `{none, lastIndex()+1}`; term mismatch →
`{termAt(prev), first index holding that term}`. Leader on a false reply at
its own term: if `conflictTerm == none` → `nextIndex = conflictIndex`; else
find its **last** index `L` with that term — found → `nextIndex = L + 1`,
not found → `nextIndex = conflictIndex`. Clamped to
`[max(1, matchIndex+1), nextIndex]` (never raised by a stale failure, never
below a proven match), then retried immediately. The catch-up sim test
asserts a 30-entry backfill lands in ≤ 5 RPCs.

### Commit rule (Figure 8)

`commitIndex` advances to the highest `N` with `N > commitIndex`, a majority
of `matchIndex` (the leader's own log counts as one replica) at ≥ `N`, and
**`termAt(N) == currentTerm`**. The third clause is the Figure 8 rule: an
old-term entry on a majority can still be overwritten by a later leader that
never saw it commit, so counting replicas of old-term entries is unsound;
they commit only indirectly when a current-term entry above them commits.
Because entry terms are nondecreasing, the scan stops at the first
non-current-term entry. Both sides are tested: the unit test holds an
all-nodes-replicated term-1 entry uncommitted under a term-2 leader until a
term-2 entry lands, and the sim test constructs the full scenario (entry on
a majority at its old term after a leader death + re-election, watched
across 300 heartbeat-rich ms, then committed indirectly). The
overwrite-before-commit branch is the divergent-log repair test.

### Apply loop

All roles: while `commitIndex > lastApplied`, increment and apply, strictly
in index order, exactly once — asserted by the recording state machine in
unit, sim, and real-TCP tests.

## Durable persistence and crash recovery (Phase 4)

### Persistent vs volatile state

- **Persistent (restored on boot):** `currentTerm`, `votedFor`, `log[]`.
- **Volatile (reset on boot):** `role = Follower`, `commitIndex = 0`,
  `lastApplied = 0`, leader `nextIndex[]`/`matchIndex[]`, and the **state
  machine itself**. After restart the node relearns `commitIndex` from the
  leader's `leaderCommit` and rebuilds the state machine by re-applying the
  log from index 1. `commitIndex`/`lastApplied`/SM state are deliberately
  not persisted — that is what snapshotting (Phase 8 stretch) would optimize.

### Durable-commit invariant (stated and tested)

**An entry is reported committed only after a majority holds it durably:**
every node fsyncs (metadata and/or log) before sending any ack that depends
on it, and commit requires a majority of acks. This is exactly why committed
entries survive a crash+restart of any node, including the leader, and a
full-cluster restart. A corollary the leader is entitled to rely on:
`matchIndex[f]` is a *durable* promise — a follower can never legitimately
"lose" an acked entry, and the `nextIndex >= matchIndex+1` clamp depends on
this (a test that tore an *acked* entry off a follower's disk produced an
eternal reject/retry cycle, correctly: that scenario violates the model;
crash-mid-append by definition means the ack never went out).

### The five persist-then-act points

The durable write completes **before** the externally visible action:
1. **Becoming candidate** — persist `{term+1, votedFor=self}` before sending
   any `RequestVote` (`startElection`).
2. **Granting a vote** — persist `{term, votedFor=candidate}` before the
   `voteGranted=true` reply (`onRequestVote`).
3. **Stepping down to a higher term** — persist `{T, votedFor=null}` before
   any action in the new term (`becomeFollower`, which the universal term
   rule runs before all other handling).
4. **Appending entries** — leader `propose` persists before counting its own
   entry toward commit; the follower append path persists before the
   `success=true` reply.
5. **`truncateSuffixFrom`** — durable before the success reply.

No Raft logic changed in this phase: all five orderings were already
structural in `raft_core.cpp` (save/append/truncate happen-before the
corresponding send), so swapping the in-memory implementations for durable
ones behind the same `PersistentState`/`RaftLog` interfaces was sufficient.
This seam is why persistence was deferred to Phase 4 in the first place.

### Metadata file (`<data-dir>/meta`) byte layout

Fixed 20 bytes, little-endian (same convention as the wire format):

| Offset | Size | Field |
|-------:|-----:|-------|
| 0 | 4 | magic `0x4D4D5352` ("RSMM") |
| 4 | 1 | version = 1 |
| 5 | 8 | `currentTerm` |
| 13 | 1 | hasVote (0/1) |
| 14 | 2 | `votedFor` (0 when hasVote = 0) |
| 16 | 4 | CRC32C over bytes [0, 16) |

Atomic update: write `meta.tmp`, `fsync(tmp)`, `rename()` over `meta`,
`fsync(dir)`. A crash at any instant leaves either the old or the new file,
never a mix; a leftover `meta.tmp` is removed on open and the real file
wins. A `meta` that fails magic/version/CRC/size cannot be a crash artifact
(rename is atomic) and is treated as real corruption: the constructor
throws; the node refuses to start.

### Log file (`<data-dir>/log`) record layout

Append-only on the happy path; records back to back, little-endian:

| Offset | Size | Field |
|-------:|-----:|-------|
| 0 | 8 | index (1-based; must equal previous index + 1) |
| 8 | 8 | term |
| 16 | 4 | commandLength |
| 20 | commandLength | command bytes |
| 20 + commandLength | 4 | CRC32C over bytes [0, 20 + commandLength) |

Index contiguity doubles as framing on replay (no separate index file). An
in-memory `index → file offset` vector is rebuilt during replay; the entries
themselves are also fully cached in memory (unchanged Phase 3 memory model —
reads never touch disk). `truncateSuffixFrom(i)` is `ftruncate(offsets[i-1])`
+ `fsync`. Appends serialize all entries of the call into one buffer, one
`pwrite`, one `fsync`. CRC32C (reflected poly `0x82F63B78`) is implemented
in-repo (table-driven, `src/storage/crc32c.*`) — no new dependency.

### Startup replay and torn-write recovery

Records are validated sequentially (CRC + index contiguity). Classification
of the first invalid record:
- **Torn tail → recover silently:** the record's claimed extent runs past
  EOF (incomplete header/body), or the record is invalid and ends exactly at
  EOF (a crash can leave a fully-sized but partially written last record).
  Discard it and `ftruncate` to the end of the last valid record, then
  fsync. Expected after crash-mid-append.
- **Corruption → fail loudly:** an invalid record with more bytes after it
  cannot be a torn append. The constructor throws rather than silently
  dropping possibly committed entries. (Known honest limitation: a corrupted
  `commandLength` that overshoots EOF is indistinguishable from a torn tail
  and is treated as one; random mid-file bit flips are detection-only,
  best-effort — the crash model this phase defends against is torn appends.)

### Storage error policy

Fail-stop: any I/O or fsync failure on the durability path throws, nothing
on the Raft path catches it, the node dies. To the cluster that is just a
crash, which Raft tolerates; continuing past a failed fsync could ack
non-durable state (the one forbidden thing). Recovery-time corruption also
refuses to start (above) rather than guessing.

### fsync policy knob

`rsm::storage::FsyncPolicy { EveryDurabilityPoint, GroupCommit }`, a
`DurableLog` constructor parameter and `raft_node --fsync every|group`.
EveryDurabilityPoint (default) fsyncs every append/truncate before
returning. GroupCommit currently behaves identically — the batched-fsync
path lands with the Phase 7 performance layer; the knob exists now per spec
§4.4 so configs and benchmarks can reference it, and selecting it cannot
accidentally weaken durability today.

### Post-restart commit nuance (full-cluster restart)

`commitIndex` is volatile, and the Figure 8 rule forbids a new leader from
counting replicas of prior-term entries, so after a **full-cluster** restart
the recovered prefix is present and identical everywhere but stays
uncommitted until the first current-term entry commits above it (the
full-cluster-restart test commits one new entry to observe re-apply). Real
deployments append a no-op entry on election win; parked until the client
phases need it. After a **single-node** restart this is invisible — the
surviving leader's `commitIndex` propagates via heartbeats immediately.

## Optimizations (before/after measurements)

*(None yet — performance layer is Phase 7.)*

## Parked non-goals

Dynamic membership, snapshotting/compaction (stretch), multi-host/WAN, BFT, TLS/auth,
query language — per spec §3.

## Phase notes

**Phase 0 — Scaffold (2026-06-10).** Repo laid out per spec §4 (`src/{transport,rpc,raft,
storage,statemachine,client,runtime,metrics}`, `test/`, `bench/`, `faults/`). Top-level
CMakeLists (C++20, `-Wall -Wextra -Werror`, sanitizer options as above) with a `test/`
subdirectory wiring a doctest `unit_tests` binary into ctest; a single sanity assertion
proves the framework executes. `build_and_test.sh` runs the Release build+tests then the
ASan/UBSan Debug build+tests, exactly matching the commands in CLAUDE.md. Acceptance
(test suite builds and runs in both configs) verified by running `./build_and_test.sh`.

**Phase 1 — Transport + RPC (2026-06-10).** Typed wire format in `src/rpc` (12-byte
little-endian envelope + six message payloads with strict, allocation-bounded
decode) and a loopback TCP transport in `src/transport` (4-byte big-endian length
framing via `FrameAssembler`, dial-your-peers connection model, drop-and-log on
unreachable peers — see the Phase 1 section above). Minimal `raft_node` executable
(`--id N --config <file>`, plus a temporary `--ping <peerId>` smoke flag). Tests:
encode/decode round-trips for every message type incl. boundary values and empty
payloads; adversarial framing reassembly (1-byte feeds, mid-prefix splits, coalesced
frames, poisoned-on-oversize); malformed-input rejection (truncations, length-field
lies, unknown type/version, huge entry counts rejected pre-allocation); and an
in-process two-endpoint integration suite exchanging all six types both directions —
plus a real two-process exchange verified by hand. Release and ASan/UBSan ctest runs
are green. One real bug found by the integration tests and fixed: the transport I/O
loop accepted new connections before draining reads, desynchronizing pollfd indices
from the connection list. The TSan gate (required because the transport runs an I/O
thread) was initially blocked on the missing `libtsan` runtime; after
`sudo dnf install -y libtsan` the ThreadSanitizer build and full test suite pass
clean, so all three configs are green: Release, ASan/UBSan, and TSan.

**Phase 2 — Leader election (2026-06-10).** Raft roles/terms/voting in
`src/raft` behind the four mandated seams (injectable `Clock`, seeded RNG,
single-threaded event loop, memory-backed `PersistentState`). Election rules
implemented literally per the phase prompt: universal term rule before all
handling, the three-and-only-three timer-reset events, votes counted only
from the current term, up-to-date check as a unit-tested pure function. One
liveness addition documented above (deposed leader re-arms its election
timer). Tests: 20 new cases / deterministic unit suite (vote rules, timer
rules, step-down, stale/duplicate reply handling) plus sim integration (cold
start, leader failure, forced split vote, 20-seed chaotic timing, partition
+ rejoin step-down) with Election Safety asserted every simulated ms, plus a
real-TCP 3-node cluster test (cold start + leader kill re-election) that
serves as the TSan gate. Release, ASan/UBSan, and TSan ctest runs all green
via `./build_and_test.sh`; a manual 3-process run showed one leader in term
1, and after `kill -9` of the leader, exactly one new leader in term 2. The
temporary `--ping` flag was removed from `raft_node` now that Raft drives
real traffic.

**Phase 3 — Log replication + commit + apply (2026-06-10).** Replicated log
behind the `RaftLog` storage seam (in-memory; Phase 4 swaps in fsync),
`StateMachine` interface with a recording test impl, and `propose()` as the
internal command entry point. Full AppendEntries receiver with strict
ordering (stale-term reject → timer reset → consistency check → walk/skip/
truncate-once/append → capped commit advance), conflict-hint backtracking on
both sides (one RPC per conflicting term — asserted ≤ 5 RPCs for a 30-entry
backfill), the Figure 8 current-term commit rule, and an exactly-once
in-order apply loop. One wire-semantics addition documented above: success
replies echo the request's match point in `conflictIndex` so matchIndex
updates survive lost/reordered replies. Heartbeats are now real AppendEntries
(prevLog* + leaderCommit always carried). Tests: 23 new cases / unit level
(log ops, consistency, hints both sides, no-spurious-truncation, Figure 8
by count, apply discipline, propose) plus sim integration (normal
replication, O(terms) catch-up, protocol-driven divergent-log repair,
delayed-duplicate AE, full Figure 8 scenario) with Log Matching and State
Machine Safety asserted in every sim test, plus a real-TCP test replicating
30 commands across a leader failover with identical applied sequences.
Release, ASan/UBSan, and TSan gates all green (68 cases, 971 assertions).

**Phase 4 — Durable persistence and crash recovery (2026-06-11).** Durable
implementations behind the existing seams — `DurablePersistentState`
(atomic-rename metadata file) and `DurableLog` (append-only CRC32C-framed
log with index→offset map and torn-tail recovery) in a new `storage`
library — plus in-repo CRC32C; **zero changes to `raft_core.cpp`**, since
all five persist-then-act points were already structurally ordered behind
the interfaces (verified by call-site audit, listed above). `raft_node` now
takes `--data-dir` (and the `--fsync every|group` knob) and prints its
recovered state on boot. Test harnesses gained kill+restart: `SimCluster`
takes a storage factory and `restart()` rebuilds a node from disk exactly
like a restarted process; the real-TCP `Cluster` does the same on the same
port. Tests: 9 unit cases (metadata round-trip/atomicity/corruption, log
round-trip incl. offset map, flipped-byte detection, four torn-tail
variants, durable truncation, CRC vector) and 5 durable-sim integration
cases (vote survives restart and a different candidate is refused in-term;
term never regresses; committed entries survive follower and leader
crash+restart; honest crash-mid-append with leader re-replication;
full-cluster restart with identical recovered prefix and in-order re-apply)
plus a real-TCP leader-crash+restart case that doubles as the TSan workload
for the new code. One instructive test bug en route, documented above:
tearing an *acked* entry off a follower violated the durable-ack invariant
and correctly produced an eternal retry cycle — the scenario, not the code,
was wrong. A manual 3-process run confirmed `kill -9` of the leader and
restart recovers `term/votedFor` from disk and rejoins at the new term.
Release, ASan/UBSan, and TSan gates all green via `./build_and_test.sh`
(83 cases, 1251 assertions in the unit binary).
