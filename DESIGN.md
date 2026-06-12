# DESIGN.md — Raft Replicated State Machine

Running log of non-obvious decisions, optimizations (with measured before/after),
and how each safety invariant is tested. Authoritative spec: `../raft_rsm_build_spec.md`.

## Safety invariants (from spec §4.3) and how each is tested

| # | Invariant | Test (filled in as phases land) |
|---|-----------|--------------------------------|
| 1 | Election Safety — at most one leader per term | Asserted over the **whole history** of every Phase 2 integration run: a transition observer records each `won-election` as (term → node); the assertion is that no term maps to two nodes. Checked after every simulated millisecond in `raft_sim_test.cpp` (cold start, leader failure, split vote, 20-seed chaotic timing, partition/rejoin) and across the real-TCP run in `raft_cluster_test.cpp`. Since Phase 6: `ElectionSafetyChecker` monitors every election win continuously through every seeded chaos run; self-tested against a fabricated double-leader sighting. |
| 2 | Leader Append-Only — a leader never overwrites/deletes its own log entries | Enforced by construction: `truncateSuffixFrom` has exactly one call site, inside the AppendEntries *receiver* after the leader role is excluded (a leader ignores a valid AE at its own term and steps down to follower before processing a higher-term one), and `propose()` only appends. Exercised by every Phase 3 leadership-change test. Since Phase 6: `LeaderAppendOnlyChecker` fingerprints every leader's log every simulated millisecond of every chaos run and flags any shrink or in-place change during one leadership; self-tested against fabricated shrink/overwrite. |
| 3 | Log Matching — same index+term ⇒ identical logs up to that point | `SimCluster::checkLogMatching()`: for every pair of live logs, at every index where both hold the same term the full prefixes are compared entry-by-entry. Run at the end of every Phase 3 sim test (normal replication, catch-up, divergent repair, duplicate-AE, Figure 8). Since Phase 6: `checkLogMatching()` sweeps all live logs every 25 simulated ms and at quiescence of every chaos run; self-tested against fabricated same-term divergence. |
| 4 | Leader Completeness — a committed entry is present in all future leaders' logs | Exercised by the Figure 8 sim test and the real-TCP failover test (new leader holds and re-commits every committed entry), and since Phase 4 across **crashes**: committed entries survive follower/leader crash+restart and full-cluster restart (durable sim + real-TCP restart tests), resting on the durable-commit invariant below. Since Phase 6: `CommitChecker` keeps the global record of every entry ever observed committed and asserts, at **every election win** in every chaos run, that the new leader holds the entire record; the deterministic five-node Figure 8 scenario closes the Phase 3 deferral (both branches). Self-tested against a fabricated short/divergent winner. |
| 5 | State Machine Safety — no two nodes apply different commands at the same log index | `SimCluster::checkStateMachineSafety()`: pairwise position-by-position comparison of the recorded applied sequences, run in every Phase 3 sim test; the real-TCP test asserts byte-identical applied sequences across nodes and across a failover. Since Phase 6: `AppliedConsistencyChecker` checks **every apply on every node** in every chaos run against the global index → (entry, result) record — including re-applies after crash recovery, and including the apply *result* (replica determinism); self-tested against fabricated divergent commands and divergent results. |

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

## Client sessions, exactly-once dedup, and the KV store (Phase 5)

### Decision points (phase prompt §"Decision points")

1. **Reads go through the log.** Every `GET` is a replicated command,
   committed and applied like a write, so it is linearizable by the same
   argument as writes (it reads at its log position). ReadIndex/leases are
   deferred to the performance phases with a measured justification.
2. **`clientId` is client-supplied** (u64; the wire field existed since
   Phase 1). No `RegisterClient` command: at this scale a random u64 per
   client makes collisions negligible, tests use fixed ids, and one fewer
   special log-entry type keeps apply uniform. Revisit if real id allocation
   is ever needed.
3. **No session eviction.** The session table grows with distinct clients —
   bounded by test scale. CONSTRAINT for whoever adds eviction later: it
   must be deterministic and driven by replicated applied state (e.g.
   logical progress), NEVER a local wall-clock timer — a local timer
   diverges replicas and breaks State Machine Safety.
4. **No leader duplicate short-circuit.** A duplicate retry is appended like
   any command; apply-time dedup turns it into a cached-result no-op. Log
   bloat per retry is acceptable at this scale, and there is exactly one
   code path that answers clients.

### Dedup design (the correctness core)

- Identity is `(clientId, seqNo)`, seqNo monotonic per client starting at 1;
  a retry NEVER gets a new seqNo (KvClient guarantees this; `resendLast()`
  is the test hook for "the ack was lost").
- The session table `clientId -> {lastSeq, lastResult}` lives INSIDE
  `KVStateMachine`, i.e. in replicated applied state, and dedup happens
  inside `apply()`, deterministically, on every replica: `seqNo <= lastSeq`
  → return cached result, no side effect; else apply, record, return.
- Why apply-time and replicated: a client retries the same request across a
  leader failover. The new leader's applied state already contains the
  session entry, so re-proposing the retry commits a duplicate entry whose
  apply is a no-op returning the cached result. Leader-side receive-time
  dedup would re-apply the side effect on the new leader. Verified with the
  non-idempotent APPEND op in sim and over TCP.
- Only the LATEST result per client is cached ⇒ one outstanding request per
  client (the standard Raft session assumption; KvClient is synchronous).
  An older seqNo still returns the latest cached result, never re-applies.
- `clientId 0` opts out of sessions (internal/test commands, no dedup).
- The session table is part of `serialize()`/`deserialize()` alongside the
  KV map, so a Phase 8 snapshot cannot lose dedup state (unit-tested:
  dedup still suppresses a replayed duplicate after a round-trip).

### KV state machine

Ops `PUT/GET/DELETE/CAS` plus **`APPEND`** (value += arg), added
deliberately as the non-idempotent probe the exactly-once tests need.
Deterministic by construction: `std::map`, no clock, no RNG. CAS treats an
absent key as `""` (create-if-absent in one primitive); empty-string values
are real values, distinct from absent, for PUT/GET/DELETE. Command bytes
(replicated verbatim, LE): `[u64 clientId][u64 seqNo][u8 op]
[u32 keyLen, key]([u32 argLen, arg]([u32 arg2Len, arg2] CAS only))`.
Result string: status byte `'O'/'N'/'F'/'E'` (ok / not-found / cas-failed /
malformed) + optional payload (GET value; APPEND new value). Malformed
commands return `'E'` with no side effect and are never cached.

### Client request path (server side)

Two new RaftCore seams, both invoked on the event-loop thread and both
logic-free for Raft: `setClientRequestHandler` (routes ClientRequest out of
`handle()`) and `setApplyObserver` (index, entry, result after each apply).
`ClientService` sits on them: non-leader → `NOT_LEADER` + believed leader
hint; leader → validate that the command's embedded 16-byte identity prefix
equals the wire `(clientId, seqNo)` (reject with `Error` otherwise — the SM
dedups on the embedded copy, reply correlation uses the wire copy, they must
agree), `propose()` verbatim, and record `index -> (client, identity)` in a
pending table. On apply at that index: identity matches → `OK` + the
apply() result; mismatch (our entry was truncated and another leader's
committed there) → `NOT_LEADER`, the client retries safely. Pending entries
are not flushed on step-down — they resolve via the identity check or the
client's timeout (tested: the overwritten-pending case answers NOT_LEADER,
never a wrong OK).

### Client library and routing protocol

`KvClient` (synchronous, raw TCP per attempt): send to the believed leader
(else round-robin), await the reply on the same connection with a
per-attempt timeout; on `NOT_LEADER` with a hint follow it immediately; on
timeout/connect-failure/hint-less `NOT_LEADER` forget the preference and
rotate after a 25 ms backoff, up to a bounded attempt budget. Retries reuse
the same `(clientId, seqNo)`. A result is surfaced only from an `OK` reply,
which the leader sends only after commit+apply.

### Transport: inbound reply routing

Clients are not in the peer table, so `send(to)` falls back to the inbound
connection that most recently delivered a message with envelope
`from == to` (map maintained by the I/O thread; the route mutex serializes
reply writes against connection close, and a failed/raced write just drops
the reply — the client times out and retries like any lost message).
Convention this rests on: client envelope ids are unique and disjoint from
cluster node ids (cluster ids are small; tests use 99/200/201, kv_cli
defaults to 100). The two-simplex-connections model for peers is unchanged.

## Deterministic simulation, fault injection, and chaos (Phase 6)

### The sim/real seam

`RaftCore` never knew about TCP: it emits through a `SendFn` callback,
receives through `handle()`, takes time from the injected `Clock`, and
randomness from its seeded PRNG. Those four seams (in place since Phases
1–2) are the entire sim/real boundary. In sim mode (`faults/`):

- `SimNetwork` replaces TCP: an in-memory bus; each node's `SendFn` (and
  its `ClientService`'s) posts onto it.
- The shared `ManualClock` replaces `SteadyClock`.
- `SimHarness::stepMs()` replaces `RaftEventLoop` (which needs a real
  clock and a thread): it advances virtual time 1 ms, ticks every live
  node in node-id order, then drains every due message — including
  same-step cascades — in the bus's deterministic order. **No OS threads
  exist in a sim run.**

Everything else — `raft_core.cpp`, `DurableLog`/`DurablePersistentState`
(chaos runs use the real Phase 4 files in a temp dir, so every crash
exercises real replay), `KVStateMachine` with its session table, and
`ClientService` — is byte-for-byte the production code. The earlier, more
limited `test/sim_cluster.h` harness is kept as-is for the Phase 2–5 tests;
the `faults/` harness is the Phase 6+ chaos instrument.

### Fault model (`faults/`, the /faults control surface)

Network — all decided at send time by one seeded RNG: **drop** probability,
**latency** uniform in [min, max] per message, **reorder** (probability of
up to `extraMax` additional delay, letting later messages overtake), and
**partition** into arbitrary groups (a cluster node in no group is fully
isolated; a partition also kills in-flight cross-group messages at
delivery — "isolate now" means now). Client traffic shares drop/latency/
reorder but bypasses partitions (clients may dial any node, like the real
`KvClient`). Node — **crash** (instant stop; messages already on the wire
keep flying — packets don't vanish when their sender dies; durable state
survives via the storage factory, volatile state is rebuilt from a fresh
`RaftCore`), **restart**, **leader kill** (crash the highest-term live
leader), and **graceful stop**, implemented as crash and documented as
observably identical here: every durability point is synchronous
(fsync-before-ack), so a clean shutdown has nothing to flush; torn-write
recovery is covered by the Phase 4 storage tests.

### Determinism rules (load-bearing)

- One RNG per concern (network, fault schedule, exec-time target picks,
  per-client workload, per-node election timers), all derived from the
  master seed via splitmix64.
- Every `send()` draws the same number of RNG words regardless of current
  fault settings, so toggling faults never desynchronizes the stream.
- Fault decisions use raw modulo/threshold on `mt19937_64` output —
  `std::uniform_*_distribution` is implementation-defined, so it is kept
  out of the sim path (reproducibility is guaranteed per binary either
  way; this just removes one source of variation, and modulo bias is
  irrelevant for chaos). `RaftCore`'s internal timeout draw still uses
  `uniform_int_distribution` — unchanged production code, deterministic
  for a seed on a given build.
- Delivery order is a strict weak order on (deliverAt, enqueueSeq); maps
  (never unordered containers) everywhere on the sim path; no wall clock.
- Restart RNG seeds derive from (master seed, node, incarnation number).
- Determinism is a **tested property**: the determinism test runs the same
  seed twice and asserts the full run traces (every delivery, fault,
  transition, apply, client invoke/ack — temp-dir paths excluded by
  construction) are identical line for line.

### Sim clients and reply correlation

The real `KvClient` is synchronous over per-attempt TCP connections and
cannot run inside the single-threaded simulator, so `SimClient` replicates
its **protocol** as a virtual-time state machine: route to believed leader
(else seeded rotation), follow NOT_LEADER hints, 25 ms backoff, 400 ms
attempt timeout, and a retry NEVER changes `(clientId, seqNo)`. Reply
correlation mirrors connection-per-attempt: each attempt uses a fresh
envelope id from the client's id range, and only a reply addressed to the
current attempt's id from the current target counts — a delayed OK from an
old attempt can never be credited to a newer operation (chaos found this
ambiguity immediately; real TCP never had it because replies arrive on the
connection that asked).

### Workload distribution (decision)

4 clients × ≤12 ops over a 4-key space ("k0".."k3" — deliberately tiny for
contention), mix PUT 30 / GET 25 / APPEND 20 / CAS 15 / DELETE 10, seeded
think time 200–1200 ms between ops so the workload **spans the fault
window** (the first cut of the suite finished all ops before the first
fault landed and proved nothing — visible because committed entries ≈
acked ops, i.e. no retry duplicates were being deduped). Every written
value encodes (client, seq) so all writes are distinguishable to the
checker. Each client holds one final marker PUT until 1.5 s after
heal-everything: it witnesses post-heal liveness and forces a current-term
commit, which is also what re-propagates `commitIndex` after a
full-cluster restart (the Phase 4 nuance).

### Checkers (reusable, self-validated — `faults/checkers.h`)

All checkers consume plain data (terms, node ids, log images = per-index
(term, FNV-1a fingerprint)), accumulate violation strings, and never
abort — so self-tests can feed fabricated violations and assert they are
FLAGGED. A checker that cannot fail proves nothing; every checker has a
must-flag self-test. Election Safety: every win, whole run. Leader
Append-Only: per-step log-prefix comparison per leadership. Log Matching:
pairwise sweep every 25 sim-ms + at quiescence. State Machine Safety:
every apply vs a global index → (entry, result) record — re-applies after
recovery are checked against pre-crash history, and result equality
additionally checks replica determinism. Leader Completeness +
no-lost-commit: `CommitChecker` extends a global committed record from
every node's `commitIndex` each step, flags any committed entry changing
on any node, and at every election win asserts the winner holds the whole
record; at run end every acked `(clientId, seqNo)` must appear in the
committed log.

### Linearizability checker (decision: per-key WGL)

History = one `ClientOp` per `(clientId, seqNo)` — invocation at first
send, response at the OK (dedup makes a retried op one logical operation;
its effect provably falls inside that window: proposed after invocation,
acked only after an apply). Keys are independent registers, so the history
is partitioned by key, then a Wing&Gong/Lowe-style search per key:
DFS over "linearize some eligible op next", where eligible means invoked
no later than the earliest response among remaining completed ops
(real-time order); a completed op's recorded result must exactly match the
sequential model (a mirror of `KVStateMachine::applyOp`, including CAS's
absent-means-"" and APPEND returning the whole new value — APPEND results
make lost updates directly visible); incomplete ops may linearize anywhere
after invocation or never. Memoized on (linearized-set bitmask, register
value); accepted when all completed ops are linearized. Sound by
construction — it only ever accepts a real linearization. Self-validated
both ways: known-good histories (sequential, validly-concurrent, pending-
op-took-effect, absent-vs-empty edge cases) accepted; hand-crafted bad
ones (stale read, ghost read, value moving backwards, impossible CAS, lost
APPEND update, ignored completed write) rejected.

### Seed/replay workflow

`ctest` runs the suite at seeds 1–20 (plus the fault-layer units, checker
self-tests, determinism test, and Figure 8); a 300-seed sweep of the same
configuration passed clean before the phase gate. Any failing seed prints
itself plus the exact replay line; `chaos_sim`'s defaults are the ctest
suite's options, so replay is exactly:

```
./build/faults/chaos_sim --seed N          # add --trace for the full run log
```

`chaos_sim --seed 1` is itself a ctest case so the replay path can't rot.
Schedule shrinking/minimization: not implemented (optional per spec);
the trace plus determinism makes manual minimization workable.

### Figure 8 (Phase 3 deferral closed)

`figure8_sim_test.cpp` builds the paper's five-server scenario
deterministically — fixed per-node election timeouts (min == max) script
every election winner; partitions do the rest. Both branches are asserted
from one shared fixture: the old-term entry sits on a majority while
`commitIndex` provably stays put across 500 heartbeat-rich ms; then (a)
the overwrite branch — a rejoining higher-term leader that never saw the
entry destroys it on a majority, legal precisely because replica count
never committed it; and (b) the commit branch — a current-term entry on
top commits it indirectly, after which it survives the disruptor's
rejoin. The run-wide checkers (notably `CommitChecker`) stay green through
both, which is the point: counting replicas of old-term entries is the bug
this design rule kills, and the harness can now demonstrate it end to end.

## The performance layer (Phase 7)

### Thread topology (decision point 2)

Four threads per node in the real runtime (`src/runtime/node_runtime.*`),
connected by hand-written lock-free rings; the Raft core remains the single
owner of Raft state (the Phase 2 invariant), and **all** of this is invisible
to the simulator, which still drives `RaftCore` directly with zero threads:

```
transport I/O thread ──(inbound MPSC)──▶ Raft thread
  (rx: read+decode)                (sole owner of RaftCore;
                                    ClientService requests/batching)
                                       │                 │
                              (raftTx SPSC)      (apply SPSC, lossless)
                                       │                 │
                                       ▼                 ▼
                                  tx thread ◀─(applyTx SPSC)─ apply thread
                              (socket writes)        (sm.apply + replies)
```

- Outbound messages are **encoded on the producing thread** (Raft or apply)
  directly into a ring slot's frame buffer; the tx thread only does socket
  writes, so a slow peer can never stall Raft logic.
- The committed→apply ring is **lossless**: a committed entry must reach the
  state machine exactly once, in order. Full means the sink REFUSES — the
  core stops advancing `lastApplied` and the Raft loop's `pumpApply()`
  re-offers the entry once the apply thread frees a slot, so **the Raft
  thread never blocks on the state machine**: a slow apply delays client
  replies, never heartbeats or elections. (The first cut blocked here; a
  group-commit burst into a slow SM could stall the Raft thread past the
  election timeout — caught in review, redesigned, and pinned by the
  slow-apply cluster test, which demonstrably fails against the blocking
  version with a spurious election.) Both tx rings and the inbound ring
  **drop** when full: Raft messages and client requests/replies are
  loss-tolerant by design (timers and client retries are the recovery path,
  identical to an unreachable peer).
- One rx and one tx thread (not split): loopback writes are cheap; the
  measured bottleneck was never the socket thread.
- Apply on its own thread via a new `RaftCore::setApplySink` seam: when set,
  `applyCommitted()` hands `(index, entry)` over in index order instead of
  applying inline. The sim never sets it, so its apply path is untouched.
- `ClientService` accordingly became thread-aware: `onClientRequest` stays
  on the Raft thread; `onApplied` runs on the apply thread, so the pending
  table is mutex-guarded (uncontended in sim/legacy) and the leader hint it
  needs is cached in an atomic refreshed on the Raft thread — the apply
  thread never touches live core state. Pending insert/apply ordering is
  safe without further coordination because a commit needs a peer ack that
  can only be handled after `onClientRequest` returns (one event at a time
  on the Raft thread); a 1-node cluster would violate this, the spec's
  topology is 3 nodes.
- `RaftEventLoop` (Phase 2's mutex+condvar loop) is kept compilable as the
  bench's `--runtime legacy` baseline; `raft_node`, the TCP test harness,
  and the bench default all run the threaded runtime.

### Ring buffers (decision point 1) and memory-ordering rationale (point 5)

Both rings live in `src/runtime/{spsc,mpsc}_ring.h`: bounded, power-of-two
capacity, zero allocation after construction, cache-line-padded indices,
TSan-clean under the high-iteration `ring_tests` stress.

**SPSC** — Lamport ring with monotonic 64-bit indices (slot = `index & mask`,
no wasted slot) plus cached opposite indices so each side touches the other's
cache line only when the ring looks full/empty, not per operation.
Orderings: the producer publishes a slot with `tail.store(release)` pairing
with the consumer's `tail.load(acquire)` (slot contents visible before they
are read); the consumer retires with `head.store(release)` pairing with the
producer's `head.load(acquire)` (move-out happens-before overwrite). Each
side reads its own index relaxed (it is that atomic's only writer). No
seq_cst anywhere: there is no invariant beyond those two pairwise edges.

**MPSC** — Vyukov bounded array MPMC restricted to one consumer: each cell
carries a sequence number encoding its state; producers claim positions with
a relaxed CAS on `enqueuePos` (the CAS only arbitrates ownership — all data
visibility flows through the cell's seq, exactly Vyukov's design), publish
with `cell.seq.store(release)`, and the consumer recycles cells with a
release store one lap ahead; both sides read seq with acquire. The dequeue
position is plain non-atomic state — single consumer by contract.

In-place `tryProduce(fill)`/`tryConsume(use)` variants write/read slots where
they live, which makes **the ring itself the buffer pool** (see allocation
discipline below).

### Blocking vs busy-spin waits (decision point 4)

Consumers choose per `NodeRuntimeConfig::waitMode` (`raft_node --wait
block|spin`, default block):

- **Block** — a `WakeGate` per consumer: an event-count packing
  `epoch<<32|waiters` in ONE atomic, RMW'd seq_cst by both sides. The two
  RMWs on the same variable are totally ordered, which closes the classic
  missed-wakeup race *without* `atomic_thread_fence` — deliberately, because
  TSan cannot model fences and GCC's `-Werror=tsan` rejects them. Producer:
  publish (ring release-store), bump epoch, notify only if waiters > 0.
  Consumer: register waiter, re-check work, then wait on the condvar for
  epoch change/work/deadline, with a 5 ms cap as a liveness floor. The Raft
  thread's wait deadline is min(core timer deadline, batch linger deadline).
- **Spin** — `SpinBackoff` escalation: ~4096 `pause` instructions (a few µs
  hot), then 64 `yield`s (plays fair under oversubscription), then parks
  20 µs per iteration so an idle cluster does not pin cores forever.

Measured (final sweep below): block is the best default at moderate
concurrency; spin buys the best p50 at low concurrency and the best
throughput at high concurrency with batching, at the cost of hot cores —
exactly the documented trade-off the knob exists for.

### Batching / group commit (decision point 3)

`ClientService::Batching{maxBatch, linger}` (`raft_node --batch N
--linger-us N`, default OFF so every pre-Phase-7 path is bit-identical). The
leader buffers validated commands and flushes via the new
`RaftCore::proposeBatch`: one multi-entry `log_.append()` call — which the
storage layer already treats as ONE durability point, i.e. **one fsync** —
then one `AppendEntries` per peer. A batch flushes on size or when the
linger deadline passes; the linger driver is the runtime's per-iteration
service hook (`flushIfDue(now)`), which in the sim is called with the
ManualClock — a deterministic scheduled event. Step-down between buffering
and flush answers every buffered client NOT_LEADER, the same as an immediate
propose failure. Equivalence is a tested property
(`batching_equivalence_test`): a fixed 10-op workload (10 % 4 ≠ 0 forces the
linger path) batched vs unbatched produces byte-identical replicated logs on
all nodes and identical per-request results. The fsync knob story: with
batching, `--fsync every|group` are equivalent by construction — every
append call is fsynced, batching is what makes one call carry N entries.

### Allocation-free steady state (§4.7)

Enforced by `alloc_tests`, which overrides global `operator new`, charges
every allocation to its thread, and asserts **zero "plumbing" allocations on
the raft, apply, and tx threads of every node** over a multi-second steady
window under concurrent client load (20k+ ops measured). Two accounting
buckets (`src/metrics/alloc_gate.h`): *plumbing* (asserted zero) and
*retained* — explicitly `AllocRetention`-tagged sites whose allocations are
data-proportional, not message-proportional: the durable log's in-memory
copy + serialization buffer, KV/session state inside `apply()`, and the one
log-bound copy of each client command. What made zero possible:

- ring slots written/read in place + **preallocated** slot buffers (2 KiB
  per tx frame, 1 KiB per apply command — first-touch growth otherwise leaks
  one allocation per slot per new high-water size, which the test caught);
- `RaftCore` reuses one `AppendEntries` message whose entry/command buffers
  cycle through a prefilled recycle pool (64 entries × 4 KiB), and stages
  propose/append entries in reusable scratch vectors; `RaftLog` gained a
  move-from-span `append()` overload and a zero-copy `entriesSpan()`;
- `ClientService` replaced its `std::map` pending table with a fixed
  8192-slot table keyed by `index & mask` (live indices can't collide —
  their window is bounded by the ring sizes; a colliding *stale* entry is
  overwritten, matching the old map's never-answered behavior), and reuses
  its OK-reply message;
- `KvClient` keeps its connection across **cleanly completed** exchanges
  only — any timeout/failure closes it, so a straggler reply can never be
  credited to a later request (the Phase 5/6 correlation guarantee holds).

The **rx thread is allocation-free per message too** (review follow-up; it
was initially left as a ~2-allocs/msg residual): the transport hands the RAW
frame to the runtime (`Transport::setRawHandler`), which decodes straight
into the inbound ring slot via `rpc::decodeMessageInto` — same strict
validation as `decodeMessage` (which now delegates to it), but reusing the
slot's existing variant alternative and salvaging buffers across alternative
switches through a per-slot `DecodePool` (prestocked at construction). Two
more per-wakeup sources fell out of the same hunt: `FrameAssembler::feed`
took `const std::function&`, so the capturing rx lambda paid a type-erasure
heap allocation per read (feed is now templated on the handler), and the
`ioLoop` pollfd set was rebuilt without capacity reuse. The allocation test
asserts rx plumbing ≤ a small constant — connection-lifecycle events (an
accept after a client reconnect) legitimately allocate per CONNECTION and
are tagged/budgeted; per-message churn would register in the thousands.

### One perf bug found and fixed en route: the AE↔ack storm

Under concurrent load the leader sustained >170k msgs/s for ~400 client
ops/s, latency growing with queue depth. Cause: `onAppendEntriesReply`
re-sent AppendEntries on EVERY success ack while any entry was unacked —
duplicate acks (from the pipelined propose-time AEs) spawned redundant AEs,
each spawning another ack: a self-sustaining storm. Fix: follow-up AE only
when the ack ADVANCED matchIndex. Safety unaffected (Raft tolerates
arbitrary duplication/loss; heartbeats remain the retransmission backstop) —
the whole chaos suite stays green, and the rule is pinned by a dedicated
regression test (duplicate/stale acks send nothing; the heartbeat carries
the retransmission). Measured: 182 → 6,410 ops/s, p50
24 ms → 0.6 ms at 4 clients. Also: accepted sockets now set TCP_NODELAY for
parity with outbound ones — measured NO delta on this workload; recorded as
config hygiene, not an optimization.

## Optimizations (before/after measurements)

Workload: `bench/phase7_bench` (in-process 3-node cluster, closed-loop
PUT clients, 16-byte values; data dirs on tmpfs unless noted), medians of 3×
via `bench/phase7_sweep.sh` where given. This laptop (i5-1235U, 2P+8E cores)
is thermally noisy: only numbers within one sweep are comparable; the
rigorous pinned harness is Phase 8.

| change | configuration | before | after |
|---|---|---|---|
| AE-storm fix (ack-advance resend rule) | 4 clients, legacy | 182 ops/s, p50 24 ms | 6,410 ops/s, p50 0.61 ms |
| client connection reuse | 4 clients, threaded (interleaved A/B) | 4,930 ops/s | 7,510 ops/s (+52%) |
| thread split + rings, block wait | 4 clients | legacy: 16.9k ops/s, p99 2.95 ms | 24.2k ops/s (+43%), p99 291 µs (10×) |
| thread split + rings, spin wait | 8 clients | legacy: 11.4k ops/s, p99 14.2 ms | 19.8k ops/s (+74%), p99 1.26 ms (11×) |
| busy-spin vs block (within threaded) | 1 client p50 | block 75 µs | spin 70 µs (legacy two-hop loop: 53 µs — fewer hops win at zero concurrency; documented trade-off) |
| group commit batch=8 + spin | 8 clients, tmpfs | legacy 11.4k ops/s | 30.9k ops/s (2.7×), p50 227 µs, p99 852 µs |
| group commit batch=4 | 4 clients, **btrfs disk** (fsync-bound) | 376 ops/s, p50 10.0 ms | 619 ops/s, p50 6.6 ms |
| group commit batch=16, 16 clients | **btrfs disk** | 376 ops/s (legacy) | 2,159 ops/s (5.7×), p50 7.1 ms |
| allocation-free steady state | alloc_tests, ~17k-op window | raft ~2.4k, rx ~8.2/msg plumbing allocs | **0** plumbing allocs on raft/apply/tx AND per-message rx, all nodes |

Known honest caveats: (a) at zero concurrency the extra pipeline hops cost
~20 µs p50 vs the legacy loop; (b) batch size must be ≤ offered concurrency
or the linger dominates (batch=8 at 4 clients regresses — the knee-of-curve
sweep is a Phase 8 deliverable); (c) the bench runs 3 nodes in ONE process
(12 pipeline threads on 12 hybrid cores), which understates the threaded
runtime relative to one-process-per-node production.

## The benchmark harness (Phase 8)

Governing principle: **credible numbers, not impressive-looking ones.** The
harness is built so that the three classic ways a benchmark lies —
coordinated omission, unreproducibility, and silently measuring a degraded
system — are each closed off by a *tested* mechanism, not by good
intentions. Everything below lives in `/bench` (plus its self-tests in
`/test`); zero production-code changes beyond the client-buffer reuse noted
at the end.

### Topology decision: instrumented in-process cluster

The system under test is the full production wiring — per node: TCP
transport on a loopback port, durable storage, `KVStateMachine`,
`ClientService`, `RaftCore`, the Phase 7 threaded `NodeRuntime` — built
**in one process** (`bench/bench_cluster.*`), exactly like the TCP test
harness and `phase7_bench`. Rationale: the spec's production topology is
already "3 processes on ONE host over loopback TCP", so the host-level
contention picture (total threads, cores, loopback sockets) is identical;
the pipeline is allocation-free so there is no hidden allocator sharing;
and in-process is what lets every instrument below attach through existing
seams instead of adding control RPCs to `raft_node`. Recorded as a
methodology caveat in the README. The cross-process wiring itself is
separately validated and on record: a real three-`raft_node`-process
cluster (plus `kv_cli` as a fourth process per op) elects a leader and
serves put/get/append/cas/del with correct semantics —
`bench/results/phase8/three_process_smoke.txt`. The headline numbers are
in-process **by design**; a real network would add an RTT-shaped, roughly
additive term on top of them, not change the comparisons between cells.

### Load generation (decision: fixed-rate open-loop + closed-loop)

- **Closed-loop** (`runClosedLoop`): N clients issue→wait→issue. Used for
  saturation throughput and *service* latency. Documented limitation: it
  self-throttles, so it understates tail latency — never the tail source.
- **Open-loop** (`runOpenLoop`): deterministic fixed-rate arrivals (spec
  §7 says "fixed arrival rate"; Poisson considered and rejected — fixed
  intervals make rate fidelity exactly checkable and match wrk2-style
  practice), total rate R striped round-robin over T synchronous threads
  (thread j fires at offset j/R, then every T/R). Bounded in-flight (≤ T)
  is a documented deviation from a true open system; T is sized well above
  the in-flight demand below saturation (48 threads for the sweeps).

### Coordinated-omission correction (decision: intended-send-time)

Every request's latency is measured **from its scheduled send time**, not
its actual send time; a generator thread that falls behind issues late
requests immediately and they record the queueing they actually suffered.
The schedule is never edited: every op with intended time inside the window
is issued and measured (or counted `abandoned` if the drain cap trips,
which marks the run oversaturated and its tail a lower bound).
HdrHistogram-style expected-interval back-fill was considered and rejected:
back-fill reconstructs samples a closed recorder *skipped*, but this
generator skips nothing, so back-fill would double-count. The
actual-send-time histogram is recorded alongside as the visible "what
coordinated omission would have reported" control.

**Self-test (must bite, like the Phase 6 checker self-tests):**
`bench_tests` injects a one-shot 500 ms stall into every node's state
machine (`StallGate`/`StallableSM`) mid-window during an open-loop run and
asserts the intended-time tail reports it while the actual-time tail hides
it. Measured: intended p99 = 469.8 ms / p99.9 = 499.1 ms vs actual p99 =
1.9 ms — a ~250× gap that IS coordinated omission, demonstrated and pinned.

### Histogram decision: in-repo HDR-style, not HdrHistogram_c

The latency instrument is the project's own `metrics::LatencyHistogram`
(Phase 7): an HDR-style log-linear histogram — 64 linear sub-buckets per
power-of-two magnitude, so worst-case quantization is 1/64 ≈ 1.6 % of the
recorded value — with fixed storage, O(1) allocation-free recording, and
lock-free per-thread instances merged after the run. The spec's allowed
dependency `HdrHistogram_c` was deliberately NOT vendored: its two
advantages are finer precision and the built-in expected-interval
coordinated-omission API, and this harness needs neither. Precision: 1.6 %
quantization is more than an order of magnitude below the run-to-run
variance of every reported number (sweep cells vary several percent across
repeats at p50 and tens of percent at p99.9 — see the medians-of-3
discipline), so library precision would be false precision. CO handling:
the correction lives in the measurement layer — latency is computed from
the intended send time before it ever reaches a histogram — so the
recording API needs no CO awareness, and the stall self-test validates the
correction end to end. Anywhere these documents say "HDR" or
"HdrHistogram-style", they mean this in-repo implementation; no external
histogram library is linked.

### Measurement path: allocation- and lock-free in steady state

The Phase 7 discipline extended to the observer: the generator hot loop
(schedule arithmetic, request/response, histogram record into the
fixed-storage in-repo `LatencyHistogram`) performs **zero** steady-state
heap allocations, asserted by `bench_alloc_tests` (global operator-new
override, `loadgen` role, plumbing == 0). This required reusing buffers
inside `KvClient` (command scratch, request `Message`, frame buffer,
`FrameAssembler` + `reset()`, `decodeMessageInto` slot) — behavior
identical, covered by the existing client/cluster suites.

### Internal commit latency (request enqueued at leader → committed)

Measured entirely **on the Raft thread**, so it needs no locks and cannot
race: the bench's client-request-handler wrapper stamps
`(clientId, seqNo) → t_enq` into a preallocated open-addressed table; the
service-hook wrapper scans `core.commitIndex()` each loop iteration,
resolves each newly committed index to its identity via the leader's own
log (`entryAt`, zero-copy), and records `now − t_enq`. Histograms leave the
Raft thread only via a request/ack handshake (the Raft thread copies into
`snapOut`), so readers never race the writer — the tap adds two table
operations per request to the Raft thread and nothing to any client path.

### Leadership stability is a benchmark health signal

Every node's transition observer feeds a lock-free, timestamped event log
(`LeadershipMonitor`). Every run reports elections-in-window and the term
trajectory; a **clean-load run with any election inside the measurement
window is INVALID** — `rsm_bench` prints the reason, writes `valid: false`
into the JSON, exits 3, and `run_benchmarks.sh` aborts, because an
unexpected election is a bug to investigate (the Phase 7 lesson), not
noise. The stress regime that exposed the apply-backpressure bug (high
concurrency + group commit + sustained duration) is explicitly in the
suite: the 30 s, 32-client, batch-8, busy-spin clean run is the headline
leadership-stability fact — measured 52,948 ops/s with ~1.59 M committed
entries and ZERO elections, term constant — plus a shortened ctest variant
(`bench_tests`, 8 s/16 clients) as the permanent regression net.

### Fault instruments

`FaultGate` filters cluster-bound sends in the bench's send hook:
probabilistic drop (per-thread xorshift; symmetric loss) and an isolation
mask ("partition now", in-flight cross-group delivery unaffected — sender
side only; client traffic always passes, matching the Phase 6 sim's
clients-bypass-partitions model). Leader kill/restart reuse the Phase 4
restart pattern (same port, durable replay, fresh volatile state).
Failover time is client-perceived: one probe `put()` (100 ms attempts,
internal redirects) spans the outage, so its return instant is "a new
leader served a committed write", discovery included.

### Thread pinning (decision: pin the three Raft threads only)

On this 12-thread hybrid host (2 P-cores ×HT + 8 E-cores) the pipeline
alone has 12 threads, so exclusive cores for everything do not exist.
The Raft threads are the ones whose scheduling delay becomes elections or
latency cliffs, so `--pin` pins exactly those, one per physical core
(cpus 0, 2 = P-cores, 4 = E-core), self-applied via the bench's
service-hook wrapper on first call — again no runtime changes. Everything
else floats. Measured honestly: the suite's first-cell `pin_ab` pair is
ORDER-CONFOUNDED (it runs before the package settles to its sustained
power limit — see host caveat below) and is superseded by an interleaved
warm-machine A/B (`pin_ab2.*`, 3× alternating). For the record: the
interleaved-redo applies to THIS pin-vs-no-pin comparison only; the
spin-vs-block conclusion was never a sequential A/B — it comes from sweep
cells that all ran in the same sustained regime, three repeats each.
The pin A/B result: median throughput equal
within noise (33.0 k/s pinned vs 32.2 k/s unpinned at 16 clients), but the
unpinned runs show occasional large negative excursions (one rep at
22.1 k/s, −33 %) that the pinned runs do not — pinning's measured value is
VARIANCE control on the election-critical thread, not a mean shift.
The canonical governored dataset agrees: its interleaved `pin_ab2` cells
measure 25.1 k/s pinned vs 25.5 k/s unpinned (parity within noise).
Pinned is the recorded default for all reported numbers.

**Host caveat discovered during the run (recorded with the results):**
this U-series laptop CPU sustains its boost clocks only briefly; under the
suite's continuous load it settles to power-limited (PL1) sustained
clocks. All numbers in the reported result set were collected in that
sustained state (mutually comparable, conservative); isolated burst runs
on an idle machine measure up to ~35 % higher (e.g. 33 k/s vs 24 k/s at 16
clients, batch off). Peak thermal-zone temperature across all 150+ runs:
82 °C; governor `performance` on all 12 cpus for every run (asserted from
the per-run machine records).

### Snapshotting (decision: deferred, with evidence)

Unbounded log growth does not constrain this benchmark: the longest run
(30 s stress at full batched throughput) ends well under a few hundred MB
of log + in-memory entries across all three nodes on a 7 GB host, and
every run starts from a fresh data dir. `rsm_bench` records `data_bytes`
in every JSON as the standing evidence. Snapshotting + `InstallSnapshot`
stay future work (the session table has been snapshot-ready since
Phase 5); per the decision protocol the simpler, reversible option wins.

### Reproducibility workflow

One script — `bench/run_benchmarks.sh` — regenerates every reported
number: it sets/verifies/records the CPU governor (warns loudly if not
`performance`), captures host state (`machine.txt` + per-run
machine-start/end blocks inside each JSON: CPU model, governor, no_turbo,
kernel, loadavg, hottest thermal zone), builds Release, runs the full
matrix (REPEATS× per cell, seeds printed, medians reported), writes one
raw JSON per run, picks the headline offered load from the saved sweep
itself, and renders all plots + `summary.txt` from the saved files via
`plot_results.py` (matplotlib — tooling, not part of the C++ deliverable).

**Stated-load criterion (`pick_rate.py`, final form):** sustainable = the
highest swept rate where EVERY repeat (a) stayed valid, (b) abandoned
nothing, (c) achieved ≥ 99 % of the offered send rate and committed ≥ 99 %
of it, (d) held CO-corrected e2e p99 ≤ 50 ms, and (e) held p99.9 ≤ 20 ms;
the headline load is 70 % of that. Clauses (d) and (e) were added
iteratively after the data showed why rate fidelity alone is not enough: a
bounded-in-flight generator can keep serving the offered rate while
per-request queueing grows toward seconds (the 20 k/s base point passed
(a)–(c) with a 3.6 s p99), and a p99 bound alone admitted a rate whose
20-second sustained run grew a 90 ms p99.9 (transient backlogs at
power-limited clocks). Both missteps are visible in the recorded sweep
data; the criterion that survived them is the one stated here.
Per-run workload determinism (same seed ⇒ same key sequence) is
unit-tested; open-loop rate fidelity (the generator holds the offered rate
within tolerance when the system keeps up) is asserted in `bench_tests`.

### Knobs swept

batch size (1–32) × wait mode (block/spin) × client concurrency (1–32) ×
storage (tmpfs vs real disk) for closed-loop throughput; offered rate
sweeps for two open-loop configs (`base` = no batching + block,
`perf` = batch 8 + spin) for the latency-vs-throughput curve and knee;
`--fsync every|group` measured once at batch=1 on disk to demonstrate the
documented by-construction equivalence (the real fsync knob is the batch
size: batch=1 ⇒ fsync per entry, batch=N ⇒ one fsync per N).

## The order-book matching engine (Phase 9)

### The framing (why this is the showcase SM)

The Raft log is a deterministic total-order sequencer. A matching engine's
price-**time** priority is "first come, first served within a price level" —
and in this system *arrival order is committed-log order*, identical on every
replica by State Machine Safety. So the matching engine is a pure
deterministic fold over the sequence Raft already agrees on: no clock, no
tie-breaking heuristics, nothing to coordinate beyond what consensus already
provides. The KV store and the order book are two folds over the same agreed
sequence behind the same `StateMachine` interface — swapping one for the
other touched zero Raft/runtime/transport code, which is the proof that the
spec's layering held.

### Command/result formats

`src/statemachine/order_book.h` documents the byte layouts. The essentials:
the command starts with the same 16-byte `(clientId, seqNo)` identity prefix
as KV commands — so `ClientService`'s identity validation and the dedup
convention carry over untouched — followed by `op` (1=NEW 2=CANCEL 3=AMEND)
and integer fields only: side (one byte, 0/1), price in ticks (u64 ≥ 1),
quantity in lots (u64 ≥ 1). **Never floating point** — FP matching can
diverge across machines/compilers and silently break State Machine Safety.
Results: `'O' orderId restingQty nFills {makerOrderId, price, qty}*` (fills
execute at the **maker's** price), `'N'` deterministic reject
(CANCEL/AMEND of an unknown/gone order), `'E'` malformed (no side effect,
never cached — the KV convention).

### Decision points (phase prompt §"Decision points")

1. **AMEND priority semantics** (exchange-like default, as recommended):
   at the same price, a quantity decrease (or no-op) amends **in place** and
   keeps time priority; a price change or quantity increase is
   cancel-replace — the order keeps its **id** but loses priority and is
   re-matched like a new order (a price change can cross and fill
   immediately). Alternatives considered: always-lose-priority (simpler,
   but punishes the common risk-reducing decrease) and new-id-per-amend
   (closer to some venues' wire protocols but complicates client tracking
   for no correctness gain). Deterministic either way; this one is
   documented and unit-tested per branch.
2. **Order-ID scheme:** a u64 counter in applied state (`nextOrderId_`,
   starts at 1), advanced once per accepted NEW; AMEND keeps the id; the
   counter is serialized with the book. Identical on every replica because
   it only moves inside `apply()`. Client-supplied or hash-derived ids were
   rejected: they push uniqueness/determinism obligations onto clients.
3. **Book structure:** per side a `std::map<price, std::deque<Resting>>`
   (bids walked from `rbegin`, asks from `begin`) plus an
   `orderId -> (side, price)` locator map; CANCEL scans its level's deque
   linearly. Deterministic iteration everywhere (ordered containers only).
   Chosen for correctness-first clarity per the prompt's "do not gold-plate
   the matcher's internals" — the measured cost is in the consensus path,
   not the matcher (see the Phase 9 numbers).
4. **Correctness checking:** golden-model equivalence + cross-replica
   identity (the recommended pair), three layers deep:
   (a) unit-level golden streams — seeded randomized command streams through
   the engine and an **independent reference matcher**
   (`test/ob_reference.h`: flat order vector, arrival stamps, linear scans,
   its own parsing/encoding — deliberately nothing like the engine), every
   per-command result byte-compared and final books compared; self-validated
   by three deliberately mis-implemented reference variants (FIFO inverted,
   price priority ignored, taker-price fills) that the comparison must FLAG;
   (b) in every order-book chaos run — at quiescence all live replicas'
   `serialize()` bytes (book + id counter + session table) must be
   identical, and a fresh engine folding the committed log must land on
   exactly that state;
   (c) per chaos seed — the committed command stream is replayed through the
   reference matcher and the final books must match (golden equivalence on
   real consensus output, not just synthetic streams).
   Extending the per-key WGL linearizability checker to order-book
   semantics is recorded as future work (the spec marks it optional); the
   direct state checks above are the matching-engine-shaped substitute.
5. **Architecture diagram:** Mermaid in the README (renders on GitHub,
   diffable in review) for module/data flow, plus the ASCII thread-topology
   diagram carried over from Phase 7 — both checked against the code.
6. **Benchmark:** yes — through the Phase 8 harness unchanged (open-loop,
   CO-corrected, leadership-validity gates, repeats/seeds/medians,
   `pick_rate.py` stated loads, `plot_results.py` plots), driven by
   `bench/run_orderbook_bench.sh`. Workload: NEW orders, side uniform,
   price uniform in [90, 110], qty 1–10 — the symmetric band makes
   matching continuous while the resting book stays a bounded random walk.

### Sessions: deliberate duplication, not a shared layer

The dedup/session table is implemented inside `OrderBookStateMachine`
exactly as inside `KVStateMachine` (same rules: apply-time check on every
replica, `seqNo <= lastSeq` returns the cached latest result with no side
effect, `clientId 0` opts out, malformed never cached, table serialized
with the state). The ~20 duplicated lines were chosen over extracting a
shared session helper because that refactor would touch the evolved,
heavily-tested KV file for zero behavior change (surgical-edits rule).
**Exactly-once has real teeth here:** re-applying a retried NEW would
re-match it — `order_session_sim_test.cpp` replays the Phase 5
retry-across-leader-failover scenario against resting liquidity and
asserts the fill happened once and the cached fills come back identical.

### Honest notes

- **Self-trade prevention is out of scope:** orders carry no owner, so a
  client's buy can match its own resting sell (deterministically). Real
  venues add policy here; it is policy, not consensus correctness.
- **No query/market-data op:** the engine answers order entry only; tests
  read the book through accessors/serialize. A read path would mirror the
  KV GET-through-the-log decision.
- **Bench observer cost:** an order-book result (~20+ bytes) exceeds
  libstdc++'s SSO, so the *load-generator* thread pays one small
  allocation per completed op copying `Result.value` (KV PUT results are
  1 byte and allocation-free, which is what `bench_alloc_tests` asserts).
  This is on the client side of the socket, far from the measured
  pipeline; recorded rather than redesigned around.
- The order-book ops ride the existing `KvClient`
  (`obNew/obCancel/obAmend` — same routing/retry/identity machinery,
  different command bytes) and `kv_cli` gained `ob-new/ob-cancel/ob-amend`
  for hand-poking; the recorded three-process smoke is
  `bench/results/phase9/three_process_smoke.txt`.

## Parked non-goals

Dynamic membership, snapshotting/compaction (stretch), multi-host/WAN, BFT, TLS/auth,
query language — per spec §3. Phase 9 adds: extending the formal
linearizability checker to order-book semantics (golden-model + identity
checks stand in), self-trade prevention, market/IOC/stop order types.

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

**Phase 5 — Client sessions, exactly-once dedup, KV store (2026-06-11).**
Real client path end to end: `KVStateMachine` (PUT/GET/DELETE/CAS plus the
non-idempotent APPEND probe) with the `(clientId, seqNo) -> (lastSeq,
lastResult)` session table inside its applied (and serializable) state,
dedup at apply time on every replica; `ClientService` per node on two new
logic-free RaftCore seams (client-request handler, apply observer) doing
redirect-with-hint, propose-verbatim with identity validation, and
commit-then-applied-then-reply via an index-keyed pending table with an
entry-identity check for overwritten proposals; synchronous `KvClient`
(same-identity retries, hint following, bounded rotation) plus a `kv_cli`
executable; transport gained inbound reply routing for non-peer (client)
connections. Decision points: reads through the log, client-supplied u64
ids, no eviction (determinism constraint recorded), no leader
short-circuit — all documented above. Two bugs caught by tests en route:
ClientService originally prepended the session prefix a second time onto
already-prefixed commands (every apply returned malformed) — fixed by
making the client own the full command bytes and the server validate the
embedded identity instead; and a sim-test predicate wrongly expected a
unique leader while the deposed one was isolated. Tests: 7 KV/dedup unit
cases (incl. serialize round-trip keeping dedup effective), 5 deterministic
sim cases (clean run, redirect hint, exactly-once across failover,
10× retry storm, overwritten-pending → NOT_LEADER), 4 real-TCP KvClient
cases (clean run, follower redirect, exactly-once across leader kill, retry
storm), and a manual 3-process `kv_cli` smoke incl. leader `kill -9` with
the client succeeding through failover. Release, ASan/UBSan, and TSan gates
all green via `./build_and_test.sh` (95 unit cases / 1409 assertions, plus
3 raft-cluster and 4 kv-cluster TCP cases).

**Phase 6 — Fault injection, invariant checking, seeded chaos (2026-06-11).**
Deterministic single-process simulator in `faults/`: `SimNetwork` (seeded
in-memory bus with drop/latency/reorder/partition), `SimHarness` (virtual-
time scheduler driving production RaftCore + real durable storage +
KVStateMachine + ClientService — no threads, no code changes to any of
them), protocol-faithful `SimClient`s with per-attempt reply correlation,
a seeded fault schedule (partitions, drops, delays, reorders, crashes,
restarts, leader kills), the five-invariant checkers plus no-lost-commit
(continuous, pure-data, every one self-validated against fabricated
violations), and a per-key WGL linearizability checker self-validated on
known-good and known-bad histories — all documented in the Phase 6 section
above. The chaos driver (`runChaos`, CLI `chaos_sim --seed N`) runs the
seeded workload + fault schedule, heals, then asserts invariants,
no-lost-commit, linearizability, and post-heal convergence; ctest runs
seeds 1–20 plus the determinism test (same seed ⇒ line-identical run
trace), fault-layer units, checker self-tests, and the five-node Figure 8
scenario closing the Phase 3 deferral (old-term majority entry provably
uncommitted; overwritten in one branch, indirectly committed in the
other). One workload bug caught and fixed en route: the first cut finished
all client ops before the first fault fired (committed ≈ acked, no retry
duplicates) — seeded think time now spreads ops across the fault window,
and committed > acked on many seeds shows dedup being exercised. A
300-seed sweep passed clean. Release, ASan/UBSan, and TSan gates all green
via `./build_and_test.sh` (faults_tests: 17 cases / 7010 assertions, plus
the `chaos_sim --seed 1` replay-path ctest case).

**Phase 7 — The performance layer (2026-06-11).** Re-plumbed the real
runtime for latency/throughput with correctness frozen: hand-written
lock-free SPSC and Vyukov-style MPSC rings (cache-line-padded, bounded,
allocation-free, per-atomic ordering rationale in-header, TSan-clean under
a 2M-op concurrent stress); a four-thread split per node (rx → Raft →
{tx, apply}) behind a new `NodeRuntime`, with the core still single-owner
via the `setApplySink` seam and sends encoded on the producing thread;
group commit (`proposeBatch` = one fsync + one AE per peer, size/linger
policy, deterministic linger in sim, equivalence test proving identical
logs and results batched vs unbatched); blocking (fence-free event-count
WakeGate) and busy-spin (pause→yield→park) wait modes, both flag-selectable
and measured; and an allocation-free steady state — ring-slot buffer pools,
core message/entry scratch + recycle pools, a flat pending table, client
connection reuse — enforced by a global-new-override test asserting ZERO
plumbing allocations on every pipeline thread (rx decode is the documented
residual). En route, found and fixed a real perf bug (the AE↔ack resend
storm: 35× throughput at 4 clients) and switched the Phase 5 TCP test
harness + `raft_node` to the threaded runtime, so the entire cluster suite
and TSan gate exercise the new topology; the sim and chaos suites are
untouched and green, including determinism. All before/after numbers in the
optimization table above; headline: +43% throughput with 10× better p99 at
4 clients (block), 2.7× at 8 clients with batching+spin, 5.7× on fsync-bound
disk with batch=16, at a documented ~20 µs single-client p50 cost vs the old
loop. Release, ASan/UBSan, and TSan gates all green via
`./build_and_test.sh` (8 ctest targets per config, incl. ring stress,
batching equivalence, allocation test, and a batched concurrent-client
TSan stress).

**Phase 8 — Benchmark harness and results (2026-06-11).** Rigorous
measurement layer in `/bench` with zero production-code changes beyond
hot-path buffer reuse inside `KvClient` (+ `FrameAssembler::reset`,
`encodeKvCommandInto`) so the load generators are allocation-free:
`bench_lib` = instrumented in-process cluster on the full production
wiring through existing seams (send hook, client-request handler, service
hook, transition observer) carrying a lock-free leadership monitor, a
Raft-thread-only commit-latency tap with handshake export, fault gate
(loss/partition), one-shot stall gate, and kill/restart; closed- and
open-loop generators (fixed-rate arrivals, latency from INTENDED send
time = the coordinated-omission correction, abandoned-schedule
accounting); `rsm_bench` driver (closed/open/failover modes, fault flags,
JSON raw data with machine state captured at start+end of every run,
clean-load-election ⇒ valid=false + exit 3); `run_benchmarks.sh` (one
script regenerates everything; governor set/record; `pick_rate.py` derives
stated loads from the saved sweep) and `plot_results.py` (plots + summary
from saved JSON only). Validation wired into ctest: the stall self-test
(injected 500 ms SM stall: intended p99 = 470 ms vs actual-send p99 =
1.9 ms — the correction shown to bite), measurement-overhead test (zero
plumbing allocations on every loadgen thread, global-new override),
open-loop rate fidelity, workload-seed reproducibility, and the
stress-regime leadership assertion (16–32 clients + group commit +
sustained: constant term). Results (i5-1235U, performance governor, ≤82 °C,
sustained-power state, medians of 3, seeds printed; full tables in
README): best throughput 82,970 ops/s (16 clients, batch 16, block,
tmpfs); fsync-bound disk 464 → 3,531 ops/s with batch 8; knee ≈ 20 k/s
unbatched and ≈ 40–60 k/s batched; headline open-loop at 28 k/s offered:
e2e p50/p99/p99.9/p99.99 = 283 µs / 967 µs / 8.3 ms / 18.1 ms, commit p50
170 µs; failover over 60 trials p50/p90/p99 = 276/451/578 ms (bimodal per
election-timeout theory); ≤10 % sustained loss absorbed with flat tails
and zero elections; periodic partitions honestly surface as CO-corrected
p99 ≈ 1.3 s with full recovery between; 30 s stress regime 52,948 ops/s,
zero elections, term constant; 150+ runs, zero invalid. *(Development-run
figures; superseded by the canonical dataset note below — same shapes,
same conclusions, refreshed magnitudes.)* Findings recorded: the
wait-mode trade-off is operating-point-dependent on this 12-thread host
(see the canonical-dataset note for the final form), pinning's value is
variance control, and the U-series sustained-power caveat. Snapshotting
deferred with evidence (356 MB max log growth). Release, ASan/UBSan, and
TSan gates all green via `./build_and_test.sh` (10 ctest targets per
config, incl. the two new bench test binaries).

**Phase 8 closeout — audit items resolved (2026-06-12).** Every item from
the post-gate self-audit, production code frozen (only `/bench`, `/test`,
docs, and the run script touched — and this round touched no C++ at all):
(1) the histogram deviation is now an explicit DESIGN decision ("Histogram
decision" section): in-repo HDR-style log-linear histogram (~1.6 %
quantization, allocation-free, mergeable) instead of vendoring
`HdrHistogram_c`, CO correction in the measurement layer, quantization an
order of magnitude below run-to-run variance; no wording anywhere claims
the C library. (2) Accuracy fixes: the interleaved-redo is documented as
the pin-vs-no-pin A/B specifically (spin-vs-block was sweep-cell-based,
never sequential); the 30 s stress run (52,948 ops/s, ~1.59 M entries,
0 elections, term constant) is cited as the headline stability fact over
the short ctest variant; the final `pick_rate.py` criterion (validity +
nothing abandoned + ≥99 % rate fidelity and commit rate + p99 ≤ 50 ms +
p99.9 ≤ 20 ms, headline = 70 %) is documented WITH the two recorded
missteps that motivated each tail bound. (3) `run_benchmarks.sh` is fully
self-contained: the best-config open sweep + `headline.best` cells and the
interleaved `pin_ab2` A/B (canonical seeds `SEED..SEED+REPEATS-1`) are in
the script, so one uninterrupted invocation regenerates the entire set.
(4) Fault loss/partition cells (and their clean references) upgraded from
single runs to the same 3-repeat/median discipline as the sweeps; README
fault section reworded accordingly (the single-run ±20 % artifact
explanation is gone because the artifact's cause is). (5) Cross-process
wiring validated for the record: a real three-`raft_node`-process cluster
+ `kv_cli` ops (put/get/append/cas/del, leader election trace) captured in
`bench/results/phase8/three_process_smoke.txt`; DESIGN + README state
plainly that the benchmark numbers are in-process by design with network
RTT excluded (additive). (6) Gate re-verified green on all three configs
(Release, ASan/UBSan, TSan; 10/10 ctest targets each), Phase 6 chaos suite
unchanged. The governored clean benchmark re-run that will stamp the
committed git rev into the recorded data is deliberately left to the
operator, per instruction.

**Phase 8 canonical dataset (2026-06-12).** The operator's single
uninterrupted `run_benchmarks.sh` invocation at rev `1db3b19` (governor
`performance`, recorded in `machine.txt` and every per-run JSON) is the
canonical result set behind the README; it supersedes the development-run
magnitudes quoted in the phase note above. 194 runs, **zero invalid**.
Canonical headlines: best throughput 69,122 ops/s (16 clients, batch 16,
block, tmpfs); single-client p50 72 µs (spin) / 98 µs (block); disk 446 →
3,216 ops/s with batch 8 (7.2×; fsync=group at batch 1 measures 444 ≈ 446,
the equivalence shown); best-config open-loop holds sub-ms p50 with p99 ≤
2.8 ms through 60 k/s, wall at 70 k/s; stated-load tables at 14 k/s (base:
e2e p50/p99 = 211/426 µs, commit 76/199 µs) and 35 k/s (best: e2e
360/655 µs, commit 182/381 µs, p99.9 10.5 ms intended vs 0.9 ms actual —
the CO gap on production data); stress regime 75,140 ops/s over 30 s,
2.25 M entries, 500 MB logs, zero elections; failover p50 = p90 = 277 ms,
p99 = max = 629 ms over 60 trials; ≤10 % loss: rate served, tail flat
(p99 418 → 381 µs), zero elections; periodic partitions: CO-corrected p99
≈ 1.31 s, p50 217 µs, the expected 5 majority-side re-elections. Two
corrections made while syncing the README to this dataset: (1) ten stale
single-run `fault.*.json` files from the development dataset had survived
into the results directory and were polluting the regenerated fault
medians (the labels matched, so `plot_results.py` merged them) — deleted,
summary regenerated from canonical data only; (2) the development-run
finding "block beats spin everywhere" did NOT replicate — the canonical
data lands on the Phase 7 trade-off as originally documented (spin wins at
1–2 clients where wake latency dominates: 13.9 k vs 10.0 k ops/s at one
client; block wins at the throughput-optimal point: batch 16 at 69.1 k vs
52.6 k) — README and the findings line above corrected accordingly. The
closed-loop fault cells measure throughput RISING with loss (26.1 k clean
→ 30.5 k at 10 %): a closed-loop-under-faults artifact (dropped cluster
traffic frees contended CPU that self-throttled clients consume), now
explicitly framed in the README as the reason faults are measured
open-loop, not as a finding.

**Phase 7 addendum — review follow-ups (2026-06-11).** Three changes from
the phase review, all gated green (Release + ASan/UBSan + TSan, 8/8): (1)
the AE↔ack storm fix is now pinned by a regression unit test (duplicate and
stale success acks send nothing; the heartbeat is the only retransmitter).
(2) Apply backpressure redesigned from blocking to refusal + `pumpApply()`:
the Raft thread never blocks on a slow state machine; the new slow-apply
cluster test (apply ring capacity 2, 25 ms per apply, 16 batched clients)
holds leadership at a constant term through sustained refusals — and was
validated to FAIL against the old blocking sink, which stalls the Raft
thread ~350 ms inside one group-commit hand-off and triggers a spurious
election. (3) The rx-decode allocation residual was eliminated rather than
accepted: raw-frame ingress + `decodeMessageInto` with per-slot buffer
pools (equivalence with `decodeMessage` proven by a cycling round-trip
test), a templated `FrameAssembler::feed` (the per-read std::function
type-erasure allocation), and a reused pollfd set; the allocation test now
asserts rx ≈ 0 (small per-connection budget) alongside the strict zero on
raft/apply/tx. Also recorded: the repo-structure finding that the outer
repo tracks `raft-rsm` as a bare gitlink, so phase commits must land in the
inner repo first — Phases 5–7 content had been sitting uncommitted.

**Phase 9 — Order-book matching engine and the writeup (2026-06-12).**
The showcase state machine and the final README. `OrderBookStateMachine`
(`src/statemachine/order_book.*`): NEW/CANCEL/AMEND limit-order matching
with price-time priority where "time" is apply/log order, integer
ticks/lots only, order ids from a counter in applied state, fills at the
maker's price, the same 16-byte identity prefix and apply-time session
dedup as the KV store (deliberately duplicated, not refactored out of the
evolved KV file), full serialize/deserialize including the id counter and
sessions, and a canonical `bookImage()` for comparisons — all decision
points (AMEND semantics, id scheme, book structure, checking strategy,
diagram format, benchmarking) recorded above. Zero changes to
Raft/runtime/transport: the KV store and the order book are two folds
behind one interface, selectable everywhere (`raft_node --sm`, `kv_cli
ob-*`, `chaos_sim --sm`, `rsm_bench --sm`, sim/cluster factories).
Correctness: 13 matching-unit cases (priority/FIFO/partial/sweep/cancel/
amend branches/integer exactness/malformed/dedup/round-trip); golden-model
equivalence vs an independent reference matcher over seeded streams,
self-validated against three deliberately broken references; exactly-once
across leader failover against resting liquidity (a re-applied NEW would
double-fill — it does not); and the FULL Phase 6 chaos suite with the
order-book SM at seeds 1–20 plus a same-seed determinism case: five
invariants + no-lost-commit + convergence unchanged, with byte-identical
replica books at quiescence, a fresh-engine fold of the committed log
matching them, and a per-seed golden replay of the committed stream
(`order_chaos_tests`; replay via `chaos_sim --seed N --sm orderbook`).
Benchmark: `bench/run_orderbook_bench.sh` reuses the Phase 8 harness,
stated-load criterion, and plot/summary tooling unchanged (labels keyed so
`pick_rate.py`/`plot_results.py` work as-is) on a NEW-order workload in a
±10-tick band: closed-loop saturation 86,348 orders/s (16 clients,
batch 16, block, tmpfs); single-client spin 14,082 orders/s at p50 65 µs;
stated loads 14 k/s (e2e p50/p99 = 154 µs/1.46 ms, commit 55/348 µs) and
28 k/s batched (291 µs/1.04 ms, commit 168 µs); batch-16 knee p99 ≤ 1.4 ms
through 40 k/s, offered rate still fully served at 70 k/s; failover under
order load p50 278 ms / max 280 ms over 30 kills; zero invalid runs.
HONEST CAVEAT recorded in the README and the raw JSON: this set was
collected at governor=powersave (no sudo available in the session; the
Phase 8 KV canonical set is governor=performance), so cross-suite absolute
comparisons are off the table; the script regenerates under performance
when run by the operator. A three-process order-book smoke incl. live
leader kill -9 and a crossing order through the new leader is recorded at
`bench/results/phase9/three_process_smoke.txt`. README rewritten as the
final §10 deliverable: sequencer framing, Mermaid + thread-topology
architecture, testing story top-billed, KV + order-book results with every
Phase 8 honest framing carried forward (in-process topology, in-repo
histogram, sustained-power caveat, open-loop-faults rationale, CO-window
partition p99, batch-vs-concurrency rule), clone instructions correct for
the inner-repo/gitlink structure and smoke-tested from a clean checkout.
Release, ASan/UBSan, and TSan gates green via `./build_and_test.sh`
(12 ctest targets per config).

**Phase 9 dataset refresh (2026-06-12).** The operator re-ran
`bench/run_orderbook_bench.sh` at rev `a7c7e8a` intending a
governor=`performance` set to supersede the original powersave-only one
(committed as `cf1152b`; the phase note's 86 k/65 µs figures are from the
superseded set). Verified independently afterward: both script invocations
carry `--sm orderbook` and all 52 JSONs record `"sm":"orderbook"` — the
showcase numbers are genuinely the matching engine; the old "PHASE 8"
summary title was a shared un-parameterized printer, now fixed
(`plot_results.py` names the recorded workload in the title and omits the
sustained-faults section when a result set has no fault cells — fault
behavior is SM-independent and lives in the Phase 8 set). One real finding
from the per-run start/end machine capture: the governor flipped
`performance` → `powersave` MID-SUITE, during `open.best.rate30000.r1`
(likely a power-profile/AC event — so the governor is not only a per-boot
concern). Cell map: closed-loop cells, the single-client cell, and the
full base sweep ran under performance; the upper best-config sweep, both
headline cells, and the failover ran under powersave. Refreshed headlines
with that caveat: closed-loop saturation 55,137 orders/s (performance);
single-client 13,302 orders/s at p50 73 µs vs the KV suite's 72 µs — the
same-state datum showing both SMs' apply cost is noise on the consensus
path; headline.base @14 k: e2e p50/p99 = 147/295 µs, commit 53/114 µs;
headline.best @49 k: e2e 307/565 µs, commit 154/315 µs, p99.9 28.6 ms
intended vs 0.86 ms actual (the CO gap on matching-engine data); failover
n=30 p50 278 ms, p99 = max = 633 ms; zero invalid runs. The
opposite-direction deltas vs KV (lower commit p50 and higher stated load,
but lower closed-loop saturation) are explained in the README: identical
pipeline cost at one client, wire-shape effects (34-byte NEW vs ~43-byte
PUT commands; fills-payload replies vs 1-byte), the powersave confound on
the headline cells, and the rep-robust stated-load criterion. A single
uninterrupted re-run under performance collapses the caveat to one line;
until then the README states the mixed host state precisely.
