#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "raft/raft_core.h"
#include "rpc/messages.h"
#include "runtime/mpsc_ring.h"
#include "runtime/spsc_ring.h"
#include "runtime/wake_gate.h"
#include "statemachine/state_machine.h"
#include "transport/transport.h"

namespace rsm::runtime {

// Phase 7 threaded runtime for one node: replaces RaftEventLoop's
// mutex+condvar queue with lock-free rings and splits the work across
// dedicated threads (topology documented in DESIGN.md):
//
//   transport I/O thread ──(inbound MPSC)──> Raft thread
//        (rx: read+decode)         (sole owner of RaftCore state)
//                                      │                │
//                              (raftTx SPSC)     (apply SPSC, lossless)
//                                      │                │
//                                      v                v
//                                  tx thread <──(applyTx SPSC)── apply thread
//                            (socket writes)        (sm.apply + replies)
//
//  - The inbound ring is MPSC because propose() may be called from any
//    thread (tests, future ingress paths) alongside the transport handler.
//  - The Raft core remains single-threaded by contract: only the Raft
//    thread calls handle()/tick()/propose() — the Phase 2 invariant.
//  - Outbound messages are ENCODED ON THE PRODUCING THREAD into a frame
//    buffer and handed to the tx thread, which only does socket writes; so
//    a slow peer never stalls Raft logic.
//  - Committed entries cross to the apply thread through a lossless SPSC
//    ring: applies must happen exactly once, in order. Full ⇒ the sink
//    REFUSES and the core stops advancing lastApplied; the Raft loop's
//    pumpApply() re-offers once the apply thread frees a slot. The Raft
//    thread therefore NEVER blocks on the state machine — a slow apply can
//    delay client replies but can never stall heartbeats or election
//    timers (pinned by the slow-apply test, which fails against a blocking
//    sink). Both tx rings DROP when full: Raft messages and client replies
//    are loss-tolerant by design.
//
// The simulator never sees any of this: it drives RaftCore directly with
// no threads, and the core's logic is identical in both worlds.

// How pipeline consumers wait when their rings are empty (decision point 4;
// both measured in DESIGN.md):
//  - Block (default): park on a WakeGate condvar, woken by producers.
//    Lowest idle cost, ~2-10us wake latency per hop.
//  - Spin: busy-poll with pause->yield->short-park escalation. Lowest
//    hand-off latency; costs a hot core per consumer thread.
enum class WaitMode : std::uint8_t { Block, Spin };

struct NodeRuntimeConfig {
    std::size_t inboundRingSize = 4096;
    std::size_t txRingSize = 4096;
    std::size_t applyRingSize = 4096;
    WaitMode waitMode = WaitMode::Block;
    // Per-slot buffer preallocation (spec §4.7). Steady-state messages that
    // fit these never allocate; a larger message grows its slot once and
    // the capacity sticks. ~2.5 MB + ~4 MB per node at the defaults.
    std::size_t txFrameReserveBytes = 2048;
    std::size_t applyCommandReserveBytes = 1024;
};

class NodeRuntime {
public:
    // After each apply on the apply thread, in index order:
    // (index, entry, sm.apply result). Bind ClientService::onApplied here.
    using ApplyFn = rsm::raft::RaftCore::ApplyFn;
    using ProposeDone = std::function<void(std::optional<rsm::rpc::LogIndex>)>;
    // Per-iteration hook on the Raft thread (the client service's
    // linger-batch driver): receives the current time, returns the next
    // deadline it needs to be called at (TimePoint::max() when idle). The
    // blocking wait honors that deadline. Set before start().
    using ServiceHook =
        std::function<rsm::raft::TimePoint(rsm::raft::TimePoint now)>;

    // The runtime installs itself as the core's apply sink in start().
    // `onApplied` may be empty (recording-SM test clusters).
    NodeRuntime(rsm::raft::RaftCore& core,
                rsm::transport::Transport& transport,
                rsm::statemachine::StateMachine& sm, ApplyFn onApplied = {},
                NodeRuntimeConfig cfg = {});
    ~NodeRuntime();  // stop()

    NodeRuntime(const NodeRuntime&) = delete;
    NodeRuntime& operator=(const NodeRuntime&) = delete;

    void setServiceHook(ServiceHook h) { serviceHook_ = std::move(h); }

    void start();
    void stop();  // idempotent; joins raft -> apply -> tx, draining in order

    // Transport-handler entry; safe from any thread. A full ring drops the
    // message (Raft retries by timer; clients retry by timeout).
    void enqueue(rsm::rpc::Envelope env, rsm::rpc::Message m);

    // Zero-copy ingress for Transport::setRawHandler (rx thread): decodes
    // the frame straight into an inbound-ring slot, reusing the slot's
    // pooled buffers — the steady-state rx path allocates nothing. A full
    // ring drops the frame (same loss model as enqueue).
    rsm::transport::Transport::RawFrameResult enqueueFrame(
        std::span<const std::uint8_t> body);

    // Thread-safe propose seam (same contract as RaftEventLoop::propose):
    // `done` runs on the Raft thread with the assigned index or nullopt.
    // If the runtime is stopped or the ring is full, done(nullopt) runs
    // inline on the caller.
    void propose(std::vector<std::uint8_t> command, ProposeDone done = {});

    // SendFn for RaftCore / ClientService: encodes on the calling pipeline
    // thread (Raft or apply) and hands the frame to the tx thread. Falls
    // back to a direct blocking transport send from foreign threads.
    void sendFromPipeline(rsm::rpc::NodeId to, const rsm::rpc::Message& m);

private:
    struct Event {
        // Decoded in place by the rx thread (enqueueFrame); the pool
        // recycles buffers across the message types this slot cycles
        // through. valid=false marks a slot the consumer must skip.
        rsm::rpc::DecodedMessage decoded;
        rsm::rpc::DecodePool pool;
        bool valid = false;
        bool isPropose = false;
        std::vector<std::uint8_t> command;
        ProposeDone done;
    };
    struct TxItem {
        rsm::rpc::NodeId to = 0;
        std::vector<std::uint8_t> frame;  // length prefix + encoded body
    };
    struct ApplyItem {
        rsm::rpc::LogIndex index = 0;
        rsm::rpc::LogEntry entry;
    };

    void raftLoop();
    void applyLoop();
    void txLoop();

    rsm::raft::RaftCore& core_;
    rsm::transport::Transport& transport_;
    rsm::statemachine::StateMachine& sm_;
    ApplyFn onApplied_;
    ServiceHook serviceHook_;
    NodeRuntimeConfig cfg_;

    MpscRing<Event> inbound_;
    SpscRing<TxItem> raftTx_;   // Raft thread -> tx thread
    SpscRing<TxItem> applyTx_;  // apply thread -> tx thread
    SpscRing<ApplyItem> applyRing_;  // Raft thread -> apply thread (lossless)

    // One gate per consumer thread (block mode); producers notify after
    // each push. In spin mode the gates sit unused.
    WakeGate raftGate_;
    WakeGate applyGate_;
    WakeGate txGate_;

    std::thread raftThread_;
    std::thread applyThread_;
    std::thread txThread_;
    std::atomic<bool> stopRaft_{false};
    std::atomic<bool> stopApply_{false};
    std::atomic<bool> stopTx_{false};
    bool started_ = false;

    // Which tx ring the CURRENT thread produces into (raft / apply thread);
    // null on foreign threads. thread_local + per-thread assignment keeps
    // sendFromPipeline branch-free of any "who am I" lookup.
    static thread_local SpscRing<TxItem>* tlsTxRing_;
};

}  // namespace rsm::runtime
