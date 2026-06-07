#include "runtime/node_runtime.h"

#include <chrono>
#include <utility>

#include "metrics/alloc_gate.h"
#include "raft/logging.h"
#include "transport/frame.h"

namespace rsm::runtime {

using rsm::raft::LogLevel;
using rsm::raft::raftLog;
using rsm::rpc::Message;
using rsm::rpc::NodeId;

thread_local SpscRing<NodeRuntime::TxItem>* NodeRuntime::tlsTxRing_ = nullptr;

namespace {

inline void shortPause() {
#if defined(__x86_64__)
    __builtin_ia32_pause();
#else
    std::this_thread::yield();
#endif
}

}  // namespace

NodeRuntime::NodeRuntime(rsm::raft::RaftCore& core,
                         rsm::transport::Transport& transport,
                         rsm::statemachine::StateMachine& sm,
                         ApplyFn onApplied, NodeRuntimeConfig cfg)
    : core_(core),
      transport_(transport),
      sm_(sm),
      onApplied_(std::move(onApplied)),
      cfg_(cfg),
      inbound_(cfg.inboundRingSize),
      raftTx_(cfg.txRingSize),
      applyTx_(cfg.txRingSize),
      applyRing_(cfg.applyRingSize) {
    // Preallocate the slot pools (§4.7): all steady-state traffic fits
    // these reserves, so the pipeline never allocates after construction
    // (asserted by the allocation test).
    const auto reserveFrame = [&](TxItem& t) {
        t.frame.reserve(cfg_.txFrameReserveBytes);
    };
    raftTx_.initSlots(reserveFrame);
    applyTx_.initSlots(reserveFrame);
    applyRing_.initSlots([&](ApplyItem& a) {
        a.entry.command.reserve(cfg_.applyCommandReserveBytes);
    });
    inbound_.initSlots([&](Event& e) {
        // Each slot's decode pool starts stocked, so the first frames a
        // slot decodes (any type mix) already find pooled buffers — the rx
        // path allocates nothing from the first message on.
        for (int i = 0; i < 16; ++i) {
            std::vector<std::uint8_t> b;
            b.reserve(cfg_.applyCommandReserveBytes);
            e.pool.putBuf(std::move(b));
        }
        std::vector<rsm::rpc::LogEntry> entries;
        entries.reserve(16);
        e.pool.entryLists.push_back(std::move(entries));
    });
}

NodeRuntime::~NodeRuntime() { stop(); }

void NodeRuntime::start() {
    if (started_) return;
    started_ = true;
    stopRaft_.store(false);
    stopApply_.store(false);
    stopTx_.store(false);
    // The core hands committed entries to the apply ring instead of
    // applying inline. Runs on the Raft thread; the entry is copied because
    // the reference dies with the callback (the log mutates afterwards).
    core_.setApplySink([this](rsm::rpc::LogIndex index,
                              const rsm::rpc::LogEntry& entry) {
        // In-place produce: the slot's command vector keeps its capacity
        // across laps, so this copy stops allocating once warmed up.
        const auto fill = [&](ApplyItem& item) {
            item.index = index;
            item.entry.term = entry.term;
            item.entry.command.assign(entry.command.begin(),
                                      entry.command.end());
        };
        if (!applyRing_.tryProduce(fill)) {
            // Apply backpressure: REFUSE rather than block. The core keeps
            // lastApplied put; the raft loop's pumpApply() re-offers the
            // entry once the apply thread frees a slot. Losslessness holds
            // (every committed entry is handed off exactly once, in order)
            // while heartbeats and election timers stay live behind a slow
            // state machine.
            if (cfg_.waitMode == WaitMode::Block) applyGate_.notify();
            return false;
        }
        if (cfg_.waitMode == WaitMode::Block) applyGate_.notify();
        return true;
    });
    txThread_ = std::thread(&NodeRuntime::txLoop, this);
    applyThread_ = std::thread(&NodeRuntime::applyLoop, this);
    raftThread_ = std::thread(&NodeRuntime::raftLoop, this);
}

void NodeRuntime::stop() {
    if (!started_) return;
    // Order matters: stop the producer end first, drain downstream after.
    // Each notify kicks the (possibly parked) consumer so it observes its
    // stop flag; the gates' bounded park caps any missed kick regardless.
    stopRaft_.store(true);
    raftGate_.notify();
    if (raftThread_.joinable()) raftThread_.join();
    stopApply_.store(true);  // apply loop drains its ring, then exits
    applyGate_.notify();
    if (applyThread_.joinable()) applyThread_.join();
    stopTx_.store(true);  // tx loop drains both rings, then exits
    txGate_.notify();
    if (txThread_.joinable()) txThread_.join();
    started_ = false;
}

void NodeRuntime::enqueue(rsm::rpc::Envelope env, rsm::rpc::Message m) {
    if (stopRaft_.load(std::memory_order_relaxed)) return;
    const bool pushed = inbound_.tryProduce([&](Event& ev) {
        ev.valid = true;
        ev.isPropose = false;
        ev.decoded.envelope = env;
        ev.decoded.message = std::move(m);
    });
    if (!pushed) {
        // Inbound overload: drop. Raft's timers retransmit everything that
        // matters; a client whose request is dropped retries by timeout.
        raftLog(LogLevel::Debug,
                "[runtime] node=%u inbound ring full, message dropped",
                transport_.selfId());
        return;
    }
    if (cfg_.waitMode == WaitMode::Block) raftGate_.notify();
}

rsm::transport::Transport::RawFrameResult NodeRuntime::enqueueFrame(
    std::span<const std::uint8_t> body) {
    // Shutdown race: reject so the transport drops the connection; the
    // peer/client reconnects against whatever comes up next.
    if (stopRaft_.load(std::memory_order_relaxed)) return {false, 0};
    bool decodeOk = false;
    rsm::rpc::NodeId from = 0;
    const bool pushed = inbound_.tryProduce([&](Event& ev) {
        ev.isPropose = false;
        // Decode straight into the slot, reusing its pooled buffers (the
        // allocation-free rx path; capacities stick to the slot).
        ev.valid = rsm::rpc::decodeMessageInto(body, ev.decoded, ev.pool);
        decodeOk = ev.valid;
        if (ev.valid) from = ev.decoded.envelope.from;
    });
    if (!pushed) {
        // Ring full: drop the frame, keep the connection. The sender is
        // unknown without decoding, so no route update — the next accepted
        // frame from that client refreshes it.
        raftLog(LogLevel::Debug,
                "[runtime] node=%u inbound ring full, frame dropped",
                transport_.selfId());
        return {true, 0};
    }
    if (cfg_.waitMode == WaitMode::Block) raftGate_.notify();
    return {decodeOk, from};
}

void NodeRuntime::propose(std::vector<std::uint8_t> command,
                          ProposeDone done) {
    if (stopRaft_.load(std::memory_order_relaxed)) {
        if (done) done(std::nullopt);
        return;
    }
    bool consumed = false;
    const bool pushed = inbound_.tryProduce([&](Event& ev) {
        ev.valid = true;
        ev.isPropose = true;
        ev.command = std::move(command);
        ev.done = std::move(done);
        consumed = true;
        // ev.decoded/ev.pool untouched: the slot keeps its decode buffers.
    });
    if (!pushed) {
        if (!consumed && done) done(std::nullopt);  // overloaded: fail fast
        return;
    }
    if (cfg_.waitMode == WaitMode::Block) raftGate_.notify();
}

void NodeRuntime::sendFromPipeline(NodeId to, const Message& m) {
    SpscRing<TxItem>* ring = tlsTxRing_;
    if (ring == nullptr) {
        // Foreign thread (not part of this runtime's pipeline): take the
        // synchronous transport path, which is thread-safe.
        transport_.send(to, m);
        return;
    }
    // Encode straight into the ring slot's frame buffer (in-place produce):
    // each slot's vector keeps its high-water capacity, so the steady-state
    // send path performs zero allocations.
    bool encodeFailed = false;
    const auto fill = [&](TxItem& item) {
        item.to = to;
        item.frame.resize(rsm::transport::kLengthPrefixSize +
                          rsm::rpc::encodedSize(m));
        const std::size_t bodyLen = rsm::rpc::encodeMessage(
            transport_.selfId(), to, m,
            std::span<std::uint8_t>(item.frame)
                .subspan(rsm::transport::kLengthPrefixSize));
        if (bodyLen == 0) {
            encodeFailed = true;  // slot is published but harmless:
            item.frame.clear();   // a zero-length frame is skipped by tx
            return;
        }
        rsm::transport::writeLengthPrefix(static_cast<std::uint32_t>(bodyLen),
                                          item.frame.data());
    };
    if (!ring->tryProduce(fill)) {
        // tx backlog: drop, same loss model as an unreachable peer.
        raftLog(LogLevel::Debug,
                "[runtime] node=%u tx ring full, message to %u dropped",
                transport_.selfId(), to);
        return;
    }
    if (encodeFailed) {
        raftLog(LogLevel::Error,
                "[runtime] node=%u outbound message failed to encode",
                transport_.selfId());
    }
    if (cfg_.waitMode == WaitMode::Block) txGate_.notify();
}

void NodeRuntime::raftLoop() {
    tlsTxRing_ = &raftTx_;
    rsm::metrics::setThreadAllocRole("raft");
    core_.start();
    SpinBackoff backoff;
    // In-place consume: the slot (and its decode pool) stays in the ring,
    // so the rx thread's next decode into it reuses the same buffers.
    const auto use = [this](Event& ev) {
        if (!ev.valid) return;  // failed decode published as a skip
        if (ev.isPropose) {
            const auto idx = core_.propose(std::move(ev.command));
            if (ev.done) {
                ev.done(idx);
                ev.done = nullptr;  // release the callback's captures now
            }
        } else {
            core_.handle(ev.decoded.envelope, ev.decoded.message);
        }
    };
    // Cap the drain so a sustained flood cannot starve tick(): heartbeats
    // and election timers must fire even under inbound pressure.
    constexpr int kMaxDrainPerTick = 1024;
    while (!stopRaft_.load(std::memory_order_relaxed)) {
        int drained = 0;
        while (drained < kMaxDrainPerTick && inbound_.tryConsume(use)) {
            ++drained;
        }
        core_.tick();
        // Re-offer any committed entries an apply-ring-full refusal left
        // pending (no-op otherwise).
        core_.pumpApply();
        // The service hook (linger batching) runs every iteration and
        // reports the next deadline it needs.
        rsm::raft::TimePoint hookDeadline = rsm::raft::TimePoint::max();
        if (serviceHook_) {
            hookDeadline = serviceHook_(std::chrono::steady_clock::now());
        }
        if (drained > 0) {
            backoff.onWork();
            continue;
        }
        if (core_.commitIndex() > core_.lastApplied()) {
            // Apply backlog pending behind a full ring: poll-with-backoff
            // instead of parking on the gate, so the hand-off resumes
            // within microseconds of the apply thread freeing a slot.
            backoff.onIdle();
            continue;
        }
        if (cfg_.waitMode == WaitMode::Block) {
            // Sleep until a producer pushes, the next Raft timer or batch
            // deadline fires, or stop. nextDeadline() is read on this
            // thread (sole core owner).
            raftGate_.waitUntil(
                [this] {
                    return inbound_.consumerHasWork() ||
                           stopRaft_.load(std::memory_order_relaxed);
                },
                hookDeadline < core_.nextDeadline() ? hookDeadline
                                                    : core_.nextDeadline());
        } else {
            backoff.onIdle();
        }
    }
    tlsTxRing_ = nullptr;
}

void NodeRuntime::applyLoop() {
    tlsTxRing_ = &applyTx_;
    rsm::metrics::setThreadAllocRole("apply");
    SpinBackoff backoff;
    // In-place consume: the slot is read where it lives, so its buffers
    // stay pooled in the ring. The result string is reused across applies
    // (assign recycles its capacity).
    std::string result;
    const auto use = [&](ApplyItem& item) {
        // The single place sm_.apply runs in the threaded runtime — one
        // thread owns the state machine, in commit-index order.
        result = sm_.apply(item.entry.command);
        if (onApplied_) onApplied_(item.index, item.entry, result);
    };
    for (;;) {
        if (applyRing_.tryConsume(use)) {
            backoff.onWork();
            continue;
        }
        // stop is only set after the Raft thread (the sole producer) has
        // joined, so empty-after-stop means fully drained.
        if (stopApply_.load(std::memory_order_relaxed)) break;
        if (cfg_.waitMode == WaitMode::Block) {
            applyGate_.waitUntil(
                [this] {
                    return applyRing_.consumerHasWork() ||
                           stopApply_.load(std::memory_order_relaxed);
                },
                std::chrono::steady_clock::time_point::max());
        } else {
            backoff.onIdle();
        }
    }
    tlsTxRing_ = nullptr;
}

void NodeRuntime::txLoop() {
    rsm::metrics::setThreadAllocRole("tx");
    SpinBackoff backoff;
    // In-place consume keeps each slot's frame buffer pooled in the ring.
    const auto use = [&](TxItem& item) {
        if (item.frame.empty()) return;  // producer-side encode failure
        transport_.sendFrame(item.to, item.frame.data(), item.frame.size());
    };
    for (;;) {
        bool did = false;
        while (raftTx_.tryConsume(use)) did = true;
        while (applyTx_.tryConsume(use)) did = true;
        if (did) {
            backoff.onWork();
            continue;
        }
        // stop is only set after raft and apply threads (the producers)
        // have joined, so empty-after-stop means fully drained.
        if (stopTx_.load(std::memory_order_relaxed)) break;
        if (cfg_.waitMode == WaitMode::Block) {
            txGate_.waitUntil(
                [this] {
                    return raftTx_.consumerHasWork() ||
                           applyTx_.consumerHasWork() ||
                           stopTx_.load(std::memory_order_relaxed);
                },
                std::chrono::steady_clock::time_point::max());
        } else {
            backoff.onIdle();
        }
    }
}

}  // namespace rsm::runtime
