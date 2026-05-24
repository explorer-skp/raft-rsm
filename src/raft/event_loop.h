#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>

#include "raft/raft_core.h"
#include "rpc/messages.h"

namespace rsm::raft {

// The single-threaded Raft event loop (design seam 3): inbound RPCs, RPC
// replies, and timer expiries are all funneled into one queue drained by one
// thread, which is the only thread that ever touches RaftCore. The transport
// I/O thread calls enqueue(); the loop thread calls core.handle()/tick().
//
// Requires a real (steady) clock behind the core: the loop sleeps until
// core.nextDeadline() with wait_until, which only lines up with a clock that
// actually advances. Deterministic tests drive RaftCore directly instead.
class RaftEventLoop {
public:
    explicit RaftEventLoop(RaftCore& core) : core_(core) {}
    ~RaftEventLoop() { stop(); }

    RaftEventLoop(const RaftEventLoop&) = delete;
    RaftEventLoop& operator=(const RaftEventLoop&) = delete;

    // Starts the loop thread; core.start() runs as its first action so even
    // the initial timer arming happens on the loop thread.
    void start();
    void stop();  // idempotent; joins the loop thread

    // Thread-safe; called by the transport handler. Events enqueued after
    // stop() are dropped.
    void enqueue(rsm::rpc::Envelope env, rsm::rpc::Message m);

    // Thread-safe propose seam: runs core.propose(command) on the loop
    // thread; `done` (optional) is invoked on the loop thread with the
    // assigned index, or nullopt if this node is not the leader. Dropped
    // (no callback) if enqueued after stop().
    using ProposeDone = std::function<void(std::optional<rsm::rpc::LogIndex>)>;
    void propose(std::vector<std::uint8_t> command, ProposeDone done = {});

private:
    struct Event {
        // A propose event carries an empty handler-path envelope; `isPropose`
        // discriminates.
        rsm::rpc::Envelope env;
        rsm::rpc::Message msg;
        bool isPropose = false;
        std::vector<std::uint8_t> command;
        ProposeDone done;
    };

    void run();

    RaftCore& core_;
    std::thread thread_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::deque<Event> queue_;
    bool running_ = false;
};

}  // namespace rsm::raft
