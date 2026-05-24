#include "raft/event_loop.h"

#include <utility>

namespace rsm::raft {

void RaftEventLoop::start() {
    {
        std::lock_guard lock(mu_);
        running_ = true;
    }
    thread_ = std::thread(&RaftEventLoop::run, this);
}

void RaftEventLoop::stop() {
    {
        std::lock_guard lock(mu_);
        if (!running_ && !thread_.joinable()) return;
        running_ = false;
    }
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();
}

void RaftEventLoop::enqueue(rsm::rpc::Envelope env, rsm::rpc::Message m) {
    {
        std::lock_guard lock(mu_);
        if (!running_) return;
        queue_.push_back(Event{env, std::move(m), false, {}, {}});
    }
    cv_.notify_one();
}

void RaftEventLoop::propose(std::vector<std::uint8_t> command,
                            ProposeDone done) {
    {
        std::lock_guard lock(mu_);
        if (!running_) return;
        queue_.push_back(
            Event{{}, {}, true, std::move(command), std::move(done)});
    }
    cv_.notify_one();
}

void RaftEventLoop::run() {
    core_.start();
    std::unique_lock lock(mu_);
    while (running_) {
        // Sleep until the next Raft timer deadline or an inbound event.
        // nextDeadline() is read on this thread (the only core toucher), and
        // wait_until handles both the infinite (TimePoint::max) and the
        // already-expired case.
        cv_.wait_until(lock, core_.nextDeadline(), [this] {
            return !running_ || !queue_.empty();
        });
        if (!running_) break;
        while (!queue_.empty()) {
            Event ev = std::move(queue_.front());
            queue_.pop_front();
            lock.unlock();  // handle() may send(); don't hold the queue lock
            if (ev.isPropose) {
                const auto idx = core_.propose(std::move(ev.command));
                if (ev.done) ev.done(idx);
            } else {
                core_.handle(ev.env, ev.msg);
            }
            lock.lock();
        }
        lock.unlock();
        core_.tick();
        lock.lock();
    }
}

}  // namespace rsm::raft
