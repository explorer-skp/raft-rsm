// Integration: two in-process Transport endpoints on ephemeral loopback
// ports exchange every defined message type in both directions.
#include <doctest/doctest.h>

#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <vector>

#include "transport/transport.h"

using namespace rsm::rpc;
using rsm::transport::PeerAddress;
using rsm::transport::Transport;

namespace {

struct Inbox {
    std::mutex mu;
    std::condition_variable cv;
    std::vector<std::pair<Envelope, Message>> received;

    Transport::Handler handler() {
        return [this](const Envelope& env, Message&& m) {
            std::lock_guard lock(mu);
            received.emplace_back(env, std::move(m));
            cv.notify_all();
        };
    }

    // Waits until `count` messages have arrived; fails the test on timeout.
    bool waitFor(std::size_t count) {
        std::unique_lock lock(mu);
        return cv.wait_for(lock, std::chrono::seconds(5),
                           [&] { return received.size() >= count; });
    }
};

std::vector<std::uint8_t> bytesOf(const char* s) {
    return {reinterpret_cast<const std::uint8_t*>(s),
            reinterpret_cast<const std::uint8_t*>(s) + std::strlen(s)};
}

std::vector<Message> allMessageTypes() {
    AppendEntries ae{7, 1, 12, 6, 10, {}};
    ae.entries.push_back(LogEntry{6, bytesOf("PUT k1 v1")});
    ae.entries.push_back(LogEntry{7, {}});
    ae.entries.push_back(LogEntry{7, std::vector<std::uint8_t>(4096, 0x5A)});
    return {
        RequestVote{42, 1, 99, 41},
        RequestVoteReply{42, true},
        std::move(ae),
        AppendEntriesReply{7, false, 5, 4},
        ClientRequest{0xC11E47, 3, bytesOf("GET k1")},
        ClientReply{ClientStatus::NotLeader, 2, bytesOf("redirect")},
    };
}

}  // namespace

TEST_CASE("two endpoints exchange every message type in both directions") {
    Transport a(1, 0), b(2, 0);
    a.addPeer(2, PeerAddress{"127.0.0.1", b.listenPort()});
    b.addPeer(1, PeerAddress{"127.0.0.1", a.listenPort()});

    Inbox inboxA, inboxB;
    a.setHandler(inboxA.handler());
    b.setHandler(inboxB.handler());
    a.start();
    b.start();

    const auto messages = allMessageTypes();
    for (const auto& m : messages) {
        REQUIRE(a.send(2, m));
        REQUIRE(b.send(1, m));
    }

    REQUIRE(inboxA.waitFor(messages.size()));
    REQUIRE(inboxB.waitFor(messages.size()));

    for (std::size_t i = 0; i < messages.size(); ++i) {
        CAPTURE(i);
        // In-order per direction: each side receives exactly what was sent.
        CHECK(inboxB.received[i].second == messages[i]);
        CHECK(inboxB.received[i].first.from == 1);
        CHECK(inboxB.received[i].first.to == 2);
        CHECK(inboxA.received[i].second == messages[i]);
        CHECK(inboxA.received[i].first.from == 2);
        CHECK(inboxA.received[i].first.to == 1);
    }

    a.stop();
    b.stop();
}

TEST_CASE("sending to an unknown or unreachable peer fails without crashing") {
    Transport a(1, 0);
    a.start();

    const Message m = RequestVote{1, 1, 1, 1};
    CHECK_FALSE(a.send(99, m));  // never added

    // Grab an ephemeral port that is actually closed: bind, look, release.
    std::uint16_t deadPort = 0;
    {
        Transport probe(3, 0);
        deadPort = probe.listenPort();
    }
    a.addPeer(4, PeerAddress{"127.0.0.1", deadPort});
    CHECK_FALSE(a.send(4, m));  // connection refused -> dropped, not crashed

    a.stop();
}

TEST_CASE("messages sent before the receiver starts its handler thread still arrive") {
    // The TCP connection and kernel buffers hold the frames; B drains them
    // once start() runs. Exercises reassembly of frames that may coalesce.
    Transport a(1, 0), b(2, 0);
    a.addPeer(2, PeerAddress{"127.0.0.1", b.listenPort()});
    Inbox inboxB;
    b.setHandler(inboxB.handler());

    const auto messages = allMessageTypes();
    for (const auto& m : messages) REQUIRE(a.send(2, m));

    b.start();
    REQUIRE(inboxB.waitFor(messages.size()));
    for (std::size_t i = 0; i < messages.size(); ++i) {
        CAPTURE(i);
        CHECK(inboxB.received[i].second == messages[i]);
    }

    a.stop();
    b.stop();
}
