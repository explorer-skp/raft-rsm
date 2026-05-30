#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <thread>

#include "rpc/messages.h"

namespace rsm::transport {

struct PeerAddress {
    std::string host;  // numeric IPv4, e.g. "127.0.0.1"
    std::uint16_t port = 0;
};

using PeerMap = std::map<rsm::rpc::NodeId, PeerAddress>;

// Parses a peer config file: one "nodeId host port" triple per line; blank
// lines and lines starting with '#' are ignored. Throws std::runtime_error
// on malformed input or duplicate node IDs.
PeerMap loadPeerConfig(const std::string& path);

// Message transport over loopback TCP with length-prefixed framing.
//
// Connection model: each node dials its peers, so a pair of nodes uses two
// simplex connections — the outbound one carries this node's sends, inbound
// ones carry receives. Nothing pairs them up, which keeps connection state
// trivial at the cost of one extra socket per peer pair (irrelevant on
// loopback).
//
// Threading: one internal I/O thread accepts inbound connections and reads
// frames; the registered handler runs on that thread and must not block for
// long. send() runs on the caller's thread.
class Transport {
public:
    using Handler =
        std::function<void(const rsm::rpc::Envelope&, rsm::rpc::Message&&)>;

    // Phase 7 zero-copy ingress: the raw frame body is handed to the
    // consumer UNDECODED, so it can decode straight into its own pooled
    // storage (the runtime's inbound-ring slots) on this thread.
    struct RawFrameResult {
        bool ok = true;     // false: protocol error, close the connection
        rsm::rpc::NodeId from = 0;  // nonzero: update the reply route
    };
    using RawHandler =
        std::function<RawFrameResult(std::span<const std::uint8_t> body)>;

    // Binds and listens on 127.0.0.1:listenPort immediately (0 picks an
    // ephemeral port — see listenPort()). Throws std::runtime_error on
    // socket errors.
    Transport(rsm::rpc::NodeId selfId, std::uint16_t listenPort);
    ~Transport();

    Transport(const Transport&) = delete;
    Transport& operator=(const Transport&) = delete;

    rsm::rpc::NodeId selfId() const { return selfId_; }
    std::uint16_t listenPort() const { return listenPort_; }

    // Registers the peer for outbound sends. Call before send() to that id.
    void addPeer(rsm::rpc::NodeId id, PeerAddress addr);

    // Must be called before start(); the handler runs on the I/O thread.
    void setHandler(Handler h);
    // Alternative to setHandler (Phase 7): skips the transport-side decode.
    void setRawHandler(RawHandler h);

    void start();
    void stop();  // idempotent; joins the I/O thread

    // Encodes and writes the message to `to`, connecting on first use and
    // reconnecting once after a stale connection fails. Returns false (and
    // logs the reason) if the peer is unknown or unreachable: the message is
    // dropped, never queued. Raft tolerates message loss by design, so
    // retrying is the caller's (i.e. the protocol's) job.
    //
    // Reply routing (Phase 5): if `to` is not a configured peer, the frame
    // is written to the inbound connection that most recently delivered a
    // message whose envelope `from` equals `to`. This is how nodes answer
    // clients, which dial in but are not in the peer table. Client node ids
    // must therefore be unique and disjoint from cluster ids (DESIGN.md).
    bool send(rsm::rpc::NodeId to, const rsm::rpc::Message& m);

    // Phase 7: writes an ALREADY-ENCODED frame (length prefix + body) using
    // the same peer/route logic as send(). This is the tx-thread entry
    // point: the runtime encodes on the producing thread into its own
    // buffers, so the transport does not allocate or serialize here.
    bool sendFrame(rsm::rpc::NodeId to, const std::uint8_t* data,
                   std::size_t len);

private:
    struct Peer {
        PeerAddress addr;
        int fd = -1;  // cached outbound connection, -1 if not connected
    };

    void ioLoop();
    static int connectTo(const PeerAddress& addr);
    bool writeFrame(Peer& peer, const std::uint8_t* data, std::size_t len);
    static bool writeAll(int fd, const std::uint8_t* data, std::size_t len);

    rsm::rpc::NodeId selfId_;
    int listenFd_ = -1;
    std::uint16_t listenPort_ = 0;
    Handler handler_;
    RawHandler rawHandler_;
    std::thread ioThread_;
    std::atomic<bool> running_{false};

    std::mutex peersMu_;  // guards peers_ (send path only; I/O thread never touches it)
    std::map<rsm::rpc::NodeId, Peer> peers_;

    // sender NodeId -> inbound fd, maintained by the I/O thread; read by
    // send() for non-peer (client) replies. The mutex also serializes
    // writes to a routed fd against the I/O thread closing it.
    std::mutex routesMu_;
    std::map<rsm::rpc::NodeId, int> inboundRoutes_;
};

}  // namespace rsm::transport
