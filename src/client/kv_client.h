#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "rpc/messages.h"
#include "statemachine/kv_store.h"
#include "transport/frame.h"
#include "transport/transport.h"

namespace rsm::client {

// Blocking KV client (Phase 5): sends a ClientRequest to a node, waits for
// the reply on the same connection, follows NOT_LEADER leader hints, and
// rotates to the next server after a per-attempt timeout or connection
// failure with a bounded backoff. Retries always reuse the SAME
// (clientId, seqNo) — a retry is the same logical request, never a new one;
// that identity is what the replicated dedup table keys on. A result is
// returned only after the leader reports the command committed and applied.
//
// Identity: `clientId` (u64) keys the replicated session table and must be
// unique per logical client. `clientNodeId` (u16) is this client's envelope
// id, used by nodes to route the reply back on the inbound connection — it
// must be unique among concurrently connected clients and disjoint from
// cluster node ids (DESIGN.md).
//
// Connection reuse (Phase 7): the connection to the current server is kept
// open across calls, but ONLY across cleanly completed request/response
// exchanges. Any timeout, send/receive failure, or server switch closes it,
// so a straggler reply from an aborted attempt can never be read as the
// answer to a later request — the per-attempt-connection correlation
// guarantee from Phase 5/6 is preserved exactly.
class KvClient {
public:
    struct Result {
        char status = 0;    // kKvOk/kKvNotFound/kKvCasFailed/kKvMalformed
        std::string value;  // payload (GET hit, APPEND new value)
    };

    // `servers`: cluster node id -> address (the same peer-config map the
    // nodes use); leader hints are node ids, so the client resolves them
    // through this map.
    KvClient(transport::PeerMap servers, std::uint64_t clientId,
             rsm::rpc::NodeId clientNodeId, int perAttemptTimeoutMs = 1000,
             int maxAttempts = 20);
    ~KvClient();

    KvClient(const KvClient&) = delete;
    KvClient& operator=(const KvClient&) = delete;

    // KV operations; nullopt only after every attempt failed (no cluster
    // majority reachable). Each call is a new logical request (seqNo + 1).
    std::optional<Result> put(const std::string& key, const std::string& value);
    std::optional<Result> get(const std::string& key);
    std::optional<Result> del(const std::string& key);
    std::optional<Result> cas(const std::string& key,
                              const std::string& expected,
                              const std::string& desired);
    std::optional<Result> append(const std::string& key,
                                 const std::string& suffix);

    // Test hook: re-issues the LAST request with its original
    // (clientId, seqNo) — exactly what the retry loop does internally after
    // a lost reply. Lets tests simulate "client never saw the ack" across a
    // failover and assert exactly-once.
    std::optional<Result> resendLast();

    // Test hook: force the next attempt at a specific node (e.g. a known
    // follower, to exercise the NOT_LEADER redirect path deterministically).
    void setPreferredNode(rsm::rpc::NodeId id) { preferred_ = id; }

    std::uint64_t lastSeqNo() const { return seqNo_; }

private:
    std::optional<Result> call(const rsm::statemachine::Command& command);
    // One attempt against one server: (re)connect if needed, send, await
    // the reply. On true, the reply is in decoded_ (a ClientReply); on
    // false (connect/send/timeout/decode failure) the connection is closed
    // (correlation safety; see class comment).
    bool attempt(rsm::rpc::NodeId serverId, const transport::PeerAddress& addr,
                 const rsm::statemachine::Command& command);
    void dropConnection();

    std::vector<std::pair<rsm::rpc::NodeId, transport::PeerAddress>> servers_;
    std::uint64_t clientId_;
    rsm::rpc::NodeId clientNodeId_;
    int timeoutMs_;
    int maxAttempts_;
    std::uint64_t seqNo_ = 0;        // last issued sequence number
    rsm::rpc::NodeId preferred_ = 0; // node believed to lead (0 = unknown)
    std::size_t rotation_ = 0;       // fallback round-robin cursor
    rsm::statemachine::Command lastCommand_;  // for resendLast()
    int fd_ = -1;                    // cached connection (clean exchanges only)
    rsm::rpc::NodeId connectedTo_ = 0;

    // Reused hot-path buffers (Phase 8): with these, a steady-state op
    // performs zero heap allocations on the calling thread — required so the
    // bench load generators do not perturb the latency they measure
    // (asserted by the bench allocation test). Capacities stick at their
    // high-water marks; behavior is identical to fresh objects per call.
    rsm::statemachine::Command cmdScratch_;            // encode target
    rsm::rpc::Message reqMsg_{rsm::rpc::ClientRequest{}};  // reused request
    std::vector<std::uint8_t> frame_;                  // encoded request frame
    transport::FrameAssembler assembler_;              // reply reassembly
    rsm::rpc::DecodedMessage decoded_;                 // reply decode target
    rsm::rpc::DecodePool decodePool_;                  // decode buffer pool
};

}  // namespace rsm::client
