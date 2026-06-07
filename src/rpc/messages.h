#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <variant>
#include <vector>

namespace rsm::rpc {

using NodeId = std::uint16_t;
using Term = std::uint64_t;
using LogIndex = std::uint64_t;

inline constexpr std::uint8_t kProtocolVersion = 1;
inline constexpr std::size_t kEnvelopeSize = 12;
// Hard cap on one message's payload; anything larger is a protocol error.
inline constexpr std::size_t kMaxPayloadSize = 16 * 1024 * 1024;

enum class MessageType : std::uint8_t {
    RequestVote = 1,
    RequestVoteReply = 2,
    AppendEntries = 3,
    AppendEntriesReply = 4,
    ClientRequest = 5,
    ClientReply = 6,
};

// Fixed 12-byte message header; exact byte layout documented in DESIGN.md.
struct Envelope {
    std::uint8_t version = kProtocolVersion;
    MessageType type{};
    NodeId from = 0;
    NodeId to = 0;
    std::uint32_t payloadLength = 0;
    bool operator==(const Envelope&) const = default;
};

struct RequestVote {
    Term term = 0;
    NodeId candidateId = 0;
    LogIndex lastLogIndex = 0;
    Term lastLogTerm = 0;
    bool operator==(const RequestVote&) const = default;
};

struct RequestVoteReply {
    Term term = 0;
    bool voteGranted = false;
    bool operator==(const RequestVoteReply&) const = default;
};

struct LogEntry {
    Term term = 0;
    std::vector<std::uint8_t> command;  // opaque to the transport/rpc layer
    bool operator==(const LogEntry&) const = default;
};

struct AppendEntries {
    Term term = 0;
    NodeId leaderId = 0;
    LogIndex prevLogIndex = 0;
    Term prevLogTerm = 0;
    LogIndex leaderCommit = 0;
    std::vector<LogEntry> entries;
    bool operator==(const AppendEntries&) const = default;
};

struct AppendEntriesReply {
    Term term = 0;
    bool success = false;
    // On failure: fast-backtracking hints (spec §4.2); conflictTerm == 0
    // means "log too short". On success: conflictIndex carries the ack —
    // request.prevLogIndex + request.entries.size() of the exact request
    // this reply answers, so the leader's matchIndex update is correct even
    // across lost or reordered replies (see DESIGN.md, Phase 3).
    LogIndex conflictIndex = 0;
    Term conflictTerm = 0;
    bool operator==(const AppendEntriesReply&) const = default;
};

enum class ClientStatus : std::uint8_t {
    Ok = 0,
    NotLeader = 1,
    Error = 2,
};

struct ClientRequest {
    std::uint64_t clientId = 0;
    std::uint64_t seqNo = 0;
    std::vector<std::uint8_t> command;
    bool operator==(const ClientRequest&) const = default;
};

struct ClientReply {
    ClientStatus status = ClientStatus::Ok;
    NodeId leaderHint = 0;
    std::vector<std::uint8_t> result;
    bool operator==(const ClientReply&) const = default;
};

using Message = std::variant<RequestVote, RequestVoteReply, AppendEntries,
                             AppendEntriesReply, ClientRequest, ClientReply>;

MessageType typeOf(const Message& m);

// Size of the encoded payload (excluding the 12-byte envelope).
std::size_t payloadSize(const Message& m);

// Total buffer encodeMessage() needs: envelope + payload.
inline std::size_t encodedSize(const Message& m) {
    return kEnvelopeSize + payloadSize(m);
}

// Encodes envelope + payload into the caller-provided `out`. Returns bytes
// written, or 0 if `out` is too small or the payload exceeds kMaxPayloadSize.
std::size_t encodeMessage(NodeId from, NodeId to, const Message& m,
                          std::span<std::uint8_t> out);

struct DecodedMessage {
    Envelope envelope;
    Message message;
};

// Recycled buffers for allocation-free decoding (Phase 7): byte buffers and
// entry lists keep their capacity as they move between the pool and the
// message alternatives a DecodedMessage cycles through.
struct DecodePool {
    std::vector<std::vector<std::uint8_t>> bufs;
    std::vector<std::vector<LogEntry>> entryLists;

    std::vector<std::uint8_t> getBuf() {
        if (bufs.empty()) return {};
        auto b = std::move(bufs.back());
        bufs.pop_back();
        return b;
    }
    void putBuf(std::vector<std::uint8_t>&& b) { bufs.push_back(std::move(b)); }
};

// Decodes one full frame body (envelope + payload). Strict: rejects unknown
// version/type, nonzero reserved bytes, payloadLength that disagrees with the
// buffer size, truncated payloads, and trailing bytes. Never over-reads and
// never allocates more than the input can justify.
std::optional<DecodedMessage> decodeMessage(std::span<const std::uint8_t> frame);

// Identical validation to decodeMessage, but decodes INTO `out`, reusing its
// buffers: when out.message already holds this frame's alternative, vectors
// are assigned in place (capacity reuse); on an alternative switch the old
// alternative's buffers are salvaged into `pool` and the new one is dressed
// from it. With a long-lived (out, pool) pair — e.g. an inbound-ring slot —
// steady-state decoding performs zero heap allocations once warm (the
// Phase 7 rx path; asserted by the allocation test). On false, `out` holds
// unspecified but destructible scratch; the caller must treat the frame as
// a protocol error exactly as for decodeMessage's nullopt.
bool decodeMessageInto(std::span<const std::uint8_t> frame,
                       DecodedMessage& out, DecodePool& pool);

}  // namespace rsm::rpc
