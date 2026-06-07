#include "rpc/messages.h"

#include "rpc/wire.h"

namespace rsm::rpc {
namespace {

// Minimum wire size of one log entry: term (8) + command length (4).
constexpr std::size_t kMinEntryWireSize = 12;

void encodeEnvelope(const Envelope& e, Writer& w) {
    w.u8(e.version);
    w.u8(static_cast<std::uint8_t>(e.type));
    w.u16(e.from);
    w.u16(e.to);
    w.u16(0);  // reserved, must be zero
    w.u32(e.payloadLength);
}

bool validType(std::uint8_t t) {
    return t >= static_cast<std::uint8_t>(MessageType::RequestVote) &&
           t <= static_cast<std::uint8_t>(MessageType::ClientReply);
}

// --- payload encoders -------------------------------------------------------

void encodePayload(const RequestVote& m, Writer& w) {
    w.u64(m.term);
    w.u16(m.candidateId);
    w.u64(m.lastLogIndex);
    w.u64(m.lastLogTerm);
}

void encodePayload(const RequestVoteReply& m, Writer& w) {
    w.u64(m.term);
    w.u8(m.voteGranted ? 1 : 0);
}

void encodePayload(const AppendEntries& m, Writer& w) {
    w.u64(m.term);
    w.u16(m.leaderId);
    w.u64(m.prevLogIndex);
    w.u64(m.prevLogTerm);
    w.u64(m.leaderCommit);
    w.u32(static_cast<std::uint32_t>(m.entries.size()));
    for (const auto& e : m.entries) {
        w.u64(e.term);
        w.u32(static_cast<std::uint32_t>(e.command.size()));
        w.bytes(e.command.data(), e.command.size());
    }
}

void encodePayload(const AppendEntriesReply& m, Writer& w) {
    w.u64(m.term);
    w.u8(m.success ? 1 : 0);
    w.u64(m.conflictIndex);
    w.u64(m.conflictTerm);
}

void encodePayload(const ClientRequest& m, Writer& w) {
    w.u64(m.clientId);
    w.u64(m.seqNo);
    w.u32(static_cast<std::uint32_t>(m.command.size()));
    w.bytes(m.command.data(), m.command.size());
}

void encodePayload(const ClientReply& m, Writer& w) {
    w.u8(static_cast<std::uint8_t>(m.status));
    w.u16(m.leaderHint);
    w.u32(static_cast<std::uint32_t>(m.result.size()));
    w.bytes(m.result.data(), m.result.size());
}

// --- payload decoders -------------------------------------------------------

bool decodeBool(Reader& r, bool& out) {
    const std::uint8_t v = r.u8();
    if (!r.ok() || v > 1) return false;
    out = (v == 1);
    return true;
}

// Reads a u32 length followed by that many bytes; rejects lengths that exceed
// the remaining input before allocating.
bool decodeBlob(Reader& r, std::vector<std::uint8_t>& out) {
    const std::uint32_t len = r.u32();
    if (!r.ok() || len > r.remaining()) return false;
    return r.readBytes(out, len);
}

bool decodePayload(Reader& r, RequestVote& m) {
    m.term = r.u64();
    m.candidateId = r.u16();
    m.lastLogIndex = r.u64();
    m.lastLogTerm = r.u64();
    return r.ok();
}

bool decodePayload(Reader& r, RequestVoteReply& m) {
    m.term = r.u64();
    return r.ok() && decodeBool(r, m.voteGranted);
}

bool decodePayload(Reader& r, AppendEntriesReply& m) {
    m.term = r.u64();
    if (!r.ok() || !decodeBool(r, m.success)) return false;
    m.conflictIndex = r.u64();
    m.conflictTerm = r.u64();
    return r.ok();
}

bool decodePayload(Reader& r, ClientRequest& m) {
    m.clientId = r.u64();
    m.seqNo = r.u64();
    return r.ok() && decodeBlob(r, m.command);
}

bool decodePayload(Reader& r, ClientReply& m) {
    const std::uint8_t status = r.u8();
    if (!r.ok() || status > static_cast<std::uint8_t>(ClientStatus::Error)) {
        return false;
    }
    m.status = static_cast<ClientStatus>(status);
    m.leaderHint = r.u16();
    return decodeBlob(r, m.result);
}

// The AppendEntries decoder is in-place by construction (Phase 7 rx path):
// existing entries are overwritten via assign (capacity reuse); growth
// pulls command buffers from the pool, shrink salvages them back. The
// count check still rejects input-unbackable counts BEFORE any growth.
bool decodePayloadInto(Reader& r, AppendEntries& m, DecodePool& pool) {
    m.term = r.u64();
    m.leaderId = r.u16();
    m.prevLogIndex = r.u64();
    m.prevLogTerm = r.u64();
    m.leaderCommit = r.u64();
    const std::uint32_t count = r.u32();
    if (!r.ok()) return false;
    if (count > r.remaining() / kMinEntryWireSize) return false;
    while (m.entries.size() > count) {
        pool.putBuf(std::move(m.entries.back().command));
        m.entries.pop_back();
    }
    for (std::uint32_t i = 0; i < count; ++i) {
        if (i == m.entries.size()) {
            LogEntry e;
            e.command = pool.getBuf();
            m.entries.push_back(std::move(e));
        }
        LogEntry& e = m.entries[i];
        e.term = r.u64();
        if (!r.ok() || !decodeBlob(r, e.command)) return false;
    }
    return true;
}

// Moves an alternative's heap buffers into the pool before the variant
// switches away from it, so they are recycled instead of freed.
void salvage(Message& m, DecodePool& pool) {
    if (auto* ae = std::get_if<AppendEntries>(&m)) {
        for (auto& e : ae->entries) pool.putBuf(std::move(e.command));
        ae->entries.clear();
        pool.entryLists.push_back(std::move(ae->entries));
    } else if (auto* cr = std::get_if<ClientRequest>(&m)) {
        pool.putBuf(std::move(cr->command));
    } else if (auto* rep = std::get_if<ClientReply>(&m)) {
        pool.putBuf(std::move(rep->result));
    }
}

// Ensures `msg` holds alternative T, recycling buffers across the switch.
template <typename T>
T& alternativeFor(Message& msg, DecodePool& pool) {
    if (auto* p = std::get_if<T>(&msg)) return *p;
    salvage(msg, pool);
    T& fresh = msg.emplace<T>();
    if constexpr (std::is_same_v<T, AppendEntries>) {
        if (!pool.entryLists.empty()) {
            fresh.entries = std::move(pool.entryLists.back());
            pool.entryLists.pop_back();
        }
    } else if constexpr (std::is_same_v<T, ClientRequest>) {
        fresh.command = pool.getBuf();
    } else if constexpr (std::is_same_v<T, ClientReply>) {
        fresh.result = pool.getBuf();
    }
    return fresh;
}

template <typename T>
bool decodeIntoAlternative(Reader& r, Message& out, DecodePool& pool) {
    T& m = alternativeFor<T>(out, pool);
    if constexpr (std::is_same_v<T, AppendEntries>) {
        return decodePayloadInto(r, m, pool);
    } else {
        // The non-AE decoders already assign into existing vectors
        // (decodeBlob -> Reader::readBytes -> assign), so in-place reuse
        // needs no separate code path.
        return decodePayload(r, m);
    }
}

}  // namespace

MessageType typeOf(const Message& m) {
    return std::visit(
        [](const auto& v) {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, RequestVote>) {
                return MessageType::RequestVote;
            } else if constexpr (std::is_same_v<T, RequestVoteReply>) {
                return MessageType::RequestVoteReply;
            } else if constexpr (std::is_same_v<T, AppendEntries>) {
                return MessageType::AppendEntries;
            } else if constexpr (std::is_same_v<T, AppendEntriesReply>) {
                return MessageType::AppendEntriesReply;
            } else if constexpr (std::is_same_v<T, ClientRequest>) {
                return MessageType::ClientRequest;
            } else {
                static_assert(std::is_same_v<T, ClientReply>);
                return MessageType::ClientReply;
            }
        },
        m);
}

std::size_t payloadSize(const Message& m) {
    return std::visit(
        [](const auto& v) -> std::size_t {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, RequestVote>) {
                return 8 + 2 + 8 + 8;
            } else if constexpr (std::is_same_v<T, RequestVoteReply>) {
                return 8 + 1;
            } else if constexpr (std::is_same_v<T, AppendEntries>) {
                std::size_t n = 8 + 2 + 8 + 8 + 8 + 4;
                for (const auto& e : v.entries) n += 8 + 4 + e.command.size();
                return n;
            } else if constexpr (std::is_same_v<T, AppendEntriesReply>) {
                return 8 + 1 + 8 + 8;
            } else if constexpr (std::is_same_v<T, ClientRequest>) {
                return 8 + 8 + 4 + v.command.size();
            } else {
                static_assert(std::is_same_v<T, ClientReply>);
                return 1 + 2 + 4 + v.result.size();
            }
        },
        m);
}

std::size_t encodeMessage(NodeId from, NodeId to, const Message& m,
                          std::span<std::uint8_t> out) {
    const std::size_t psize = payloadSize(m);
    if (psize > kMaxPayloadSize || out.size() < kEnvelopeSize + psize) return 0;
    Envelope env;
    env.version = kProtocolVersion;
    env.type = typeOf(m);
    env.from = from;
    env.to = to;
    env.payloadLength = static_cast<std::uint32_t>(psize);
    Writer w(out);
    encodeEnvelope(env, w);
    std::visit([&w](const auto& v) { encodePayload(v, w); }, m);
    return w.ok() ? w.written() : 0;
}

bool decodeMessageInto(std::span<const std::uint8_t> frame,
                       DecodedMessage& out, DecodePool& pool) {
    if (frame.size() < kEnvelopeSize ||
        frame.size() > kEnvelopeSize + kMaxPayloadSize) {
        return false;
    }
    Reader r(frame);
    Envelope& env = out.envelope;
    env.version = r.u8();
    const std::uint8_t rawType = r.u8();
    env.from = r.u16();
    env.to = r.u16();
    const std::uint16_t reserved = r.u16();
    env.payloadLength = r.u32();
    if (!r.ok() || env.version != kProtocolVersion || reserved != 0 ||
        !validType(rawType) ||
        env.payloadLength != frame.size() - kEnvelopeSize) {
        return false;
    }
    env.type = static_cast<MessageType>(rawType);

    bool good = false;
    switch (env.type) {
        case MessageType::RequestVote:
            good = decodeIntoAlternative<RequestVote>(r, out.message, pool);
            break;
        case MessageType::RequestVoteReply:
            good = decodeIntoAlternative<RequestVoteReply>(r, out.message,
                                                           pool);
            break;
        case MessageType::AppendEntries:
            good = decodeIntoAlternative<AppendEntries>(r, out.message, pool);
            break;
        case MessageType::AppendEntriesReply:
            good = decodeIntoAlternative<AppendEntriesReply>(r, out.message,
                                                             pool);
            break;
        case MessageType::ClientRequest:
            good = decodeIntoAlternative<ClientRequest>(r, out.message, pool);
            break;
        case MessageType::ClientReply:
            good = decodeIntoAlternative<ClientReply>(r, out.message, pool);
            break;
    }
    return good && r.exhausted();
}

std::optional<DecodedMessage> decodeMessage(std::span<const std::uint8_t> frame) {
    DecodedMessage out;
    DecodePool pool;
    if (!decodeMessageInto(frame, out, pool)) return std::nullopt;
    return out;
}

}  // namespace rsm::rpc
