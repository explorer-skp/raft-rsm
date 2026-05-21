#include <doctest/doctest.h>

#include <cstdint>
#include <cstring>
#include <limits>
#include <vector>

#include "rpc/messages.h"

using namespace rsm::rpc;

namespace {

std::vector<std::uint8_t> bytesOf(const char* s) {
    return {reinterpret_cast<const std::uint8_t*>(s),
            reinterpret_cast<const std::uint8_t*>(s) + std::strlen(s)};
}

// Encodes with the given endpoints into an exactly-sized buffer, decodes,
// and checks the envelope and message both survive the trip.
DecodedMessage roundTrip(NodeId from, NodeId to, const Message& m) {
    std::vector<std::uint8_t> buf(encodedSize(m));
    const std::size_t n = encodeMessage(from, to, m, buf);
    REQUIRE(n == buf.size());

    auto decoded = decodeMessage(buf);
    REQUIRE(decoded.has_value());
    CHECK(decoded->envelope.version == kProtocolVersion);
    CHECK(decoded->envelope.type == typeOf(m));
    CHECK(decoded->envelope.from == from);
    CHECK(decoded->envelope.to == to);
    CHECK(decoded->envelope.payloadLength == buf.size() - kEnvelopeSize);
    CHECK(decoded->message == m);
    return *decoded;
}

constexpr std::uint64_t kMaxU64 = std::numeric_limits<std::uint64_t>::max();
constexpr NodeId kMaxNode = std::numeric_limits<NodeId>::max();

}  // namespace

TEST_CASE("RequestVote round-trips") {
    roundTrip(1, 2, RequestVote{42, 1, 99, 41});
    SUBCASE("boundary values") {
        roundTrip(kMaxNode, 0, RequestVote{kMaxU64, kMaxNode, kMaxU64, kMaxU64});
        roundTrip(0, kMaxNode, RequestVote{0, 0, 0, 0});
    }
}

TEST_CASE("RequestVoteReply round-trips") {
    roundTrip(2, 1, RequestVoteReply{42, true});
    roundTrip(2, 1, RequestVoteReply{kMaxU64, false});
}

TEST_CASE("AppendEntries round-trips") {
    SUBCASE("empty entries (heartbeat shape)") {
        roundTrip(1, 3, AppendEntries{7, 1, 12, 6, 10, {}});
    }
    SUBCASE("multi-entry with mixed payloads") {
        AppendEntries ae{7, 1, 12, 6, 10, {}};
        ae.entries.push_back(LogEntry{7, bytesOf("PUT k1 v1")});
        ae.entries.push_back(LogEntry{7, {}});  // empty command payload
        ae.entries.push_back(LogEntry{kMaxU64, std::vector<std::uint8_t>(1000, 0xAB)});
        roundTrip(1, 3, ae);
    }
    SUBCASE("boundary scalar values") {
        roundTrip(kMaxNode, kMaxNode,
                  AppendEntries{kMaxU64, kMaxNode, kMaxU64, kMaxU64, kMaxU64,
                                {LogEntry{kMaxU64, bytesOf("x")}}});
    }
}

TEST_CASE("AppendEntriesReply round-trips") {
    roundTrip(3, 1, AppendEntriesReply{7, true, 0, 0});
    roundTrip(3, 1, AppendEntriesReply{7, false, 5, 4});
    roundTrip(3, 1, AppendEntriesReply{kMaxU64, false, kMaxU64, kMaxU64});
}

TEST_CASE("ClientRequest round-trips") {
    roundTrip(0, 1, ClientRequest{0xDEADBEEF, 17, bytesOf("GET k1")});
    SUBCASE("empty command payload") {
        roundTrip(0, 1, ClientRequest{kMaxU64, kMaxU64, {}});
    }
}

TEST_CASE("ClientReply round-trips") {
    roundTrip(1, 0, ClientReply{ClientStatus::Ok, 0, bytesOf("v1")});
    roundTrip(1, 0, ClientReply{ClientStatus::NotLeader, 2, {}});
    roundTrip(1, 0, ClientReply{ClientStatus::Error, kMaxNode, bytesOf("boom")});
}

TEST_CASE("encode fails cleanly into a too-small buffer") {
    const Message m = RequestVote{1, 1, 1, 1};
    std::vector<std::uint8_t> small(encodedSize(m) - 1);
    CHECK(encodeMessage(1, 2, m, small) == 0);
    CHECK(encodeMessage(1, 2, m, std::span<std::uint8_t>{}) == 0);
}
