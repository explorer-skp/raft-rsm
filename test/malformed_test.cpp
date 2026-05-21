// Decoder rejection tests: every case must fail cleanly (nullopt) with no
// crash and no over-read. The whole file must be ASan/UBSan-clean.
#include <doctest/doctest.h>

#include <cstdint>
#include <vector>

#include "rpc/messages.h"
#include "rpc/wire.h"

using namespace rsm::rpc;

namespace {

// Envelope byte offsets (see DESIGN.md).
constexpr std::size_t kOffVersion = 0;
constexpr std::size_t kOffType = 1;
constexpr std::size_t kOffReserved = 6;
constexpr std::size_t kOffPayloadLen = 8;

std::vector<std::uint8_t> encoded(const Message& m) {
    std::vector<std::uint8_t> buf(encodedSize(m));
    REQUIRE(encodeMessage(1, 2, m, buf) == buf.size());
    return buf;
}

void patchU32(std::vector<std::uint8_t>& buf, std::size_t off, std::uint32_t v) {
    Writer w(std::span<std::uint8_t>(buf).subspan(off, 4));
    w.u32(v);
    REQUIRE(w.ok());
}

}  // namespace

TEST_CASE("truncated envelope is rejected") {
    const auto buf = encoded(RequestVote{1, 1, 1, 1});
    for (std::size_t n = 0; n < kEnvelopeSize; ++n) {
        CAPTURE(n);
        CHECK_FALSE(decodeMessage(std::span(buf).subspan(0, n)).has_value());
    }
}

TEST_CASE("declared payload length longer than actual payload is rejected") {
    auto buf = encoded(RequestVote{1, 1, 1, 1});
    patchU32(buf, kOffPayloadLen,
             static_cast<std::uint32_t>(buf.size() - kEnvelopeSize + 1));
    CHECK_FALSE(decodeMessage(buf).has_value());
}

TEST_CASE("declared payload length shorter than actual (trailing bytes) is rejected") {
    auto buf = encoded(RequestVoteReply{1, true});
    buf.push_back(0x00);  // payloadLength now disagrees with the buffer
    CHECK_FALSE(decodeMessage(buf).has_value());
}

TEST_CASE("truncated payload is rejected") {
    auto buf = encoded(AppendEntries{1, 1, 1, 1, 1, {LogEntry{1, {1, 2, 3}}}});
    buf.pop_back();  // lose one command byte
    patchU32(buf, kOffPayloadLen,
             static_cast<std::uint32_t>(buf.size() - kEnvelopeSize));
    CHECK_FALSE(decodeMessage(buf).has_value());
}

TEST_CASE("unknown message type is rejected") {
    for (const std::uint8_t type : {std::uint8_t{0}, std::uint8_t{7}, std::uint8_t{0xFF}}) {
        CAPTURE(type);
        auto buf = encoded(RequestVote{1, 1, 1, 1});
        buf[kOffType] = type;
        CHECK_FALSE(decodeMessage(buf).has_value());
    }
}

TEST_CASE("wrong protocol version is rejected") {
    auto buf = encoded(RequestVote{1, 1, 1, 1});
    buf[kOffVersion] = kProtocolVersion + 1;
    CHECK_FALSE(decodeMessage(buf).has_value());
}

TEST_CASE("nonzero reserved bytes are rejected") {
    auto buf = encoded(RequestVote{1, 1, 1, 1});
    buf[kOffReserved] = 0x01;
    CHECK_FALSE(decodeMessage(buf).has_value());
}

TEST_CASE("entry count the input cannot back is rejected without allocating") {
    auto buf = encoded(AppendEntries{1, 1, 1, 1, 1, {}});
    // entryCount is the last u32 of the empty-entries payload.
    patchU32(buf, buf.size() - 4, 0xFFFFFFFF);
    CHECK_FALSE(decodeMessage(buf).has_value());
}

TEST_CASE("command length overrunning the entry is rejected") {
    auto buf = encoded(AppendEntries{1, 1, 1, 1, 1, {LogEntry{1, {1, 2, 3}}}});
    // The entry's command length is the u32 right before its 3 command bytes.
    patchU32(buf, buf.size() - 3 - 4, 0xFFFFFFF0);
    CHECK_FALSE(decodeMessage(buf).has_value());
}

TEST_CASE("out-of-range bool and status bytes are rejected") {
    auto vote = encoded(RequestVoteReply{1, true});
    vote.back() = 7;  // voteGranted is the final payload byte
    CHECK_FALSE(decodeMessage(vote).has_value());

    auto reply = encoded(ClientReply{ClientStatus::Ok, 0, {}});
    reply[kEnvelopeSize] = 9;  // status is the first payload byte
    CHECK_FALSE(decodeMessage(reply).has_value());
}

TEST_CASE("inner lengths that disagree with payloadLength are rejected") {
    // ClientRequest whose command length field claims one byte fewer than
    // the payload actually carries: decoder must reject the leftover byte.
    auto buf = encoded(ClientRequest{1, 1, {1, 2, 3}});
    patchU32(buf, buf.size() - 3 - 4, 2);
    CHECK_FALSE(decodeMessage(buf).has_value());
}
