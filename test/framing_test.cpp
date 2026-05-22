#include <doctest/doctest.h>

#include <cstdint>
#include <vector>

#include "transport/frame.h"

using rsm::transport::FrameAssembler;
using rsm::transport::kLengthPrefixSize;
using rsm::transport::writeLengthPrefix;

namespace {

std::vector<std::uint8_t> makeFrame(const std::vector<std::uint8_t>& body) {
    std::vector<std::uint8_t> f(kLengthPrefixSize + body.size());
    writeLengthPrefix(static_cast<std::uint32_t>(body.size()), f.data());
    std::copy(body.begin(), body.end(), f.begin() + kLengthPrefixSize);
    return f;
}

struct Collector {
    std::vector<std::vector<std::uint8_t>> frames;
    FrameAssembler::FrameHandler handler() {
        return [this](std::span<const std::uint8_t> body) {
            frames.emplace_back(body.begin(), body.end());
        };
    }
};

}  // namespace

TEST_CASE("one byte at a time across two frames") {
    const std::vector<std::uint8_t> bodyA = {1, 2, 3};
    const std::vector<std::uint8_t> bodyB = {9, 8, 7, 6, 5};
    std::vector<std::uint8_t> stream = makeFrame(bodyA);
    const auto frameB = makeFrame(bodyB);
    stream.insert(stream.end(), frameB.begin(), frameB.end());

    FrameAssembler asmblr;
    Collector got;
    for (const std::uint8_t b : stream) {
        CHECK(asmblr.feed(std::span<const std::uint8_t>(&b, 1), got.handler()));
    }
    REQUIRE(got.frames.size() == 2);
    CHECK(got.frames[0] == bodyA);
    CHECK(got.frames[1] == bodyB);
}

TEST_CASE("split in the middle of the length prefix") {
    const std::vector<std::uint8_t> body = {42, 43, 44, 45};
    const auto frame = makeFrame(body);

    FrameAssembler asmblr;
    Collector got;
    // First two bytes of the 4-byte prefix, then the rest.
    CHECK(asmblr.feed(std::span(frame).subspan(0, 2), got.handler()));
    CHECK(got.frames.empty());
    CHECK(asmblr.feed(std::span(frame).subspan(2), got.handler()));
    REQUIRE(got.frames.size() == 1);
    CHECK(got.frames[0] == body);
}

TEST_CASE("two frames coalesced into one buffer") {
    const std::vector<std::uint8_t> bodyA(100, 0x11);
    const std::vector<std::uint8_t> bodyB = {0xFF};
    auto stream = makeFrame(bodyA);
    const auto frameB = makeFrame(bodyB);
    stream.insert(stream.end(), frameB.begin(), frameB.end());

    FrameAssembler asmblr;
    Collector got;
    CHECK(asmblr.feed(stream, got.handler()));
    REQUIRE(got.frames.size() == 2);
    CHECK(got.frames[0] == bodyA);
    CHECK(got.frames[1] == bodyB);
}

TEST_CASE("frame split with second frame's prefix attached to first body") {
    const std::vector<std::uint8_t> bodyA = {1};
    const std::vector<std::uint8_t> bodyB = {2, 3};
    auto stream = makeFrame(bodyA);
    const auto frameB = makeFrame(bodyB);
    stream.insert(stream.end(), frameB.begin(), frameB.end());

    FrameAssembler asmblr;
    Collector got;
    // Cut inside frame B's length prefix.
    const std::size_t cut = kLengthPrefixSize + bodyA.size() + 2;
    CHECK(asmblr.feed(std::span(stream).subspan(0, cut), got.handler()));
    CHECK(got.frames.size() == 1);
    CHECK(asmblr.feed(std::span(stream).subspan(cut), got.handler()));
    REQUIRE(got.frames.size() == 2);
    CHECK(got.frames[0] == bodyA);
    CHECK(got.frames[1] == bodyB);
}

TEST_CASE("zero-length frame is a protocol error and poisons the assembler") {
    const std::uint8_t zeroPrefix[4] = {0, 0, 0, 0};
    FrameAssembler asmblr;
    Collector got;
    CHECK_FALSE(asmblr.feed(std::span<const std::uint8_t>(zeroPrefix, 4),
                            got.handler()));
    CHECK(got.frames.empty());
    // Poisoned: even a valid frame is rejected afterwards.
    const auto frame = makeFrame({1, 2, 3});
    CHECK_FALSE(asmblr.feed(frame, got.handler()));
    CHECK(got.frames.empty());
}

TEST_CASE("oversize declared length is a protocol error") {
    std::uint8_t prefix[4];
    writeLengthPrefix(0xFFFFFFFF, prefix);
    FrameAssembler asmblr;
    Collector got;
    CHECK_FALSE(asmblr.feed(std::span<const std::uint8_t>(prefix, 4),
                            got.handler()));
    CHECK(got.frames.empty());
}
