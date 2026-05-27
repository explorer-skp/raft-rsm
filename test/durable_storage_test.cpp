// Phase 4 unit tests: durable metadata (atomic replace + recovery) and the
// durable log (round-trip replay, checksum detection, torn-tail recovery,
// durable truncation). Everything here reopens real files — durability is
// asserted by constructing a fresh object over the same directory, which is
// exactly what a restarted process does.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "doctest/doctest.h"
#include "storage/crc32c.h"
#include "storage/durable_log.h"
#include "storage/durable_state.h"
#include "temp_dir.h"

using rsm::rpc::LogEntry;
using rsm::storage::crc32c;
using rsm::storage::DurableLog;
using rsm::storage::DurablePersistentState;
using rsm::storage::FsyncPolicy;
using testutil::TempDir;

namespace {

std::vector<std::uint8_t> readFile(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    REQUIRE(f.good());
    return {std::istreambuf_iterator<char>(f),
            std::istreambuf_iterator<char>()};
}

void writeFile(const std::string& path, const std::vector<std::uint8_t>& b) {
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    REQUIRE(f.good());
    f.write(reinterpret_cast<const char*>(b.data()),
            static_cast<std::streamsize>(b.size()));
    REQUIRE(f.good());
}

LogEntry entry(std::uint64_t term, std::vector<std::uint8_t> cmd) {
    return LogEntry{term, std::move(cmd)};
}

}  // namespace

TEST_CASE("crc32c matches the published check value") {
    // RFC 3720 / common CRC catalog check value: crc32c("123456789").
    const std::uint8_t msg[] = {'1', '2', '3', '4', '5', '6', '7', '8', '9'};
    CHECK(crc32c(msg, 9) == 0xE3069283u);
    // Chained computation must equal one-shot.
    CHECK(crc32c(msg + 4, 5, crc32c(msg, 4)) == 0xE3069283u);
}

TEST_CASE("metadata: first start initializes to term 0, no vote") {
    TempDir dir;
    DurablePersistentState s(dir.path());
    CHECK(s.currentTerm() == 0);
    CHECK_FALSE(s.votedFor().has_value());
}

TEST_CASE("metadata: save/reopen round-trips term and votedFor") {
    TempDir dir;
    {
        DurablePersistentState s(dir.path());
        s.save(5, 2);
        CHECK(s.currentTerm() == 5);
        CHECK(s.votedFor() == 2);
    }
    {
        DurablePersistentState s(dir.path());
        CHECK(s.currentTerm() == 5);
        CHECK(s.votedFor() == 2);
        s.save(7, std::nullopt);  // overwrite, clearing the vote
    }
    DurablePersistentState s(dir.path());
    CHECK(s.currentTerm() == 7);
    CHECK_FALSE(s.votedFor().has_value());
}

TEST_CASE("metadata: crash between temp-write and rename leaves the previous "
          "metadata intact") {
    TempDir dir;
    {
        DurablePersistentState s(dir.path());
        s.save(5, 2);
    }
    // Simulate the crash: a meta.tmp exists (even a fully written one) but
    // the rename never happened. The previous file must win and the tmp must
    // not survive recovery.
    writeFile(dir.path() + "/meta.tmp", {0xDE, 0xAD, 0xBE, 0xEF});
    DurablePersistentState s(dir.path());
    CHECK(s.currentTerm() == 5);
    CHECK(s.votedFor() == 2);
    CHECK_FALSE(std::filesystem::exists(dir.path() + "/meta.tmp"));
}

TEST_CASE("metadata: a corrupted file is detected, never half-read") {
    TempDir dir;
    {
        DurablePersistentState s(dir.path());
        s.save(5, 2);
    }
    auto bytes = readFile(dir.path() + "/meta");
    REQUIRE(bytes.size() == 20);
    bytes[8] ^= 0x01;  // flip a bit inside currentTerm
    writeFile(dir.path() + "/meta", bytes);
    CHECK_THROWS(DurablePersistentState(dir.path()));
}

TEST_CASE("log: append/reopen replays an identical log and offset map") {
    TempDir dir;
    const std::vector<LogEntry> expect = {
        entry(1, {0x01}),
        entry(1, {}),  // empty command is legal
        entry(2, {0x10, 0x20, 0x30, 0x40, 0x50}),
    };
    std::vector<std::uint64_t> offsets;
    std::uint64_t fileSize = 0;
    {
        DurableLog log(dir.path());
        CHECK(log.lastIndex() == 0);
        log.append(expect);
        REQUIRE(log.lastIndex() == 3);
        for (rsm::rpc::LogIndex i = 1; i <= 3; ++i) {
            offsets.push_back(log.fileOffsetOf(i));
        }
        fileSize = log.fileSize();
    }
    DurableLog log(dir.path());
    CHECK(log.tornBytesDiscarded() == 0);
    REQUIRE(log.lastIndex() == 3);
    CHECK(log.lastTerm() == 2);
    CHECK(log.entriesFrom(1) == expect);
    CHECK(log.fileSize() == fileSize);
    for (rsm::rpc::LogIndex i = 1; i <= 3; ++i) {
        CHECK(log.fileOffsetOf(i) == offsets[i - 1]);  // identical offset map
    }
    // Records are dense: each offset is the previous record's end
    // (20-byte header + command + 4-byte CRC).
    CHECK(offsets[0] == 0);
    CHECK(offsets[1] == 20 + 1 + 4);
    CHECK(offsets[2] == offsets[1] + 20 + 0 + 4);
    // Appending after a reopen continues the file correctly.
    log.append({entry(2, {0x77})});
    DurableLog again(dir.path());
    CHECK(again.lastIndex() == 4);
    CHECK(again.entryAt(4).command == std::vector<std::uint8_t>{0x77});
}

TEST_CASE("log: a flipped byte in a non-trailing record is detected as "
          "corruption on replay") {
    TempDir dir;
    {
        DurableLog log(dir.path());
        log.append({entry(1, {0xAA, 0xBB}), entry(1, {0xCC}),
                    entry(2, {0xDD})});
    }
    auto bytes = readFile(dir.path() + "/log");
    bytes[20] ^= 0x01;  // first command byte of record 1 (mid-file)
    writeFile(dir.path() + "/log", bytes);
    CHECK_THROWS(DurableLog(dir.path()));
}

TEST_CASE("log: torn trailing record is discarded and the file truncated to "
          "the last valid prefix") {
    TempDir dir;
    std::uint64_t sizeAfterTwo = 0;
    {
        DurableLog log(dir.path());
        log.append({entry(1, {0x01}), entry(1, {0x02})});
        sizeAfterTwo = log.fileSize();
        log.append({entry(2, {0x03, 0x04, 0x05})});
    }
    auto full = readFile(dir.path() + "/log");

    SUBCASE("tail cut mid-record (incomplete body)") {
        writeFile(dir.path() + "/log",
                  {full.begin(), full.end() - 3});
    }
    SUBCASE("tail cut inside the record header") {
        writeFile(dir.path() + "/log",
                  {full.begin(),
                   full.begin() + static_cast<std::ptrdiff_t>(sizeAfterTwo + 7)});
    }
    SUBCASE("trailing record fully sized but corrupted (partial write)") {
        auto bytes = full;
        bytes[sizeAfterTwo + 20] ^= 0xFF;  // flip inside the LAST record
        writeFile(dir.path() + "/log", bytes);
    }
    SUBCASE("crash before the record body: only header-fragment garbage "
            "after the last full record") {
        std::vector<std::uint8_t> bytes{
            full.begin(),
            full.begin() + static_cast<std::ptrdiff_t>(sizeAfterTwo)};
        bytes.insert(bytes.end(), {0xDE, 0xAD});  // 2 bytes of a header
        writeFile(dir.path() + "/log", bytes);
    }

    DurableLog log(dir.path());
    CHECK(log.tornBytesDiscarded() > 0);
    REQUIRE(log.lastIndex() == 2);
    CHECK(log.entryAt(1).command == std::vector<std::uint8_t>{0x01});
    CHECK(log.entryAt(2).command == std::vector<std::uint8_t>{0x02});
    // The recovery ftruncate is itself durable: a second reopen is clean.
    DurableLog again(dir.path());
    CHECK(again.tornBytesDiscarded() == 0);
    CHECK(again.lastIndex() == 2);
    // And the log keeps working: the discarded entry can be re-appended.
    again.append({entry(2, {0x03, 0x04, 0x05})});
    CHECK(again.lastIndex() == 3);
}

TEST_CASE("log: truncateSuffixFrom is durable across reopen") {
    TempDir dir;
    {
        DurableLog log(dir.path());
        log.append({entry(1, {0x01}), entry(1, {0x02}), entry(1, {0x03}),
                    entry(2, {0x04})});
        log.truncateSuffixFrom(3);
        REQUIRE(log.lastIndex() == 2);
    }
    DurableLog log(dir.path());
    CHECK(log.tornBytesDiscarded() == 0);
    REQUIRE(log.lastIndex() == 2);
    CHECK(log.lastTerm() == 1);
    CHECK(log.entryAt(2).command == std::vector<std::uint8_t>{0x02});
    // Truncate-then-append (the AppendEntries conflict path) round-trips.
    log.append({entry(3, {0x33}), entry(3, {0x44})});
    DurableLog again(dir.path());
    REQUIRE(again.lastIndex() == 4);
    CHECK(again.termAt(3) == 3);
    CHECK(again.entryAt(4).command == std::vector<std::uint8_t>{0x44});
}

TEST_CASE("log: GroupCommit policy knob exists and is currently as durable "
          "as the default") {
    TempDir dir;
    {
        DurableLog log(dir.path(), FsyncPolicy::GroupCommit);
        log.append({entry(1, {0x09})});
    }
    DurableLog log(dir.path());
    REQUIRE(log.lastIndex() == 1);
    CHECK(log.entryAt(1).command == std::vector<std::uint8_t>{0x09});
}
