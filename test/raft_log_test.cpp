// Phase 3: log storage seam unit tests — 1-based indexing, 0 sentinel,
// boundaries at index 0/1.

#include "doctest/doctest.h"
#include "storage/log.h"

using rsm::rpc::LogEntry;
using rsm::storage::InMemoryLog;

namespace {

LogEntry entry(rsm::rpc::Term term, std::uint8_t tag) {
    return LogEntry{term, {tag}};
}

}  // namespace

TEST_CASE("empty log: sentinel values") {
    InMemoryLog log;
    CHECK(log.lastIndex() == 0);
    CHECK(log.lastTerm() == 0);
    CHECK(log.termAt(0) == 0);
    CHECK(log.entriesFrom(1).empty());
}

TEST_CASE("append assigns 1-based indices; termAt/entryAt/lastTerm") {
    InMemoryLog log;
    log.append({entry(1, 0xA), entry(1, 0xB), entry(2, 0xC)});
    CHECK(log.lastIndex() == 3);
    CHECK(log.lastTerm() == 2);
    CHECK(log.termAt(0) == 0);
    CHECK(log.termAt(1) == 1);
    CHECK(log.termAt(3) == 2);
    CHECK(log.entryAt(1).command == std::vector<std::uint8_t>{0xA});
    CHECK(log.entryAt(3).command == std::vector<std::uint8_t>{0xC});

    log.append({entry(3, 0xD)});
    CHECK(log.lastIndex() == 4);
    CHECK(log.lastTerm() == 3);
}

TEST_CASE("entriesFrom slices [i, last] and handles boundaries") {
    InMemoryLog log;
    log.append({entry(1, 1), entry(1, 2), entry(2, 3)});
    CHECK(log.entriesFrom(1).size() == 3);
    const auto tail = log.entriesFrom(2);
    REQUIRE(tail.size() == 2);
    CHECK(tail[0] == entry(1, 2));
    CHECK(tail[1] == entry(2, 3));
    CHECK(log.entriesFrom(3).size() == 1);
    CHECK(log.entriesFrom(4).empty());  // one past the end: empty, no error
}

TEST_CASE("truncateSuffixFrom deletes [i, last]; boundaries at 1 and past "
          "the end") {
    InMemoryLog log;
    log.append({entry(1, 1), entry(1, 2), entry(2, 3)});

    log.truncateSuffixFrom(4);  // past the end: no-op
    CHECK(log.lastIndex() == 3);

    log.truncateSuffixFrom(3);
    CHECK(log.lastIndex() == 2);
    CHECK(log.lastTerm() == 1);

    log.truncateSuffixFrom(1);  // down to empty
    CHECK(log.lastIndex() == 0);
    CHECK(log.lastTerm() == 0);

    log.append({entry(5, 9)});  // usable again after truncation to empty
    CHECK(log.lastIndex() == 1);
    CHECK(log.termAt(1) == 5);
}
