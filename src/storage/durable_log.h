#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "storage/log.h"

namespace rsm::storage {

// Spec §4.4 fsync policy knob. EveryDurabilityPoint (default) fsyncs on
// every append/truncate before returning. GroupCommit will coalesce multiple
// appends into one fsync when the Phase 7 batching layer lands; until then
// it behaves identically to the safe default, so selecting it can never
// weaken durability by accident.
enum class FsyncPolicy : std::uint8_t { EveryDurabilityPoint, GroupCommit };

// Durable RaftLog (Phase 4): append-only file on the happy path, with a full
// in-memory copy of the entries (reads never touch disk — same memory model
// as InMemoryLog) and an index → file-offset map so truncateSuffixFrom() can
// ftruncate() at a record boundary. append() and truncateSuffixFrom() return
// only once the change is durable.
//
// Record layout (little-endian; mirrored in DESIGN.md). Records are written
// back to back; index contiguity (record i holds index i) is what lets
// replay find boundaries without a separate index file:
//   [0]        u64 index   (1-based, must be previous index + 1)
//   [8]        u64 term
//   [16]       u32 commandLength
//   [20]       command bytes
//   [20+len]   u32 CRC32C over bytes [0, 20+len) of the record
//
// Replay/recovery rules (DESIGN.md has the rationale):
//   - A record whose claimed extent runs past end-of-file, or any invalid
//     record that ends exactly at end-of-file, is a torn append: it is
//     discarded and the file ftruncate()d to the end of the last valid
//     record. Expected after a crash mid-append; recovers cleanly.
//   - An invalid record (bad CRC, non-contiguous index) with more bytes
//     after it cannot be a torn tail: that is corruption, and the
//     constructor throws rather than silently dropping committed entries.
class DurableLog final : public RaftLog {
public:
    // Opens (or creates) `dir`/log and replays it, validating every record.
    explicit DurableLog(const std::string& dir,
                        FsyncPolicy policy = FsyncPolicy::EveryDurabilityPoint);
    ~DurableLog() override;

    DurableLog(const DurableLog&) = delete;
    DurableLog& operator=(const DurableLog&) = delete;

    void append(std::vector<LogEntry> entries) override;
    Term termAt(LogIndex i) const override;
    const LogEntry& entryAt(LogIndex i) const override;
    std::vector<LogEntry> entriesFrom(LogIndex i) const override;
    void truncateSuffixFrom(LogIndex i) override;
    LogIndex lastIndex() const override { return entries_.size(); }
    Term lastTerm() const override {
        return entries_.empty() ? 0 : entries_.back().term;
    }

    // Introspection for tests.
    std::uint64_t fileSize() const { return size_; }
    std::uint64_t fileOffsetOf(LogIndex i) const { return offsets_[i - 1]; }
    // Bytes discarded as a torn tail during replay (0 on a clean open).
    std::uint64_t tornBytesDiscarded() const { return tornBytesDiscarded_; }

private:
    void replay();

    std::string path_;
    FsyncPolicy policy_;
    int fd_ = -1;
    std::vector<LogEntry> entries_;       // entries_[i-1] holds index i
    std::vector<std::uint64_t> offsets_;  // offsets_[i-1]: record i's offset
    std::uint64_t size_ = 0;              // file end == next record's offset
    std::uint64_t tornBytesDiscarded_ = 0;
};

}  // namespace rsm::storage
