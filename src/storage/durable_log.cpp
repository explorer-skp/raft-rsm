#include "storage/durable_log.h"

#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstring>

#include "rpc/wire.h"
#include "storage/crc32c.h"
#include "storage/posix_io.h"

namespace rsm::storage {

namespace {

constexpr std::size_t kHeaderSize = 20;  // index + term + commandLength
constexpr std::size_t kCrcSize = 4;

std::size_t recordSize(const LogEntry& e) {
    return kHeaderSize + e.command.size() + kCrcSize;
}

}  // namespace

DurableLog::DurableLog(const std::string& dir, FsyncPolicy policy)
    : path_(dir + "/log"), policy_(policy) {
    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) detail::throwErrno("open " + path_);
    try {
        // Unconditional (cheap, once per boot): covers the create case,
        // where the file's directory entry is not yet durable.
        detail::fsyncDir(dir);
        replay();
    } catch (...) {
        ::close(fd_);
        throw;
    }
}

DurableLog::~DurableLog() {
    if (fd_ >= 0) ::close(fd_);
}

void DurableLog::replay() {
    struct stat st{};
    if (::fstat(fd_, &st) != 0) detail::throwErrno("fstat " + path_);
    const auto fileLen = static_cast<std::uint64_t>(st.st_size);

    // Read the whole file up front: the log is fully cached in memory anyway
    // (Phase 3 model, unchanged), so replay may as well be one read.
    std::vector<std::uint8_t> buf(fileLen);
    for (std::uint64_t off = 0; off < fileLen;) {
        const ssize_t r = ::pread(fd_, buf.data() + off, fileLen - off,
                                  static_cast<off_t>(off));
        if (r < 0) {
            if (errno == EINTR) continue;
            detail::throwErrno("pread " + path_);
        }
        if (r == 0) detail::throwErrno("short read " + path_);
        off += static_cast<std::uint64_t>(r);
    }

    std::uint64_t off = 0;
    while (off < fileLen) {
        const std::uint64_t remaining = fileLen - off;
        bool torn = false;
        if (remaining < kHeaderSize) {
            torn = true;  // not even a full header: crash mid-append
        } else {
            rsm::rpc::Reader r({buf.data() + off, remaining});
            const LogIndex index = r.u64();
            const Term term = r.u64();
            const std::uint32_t len = r.u32();
            const std::uint64_t extent = kHeaderSize + std::uint64_t{len} +
                                         kCrcSize;
            if (extent > remaining) {
                // Claimed extent runs past EOF. Either a torn append or a
                // corrupted length field — indistinguishable, and both end
                // the parseable log here.
                torn = true;
            } else {
                const std::uint32_t crc =
                    crc32c(buf.data() + off, kHeaderSize + len);
                const std::uint8_t* cp = buf.data() + off + kHeaderSize + len;
                const std::uint32_t storedCrc =
                    static_cast<std::uint32_t>(cp[0]) |
                    (static_cast<std::uint32_t>(cp[1]) << 8) |
                    (static_cast<std::uint32_t>(cp[2]) << 16) |
                    (static_cast<std::uint32_t>(cp[3]) << 24);
                const bool valid =
                    storedCrc == crc && index == entries_.size() + 1;
                if (!valid) {
                    if (extent == remaining) {
                        // Invalid trailing record: a crash mid-append can
                        // leave a full-sized but partially written record.
                        torn = true;
                    } else {
                        // Invalid with bytes after it: not a torn tail.
                        throw std::runtime_error(
                            "corrupt log record at offset " +
                            std::to_string(off) + " in " + path_);
                    }
                } else {
                    LogEntry e;
                    e.term = term;
                    e.command.assign(buf.begin() + static_cast<std::ptrdiff_t>(
                                                       off + kHeaderSize),
                                     buf.begin() + static_cast<std::ptrdiff_t>(
                                                       off + kHeaderSize + len));
                    entries_.push_back(std::move(e));
                    offsets_.push_back(off);
                    off += extent;
                }
            }
        }
        if (torn) {
            tornBytesDiscarded_ = fileLen - off;
            if (::ftruncate(fd_, static_cast<off_t>(off)) != 0) {
                detail::throwErrno("ftruncate " + path_);
            }
            detail::fsyncFd(fd_, path_);
            break;
        }
    }
    size_ = off;
}

void DurableLog::append(std::vector<LogEntry> entries) {
    if (entries.empty()) return;

    std::size_t total = 0;
    for (const auto& e : entries) total += recordSize(e);
    std::vector<std::uint8_t> buf(total);
    rsm::rpc::Writer w(buf);
    LogIndex index = lastIndex();
    for (const auto& e : entries) {
        const std::size_t recOff = w.written();
        w.u64(++index);
        w.u64(e.term);
        w.u32(static_cast<std::uint32_t>(e.command.size()));
        w.bytes(e.command.data(), e.command.size());
        w.u32(crc32c(buf.data() + recOff, kHeaderSize + e.command.size()));
    }
    assert(w.ok() && w.written() == total);

    detail::pwriteAll(fd_, buf.data(), total, size_, path_);
    // Both policies fsync here until Phase 7 adds the batched group-commit
    // path; see FsyncPolicy. The entries are durable when append() returns.
    detail::fsyncFd(fd_, path_);

    std::uint64_t off = size_;
    for (auto& e : entries) {
        offsets_.push_back(off);
        off += recordSize(e);
        entries_.push_back(std::move(e));
    }
    size_ = off;
}

Term DurableLog::termAt(LogIndex i) const {
    if (i == 0) return 0;
    assert(i <= lastIndex());
    if (i > lastIndex()) return 0;  // defensive in release builds
    return entries_[i - 1].term;
}

const LogEntry& DurableLog::entryAt(LogIndex i) const {
    assert(i >= 1 && i <= lastIndex());
    return entries_[i - 1];
}

std::vector<LogEntry> DurableLog::entriesFrom(LogIndex i) const {
    if (i < 1) i = 1;
    if (i > lastIndex()) return {};
    return {entries_.begin() + static_cast<std::ptrdiff_t>(i - 1),
            entries_.end()};
}

void DurableLog::truncateSuffixFrom(LogIndex i) {
    if (i < 1) i = 1;
    if (i > lastIndex()) return;
    const std::uint64_t newSize = offsets_[i - 1];
    if (::ftruncate(fd_, static_cast<off_t>(newSize)) != 0) {
        detail::throwErrno("ftruncate " + path_);
    }
    detail::fsyncFd(fd_, path_);  // truncation durable before we return
    size_ = newSize;
    offsets_.resize(i - 1);
    entries_.resize(i - 1);
}

}  // namespace rsm::storage
