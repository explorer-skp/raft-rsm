#include "storage/durable_state.h"

#include <unistd.h>

#include <array>
#include <cstdio>

#include "rpc/wire.h"
#include "storage/crc32c.h"
#include "storage/posix_io.h"

namespace rsm::storage {

namespace {

constexpr std::uint32_t kMagic = 0x4D4D5352u;  // "RSMM" when read LE
constexpr std::uint8_t kVersion = 1;
constexpr std::size_t kMetaSize = 20;
constexpr std::size_t kCrcOffset = 16;  // CRC covers bytes [0, 16)

}  // namespace

DurablePersistentState::DurablePersistentState(const std::string& dir)
    : dir_(dir), path_(dir + "/meta"), tmpPath_(dir + "/meta.tmp") {
    // A meta.tmp can only be a crash leftover from before a rename; the real
    // file (or its absence) is the truth. Remove it so it cannot linger.
    ::unlink(tmpPath_.c_str());

    std::FILE* f = std::fopen(path_.c_str(), "rb");
    if (f == nullptr) {
        if (errno == ENOENT) return;  // first ever start: term 0, no vote
        detail::throwErrno("open " + path_);
    }
    std::array<std::uint8_t, kMetaSize + 1> buf{};  // +1 to detect oversize
    const std::size_t n = std::fread(buf.data(), 1, buf.size(), f);
    std::fclose(f);
    if (n != kMetaSize) {
        throw std::runtime_error("corrupt metadata file " + path_ +
                                 ": wrong size");
    }
    const std::uint32_t crc = crc32c(buf.data(), kCrcOffset);
    rsm::rpc::Reader r({buf.data(), kMetaSize});
    const std::uint32_t magic = r.u32();
    const std::uint8_t version = r.u8();
    const Term term = r.u64();
    const std::uint8_t hasVote = r.u8();
    const NodeId vote = r.u16();
    const std::uint32_t storedCrc = r.u32();
    if (magic != kMagic || version != kVersion || hasVote > 1 ||
        storedCrc != crc) {
        // Never half-written by construction (atomic rename), so a bad file
        // is real corruption, not a crash artifact: fail loudly.
        throw std::runtime_error("corrupt metadata file " + path_);
    }
    term_ = term;
    if (hasVote == 1) votedFor_ = vote;
}

void DurablePersistentState::save(Term term, std::optional<NodeId> votedFor) {
    std::array<std::uint8_t, kMetaSize> buf{};
    rsm::rpc::Writer w(buf);
    w.u32(kMagic);
    w.u8(kVersion);
    w.u64(term);
    w.u8(votedFor.has_value() ? 1 : 0);
    w.u16(votedFor.value_or(0));
    w.u32(crc32c(buf.data(), kCrcOffset));

    const int fd = ::open(tmpPath_.c_str(), O_WRONLY | O_CREAT | O_TRUNC,
                          0644);
    if (fd < 0) detail::throwErrno("open " + tmpPath_);
    try {
        detail::pwriteAll(fd, buf.data(), buf.size(), 0, tmpPath_);
        detail::fsyncFd(fd, tmpPath_);
    } catch (...) {
        ::close(fd);
        throw;
    }
    ::close(fd);
    if (::rename(tmpPath_.c_str(), path_.c_str()) != 0) {
        detail::throwErrno("rename " + tmpPath_);
    }
    detail::fsyncDir(dir_);  // make the replacement itself durable

    term_ = term;
    votedFor_ = votedFor;
}

}  // namespace rsm::storage
