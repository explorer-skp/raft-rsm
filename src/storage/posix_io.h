#pragma once

// Internal POSIX helpers shared by the durable storage implementations.
//
// Error-handling policy (DESIGN.md, Phase 4): durability failures are
// fail-stop. Every helper throws std::runtime_error on error; nothing above
// catches it on the Raft path, so the node dies — which to the rest of the
// cluster is indistinguishable from a crash, and Raft tolerates crashes.
// Continuing past a failed write/fsync could acknowledge state that is not
// actually durable, which is the one thing this layer must never do.

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

namespace rsm::storage::detail {

[[noreturn]] inline void throwErrno(const std::string& what) {
    throw std::runtime_error(what + ": " + std::strerror(errno));
}

// Writes all n bytes at file offset off (pwrite loop; EINTR-safe).
inline void pwriteAll(int fd, const std::uint8_t* p, std::size_t n,
                      std::uint64_t off, const std::string& what) {
    while (n > 0) {
        const ssize_t w = ::pwrite(fd, p, n, static_cast<off_t>(off));
        if (w < 0) {
            if (errno == EINTR) continue;
            throwErrno("pwrite " + what);
        }
        p += w;
        n -= static_cast<std::size_t>(w);
        off += static_cast<std::uint64_t>(w);
    }
}

inline void fsyncFd(int fd, const std::string& what) {
    if (::fsync(fd) != 0) throwErrno("fsync " + what);
}

// fsyncs the directory itself so a freshly created or renamed entry is
// durable (the file's own fsync does not cover its directory entry).
inline void fsyncDir(const std::string& dir) {
    const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) throwErrno("open dir " + dir);
    if (::fsync(fd) != 0) {
        const int e = errno;
        ::close(fd);
        errno = e;
        throwErrno("fsync dir " + dir);
    }
    ::close(fd);
}

}  // namespace rsm::storage::detail
