#pragma once

// RAII temporary directory for the durable-storage tests: mkdtemp under the
// system temp dir, recursively removed on destruction.

#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>

namespace testutil {

class TempDir {
public:
    TempDir() {
        std::string tmpl =
            (std::filesystem::temp_directory_path() / "raftrsm-XXXXXX")
                .string();
        if (::mkdtemp(tmpl.data()) == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        path_ = tmpl;
    }
    ~TempDir() {
        std::error_code ec;  // best effort; never throw from a destructor
        std::filesystem::remove_all(path_, ec);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    const std::string& path() const { return path_; }
    // Per-node subdirectory, created on demand.
    std::string subdir(const std::string& name) const {
        const auto p = std::filesystem::path(path_) / name;
        std::filesystem::create_directories(p);
        return p.string();
    }

private:
    std::string path_;
};

}  // namespace testutil
