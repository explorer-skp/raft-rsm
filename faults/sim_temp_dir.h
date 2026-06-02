#pragma once

// RAII temp directory for the chaos driver's durable-storage runs.
// Deliberately mirrors test/temp_dir.h rather than including it: the faults
// library must not depend on test/ headers (the dependency points the other
// way). The path is never logged into the run trace — it differs per run
// and would break the same-seed-identical-trace property.

#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>

namespace rsm::sim {

class SimTempDir {
public:
    SimTempDir() {
        std::string tmpl =
            (std::filesystem::temp_directory_path() / "raftrsm-chaos-XXXXXX")
                .string();
        if (::mkdtemp(tmpl.data()) == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        path_ = tmpl;
    }
    ~SimTempDir() {
        std::error_code ec;  // best effort; never throw from a destructor
        std::filesystem::remove_all(path_, ec);
    }
    SimTempDir(const SimTempDir&) = delete;
    SimTempDir& operator=(const SimTempDir&) = delete;

    std::string subdir(const std::string& name) const {
        const auto p = std::filesystem::path(path_) / name;
        std::filesystem::create_directories(p);
        return p.string();
    }

private:
    std::string path_;
};

}  // namespace rsm::sim
