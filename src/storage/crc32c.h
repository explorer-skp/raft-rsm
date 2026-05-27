#pragma once

#include <cstddef>
#include <cstdint>

namespace rsm::storage {

// CRC32C (Castagnoli, reflected polynomial 0x82F63B78) over `len` bytes.
// Table-driven software implementation — hardware acceleration is a Phase 7
// concern if it ever shows up in a profile. `crc` chains partial computations
// (pass the previous return value); 0 starts a fresh checksum.
std::uint32_t crc32c(const std::uint8_t* data, std::size_t len,
                     std::uint32_t crc = 0);

}  // namespace rsm::storage
