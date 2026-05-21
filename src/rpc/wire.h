#pragma once

#include <cstdint>
#include <cstring>
#include <span>
#include <vector>

namespace rsm::rpc {

// Bounds-checked little-endian write cursor over a caller-provided buffer.
// On overflow, ok() turns false and all further writes are no-ops.
class Writer {
public:
    explicit Writer(std::span<std::uint8_t> out) : out_(out) {}

    void u8(std::uint8_t v) { put(&v, 1); }

    void u16(std::uint16_t v) {
        std::uint8_t b[2] = {static_cast<std::uint8_t>(v),
                             static_cast<std::uint8_t>(v >> 8)};
        put(b, 2);
    }

    void u32(std::uint32_t v) {
        std::uint8_t b[4];
        for (int i = 0; i < 4; ++i) b[i] = static_cast<std::uint8_t>(v >> (8 * i));
        put(b, 4);
    }

    void u64(std::uint64_t v) {
        std::uint8_t b[8];
        for (int i = 0; i < 8; ++i) b[i] = static_cast<std::uint8_t>(v >> (8 * i));
        put(b, 8);
    }

    void bytes(const std::uint8_t* p, std::size_t n) { put(p, n); }

    bool ok() const { return ok_; }
    std::size_t written() const { return pos_; }

private:
    void put(const std::uint8_t* p, std::size_t n) {
        if (!ok_ || n > out_.size() - pos_) {
            ok_ = false;
            return;
        }
        if (n > 0) std::memcpy(out_.data() + pos_, p, n);
        pos_ += n;
    }

    std::span<std::uint8_t> out_;
    std::size_t pos_ = 0;
    bool ok_ = true;
};

// Bounds-checked little-endian read cursor. Reading past the end turns ok()
// false and returns 0; callers check ok() once at the end of a struct.
class Reader {
public:
    explicit Reader(std::span<const std::uint8_t> in) : in_(in) {}

    std::uint8_t u8() {
        std::uint8_t b[1];
        if (!take(b, 1)) return 0;
        return b[0];
    }

    std::uint16_t u16() {
        std::uint8_t b[2];
        if (!take(b, 2)) return 0;
        return static_cast<std::uint16_t>(b[0] | (b[1] << 8));
    }

    std::uint32_t u32() {
        std::uint8_t b[4];
        if (!take(b, 4)) return 0;
        std::uint32_t v = 0;
        for (int i = 3; i >= 0; --i) v = (v << 8) | b[i];
        return v;
    }

    std::uint64_t u64() {
        std::uint8_t b[8];
        if (!take(b, 8)) return 0;
        std::uint64_t v = 0;
        for (int i = 7; i >= 0; --i) v = (v << 8) | b[i];
        return v;
    }

    // Reads exactly n bytes into dst; fails (ok() false) without allocating
    // if fewer than n bytes remain.
    bool readBytes(std::vector<std::uint8_t>& dst, std::size_t n) {
        if (!ok_ || n > remaining()) {
            ok_ = false;
            return false;
        }
        dst.assign(in_.begin() + static_cast<std::ptrdiff_t>(pos_),
                   in_.begin() + static_cast<std::ptrdiff_t>(pos_ + n));
        pos_ += n;
        return true;
    }

    std::size_t remaining() const { return in_.size() - pos_; }
    bool ok() const { return ok_; }
    bool exhausted() const { return ok_ && pos_ == in_.size(); }

private:
    bool take(std::uint8_t* dst, std::size_t n) {
        if (!ok_ || n > remaining()) {
            ok_ = false;
            return false;
        }
        std::memcpy(dst, in_.data() + pos_, n);
        pos_ += n;
        return true;
    }

    std::span<const std::uint8_t> in_;
    std::size_t pos_ = 0;
    bool ok_ = true;
};

}  // namespace rsm::rpc
