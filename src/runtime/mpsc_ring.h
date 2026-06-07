#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace rsm::runtime {

// Bounded lock-free multi-producer/single-consumer ring (Phase 7).
//
// Design (decision point 1, DESIGN.md): Vyukov's bounded array MPMC queue
// restricted to one consumer. Each cell carries a sequence number that
// encodes its state relative to the monotonically increasing enqueue /
// dequeue positions:
//   seq == pos          : cell is free for the producer claiming `pos`
//   seq == pos + 1      : cell holds the value for the consumer at `pos`
//   seq <  pos          : previous lap's value not yet consumed (full)
// Producers claim a position with a CAS on enqueuePos_, then write the
// value and publish it by bumping the cell's seq — so a slow producer only
// delays ITS cell, not a shared "ready" index for the whole ring.
//
// Memory ordering:
//  - cell.seq.load(acquire) on both sides pairs with the opposite side's
//    seq.store(release): the producer's value write happens-before the
//    consumer reading it (publish), and the consumer's move-out
//    happens-before a lapping producer's overwrite (recycle).
//  - The CAS on enqueuePos_ is relaxed: it only arbitrates WHICH producer
//    owns a position; all data visibility flows through the cell's seq
//    (this is Vyukov's original choice, for exactly this reason).
//  - dequeuePos_ is plain non-atomic state: single consumer by contract.
//
// Single consumer is load-bearing for dequeuePos_; multiple consumers would
// need the full Vyukov MPMC. No allocation after construction; capacity is
// rounded up to a power of two. T must be default-constructible + movable.
template <typename T>
class MpscRing {
public:
    explicit MpscRing(std::size_t minCapacity)
        : mask_(std::bit_ceil(minCapacity < 2 ? 2 : minCapacity) - 1),
          cells_(mask_ + 1) {
        for (std::uint64_t i = 0; i <= mask_; ++i) {
            // Initial state: cell i is free for position i (first lap).
            cells_[i].seq.store(i, std::memory_order_relaxed);
        }
    }

    MpscRing(const MpscRing&) = delete;
    MpscRing& operator=(const MpscRing&) = delete;

    std::size_t capacity() const { return mask_ + 1; }

    // Pre-start slot initialization (NOT thread-safe): visits every slot so
    // buffers/pools can be reserved up front — spec §4.7 preallocation.
    template <typename F>
    void initSlots(F&& init) {
        for (auto& c : cells_) init(c.value);
    }

    // Any thread. False if full. Never blocks, never allocates.
    bool tryPush(T&& v) {
        std::uint64_t pos = enqueuePos_.load(std::memory_order_relaxed);
        for (;;) {
            Cell& cell = cells_[pos & mask_];
            // Acquire pairs with the consumer's recycle store so that, on a
            // lap, the consumer's move-out happens-before our overwrite.
            const std::uint64_t seq = cell.seq.load(std::memory_order_acquire);
            const auto dif = static_cast<std::int64_t>(seq) -
                             static_cast<std::int64_t>(pos);
            if (dif == 0) {
                // Cell free for this position: try to claim it. Relaxed is
                // enough — the claim only picks a winner among producers;
                // publication is the seq store below.
                if (enqueuePos_.compare_exchange_weak(
                        pos, pos + 1, std::memory_order_relaxed)) {
                    cell.value = std::move(v);
                    // Release publishes cell.value to the consumer's
                    // seq.load(acquire).
                    cell.seq.store(pos + 1, std::memory_order_release);
                    return true;
                }
                // CAS failure updated pos to the current value; retry there.
            } else if (dif < 0) {
                // The cell one lap behind has not been recycled: the ring
                // is full at this instant.
                return false;
            } else {
                // Another producer claimed this position; chase the tail.
                pos = enqueuePos_.load(std::memory_order_relaxed);
            }
        }
    }

    // In-place variants (Phase 7 allocation-free steady state): slots are
    // written/read where they live, so members with capacity act as pooled
    // buffers across laps. Orderings identical to tryPush/tryPop; the fill
    // runs between the position claim (CAS) and the publishing seq store,
    // exactly where tryPush writes the value.
    template <typename Fill>
    bool tryProduce(Fill&& fill) {
        std::uint64_t pos = enqueuePos_.load(std::memory_order_relaxed);
        for (;;) {
            Cell& cell = cells_[pos & mask_];
            const std::uint64_t seq =
                cell.seq.load(std::memory_order_acquire);
            const auto dif = static_cast<std::int64_t>(seq) -
                             static_cast<std::int64_t>(pos);
            if (dif == 0) {
                if (enqueuePos_.compare_exchange_weak(
                        pos, pos + 1, std::memory_order_relaxed)) {
                    fill(cell.value);
                    cell.seq.store(pos + 1, std::memory_order_release);
                    return true;
                }
            } else if (dif < 0) {
                return false;
            } else {
                pos = enqueuePos_.load(std::memory_order_relaxed);
            }
        }
    }

    template <typename Use>
    bool tryConsume(Use&& use) {
        Cell& cell = cells_[dequeuePos_ & mask_];
        const std::uint64_t seq = cell.seq.load(std::memory_order_acquire);
        if (seq != dequeuePos_ + 1) return false;
        use(cell.value);
        cell.seq.store(dequeuePos_ + mask_ + 1, std::memory_order_release);
        ++dequeuePos_;
        return true;
    }

    // Consumer thread ONLY: true iff tryPop would succeed right now. Used
    // as the wake-gate predicate; same acquire pairing as tryPop.
    bool consumerHasWork() const {
        const Cell& cell = cells_[dequeuePos_ & mask_];
        return cell.seq.load(std::memory_order_acquire) == dequeuePos_ + 1;
    }

    // Consumer thread ONLY. False if empty (or if the producer that claimed
    // the next position has not finished publishing yet — bounded wait, the
    // caller's poll loop simply retries).
    bool tryPop(T& out) {
        Cell& cell = cells_[dequeuePos_ & mask_];
        // Acquire pairs with the producer's publish store: makes cell.value
        // fully visible before we move out of it.
        const std::uint64_t seq = cell.seq.load(std::memory_order_acquire);
        if (seq != dequeuePos_ + 1) return false;  // empty / not yet published
        out = std::move(cell.value);
        // Release recycles the cell for the producer one lap ahead
        // (position dequeuePos_ + capacity), ordering our move-out before
        // its overwrite.
        cell.seq.store(dequeuePos_ + mask_ + 1, std::memory_order_release);
        ++dequeuePos_;
        return true;
    }

private:
    static constexpr std::size_t kCacheLine = 64;

    struct Cell {
        std::atomic<std::uint64_t> seq{0};
        T value{};
    };

    const std::uint64_t mask_;
    std::vector<Cell> cells_;

    // Producers' contended line, padded away from the cells and from the
    // consumer's state (false-sharing).
    alignas(kCacheLine) std::atomic<std::uint64_t> enqueuePos_{0};

    // Consumer-owned line.
    alignas(kCacheLine) std::uint64_t dequeuePos_ = 0;

    alignas(kCacheLine) std::byte pad_[kCacheLine]{};
};

}  // namespace rsm::runtime
