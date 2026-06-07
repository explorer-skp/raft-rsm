#pragma once

#include <atomic>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <new>
#include <utility>
#include <vector>

namespace rsm::runtime {

// Bounded lock-free single-producer/single-consumer ring (Phase 7,
// implemented from scratch per spec §4.7).
//
// Design: classic Lamport ring with monotonically increasing 64-bit indices
// (slot = index & mask, so full/empty never needs a wasted slot) plus the
// standard cached-opposite-index optimization: the producer keeps a local
// copy of the consumer index and only reloads the shared atomic when the
// ring LOOKS full (and symmetrically for the consumer), so in steady state
// each side touches the other's cache line rarely instead of per operation.
//
// Memory ordering (the entire correctness argument):
//  - tail_ is written ONLY by the producer, head_ ONLY by the consumer.
//  - Producer publishes a slot with tail_.store(release): everything it
//    wrote into the slot happens-before a consumer that observes the new
//    tail via load(acquire). That release/acquire pair is what makes the
//    slot's contents visible — without it the consumer could read a
//    half-constructed T.
//  - Consumer retires a slot with head_.store(release): its reads (the
//    move-out) happen-before a producer that observes the new head via
//    load(acquire), so the producer can never overwrite a slot that is
//    still being read.
//  - Each side reads ITS OWN index with relaxed: it is the only writer of
//    that atomic, so there is nothing to synchronize with.
//  - seq_cst is never needed: there is no multi-variable invariant beyond
//    the two pairwise happens-before edges above.
//
// No allocation after construction; capacity is rounded up to a power of
// two. T must be default-constructible and movable. Not resizable.
template <typename T>
class SpscRing {
public:
    explicit SpscRing(std::size_t minCapacity)
        : mask_(std::bit_ceil(minCapacity < 2 ? 2 : minCapacity) - 1),
          slots_(mask_ + 1) {}

    SpscRing(const SpscRing&) = delete;
    SpscRing& operator=(const SpscRing&) = delete;

    std::size_t capacity() const { return mask_ + 1; }

    // Pre-start slot initialization (NOT thread-safe): visits every slot so
    // buffers can be reserve()d up front — spec §4.7 "preallocate buffers
    // and entry/message pools". Without this, each slot's first use after
    // start would pay a one-time allocation as the indices walk the ring.
    template <typename F>
    void initSlots(F&& init) {
        for (auto& s : slots_) init(s);
    }

    // Producer side. False if full (caller decides: spin, drop, or fall
    // back). Never blocks, never allocates.
    bool tryPush(T&& v) {
        // Own index: relaxed (single writer is this thread).
        const std::uint64_t tail = tail_.load(std::memory_order_relaxed);
        if (tail - cachedHead_ > mask_) {
            // Looks full: refresh the cached consumer index. Acquire pairs
            // with the consumer's head_.store(release), ordering its
            // move-out of the slot before our overwrite below.
            cachedHead_ = head_.load(std::memory_order_acquire);
            if (tail - cachedHead_ > mask_) return false;  // really full
        }
        slots_[tail & mask_] = std::move(v);
        // Release publishes the slot write above to the consumer's
        // tail_.load(acquire).
        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

    // In-place variants (Phase 7 allocation-free steady state): the slot
    // objects live for the ring's lifetime and are written/read IN PLACE,
    // so members with capacity (vectors, strings) keep their high-water
    // storage across laps — the ring doubles as the buffer pool, and steady
    // state allocates nothing. Same memory-ordering pairs as push/pop.
    template <typename Fill>
    bool tryProduce(Fill&& fill) {
        const std::uint64_t tail = tail_.load(std::memory_order_relaxed);
        if (tail - cachedHead_ > mask_) {
            cachedHead_ = head_.load(std::memory_order_acquire);
            if (tail - cachedHead_ > mask_) return false;
        }
        fill(slots_[tail & mask_]);
        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

    template <typename Use>
    bool tryConsume(Use&& use) {
        const std::uint64_t head = head_.load(std::memory_order_relaxed);
        if (head == cachedTail_) {
            cachedTail_ = tail_.load(std::memory_order_acquire);
            if (head == cachedTail_) return false;
        }
        use(slots_[head & mask_]);
        head_.store(head + 1, std::memory_order_release);
        return true;
    }

    // Consumer side. False if empty.
    bool tryPop(T& out) {
        const std::uint64_t head = head_.load(std::memory_order_relaxed);
        if (head == cachedTail_) {
            // Looks empty: refresh. Acquire pairs with the producer's
            // tail_.store(release), making the slot contents visible.
            cachedTail_ = tail_.load(std::memory_order_acquire);
            if (head == cachedTail_) return false;  // really empty
        }
        out = std::move(slots_[head & mask_]);
        // Release hands the slot back: our move-out is ordered before the
        // producer's overwrite (it acquires head_ when it looks full).
        head_.store(head + 1, std::memory_order_release);
        return true;
    }

    // Consumer thread ONLY: true iff tryPop would succeed right now. Used
    // as the wake-gate predicate; same acquire pairing as tryPop.
    bool consumerHasWork() {
        const std::uint64_t head = head_.load(std::memory_order_relaxed);
        if (head != cachedTail_) return true;
        cachedTail_ = tail_.load(std::memory_order_acquire);
        return head != cachedTail_;
    }

    // Approximate (racy by nature); exact only when called by the consumer
    // for the lower bound or the producer for the upper.
    std::size_t sizeApprox() const {
        return static_cast<std::size_t>(
            tail_.load(std::memory_order_acquire) -
            head_.load(std::memory_order_acquire));
    }

private:
    static constexpr std::size_t kCacheLine = 64;

    const std::uint64_t mask_;
    std::vector<T> slots_;

    // Producer-owned line: its index plus its cache of the consumer's.
    // alignas keeps each hot index on its own cache line so the producer's
    // stores never invalidate the consumer's line (false sharing).
    alignas(kCacheLine) std::atomic<std::uint64_t> tail_{0};
    std::uint64_t cachedHead_ = 0;  // producer-local, same line as tail_ is fine

    // Consumer-owned line.
    alignas(kCacheLine) std::atomic<std::uint64_t> head_{0};
    std::uint64_t cachedTail_ = 0;

    // Pad the tail so head_'s line is not shared with whatever the user
    // places after the ring object.
    alignas(kCacheLine) std::byte pad_[kCacheLine]{};
};

}  // namespace rsm::runtime
