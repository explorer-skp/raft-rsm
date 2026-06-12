#pragma once

#include <cstdint>
#include <deque>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "statemachine/state_machine.h"

namespace rsm::statemachine {

// ---------------------------------------------------------------------------
// Order-book / matching-engine state machine (Phase 9; documented in
// DESIGN.md). Same command envelope as the KV store: the first 16 bytes are
// the (clientId, seqNo) session identity the dedup layer keys on, so the
// matching engine sits behind the exact same ClientService/exactly-once
// machinery — only the bytes after the identity differ.
//
//   [0]  u64 clientId   (0 = no session: no dedup, nothing recorded)
//   [8]  u64 seqNo      (monotonic per client, starting at 1)
//   [16] u8  op         (1=NEW 2=CANCEL 3=AMEND)
//   NEW    : u8 side (0=bid/buy, 1=ask/sell), u64 price, u64 qty
//   CANCEL : u64 orderId
//   AMEND  : u64 orderId, u64 newPrice, u64 newQty
//
// Prices are integer ticks, quantities integer lots — never floating point:
// FP matching can diverge across machines and silently break State Machine
// Safety. price >= 1 and qty >= 1; a zero is malformed. Trailing bytes or
// truncation make the command malformed ('E', no side effect, never cached —
// the KV convention).
//
// The result (apply()'s return, relayed verbatim in ClientReply.result):
//   'O' u64 orderId, u64 restingQty, u32 nFills,
//       nFills x { u64 makerOrderId, u64 price, u64 qty }
//        NEW/AMEND: the fills produced and the residual resting quantity
//        (0 = fully filled or nothing rested); CANCEL: restingQty = the
//        quantity removed, nFills = 0.
//   'N'  reject: CANCEL/AMEND of an unknown (never existed, fully filled,
//        or already canceled) order id. Deterministic, cached like any
//        result.
//   'E'  malformed command (no side effect, never cached)
//
// Determinism (the headline correctness property):
//   - "time" priority IS apply order: within a price level orders queue
//     FIFO in the order apply() saw them — i.e. committed-log order, which
//     Raft already agrees on. No wall clock anywhere.
//   - order ids come from a counter in applied state (nextOrderId_),
//     advanced per accepted NEW: identical on every replica by State
//     Machine Safety.
//   - ordered std::map levels + std::deque FIFOs; no RNG, no
//     unordered-container iteration anywhere.
// ---------------------------------------------------------------------------

enum class ObOp : std::uint8_t {
    New = 1,
    Cancel = 2,
    Amend = 3,
};

enum class ObSide : std::uint8_t {
    Bid = 0,  // buy
    Ask = 1,  // sell
};

inline constexpr char kObOk = 'O';
inline constexpr char kObReject = 'N';
inline constexpr char kObMalformed = 'E';

// Command builders (allocation-reusing Into variants for hot loops, like
// encodeKvCommandInto).
Command encodeObNew(std::uint64_t clientId, std::uint64_t seqNo, ObSide side,
                    std::uint64_t price, std::uint64_t qty);
Command encodeObCancel(std::uint64_t clientId, std::uint64_t seqNo,
                       std::uint64_t orderId);
Command encodeObAmend(std::uint64_t clientId, std::uint64_t seqNo,
                      std::uint64_t orderId, std::uint64_t newPrice,
                      std::uint64_t newQty);
void encodeObNewInto(Command& out, std::uint64_t clientId, std::uint64_t seqNo,
                     ObSide side, std::uint64_t price, std::uint64_t qty);
void encodeObCancelInto(Command& out, std::uint64_t clientId,
                        std::uint64_t seqNo, std::uint64_t orderId);
void encodeObAmendInto(Command& out, std::uint64_t clientId,
                       std::uint64_t seqNo, std::uint64_t orderId,
                       std::uint64_t newPrice, std::uint64_t newQty);

// Decoded result, for tests/clients (the wire form is the string above).
struct ObFill {
    std::uint64_t makerOrderId = 0;
    std::uint64_t price = 0;  // the MAKER's (resting order's) price
    std::uint64_t qty = 0;

    bool operator==(const ObFill&) const = default;
};
struct ObResult {
    char status = 0;
    std::uint64_t orderId = 0;
    std::uint64_t restingQty = 0;
    std::vector<ObFill> fills;
};
// nullopt iff the bytes are not a well-formed result string.
std::optional<ObResult> decodeObResult(const std::string& result);

class OrderBookStateMachine final : public StateMachine {
public:
    std::string apply(const Command& cmd) override;

    // Full applied state, deterministic byte-for-byte: the two book sides,
    // the order-id counter, and the session table (same contract as
    // KVStateMachine::serialize — cross-replica identity is asserted on
    // these bytes, and a snapshot must not lose dedup state).
    std::vector<std::uint8_t> serialize() const;
    bool deserialize(const std::vector<std::uint8_t>& bytes);

    // Canonical text dump of the book only (no sessions): bids best-first,
    // then asks best-first, each level "price: (id,qty) (id,qty)..." in
    // queue order. The golden-model tests compare on this.
    std::string bookImage() const;

    // Test/diagnostic accessors.
    struct RestingOrder {
        ObSide side{};
        std::uint64_t price = 0;
        std::uint64_t qty = 0;
    };
    std::optional<RestingOrder> order(std::uint64_t orderId) const;
    std::size_t restingCount() const { return index_.size(); }
    // (best price, total qty at that level)
    std::optional<std::pair<std::uint64_t, std::uint64_t>> bestBid() const;
    std::optional<std::pair<std::uint64_t, std::uint64_t>> bestAsk() const;
    std::size_t sessionCount() const { return sessions_.size(); }

private:
    struct Resting {
        std::uint64_t id = 0;
        std::uint64_t qty = 0;
    };
    using Level = std::deque<Resting>;   // FIFO by apply order
    using Book = std::map<std::uint64_t, Level>;  // price -> level
    struct Session {
        std::uint64_t lastSeq = 0;
        std::string lastResult;
    };

    // Matches `qty` of a taker at `limit` against the opposite book in
    // price-time priority, appending fills; returns the unmatched residual.
    std::uint64_t match(ObSide taker, std::uint64_t limit, std::uint64_t qty,
                        std::vector<ObFill>& fills);
    void rest(ObSide side, std::uint64_t price, std::uint64_t id,
              std::uint64_t qty);
    // Removes a resting order (must exist); returns its remaining qty.
    std::uint64_t remove(std::uint64_t id);

    Book bids_;
    Book asks_;
    // orderId -> (side, price): the cancel/amend locator.
    std::map<std::uint64_t, std::pair<ObSide, std::uint64_t>> index_;
    std::uint64_t nextOrderId_ = 1;
    std::map<std::uint64_t, Session> sessions_;
};

}  // namespace rsm::statemachine
