// Phase 9 matching-engine unit tests: price-time priority, partial/full
// fills, CANCEL/AMEND semantics, integer-price exactness, dedup, and the
// serialize/deserialize round-trip. All commands here use clientId 0
// (sessionless) unless the test is about sessions, so dedup never hides a
// matching bug.

#include <cstdint>
#include <string>

#include "doctest/doctest.h"
#include "statemachine/order_book.h"

using rsm::statemachine::decodeObResult;
using rsm::statemachine::encodeObAmend;
using rsm::statemachine::encodeObCancel;
using rsm::statemachine::encodeObNew;
using rsm::statemachine::kObMalformed;
using rsm::statemachine::kObOk;
using rsm::statemachine::kObReject;
using rsm::statemachine::ObFill;
using rsm::statemachine::ObResult;
using rsm::statemachine::ObSide;
using rsm::statemachine::OrderBookStateMachine;

namespace {

// Sessionless helpers: every op is applied exactly as sent.
ObResult newOrder(OrderBookStateMachine& ob, ObSide side, std::uint64_t price,
                  std::uint64_t qty) {
    const auto r = decodeObResult(ob.apply(encodeObNew(0, 0, side, price, qty)));
    REQUIRE(r.has_value());
    return *r;
}

ObResult cancel(OrderBookStateMachine& ob, std::uint64_t id) {
    const auto r = decodeObResult(ob.apply(encodeObCancel(0, 0, id)));
    REQUIRE(r.has_value());
    return *r;
}

ObResult amend(OrderBookStateMachine& ob, std::uint64_t id,
               std::uint64_t price, std::uint64_t qty) {
    const auto r = decodeObResult(ob.apply(encodeObAmend(0, 0, id, price, qty)));
    REQUIRE(r.has_value());
    return *r;
}

}  // namespace

TEST_CASE("ob: a non-crossing order rests; ids are sequential from 1") {
    OrderBookStateMachine ob;
    const auto b = newOrder(ob, ObSide::Bid, 100, 5);
    CHECK(b.status == kObOk);
    CHECK(b.orderId == 1);
    CHECK(b.restingQty == 5);
    CHECK(b.fills.empty());
    const auto a = newOrder(ob, ObSide::Ask, 101, 7);
    CHECK(a.orderId == 2);
    CHECK(a.restingQty == 7);
    CHECK(ob.bestBid() == std::make_pair(std::uint64_t{100}, std::uint64_t{5}));
    CHECK(ob.bestAsk() == std::make_pair(std::uint64_t{101}, std::uint64_t{7}));
    CHECK(ob.restingCount() == 2);
}

TEST_CASE("ob: crossing order matches at the MAKER's price") {
    OrderBookStateMachine ob;
    newOrder(ob, ObSide::Ask, 100, 5);          // id 1 rests at 100
    const auto buy = newOrder(ob, ObSide::Bid, 105, 5);  // willing to pay 105
    CHECK(buy.restingQty == 0);
    REQUIRE(buy.fills.size() == 1);
    CHECK(buy.fills[0] == ObFill{1, 100, 5});   // executes at 100, not 105
    CHECK(ob.restingCount() == 0);
}

TEST_CASE("ob: price priority — best opposite price fills first") {
    OrderBookStateMachine ob;
    newOrder(ob, ObSide::Ask, 102, 5);  // id 1, worse
    newOrder(ob, ObSide::Ask, 101, 5);  // id 2, better
    const auto buy = newOrder(ob, ObSide::Bid, 102, 7);
    REQUIRE(buy.fills.size() == 2);
    CHECK(buy.fills[0] == ObFill{2, 101, 5});  // better price first
    CHECK(buy.fills[1] == ObFill{1, 102, 2});  // then the worse level
    CHECK(buy.restingQty == 0);
    // Seller side mirror: highest bid fills first.
    OrderBookStateMachine ob2;
    const auto b1 = newOrder(ob2, ObSide::Bid, 99, 4);
    const auto b2 = newOrder(ob2, ObSide::Bid, 100, 4);
    const auto sell = newOrder(ob2, ObSide::Ask, 99, 6);
    REQUIRE(sell.fills.size() == 2);
    CHECK(sell.fills[0] == ObFill{b2.orderId, 100, 4});
    CHECK(sell.fills[1] == ObFill{b1.orderId, 99, 2});
}

TEST_CASE("ob: time priority — FIFO by apply order within a price level") {
    OrderBookStateMachine ob;
    const auto first = newOrder(ob, ObSide::Ask, 100, 3);
    const auto second = newOrder(ob, ObSide::Ask, 100, 3);
    const auto buy = newOrder(ob, ObSide::Bid, 100, 4);
    REQUIRE(buy.fills.size() == 2);
    CHECK(buy.fills[0] == ObFill{first.orderId, 100, 3});   // oldest first
    CHECK(buy.fills[1] == ObFill{second.orderId, 100, 1});  // then next in line
    CHECK(ob.order(second.orderId)->qty == 2);  // partially filled, still queued
}

TEST_CASE("ob: partial fill rests the residual at the taker's limit") {
    OrderBookStateMachine ob;
    newOrder(ob, ObSide::Ask, 100, 3);  // id 1
    const auto buy = newOrder(ob, ObSide::Bid, 100, 10);
    CHECK(buy.fills.size() == 1);
    CHECK(buy.restingQty == 7);
    const auto resting = ob.order(buy.orderId);
    REQUIRE(resting.has_value());
    CHECK(resting->side == ObSide::Bid);
    CHECK(resting->price == 100);
    CHECK(resting->qty == 7);
}

TEST_CASE("ob: a taker sweeps multiple levels up to its limit, then rests") {
    OrderBookStateMachine ob;
    newOrder(ob, ObSide::Ask, 100, 2);  // id 1
    newOrder(ob, ObSide::Ask, 101, 2);  // id 2
    newOrder(ob, ObSide::Ask, 103, 2);  // id 3 — beyond the limit
    const auto buy = newOrder(ob, ObSide::Bid, 102, 10);
    REQUIRE(buy.fills.size() == 2);
    CHECK(buy.fills[0] == ObFill{1, 100, 2});
    CHECK(buy.fills[1] == ObFill{2, 101, 2});
    CHECK(buy.restingQty == 6);
    CHECK(ob.bestAsk()->first == 103);          // id 3 untouched
    CHECK(ob.bestBid() == std::make_pair(std::uint64_t{102}, std::uint64_t{6}));
}

TEST_CASE("ob: CANCEL removes a resting order; filled/unknown ids reject") {
    OrderBookStateMachine ob;
    const auto o = newOrder(ob, ObSide::Bid, 100, 5);
    const auto c = cancel(ob, o.orderId);
    CHECK(c.status == kObOk);
    CHECK(c.orderId == o.orderId);
    CHECK(c.restingQty == 5);  // qty removed
    CHECK(ob.restingCount() == 0);
    CHECK(cancel(ob, o.orderId).status == kObReject);  // already canceled
    CHECK(cancel(ob, 999).status == kObReject);        // never existed
    // Fully filled order: cancel rejects too.
    const auto a = newOrder(ob, ObSide::Ask, 100, 2);
    newOrder(ob, ObSide::Bid, 100, 2);
    CHECK(cancel(ob, a.orderId).status == kObReject);
}

TEST_CASE("ob: AMEND qty decrease keeps time priority") {
    OrderBookStateMachine ob;
    const auto first = newOrder(ob, ObSide::Ask, 100, 5);
    const auto second = newOrder(ob, ObSide::Ask, 100, 5);
    const auto am = amend(ob, first.orderId, 100, 2);  // decrease in place
    CHECK(am.status == kObOk);
    CHECK(am.restingQty == 2);
    CHECK(am.fills.empty());
    const auto buy = newOrder(ob, ObSide::Bid, 100, 3);
    REQUIRE(buy.fills.size() == 2);
    // first kept its place at the front of the queue...
    CHECK(buy.fills[0] == ObFill{first.orderId, 100, 2});
    CHECK(buy.fills[1] == ObFill{second.orderId, 100, 1});
}

TEST_CASE("ob: AMEND qty increase loses time priority") {
    OrderBookStateMachine ob;
    const auto first = newOrder(ob, ObSide::Ask, 100, 5);
    const auto second = newOrder(ob, ObSide::Ask, 100, 5);
    const auto am = amend(ob, first.orderId, 100, 8);  // increase: re-queue
    CHECK(am.status == kObOk);
    CHECK(am.restingQty == 8);
    const auto buy = newOrder(ob, ObSide::Bid, 100, 6);
    REQUIRE(buy.fills.size() == 2);
    // second now has priority; first went to the back of the level.
    CHECK(buy.fills[0] == ObFill{second.orderId, 100, 5});
    CHECK(buy.fills[1] == ObFill{first.orderId, 100, 1});
}

TEST_CASE("ob: AMEND price change loses priority and can cross") {
    OrderBookStateMachine ob;
    const auto bid = newOrder(ob, ObSide::Bid, 100, 4);   // id 1
    const auto ask = newOrder(ob, ObSide::Ask, 105, 6);   // id 2, no cross
    // Amend the ask down through the bid: cancel-replace, same id, matches.
    const auto am = amend(ob, ask.orderId, 100, 6);
    CHECK(am.status == kObOk);
    CHECK(am.orderId == ask.orderId);
    REQUIRE(am.fills.size() == 1);
    CHECK(am.fills[0] == ObFill{bid.orderId, 100, 4});
    CHECK(am.restingQty == 2);
    const auto resting = ob.order(ask.orderId);
    REQUIRE(resting.has_value());
    CHECK(resting->price == 100);
    CHECK(resting->qty == 2);
    // Amending an unknown id rejects.
    CHECK(amend(ob, 999, 100, 1).status == kObReject);
}

TEST_CASE("ob: integer prices are exact — adjacent ticks never cross") {
    OrderBookStateMachine ob;
    newOrder(ob, ObSide::Ask, 1'000'001, 1);
    const auto buy = newOrder(ob, ObSide::Bid, 1'000'000, 1);  // one tick below
    CHECK(buy.fills.empty());
    CHECK(ob.restingCount() == 2);
    const auto cross = newOrder(ob, ObSide::Bid, 1'000'001, 1);  // exact touch
    CHECK(cross.fills.size() == 1);
}

TEST_CASE("ob: malformed commands reject with no side effect") {
    OrderBookStateMachine ob;
    newOrder(ob, ObSide::Bid, 100, 5);
    const auto before = ob.serialize();
    // zero qty / zero price / bad side / bad op / truncation / trailing junk
    CHECK(ob.apply(encodeObNew(0, 0, ObSide::Bid, 100, 0)) ==
          std::string{kObMalformed});
    CHECK(ob.apply(encodeObNew(0, 0, ObSide::Bid, 0, 5)) ==
          std::string{kObMalformed});
    auto badSide = encodeObNew(0, 0, ObSide::Bid, 100, 5);
    badSide[17] = 2;
    CHECK(ob.apply(badSide) == std::string{kObMalformed});
    auto badOp = encodeObCancel(0, 0, 1);
    badOp[16] = 9;
    CHECK(ob.apply(badOp) == std::string{kObMalformed});
    auto truncated = encodeObNew(0, 0, ObSide::Bid, 100, 5);
    truncated.resize(truncated.size() - 1);
    CHECK(ob.apply(truncated) == std::string{kObMalformed});
    auto trailing = encodeObCancel(0, 0, 1);
    trailing.push_back(0);
    CHECK(ob.apply(trailing) == std::string{kObMalformed});
    CHECK(ob.apply({}) == std::string{kObMalformed});
    CHECK(ob.serialize() == before);  // nothing changed
}

TEST_CASE("ob dedup: a duplicate (clientId, seqNo) NEW creates exactly one "
          "order and returns the cached fills") {
    OrderBookStateMachine ob;
    // Resting liquidity a re-applied NEW would (wrongly) match again.
    ob.apply(encodeObNew(0, 0, ObSide::Ask, 100, 10));  // id 1
    const auto first =
        decodeObResult(ob.apply(encodeObNew(7, 1, ObSide::Bid, 100, 4)));
    REQUIRE(first.has_value());
    REQUIRE(first->fills.size() == 1);
    CHECK(first->fills[0].qty == 4);
    CHECK(ob.order(1)->qty == 6);
    // The retry: same identity, byte-identical command.
    const auto retry =
        decodeObResult(ob.apply(encodeObNew(7, 1, ObSide::Bid, 100, 4)));
    REQUIRE(retry.has_value());
    CHECK(retry->orderId == first->orderId);
    CHECK(retry->fills == first->fills);   // cached, identical
    CHECK(ob.order(1)->qty == 6);          // NOT filled twice
    CHECK(ob.restingCount() == 1);
    // An older seqNo also returns the cached (latest) result, no side effect.
    const auto stale =
        decodeObResult(ob.apply(encodeObNew(7, 1, ObSide::Bid, 100, 9)));
    CHECK(stale->orderId == first->orderId);
    CHECK(ob.order(1)->qty == 6);
}

TEST_CASE("ob: serialize/deserialize round-trip preserves the book, the id "
          "counter, and the dedup table") {
    OrderBookStateMachine ob;
    ob.apply(encodeObNew(1, 1, ObSide::Bid, 100, 5));
    ob.apply(encodeObNew(2, 1, ObSide::Ask, 102, 3));
    ob.apply(encodeObNew(1, 2, ObSide::Bid, 99, 2));
    const auto bytes = ob.serialize();

    OrderBookStateMachine copy;
    REQUIRE(copy.deserialize(bytes));
    CHECK(copy.serialize() == bytes);
    CHECK(copy.bookImage() == ob.bookImage());
    // The id counter continues, not restarts: identical next assignment.
    const auto a = decodeObResult(ob.apply(encodeObNew(0, 0, ObSide::Bid, 98, 1)));
    const auto b =
        decodeObResult(copy.apply(encodeObNew(0, 0, ObSide::Bid, 98, 1)));
    CHECK(a->orderId == b->orderId);
    // Dedup survives the round-trip (the snapshot contract).
    const auto cached =
        decodeObResult(copy.apply(encodeObNew(1, 2, ObSide::Bid, 99, 2)));
    CHECK(cached->orderId == 3);
    CHECK(copy.restingCount() == 3 + 1);  // no fourth copy of (1,2)

    // Malformed snapshots are rejected without touching state.
    OrderBookStateMachine untouched;
    auto bad = bytes;
    bad.pop_back();
    CHECK_FALSE(untouched.deserialize(bad));
    CHECK(untouched.restingCount() == 0);
}

TEST_CASE("ob: identical command sequence => byte-identical state and "
          "results (replica determinism, unit level)") {
    // A fixed mixed sequence with crossings, cancels, and amends.
    std::vector<rsm::statemachine::Command> seq;
    seq.push_back(encodeObNew(1, 1, ObSide::Bid, 100, 5));
    seq.push_back(encodeObNew(2, 1, ObSide::Ask, 101, 3));
    seq.push_back(encodeObNew(3, 1, ObSide::Bid, 101, 4));   // crosses
    seq.push_back(encodeObAmend(1, 2, 1, 102, 5));           // re-prices, crosses?
    seq.push_back(encodeObCancel(2, 2, 3));
    seq.push_back(encodeObNew(3, 2, ObSide::Ask, 99, 10));   // sweeps bids
    OrderBookStateMachine a, b;
    for (const auto& cmd : seq) {
        CHECK(a.apply(cmd) == b.apply(cmd));
    }
    CHECK(a.serialize() == b.serialize());
}
