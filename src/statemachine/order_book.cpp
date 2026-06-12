#include "statemachine/order_book.h"

#include "metrics/alloc_gate.h"
#include "rpc/wire.h"

namespace rsm::statemachine {

namespace {

void putU64(std::vector<std::uint8_t>& out, std::uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
    }
}

void putU32(std::vector<std::uint8_t>& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) {
        out.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
    }
}

// Result strings carry little-endian integers after the status byte.
void putU64s(std::string& out, std::uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<char>(
            static_cast<std::uint8_t>(v >> (8 * i))));
    }
}

void putU32s(std::string& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) {
        out.push_back(static_cast<char>(
            static_cast<std::uint8_t>(v >> (8 * i))));
    }
}

void identityPrefix(Command& out, std::uint64_t clientId,
                    std::uint64_t seqNo, ObOp op) {
    out.clear();
    putU64(out, clientId);
    putU64(out, seqNo);
    out.push_back(static_cast<std::uint8_t>(op));
}

}  // namespace

Command encodeObNew(std::uint64_t clientId, std::uint64_t seqNo, ObSide side,
                    std::uint64_t price, std::uint64_t qty) {
    Command cmd;
    encodeObNewInto(cmd, clientId, seqNo, side, price, qty);
    return cmd;
}

Command encodeObCancel(std::uint64_t clientId, std::uint64_t seqNo,
                       std::uint64_t orderId) {
    Command cmd;
    encodeObCancelInto(cmd, clientId, seqNo, orderId);
    return cmd;
}

Command encodeObAmend(std::uint64_t clientId, std::uint64_t seqNo,
                      std::uint64_t orderId, std::uint64_t newPrice,
                      std::uint64_t newQty) {
    Command cmd;
    encodeObAmendInto(cmd, clientId, seqNo, orderId, newPrice, newQty);
    return cmd;
}

void encodeObNewInto(Command& out, std::uint64_t clientId, std::uint64_t seqNo,
                     ObSide side, std::uint64_t price, std::uint64_t qty) {
    identityPrefix(out, clientId, seqNo, ObOp::New);
    out.push_back(static_cast<std::uint8_t>(side));
    putU64(out, price);
    putU64(out, qty);
}

void encodeObCancelInto(Command& out, std::uint64_t clientId,
                        std::uint64_t seqNo, std::uint64_t orderId) {
    identityPrefix(out, clientId, seqNo, ObOp::Cancel);
    putU64(out, orderId);
}

void encodeObAmendInto(Command& out, std::uint64_t clientId,
                       std::uint64_t seqNo, std::uint64_t orderId,
                       std::uint64_t newPrice, std::uint64_t newQty) {
    identityPrefix(out, clientId, seqNo, ObOp::Amend);
    putU64(out, orderId);
    putU64(out, newPrice);
    putU64(out, newQty);
}

std::optional<ObResult> decodeObResult(const std::string& result) {
    if (result.empty()) return std::nullopt;
    ObResult r;
    r.status = result.front();
    if (r.status == kObReject || r.status == kObMalformed) {
        if (result.size() != 1) return std::nullopt;
        return r;
    }
    if (r.status != kObOk) return std::nullopt;
    rsm::rpc::Reader rd(std::span<const std::uint8_t>(
        reinterpret_cast<const std::uint8_t*>(result.data()) + 1,
        result.size() - 1));
    r.orderId = rd.u64();
    r.restingQty = rd.u64();
    const std::uint32_t n = rd.u32();
    if (!rd.ok() || n > rd.remaining() / 24) return std::nullopt;
    r.fills.resize(n);
    for (auto& f : r.fills) {
        f.makerOrderId = rd.u64();
        f.price = rd.u64();
        f.qty = rd.u64();
    }
    if (!rd.ok() || !rd.exhausted()) return std::nullopt;
    return r;
}

std::uint64_t OrderBookStateMachine::match(ObSide taker, std::uint64_t limit,
                                           std::uint64_t qty,
                                           std::vector<ObFill>& fills) {
    Book& opposite = taker == ObSide::Bid ? asks_ : bids_;
    while (qty > 0 && !opposite.empty()) {
        // Best opposite level: lowest ask for a buyer, highest bid for a
        // seller. Trades execute at the MAKER's (resting) price.
        const auto levelIt = taker == ObSide::Bid
                                 ? opposite.begin()
                                 : std::prev(opposite.end());
        const std::uint64_t price = levelIt->first;
        const bool crosses =
            taker == ObSide::Bid ? price <= limit : price >= limit;
        if (!crosses) break;
        Level& level = levelIt->second;
        Resting& maker = level.front();  // FIFO: oldest by apply order
        const std::uint64_t fillQty = qty < maker.qty ? qty : maker.qty;
        fills.push_back(ObFill{maker.id, price, fillQty});
        qty -= fillQty;
        maker.qty -= fillQty;
        if (maker.qty == 0) {
            index_.erase(maker.id);
            level.pop_front();
            if (level.empty()) opposite.erase(levelIt);
        }
    }
    return qty;
}

void OrderBookStateMachine::rest(ObSide side, std::uint64_t price,
                                 std::uint64_t id, std::uint64_t qty) {
    Book& book = side == ObSide::Bid ? bids_ : asks_;
    book[price].push_back(Resting{id, qty});
    index_[id] = {side, price};
}

std::uint64_t OrderBookStateMachine::remove(std::uint64_t id) {
    const auto [side, price] = index_.at(id);
    Book& book = side == ObSide::Bid ? bids_ : asks_;
    const auto levelIt = book.find(price);
    Level& level = levelIt->second;
    std::uint64_t qty = 0;
    for (auto it = level.begin(); it != level.end(); ++it) {
        if (it->id == id) {
            qty = it->qty;
            level.erase(it);
            break;
        }
    }
    if (level.empty()) book.erase(levelIt);
    index_.erase(id);
    return qty;
}

std::string OrderBookStateMachine::apply(const Command& cmd) {
    // Book/session/result allocations are application state —
    // data-proportional, exempt from the plumbing-must-be-zero rule
    // (same accounting as KVStateMachine::apply).
    const rsm::metrics::AllocRetention allocTag;
    rsm::rpc::Reader r(cmd);
    const std::uint64_t clientId = r.u64();
    const std::uint64_t seqNo = r.u64();
    const std::uint8_t opByte = r.u8();
    if (!r.ok() || opByte < 1 || opByte > 3) return {kObMalformed};
    const auto op = static_cast<ObOp>(opByte);

    // Exactly-once: identical to the KV store — duplicates return the
    // cached result with no side effect, on every replica. Re-matching a
    // retried NEW would be a double execution, the matching-engine failure
    // mode the session table exists to prevent.
    if (clientId != 0) {
        const auto it = sessions_.find(clientId);
        if (it != sessions_.end() && seqNo <= it->second.lastSeq) {
            return it->second.lastResult;
        }
    }

    std::string result;
    switch (op) {
        case ObOp::New: {
            const std::uint8_t side = r.u8();
            const std::uint64_t price = r.u64();
            const std::uint64_t qty = r.u64();
            if (!r.ok() || !r.exhausted() || side > 1 || price == 0 ||
                qty == 0) {
                return {kObMalformed};
            }
            const std::uint64_t orderId = nextOrderId_++;
            std::vector<ObFill> fills;
            const std::uint64_t residual =
                match(static_cast<ObSide>(side), price, qty, fills);
            if (residual > 0) {
                rest(static_cast<ObSide>(side), price, orderId, residual);
            }
            result.push_back(kObOk);
            putU64s(result, orderId);
            putU64s(result, residual);
            putU32s(result, static_cast<std::uint32_t>(fills.size()));
            for (const auto& f : fills) {
                putU64s(result, f.makerOrderId);
                putU64s(result, f.price);
                putU64s(result, f.qty);
            }
            break;
        }
        case ObOp::Cancel: {
            const std::uint64_t orderId = r.u64();
            if (!r.ok() || !r.exhausted()) return {kObMalformed};
            if (!index_.contains(orderId)) {
                result.push_back(kObReject);
                break;
            }
            const std::uint64_t removed = remove(orderId);
            result.push_back(kObOk);
            putU64s(result, orderId);
            putU64s(result, removed);
            putU32s(result, 0);
            break;
        }
        case ObOp::Amend: {
            const std::uint64_t orderId = r.u64();
            const std::uint64_t newPrice = r.u64();
            const std::uint64_t newQty = r.u64();
            if (!r.ok() || !r.exhausted() || newPrice == 0 || newQty == 0) {
                return {kObMalformed};
            }
            const auto it = index_.find(orderId);
            if (it == index_.end()) {
                result.push_back(kObReject);
                break;
            }
            const auto [side, price] = it->second;
            std::vector<ObFill> fills;
            std::uint64_t residual;
            if (newPrice == price) {
                // Same price: a decrease (or no change) keeps time
                // priority, amended in place. An increase falls through to
                // cancel-replace below.
                Book& book = side == ObSide::Bid ? bids_ : asks_;
                Level& level = book.at(price);
                auto pos = level.begin();
                while (pos->id != orderId) ++pos;
                if (newQty <= pos->qty) {
                    pos->qty = newQty;
                    residual = newQty;
                    result.push_back(kObOk);
                    putU64s(result, orderId);
                    putU64s(result, residual);
                    putU32s(result, 0);
                    break;
                }
            }
            // Price change or quantity increase: loses time priority —
            // cancel-replace with the SAME order id, re-matched like a new
            // order (a price change can cross).
            remove(orderId);
            residual = match(side, newPrice, newQty, fills);
            if (residual > 0) rest(side, newPrice, orderId, residual);
            result.push_back(kObOk);
            putU64s(result, orderId);
            putU64s(result, residual);
            putU32s(result, static_cast<std::uint32_t>(fills.size()));
            for (const auto& f : fills) {
                putU64s(result, f.makerOrderId);
                putU64s(result, f.price);
                putU64s(result, f.qty);
            }
            break;
        }
    }

    if (clientId != 0) {
        sessions_[clientId] = Session{seqNo, result};
    }
    return result;
}

std::vector<std::uint8_t> OrderBookStateMachine::serialize() const {
    std::vector<std::uint8_t> out;
    putU64(out, nextOrderId_);
    for (const Book* book : {&bids_, &asks_}) {
        putU64(out, book->size());
        for (const auto& [price, level] : *book) {  // std::map: ordered
            putU64(out, price);
            putU64(out, level.size());
            for (const auto& o : level) {
                putU64(out, o.id);
                putU64(out, o.qty);
            }
        }
    }
    putU64(out, sessions_.size());
    for (const auto& [clientId, session] : sessions_) {
        putU64(out, clientId);
        putU64(out, session.lastSeq);
        putU32(out, static_cast<std::uint32_t>(session.lastResult.size()));
        out.insert(out.end(), session.lastResult.begin(),
                   session.lastResult.end());
    }
    return out;
}

bool OrderBookStateMachine::deserialize(const std::vector<std::uint8_t>& bytes) {
    rsm::rpc::Reader r(bytes);
    const std::uint64_t nextId = r.u64();
    Book bids, asks;
    std::map<std::uint64_t, std::pair<ObSide, std::uint64_t>> index;
    for (Book* book : {&bids, &asks}) {
        const ObSide side = book == &bids ? ObSide::Bid : ObSide::Ask;
        const std::uint64_t nLevels = r.u64();
        if (!r.ok() || nLevels > r.remaining() / 16) return false;
        for (std::uint64_t i = 0; i < nLevels; ++i) {
            const std::uint64_t price = r.u64();
            const std::uint64_t nOrders = r.u64();
            if (!r.ok() || price == 0 || nOrders == 0 ||
                nOrders > r.remaining() / 16 || book->contains(price)) {
                return false;
            }
            Level& level = (*book)[price];
            for (std::uint64_t j = 0; j < nOrders; ++j) {
                const std::uint64_t id = r.u64();
                const std::uint64_t qty = r.u64();
                if (!r.ok() || qty == 0 || index.contains(id)) return false;
                level.push_back(Resting{id, qty});
                index[id] = {side, price};
            }
        }
    }
    std::map<std::uint64_t, Session> sessions;
    const std::uint64_t nSessions = r.u64();
    if (!r.ok() || nSessions > r.remaining() / 20) return false;
    for (std::uint64_t i = 0; i < nSessions; ++i) {
        const std::uint64_t clientId = r.u64();
        Session s;
        s.lastSeq = r.u64();
        const std::uint32_t len = r.u32();
        if (!r.ok() || len > r.remaining()) return false;
        std::vector<std::uint8_t> raw;
        if (!r.readBytes(raw, len)) return false;
        s.lastResult.assign(raw.begin(), raw.end());
        sessions[clientId] = std::move(s);
    }
    if (!r.exhausted()) return false;
    bids_ = std::move(bids);
    asks_ = std::move(asks);
    index_ = std::move(index);
    nextOrderId_ = nextId;
    sessions_ = std::move(sessions);
    return true;
}

std::string OrderBookStateMachine::bookImage() const {
    std::string out;
    const auto dump = [&out](const char* tag, const Level& level,
                             std::uint64_t price) {
        out += tag;
        out += ' ' + std::to_string(price) + ':';
        for (const auto& o : level) {
            out += " (" + std::to_string(o.id) + ',' +
                   std::to_string(o.qty) + ')';
        }
        out += '\n';
    };
    for (auto it = bids_.rbegin(); it != bids_.rend(); ++it) {
        dump("B", it->second, it->first);  // best (highest) bid first
    }
    for (const auto& [price, level] : asks_) {
        dump("A", level, price);  // best (lowest) ask first
    }
    return out;
}

std::optional<OrderBookStateMachine::RestingOrder>
OrderBookStateMachine::order(std::uint64_t orderId) const {
    const auto it = index_.find(orderId);
    if (it == index_.end()) return std::nullopt;
    const auto [side, price] = it->second;
    const Book& book = side == ObSide::Bid ? bids_ : asks_;
    for (const auto& o : book.at(price)) {
        if (o.id == orderId) return RestingOrder{side, price, o.qty};
    }
    return std::nullopt;  // unreachable: index and levels stay in sync
}

std::optional<std::pair<std::uint64_t, std::uint64_t>>
OrderBookStateMachine::bestBid() const {
    if (bids_.empty()) return std::nullopt;
    const auto it = bids_.rbegin();
    std::uint64_t total = 0;
    for (const auto& o : it->second) total += o.qty;
    return std::make_pair(it->first, total);
}

std::optional<std::pair<std::uint64_t, std::uint64_t>>
OrderBookStateMachine::bestAsk() const {
    if (asks_.empty()) return std::nullopt;
    const auto it = asks_.begin();
    std::uint64_t total = 0;
    for (const auto& o : it->second) total += o.qty;
    return std::make_pair(it->first, total);
}

}  // namespace rsm::statemachine
