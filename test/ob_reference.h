#pragma once

// Independent golden-model reference matcher (Phase 9, decision point 4).
//
// Deliberately written NOTHING like the production engine: one flat vector
// of resting orders with explicit arrival stamps, linear best-order scans
// per fill, and its own byte parsing/encoding — no shared code beyond the
// command/result format documented in order_book.h. If the two
// implementations agree on every result and on the final book over seeded
// randomized streams, a shared structural bug is about as likely as the
// same bug written twice from opposite directions.
//
// The Bug enum makes the golden check self-validating (the Phase 6 checker
// discipline: a check that cannot fail proves nothing): each variant
// mis-implements one priority rule, and the equivalence test asserts the
// comparison FLAGS it.

#include <algorithm>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace obref {

class ReferenceMatcher {
public:
    enum class Bug {
        None,
        IgnoreTimePriority,   // newest order at a level fills first
        IgnorePricePriority,  // any crossing order fills, scan order
        TakerPrice,           // trades execute at the taker's limit price
    };

    explicit ReferenceMatcher(Bug bug = Bug::None) : bug_(bug) {}

    std::string apply(const std::vector<std::uint8_t>& cmd) {
        std::size_t pos = 0;
        std::uint64_t clientId, seqNo;
        std::uint8_t op;
        if (!u64(cmd, pos, clientId) || !u64(cmd, pos, seqNo) ||
            !u8(cmd, pos, op) || op < 1 || op > 3) {
            return "E";
        }
        if (clientId != 0) {
            const auto it = sessions_.find(clientId);
            if (it != sessions_.end() && seqNo <= it->second.first) {
                return it->second.second;
            }
        }
        std::string result;
        if (op == 1) {  // NEW
            std::uint8_t side;
            std::uint64_t price, qty;
            if (!u8(cmd, pos, side) || !u64(cmd, pos, price) ||
                !u64(cmd, pos, qty) || pos != cmd.size() || side > 1 ||
                price == 0 || qty == 0) {
                return "E";
            }
            const std::uint64_t id = nextId_++;
            result = execute(id, side, price, qty);
        } else if (op == 2) {  // CANCEL
            std::uint64_t id;
            if (!u64(cmd, pos, id) || pos != cmd.size()) return "E";
            const auto it = find(id);
            if (it == book_.end()) {
                result = "N";
            } else {
                result.push_back('O');
                putU64(result, id);
                putU64(result, it->qty);
                putU32(result, 0);
                book_.erase(it);
            }
        } else {  // AMEND
            std::uint64_t id, price, qty;
            if (!u64(cmd, pos, id) || !u64(cmd, pos, price) ||
                !u64(cmd, pos, qty) || pos != cmd.size() || price == 0 ||
                qty == 0) {
                return "E";
            }
            const auto it = find(id);
            if (it == book_.end()) {
                result = "N";
            } else if (price == it->price && qty <= it->qty) {
                it->qty = qty;  // keeps its arrival stamp (time priority)
                result.push_back('O');
                putU64(result, id);
                putU64(result, qty);
                putU32(result, 0);
            } else {
                const std::uint8_t side = it->side;
                book_.erase(it);
                result = execute(id, side, price, qty);
            }
        }
        if (clientId != 0) sessions_[clientId] = {seqNo, result};
        return result;
    }

    // Same canonical format as OrderBookStateMachine::bookImage().
    std::string bookImage() const {
        std::string out;
        for (int side = 0; side < 2; ++side) {
            std::vector<std::uint64_t> prices;
            for (const auto& o : book_) {
                if (o.side == side &&
                    std::find(prices.begin(), prices.end(), o.price) ==
                        prices.end()) {
                    prices.push_back(o.price);
                }
            }
            std::sort(prices.begin(), prices.end());
            if (side == 0) std::reverse(prices.begin(), prices.end());
            for (const std::uint64_t price : prices) {
                std::vector<Order> level;
                for (const auto& o : book_) {
                    if (o.side == side && o.price == price) {
                        level.push_back(o);
                    }
                }
                std::sort(level.begin(), level.end(),
                          [](const Order& a, const Order& b) {
                              return a.arrival < b.arrival;
                          });
                out += side == 0 ? 'B' : 'A';
                out += ' ' + std::to_string(price) + ':';
                for (const auto& o : level) {
                    out += " (" + std::to_string(o.id) + ',' +
                           std::to_string(o.qty) + ')';
                }
                out += '\n';
            }
        }
        return out;
    }

private:
    struct Order {
        std::uint64_t id = 0;
        std::uint8_t side = 0;  // 0 bid, 1 ask
        std::uint64_t price = 0;
        std::uint64_t qty = 0;
        std::uint64_t arrival = 0;
    };

    std::vector<Order>::iterator find(std::uint64_t id) {
        for (auto it = book_.begin(); it != book_.end(); ++it) {
            if (it->id == id) return it;
        }
        return book_.end();
    }

    // Match-then-rest of `qty` at `price` for taker `side`; emits the 'O'
    // result string.
    std::string execute(std::uint64_t id, std::uint8_t side,
                        std::uint64_t price, std::uint64_t qty) {
        std::string fills;
        std::uint32_t nFills = 0;
        while (qty > 0) {
            auto best = book_.end();
            for (auto it = book_.begin(); it != book_.end(); ++it) {
                if (it->side == side) continue;
                const bool crosses = side == 0 ? it->price <= price
                                               : it->price >= price;
                if (!crosses) continue;
                if (bug_ == Bug::IgnorePricePriority) {
                    best = it;  // any crossing order, scan order
                    break;
                }
                const auto better = [&](const Order& a, const Order& b) {
                    if (a.price != b.price) {
                        return side == 0 ? a.price < b.price
                                         : a.price > b.price;
                    }
                    return bug_ == Bug::IgnoreTimePriority
                               ? a.arrival > b.arrival
                               : a.arrival < b.arrival;
                };
                if (best == book_.end() || better(*it, *best)) best = it;
            }
            if (best == book_.end()) break;
            const std::uint64_t fillQty = std::min(qty, best->qty);
            putU64(fills, best->id);
            putU64(fills,
                   bug_ == Bug::TakerPrice ? price : best->price);
            putU64(fills, fillQty);
            ++nFills;
            qty -= fillQty;
            best->qty -= fillQty;
            if (best->qty == 0) book_.erase(best);
        }
        if (qty > 0) {
            book_.push_back(Order{id, side, price, qty, arrival_++});
        }
        std::string result;
        result.push_back('O');
        putU64(result, id);
        putU64(result, qty);
        putU32(result, nFills);
        result += fills;
        return result;
    }

    static bool u8(const std::vector<std::uint8_t>& b, std::size_t& pos,
                   std::uint8_t& out) {
        if (pos + 1 > b.size()) return false;
        out = b[pos++];
        return true;
    }
    static bool u64(const std::vector<std::uint8_t>& b, std::size_t& pos,
                    std::uint64_t& out) {
        if (pos + 8 > b.size()) return false;
        out = 0;
        for (int i = 7; i >= 0; --i) {
            out = (out << 8) | b[pos + static_cast<std::size_t>(i)];
        }
        pos += 8;
        return true;
    }
    static void putU64(std::string& out, std::uint64_t v) {
        for (int i = 0; i < 8; ++i) {
            out.push_back(static_cast<char>(
                static_cast<std::uint8_t>(v >> (8 * i))));
        }
    }
    static void putU32(std::string& out, std::uint32_t v) {
        for (int i = 0; i < 4; ++i) {
            out.push_back(static_cast<char>(
                static_cast<std::uint8_t>(v >> (8 * i))));
        }
    }

    std::vector<Order> book_;
    std::uint64_t nextId_ = 1;
    std::uint64_t arrival_ = 0;
    std::map<std::uint64_t, std::pair<std::uint64_t, std::string>> sessions_;
    Bug bug_;
};

// Seeded random command-stream generator shared by the golden tests (and
// the order-book chaos run's replay check uses real committed streams
// instead). Mix: mostly NEW in a band tight enough to cross constantly,
// plus CANCEL/AMEND of ids that may or may not rest, plus duplicate
// resends that exercise dedup in both implementations.
struct StreamGen {
    std::uint64_t state;
    std::uint64_t maxIdGuess = 1;
    std::vector<std::vector<std::uint8_t>> lastPerClient =
        std::vector<std::vector<std::uint8_t>>(5);
    std::vector<std::uint64_t> seqPerClient = std::vector<std::uint64_t>(5, 0);

    explicit StreamGen(std::uint64_t seed) : state(seed ? seed : 1) {}

    std::uint64_t next() {  // xorshift64
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        return state;
    }

    std::vector<std::uint8_t> command() {
        const std::uint64_t client = 1 + next() % 4;
        std::vector<std::uint8_t>& last = lastPerClient[client];
        if (!last.empty() && next() % 10 == 0) {
            return last;  // duplicate resend: the dedup path, both models
        }
        std::vector<std::uint8_t> cmd;
        const auto putU64v = [&cmd](std::uint64_t v) {
            for (int i = 0; i < 8; ++i) {
                cmd.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
            }
        };
        putU64v(client);
        putU64v(++seqPerClient[client]);
        const std::uint64_t pick = next() % 100;
        if (pick < 70) {  // NEW: tight band so streams cross constantly
            cmd.push_back(1);
            cmd.push_back(static_cast<std::uint8_t>(next() % 2));
            putU64v(90 + next() % 21);  // price in [90, 110]
            putU64v(1 + next() % 10);   // qty in [1, 10]
            ++maxIdGuess;
        } else if (pick < 85) {  // CANCEL a recent id (may be gone: reject)
            cmd.push_back(2);
            putU64v(1 + next() % maxIdGuess);
        } else {  // AMEND (may re-price across the book, may reject)
            cmd.push_back(3);
            putU64v(1 + next() % maxIdGuess);
            putU64v(90 + next() % 21);
            putU64v(1 + next() % 10);
        }
        last = cmd;
        return cmd;
    }
};

}  // namespace obref
