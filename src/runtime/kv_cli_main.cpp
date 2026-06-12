// Hand-poking KV client (Phase 5):
//   kv_cli --config peers.conf [--client-node N] <op> <args...>
//     put <key> <value> | get <key> | del <key>
//     cas <key> <expected> <new> | append <key> <suffix>
// Order-book ops (Phase 9; against a cluster started with --sm orderbook):
//     ob-new buy|sell <price> <qty> | ob-cancel <id> |
//     ob-amend <id> <price> <qty>
// Each invocation is one logical client (random clientId, seqNo 1): within
// a run the request is exactly-once across internal retries/failover; runs
// are independent clients. --client-node sets the envelope id used for
// reply routing (default 100; must not collide with cluster ids or other
// concurrently running clients).

#include <cstdio>
#include <cstring>
#include <random>
#include <string>

#include "client/kv_client.h"
#include "statemachine/kv_store.h"
#include "statemachine/order_book.h"
#include "transport/transport.h"

namespace {

int usage(const char* argv0) {
    std::fprintf(stderr,
                 "usage: %s --config <file> [--client-node N] "
                 "(put k v | get k | del k | cas k expected new | "
                 "append k suffix |\n"
                 "        ob-new buy|sell price qty | ob-cancel id | "
                 "ob-amend id price qty)\n",
                 argv0);
    return 2;
}

// ob-* results: print the order id, residual, and every fill.
int printObResult(const rsm::client::KvClient::Result& r) {
    const auto decoded =
        rsm::statemachine::decodeObResult(std::string(1, r.status) + r.value);
    if (!decoded) {
        std::fprintf(stderr, "error: undecodable order-book result\n");
        return 1;
    }
    if (decoded->status == rsm::statemachine::kObReject) {
        std::printf("REJECTED (unknown or no-longer-resting order)\n");
        return 0;
    }
    if (decoded->status != rsm::statemachine::kObOk) {
        std::fprintf(stderr, "error: malformed command result\n");
        return 1;
    }
    std::printf("OK order=%llu resting=%llu\n",
                static_cast<unsigned long long>(decoded->orderId),
                static_cast<unsigned long long>(decoded->restingQty));
    for (const auto& f : decoded->fills) {
        std::printf("  fill maker=%llu price=%llu qty=%llu\n",
                    static_cast<unsigned long long>(f.makerOrderId),
                    static_cast<unsigned long long>(f.price),
                    static_cast<unsigned long long>(f.qty));
    }
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    std::string configPath;
    long clientNode = 100;
    int i = 1;
    for (; i + 1 < argc && argv[i][0] == '-'; i += 2) {
        if (std::strcmp(argv[i], "--config") == 0) {
            configPath = argv[i + 1];
        } else if (std::strcmp(argv[i], "--client-node") == 0) {
            clientNode = std::strtol(argv[i + 1], nullptr, 10);
        } else {
            return usage(argv[0]);
        }
    }
    if (configPath.empty() || clientNode < 1 || clientNode > 0xFFFF ||
        i >= argc) {
        return usage(argv[0]);
    }
    const std::string op = argv[i];
    const int args = argc - i - 1;

    try {
        const auto servers = rsm::transport::loadPeerConfig(configPath);
        std::random_device rd;
        const std::uint64_t clientId =
            (static_cast<std::uint64_t>(rd()) << 32) | rd();
        rsm::client::KvClient client(
            servers, clientId, static_cast<rsm::rpc::NodeId>(clientNode));

        std::optional<rsm::client::KvClient::Result> r;
        bool isOrderOp = false;
        const auto u64arg = [&](int k) {
            return std::strtoull(argv[i + k], nullptr, 10);
        };
        if (op == "ob-new" && args == 3) {
            const std::string side = argv[i + 1];
            if (side != "buy" && side != "sell") return usage(argv[0]);
            isOrderOp = true;
            r = client.obNew(side == "buy"
                                 ? rsm::statemachine::ObSide::Bid
                                 : rsm::statemachine::ObSide::Ask,
                             u64arg(2), u64arg(3));
        } else if (op == "ob-cancel" && args == 1) {
            isOrderOp = true;
            r = client.obCancel(u64arg(1));
        } else if (op == "ob-amend" && args == 3) {
            isOrderOp = true;
            r = client.obAmend(u64arg(1), u64arg(2), u64arg(3));
        } else if (op == "put" && args == 2) {
            r = client.put(argv[i + 1], argv[i + 2]);
        } else if (op == "get" && args == 1) {
            r = client.get(argv[i + 1]);
        } else if (op == "del" && args == 1) {
            r = client.del(argv[i + 1]);
        } else if (op == "cas" && args == 3) {
            r = client.cas(argv[i + 1], argv[i + 2], argv[i + 3]);
        } else if (op == "append" && args == 2) {
            r = client.append(argv[i + 1], argv[i + 2]);
        } else {
            return usage(argv[0]);
        }

        if (!r) {
            std::fprintf(stderr, "error: no reply from cluster\n");
            return 1;
        }
        if (isOrderOp) return printObResult(*r);
        switch (r->status) {
            case rsm::statemachine::kKvOk:
                std::printf("OK%s%s\n", r->value.empty() ? "" : " ",
                            r->value.c_str());
                return 0;
            case rsm::statemachine::kKvNotFound:
                std::printf("NOT_FOUND\n");
                return 0;
            case rsm::statemachine::kKvCasFailed:
                std::printf("CAS_FAILED\n");
                return 0;
            default:
                std::fprintf(stderr, "error: malformed command result\n");
                return 1;
        }
    } catch (const std::exception& e) {
        std::fprintf(stderr, "fatal: %s\n", e.what());
        return 1;
    }
}
