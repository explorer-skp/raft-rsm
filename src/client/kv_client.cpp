#include "client/kv_client.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <thread>

#include "transport/frame.h"

namespace rsm::client {

using rsm::rpc::ClientReply;
using rsm::rpc::ClientRequest;
using rsm::rpc::ClientStatus;
using rsm::rpc::Message;
using rsm::rpc::NodeId;
using rsm::statemachine::Command;
using rsm::statemachine::KvOp;

namespace {

constexpr int kBackoffMs = 25;  // between failed attempt cycles

}  // namespace

KvClient::KvClient(transport::PeerMap servers, std::uint64_t clientId,
                   NodeId clientNodeId, int perAttemptTimeoutMs,
                   int maxAttempts)
    : clientId_(clientId),
      clientNodeId_(clientNodeId),
      timeoutMs_(perAttemptTimeoutMs),
      maxAttempts_(maxAttempts) {
    for (auto& [id, addr] : servers) servers_.emplace_back(id, addr);
}

KvClient::~KvClient() {
    dropConnection();
}

void KvClient::dropConnection() {
    if (fd_ >= 0) ::close(fd_);
    fd_ = -1;
    connectedTo_ = 0;
}

std::optional<KvClient::Result> KvClient::put(const std::string& key,
                                              const std::string& value) {
    return call(rsm::statemachine::encodeKvCommand(clientId_, ++seqNo_,
                                                   KvOp::Put, key, value));
}

std::optional<KvClient::Result> KvClient::get(const std::string& key) {
    return call(rsm::statemachine::encodeKvCommand(clientId_, ++seqNo_,
                                                   KvOp::Get, key));
}

std::optional<KvClient::Result> KvClient::del(const std::string& key) {
    return call(rsm::statemachine::encodeKvCommand(clientId_, ++seqNo_,
                                                   KvOp::Delete, key));
}

std::optional<KvClient::Result> KvClient::cas(const std::string& key,
                                              const std::string& expected,
                                              const std::string& desired) {
    return call(rsm::statemachine::encodeKvCommand(
        clientId_, ++seqNo_, KvOp::Cas, key, expected, desired));
}

std::optional<KvClient::Result> KvClient::append(const std::string& key,
                                                 const std::string& suffix) {
    return call(rsm::statemachine::encodeKvCommand(clientId_, ++seqNo_,
                                                   KvOp::Append, key, suffix));
}

std::optional<KvClient::Result> KvClient::resendLast() {
    // Same bytes, same (clientId, seqNo): the dedup table makes this safe no
    // matter how many times it lands or on which leader.
    return call(lastCommand_);
}

std::optional<KvClient::Result> KvClient::call(const Command& command) {
    lastCommand_ = command;
    for (int tries = 0; tries < maxAttempts_; ++tries) {
        // Prefer the believed leader; otherwise rotate through the servers.
        std::size_t idx = rotation_ % servers_.size();
        if (preferred_ != 0) {
            for (std::size_t i = 0; i < servers_.size(); ++i) {
                if (servers_[i].first == preferred_) {
                    idx = i;
                    break;
                }
            }
        }
        const auto& [serverId, addr] = servers_[idx];
        const auto reply = attempt(serverId, addr, command);
        if (reply && reply->status == ClientStatus::Ok) {
            if (reply->result.empty()) return Result{};  // defensive
            Result r;
            r.status = static_cast<char>(reply->result.front());
            r.value.assign(reply->result.begin() + 1, reply->result.end());
            preferred_ = serverId;
            return r;
        }
        if (reply && reply->status == ClientStatus::NotLeader &&
            reply->leaderHint != 0 && reply->leaderHint != serverId) {
            preferred_ = reply->leaderHint;  // follow the redirect, no backoff
            continue;
        }
        // Unreachable, timed out, errored, or hint-less NOT_LEADER: forget
        // the preference, try the next server after a bounded backoff.
        preferred_ = 0;
        rotation_ = idx + 1;
        std::this_thread::sleep_for(std::chrono::milliseconds(kBackoffMs));
    }
    return std::nullopt;
}

std::optional<ClientReply> KvClient::attempt(NodeId serverId,
                                             const transport::PeerAddress& addr,
                                             const Command& command) {
    // Reuse the cached connection only if it points at this server and its
    // last exchange completed cleanly (any failure below drops it).
    if (fd_ >= 0 && connectedTo_ != serverId) dropConnection();
    if (fd_ < 0) {
        sockaddr_in sa{};
        sa.sin_family = AF_INET;
        sa.sin_port = htons(addr.port);
        if (::inet_pton(AF_INET, addr.host.c_str(), &sa.sin_addr) != 1) {
            return std::nullopt;
        }
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd_ < 0) return std::nullopt;
        if (::connect(fd_, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) <
            0) {
            dropConnection();
            return std::nullopt;
        }
        const int one = 1;
        ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        connectedTo_ = serverId;
    }

    const Message msg{ClientRequest{clientId_, seqNo_, command}};
    std::vector<std::uint8_t> frame(transport::kLengthPrefixSize +
                                    rsm::rpc::encodedSize(msg));
    const std::size_t bodyLen = rsm::rpc::encodeMessage(
        clientNodeId_, serverId, msg,
        std::span<std::uint8_t>(frame).subspan(transport::kLengthPrefixSize));
    if (bodyLen == 0) return std::nullopt;
    transport::writeLengthPrefix(static_cast<std::uint32_t>(bodyLen),
                                 frame.data());
    for (std::size_t sent = 0; sent < frame.size();) {
        const ssize_t n = ::send(fd_, frame.data() + sent,
                                 frame.size() - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            dropConnection();
            return std::nullopt;
        }
        sent += static_cast<std::size_t>(n);
    }

    // Await the reply on this connection until the attempt deadline.
    transport::FrameAssembler assembler;
    std::optional<ClientReply> result;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeoutMs_);
    while (!result) {
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now());
        if (left.count() <= 0) {
            dropConnection();  // a late reply must not leak into the next op
            return std::nullopt;
        }
        pollfd pfd{fd_, POLLIN, 0};
        const int ready = ::poll(&pfd, 1, static_cast<int>(left.count()));
        if (ready < 0 && errno == EINTR) continue;
        if (ready <= 0) {  // timeout or poll error
            dropConnection();
            return std::nullopt;
        }
        std::uint8_t buf[64 * 1024];
        const ssize_t n = ::read(fd_, buf, sizeof(buf));
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            dropConnection();
            return std::nullopt;  // server closed mid-reply
        }
        bool bad = false;
        const bool fed = assembler.feed(
            std::span<const std::uint8_t>(buf, static_cast<std::size_t>(n)),
            [&result, &bad](std::span<const std::uint8_t> body) {
                const auto decoded = rsm::rpc::decodeMessage(body);
                if (!decoded) {
                    bad = true;
                    return;
                }
                if (const auto* r =
                        std::get_if<ClientReply>(&decoded->message)) {
                    result = *r;
                }
            });
        if (!fed || bad) {
            dropConnection();
            return std::nullopt;
        }
    }
    return result;
}

}  // namespace rsm::client
