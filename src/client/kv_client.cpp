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
    rsm::statemachine::encodeKvCommandInto(cmdScratch_, clientId_, ++seqNo_,
                                           KvOp::Put, key, value);
    return call(cmdScratch_);
}

std::optional<KvClient::Result> KvClient::get(const std::string& key) {
    rsm::statemachine::encodeKvCommandInto(cmdScratch_, clientId_, ++seqNo_,
                                           KvOp::Get, key);
    return call(cmdScratch_);
}

std::optional<KvClient::Result> KvClient::del(const std::string& key) {
    rsm::statemachine::encodeKvCommandInto(cmdScratch_, clientId_, ++seqNo_,
                                           KvOp::Delete, key);
    return call(cmdScratch_);
}

std::optional<KvClient::Result> KvClient::cas(const std::string& key,
                                              const std::string& expected,
                                              const std::string& desired) {
    rsm::statemachine::encodeKvCommandInto(cmdScratch_, clientId_, ++seqNo_,
                                           KvOp::Cas, key, expected, desired);
    return call(cmdScratch_);
}

std::optional<KvClient::Result> KvClient::append(const std::string& key,
                                                 const std::string& suffix) {
    rsm::statemachine::encodeKvCommandInto(cmdScratch_, clientId_, ++seqNo_,
                                           KvOp::Append, key, suffix);
    return call(cmdScratch_);
}

std::optional<KvClient::Result> KvClient::resendLast() {
    // Same bytes, same (clientId, seqNo): the dedup table makes this safe no
    // matter how many times it lands or on which leader.
    return call(lastCommand_);
}

std::optional<KvClient::Result> KvClient::call(const Command& command) {
    if (&command != &lastCommand_) lastCommand_ = command;  // resendLast aliases
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
        const ClientReply* reply =
            attempt(serverId, addr, command)
                ? std::get_if<ClientReply>(&decoded_.message)
                : nullptr;
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

bool KvClient::attempt(NodeId serverId,
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
            return false;
        }
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd_ < 0) return false;
        if (::connect(fd_, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) <
            0) {
            dropConnection();
            return false;
        }
        const int one = 1;
        ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        connectedTo_ = serverId;
    }

    // Build the request in the reused message/frame buffers (capacity
    // sticks across calls; same bytes on the wire as a fresh build).
    auto& req = std::get<ClientRequest>(reqMsg_);
    req.clientId = clientId_;
    req.seqNo = seqNo_;
    req.command.assign(command.begin(), command.end());
    frame_.resize(transport::kLengthPrefixSize + rsm::rpc::encodedSize(reqMsg_));
    const std::size_t bodyLen = rsm::rpc::encodeMessage(
        clientNodeId_, serverId, reqMsg_,
        std::span<std::uint8_t>(frame_).subspan(transport::kLengthPrefixSize));
    if (bodyLen == 0) return false;
    transport::writeLengthPrefix(static_cast<std::uint32_t>(bodyLen),
                                 frame_.data());
    for (std::size_t sent = 0; sent < frame_.size();) {
        const ssize_t n = ::send(fd_, frame_.data() + sent,
                                 frame_.size() - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            dropConnection();
            return false;
        }
        sent += static_cast<std::size_t>(n);
    }

    // Await the reply on this connection until the attempt deadline. The
    // assembler is reset per attempt (same semantics as a fresh one: bytes
    // from an earlier exchange never carry over), keeping its capacity.
    assembler_.reset();
    bool gotReply = false;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeoutMs_);
    while (!gotReply) {
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now());
        if (left.count() <= 0) {
            dropConnection();  // a late reply must not leak into the next op
            return false;
        }
        pollfd pfd{fd_, POLLIN, 0};
        const int ready = ::poll(&pfd, 1, static_cast<int>(left.count()));
        if (ready < 0 && errno == EINTR) continue;
        if (ready <= 0) {  // timeout or poll error
            dropConnection();
            return false;
        }
        std::uint8_t buf[64 * 1024];
        const ssize_t n = ::read(fd_, buf, sizeof(buf));
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            dropConnection();
            return false;  // server closed mid-reply
        }
        bool bad = false;
        const bool fed = assembler_.feed(
            std::span<const std::uint8_t>(buf, static_cast<std::size_t>(n)),
            [this, &gotReply, &bad](std::span<const std::uint8_t> body) {
                if (gotReply) return;  // keep the reply; ignore trailing frames
                // Decode into the reused slot (zero steady-state
                // allocations); identical validation to decodeMessage.
                if (!rsm::rpc::decodeMessageInto(body, decoded_,
                                                 decodePool_)) {
                    bad = true;
                    return;
                }
                if (std::get_if<ClientReply>(&decoded_.message) != nullptr) {
                    gotReply = true;
                }
            });
        if (!fed || bad) {
            dropConnection();
            return false;
        }
    }
    return true;
}

}  // namespace rsm::client
