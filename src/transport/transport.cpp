#include "transport/transport.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "transport/frame.h"

namespace rsm::transport {

using rsm::rpc::Message;
using rsm::rpc::NodeId;

namespace {

void logf(NodeId self, const char* what, const char* detail) {
    std::fprintf(stderr, "[transport %u] %s: %s\n", self, what, detail);
}

}  // namespace

PeerMap loadPeerConfig(const std::string& path) {
    std::ifstream in(path);
    if (!in) throw std::runtime_error("cannot open peer config: " + path);
    PeerMap peers;
    std::string line;
    int lineNo = 0;
    while (std::getline(in, line)) {
        ++lineNo;
        const auto first = line.find_first_not_of(" \t");
        if (first == std::string::npos || line[first] == '#') continue;
        std::istringstream fields(line);
        long id = -1;
        std::string host;
        long port = -1;
        if (!(fields >> id >> host >> port) || id < 0 || id > 0xFFFF ||
            port < 1 || port > 0xFFFF) {
            throw std::runtime_error("bad peer config line " +
                                     std::to_string(lineNo) + ": " + line);
        }
        std::string extra;
        if (fields >> extra) {
            throw std::runtime_error("trailing tokens on peer config line " +
                                     std::to_string(lineNo) + ": " + line);
        }
        const auto nodeId = static_cast<NodeId>(id);
        if (peers.count(nodeId)) {
            throw std::runtime_error("duplicate node id on peer config line " +
                                     std::to_string(lineNo) + ": " + line);
        }
        peers[nodeId] = PeerAddress{host, static_cast<std::uint16_t>(port)};
    }
    return peers;
}

Transport::Transport(NodeId selfId, std::uint16_t listenPort)
    : selfId_(selfId) {
    listenFd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd_ < 0) throw std::runtime_error("socket() failed");
    const int one = 1;
    ::setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(listenPort);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(listenFd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0 ||
        ::listen(listenFd_, 16) < 0) {
        ::close(listenFd_);
        throw std::runtime_error("bind/listen failed on port " +
                                 std::to_string(listenPort));
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(listenFd_, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
        ::close(listenFd_);
        throw std::runtime_error("getsockname failed");
    }
    listenPort_ = ntohs(addr.sin_port);
}

Transport::~Transport() {
    stop();
    if (listenFd_ >= 0) ::close(listenFd_);
    for (auto& [id, peer] : peers_) {
        if (peer.fd >= 0) ::close(peer.fd);
    }
}

void Transport::addPeer(NodeId id, PeerAddress addr) {
    std::lock_guard lock(peersMu_);
    peers_[id] = Peer{std::move(addr), -1};
}

void Transport::setHandler(Handler h) {
    handler_ = std::move(h);
}

void Transport::start() {
    running_.store(true);
    ioThread_ = std::thread(&Transport::ioLoop, this);
}

void Transport::stop() {
    running_.store(false);
    if (ioThread_.joinable()) ioThread_.join();
}

int Transport::connectTo(const PeerAddress& addr) {
    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(addr.port);
    if (::inet_pton(AF_INET, addr.host.c_str(), &sa.sin_addr) != 1) return -1;
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    if (::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) < 0) {
        ::close(fd);
        return -1;
    }
    const int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    return fd;
}

bool Transport::writeFrame(Peer& peer, const std::uint8_t* data,
                           std::size_t len) {
    std::size_t sent = 0;
    while (sent < len) {
        const ssize_t n =
            ::send(peer.fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            ::close(peer.fd);
            peer.fd = -1;
            return false;
        }
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

bool Transport::send(NodeId to, const Message& m) {
    std::vector<std::uint8_t> frame(kLengthPrefixSize + rsm::rpc::encodedSize(m));
    const std::size_t bodyLen = rsm::rpc::encodeMessage(
        selfId_, to, m,
        std::span<std::uint8_t>(frame).subspan(kLengthPrefixSize));
    if (bodyLen == 0) {
        logf(selfId_, "send dropped", "message failed to encode (oversize?)");
        return false;
    }
    writeLengthPrefix(static_cast<std::uint32_t>(bodyLen), frame.data());

    std::lock_guard lock(peersMu_);
    const auto it = peers_.find(to);
    if (it == peers_.end()) {
        logf(selfId_, "send dropped", "unknown peer id");
        return false;
    }
    Peer& peer = it->second;
    if (peer.fd < 0) {
        peer.fd = connectTo(peer.addr);
        if (peer.fd < 0) {
            logf(selfId_, "send dropped", "peer unreachable (connect failed)");
            return false;
        }
    }
    if (writeFrame(peer, frame.data(), frame.size())) return true;
    // The cached connection may have died since the last send (peer restart);
    // retry once on a fresh connection before declaring the peer unreachable.
    peer.fd = connectTo(peer.addr);
    if (peer.fd >= 0 && writeFrame(peer, frame.data(), frame.size())) {
        return true;
    }
    logf(selfId_, "send dropped", "peer unreachable (write failed)");
    return false;
}

void Transport::ioLoop() {
    struct Conn {
        int fd;
        FrameAssembler assembler;
    };
    std::vector<Conn> conns;

    const auto closeConn = [&conns](std::size_t i) {
        ::close(conns[i].fd);
        conns.erase(conns.begin() + static_cast<std::ptrdiff_t>(i));
    };

    while (running_.load()) {
        std::vector<pollfd> fds;
        fds.push_back(pollfd{listenFd_, POLLIN, 0});
        for (const auto& c : conns) fds.push_back(pollfd{c.fd, POLLIN, 0});

        const int ready = ::poll(fds.data(), fds.size(), 100 /*ms*/);
        if (ready < 0) {
            if (errno == EINTR) continue;
            logf(selfId_, "io loop exiting", "poll failed");
            break;
        }
        if (ready == 0) continue;

        // Handle reads before accepting: fds[i + 1] must keep lining up with
        // conns[i], and accept() appends to conns. Iterate backwards so
        // closing a connection does not shift the indices not yet visited.
        const std::size_t polled = conns.size();
        for (std::size_t i = polled; i-- > 0;) {
            const auto& revents = fds[i + 1].revents;
            if (revents == 0) continue;
            if (revents & (POLLERR | POLLHUP | POLLNVAL)) {
                closeConn(i);
                continue;
            }
            std::uint8_t buf[64 * 1024];
            const ssize_t n = ::read(conns[i].fd, buf, sizeof(buf));
            if (n <= 0) {
                if (n < 0 && errno == EINTR) continue;
                closeConn(i);  // EOF or error
                continue;
            }
            bool bad = false;
            const bool fed = conns[i].assembler.feed(
                std::span<const std::uint8_t>(buf, static_cast<std::size_t>(n)),
                [this, &bad](std::span<const std::uint8_t> body) {
                    auto decoded = rsm::rpc::decodeMessage(body);
                    if (!decoded) {
                        bad = true;
                        return;
                    }
                    if (handler_) {
                        handler_(decoded->envelope, std::move(decoded->message));
                    }
                });
            if (!fed || bad) {
                logf(selfId_, "closing connection", "framing/decode error");
                closeConn(i);
            }
        }

        if (fds[0].revents & POLLIN) {
            const int fd = ::accept(listenFd_, nullptr, nullptr);
            if (fd >= 0) conns.push_back(Conn{fd, FrameAssembler{}});
        }
    }
    for (const auto& c : conns) ::close(c.fd);
}

}  // namespace rsm::transport
