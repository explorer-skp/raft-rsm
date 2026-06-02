#pragma once

// The simulated transport for the Phase 6 deterministic harness: an
// in-memory message bus whose every behavior — loss, latency, jitter,
// reordering, partitions — is a pure function of the seed and the sequence
// of send() calls. This is the sim-side implementation of the transport
// seam: production RaftCore emits through a SendFn and receives through
// handle(); here the SendFn feeds this bus instead of TCP, and the
// scheduler drains it. The Raft core cannot tell the difference.
//
// Determinism notes (these are load-bearing, see DESIGN.md):
//  - one RNG, consumed in send order; every send draws the same number of
//    words regardless of current fault settings, so toggling faults never
//    desynchronizes the stream;
//  - fault decisions use raw modulo / threshold on mt19937_64 output, not
//    std::uniform_*_distribution, whose algorithm is implementation-defined
//    (the tiny modulo bias is irrelevant for chaos schedules);
//  - delivery order is a strict weak order on (deliverAt, enqueueSeq):
//    equal-time messages deliver in enqueue order.

#include <cstdint>
#include <functional>
#include <map>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "raft/clock.h"
#include "rpc/messages.h"

namespace rsm::sim {

using rsm::raft::Duration;
using rsm::raft::TimePoint;
using rsm::rpc::Message;
using rsm::rpc::NodeId;

class SimNetwork {
public:
    // `clusterNodes` lists the Raft node ids. Partitions apply only between
    // cluster nodes; other ids (sim clients) can always reach every node —
    // they model clients that may dial any node, like the real KvClient.
    // Drop, latency, and reorder apply to ALL traffic, clients included.
    SimNetwork(std::uint64_t seed, std::vector<NodeId> clusterNodes);

    // ---- fault control surface (/faults) ----
    void setDrop(double probability);  // [0, 1]
    void setLatency(Duration min, Duration max);  // per-message, uniform
    // With `probability`, a message gets up to `extraMax` additional delay,
    // letting later messages overtake it.
    void setReorder(double probability, Duration extraMax);
    // Splits the cluster: traffic flows only within a group. A cluster node
    // absent from every group is isolated entirely.
    void partition(const std::vector<std::vector<NodeId>>& groups);
    void isolate(NodeId id);  // shorthand: {id} vs everyone else
    void heal();              // remove the partition (drop/latency persist)
    bool partitioned() const { return !groupOf_.empty(); }
    double dropProbability() const { return dropProb_; }

    // ---- bus ----
    // Applies drop/partition/latency/reorder at send time; a partition also
    // kills in-flight cross-group traffic at delivery time.
    void send(NodeId from, NodeId to, Message m, TimePoint now);

    using DeliverFn = std::function<void(NodeId from, NodeId to, Message&&)>;
    // Delivers every message due at `now` in (deliverAt, enqueueSeq) order,
    // including messages the handlers send that fall due immediately
    // (cascades), until none remain.
    void deliverDue(TimePoint now, const DeliverFn& deliver);

    std::size_t inFlight() const { return queue_.size(); }

    // Observability tap for tests and the trace: every send() reports its
    // fate (delay is meaningless when dropped).
    std::function<void(NodeId from, NodeId to, const Message&, bool dropped,
                       Duration delay)>
        onSend;

private:
    bool sameSide(NodeId a, NodeId b) const;
    bool isClusterNode(NodeId id) const;

    struct Wire {
        NodeId from = 0;
        NodeId to = 0;
        Message msg;
    };

    std::mt19937_64 rng_;
    std::vector<NodeId> cluster_;
    std::map<NodeId, int> groupOf_;  // empty == fully connected
    double dropProb_ = 0.0;
    Duration latencyMin_{1};
    Duration latencyMax_{1};
    double reorderProb_ = 0.0;
    Duration reorderExtraMax_{0};
    // Keyed by (deliverAt, enqueueSeq): deterministic order, and the map's
    // node-based storage never moves a queued Message.
    std::map<std::pair<TimePoint, std::uint64_t>, Wire> queue_;
    std::uint64_t nextSeq_ = 0;
};

}  // namespace rsm::sim
