#include "faults/sim_network.h"

#include <algorithm>

namespace rsm::sim {

namespace {

// Threshold compare on a raw 64-bit draw: r < p * 2^64. Exact for p == 0
// (never fires) and p == 1 (clamped to always fire).
bool chance(std::uint64_t draw, double p) {
    if (p <= 0.0) return false;
    if (p >= 1.0) return true;
    return static_cast<double>(draw) <
           p * 18446744073709551616.0;  // 2^64
}

}  // namespace

SimNetwork::SimNetwork(std::uint64_t seed, std::vector<NodeId> clusterNodes)
    : rng_(seed), cluster_(std::move(clusterNodes)) {}

void SimNetwork::setDrop(double probability) { dropProb_ = probability; }

void SimNetwork::setLatency(Duration min, Duration max) {
    latencyMin_ = min;
    latencyMax_ = std::max(min, max);
}

void SimNetwork::setReorder(double probability, Duration extraMax) {
    reorderProb_ = probability;
    reorderExtraMax_ = extraMax;
}

void SimNetwork::partition(const std::vector<std::vector<NodeId>>& groups) {
    groupOf_.clear();
    int g = 1;
    for (const auto& group : groups) {
        for (const NodeId id : group) groupOf_[id] = g;
        ++g;
    }
    if (groupOf_.empty()) groupOf_[0] = 0;  // "partition({})" isolates all
}

void SimNetwork::isolate(NodeId id) {
    std::vector<NodeId> rest;
    for (const NodeId n : cluster_) {
        if (n != id) rest.push_back(n);
    }
    partition({{id}, rest});
}

void SimNetwork::heal() { groupOf_.clear(); }

bool SimNetwork::isClusterNode(NodeId id) const {
    return std::find(cluster_.begin(), cluster_.end(), id) != cluster_.end();
}

bool SimNetwork::sameSide(NodeId a, NodeId b) const {
    if (groupOf_.empty()) return true;
    // Partitions only separate cluster nodes; client traffic always flows.
    if (!isClusterNode(a) || !isClusterNode(b)) return true;
    const auto ga = groupOf_.find(a);
    const auto gb = groupOf_.find(b);
    // A cluster node not in any group is fully isolated.
    if (ga == groupOf_.end() || gb == groupOf_.end()) return false;
    return ga->second == gb->second;
}

void SimNetwork::send(NodeId from, NodeId to, Message m, TimePoint now) {
    // Always draw the same number of words per send so the RNG stream never
    // depends on the current fault settings.
    const std::uint64_t dropDraw = rng_();
    const std::uint64_t latencyDraw = rng_();
    const std::uint64_t reorderDraw = rng_();
    const std::uint64_t reorderExtraDraw = rng_();

    const bool cut = !sameSide(from, to);
    const bool dropped = cut || chance(dropDraw, dropProb_);
    Duration delay = latencyMin_;
    const auto window =
        static_cast<std::uint64_t>((latencyMax_ - latencyMin_).count());
    delay += Duration(static_cast<std::int64_t>(latencyDraw % (window + 1)));
    if (chance(reorderDraw, reorderProb_) && reorderExtraMax_.count() > 0) {
        delay += Duration(static_cast<std::int64_t>(
            reorderExtraDraw %
            static_cast<std::uint64_t>(reorderExtraMax_.count() + 1)));
    }
    if (onSend) onSend(from, to, m, dropped, delay);
    if (dropped) return;
    queue_.emplace(std::make_pair(now + delay, nextSeq_++),
                   Wire{from, to, std::move(m)});
}

void SimNetwork::deliverDue(TimePoint now, const DeliverFn& deliver) {
    while (!queue_.empty() && queue_.begin()->first.first <= now) {
        auto node = queue_.extract(queue_.begin());
        Wire& f = node.mapped();
        // A partition raised after the send kills in-flight cross-group
        // traffic too ("isolate now" means now).
        if (!sameSide(f.from, f.to)) continue;
        deliver(f.from, f.to, std::move(f.msg));
    }
}

}  // namespace rsm::sim
