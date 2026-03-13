#include "scheduler/hierarchical_scheduler.h"

#include "backend/execution_backend.h"
#include "model/request_sizing.h"

#include <algorithm>
#include <cstddef>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace {

std::unordered_map<uint32_t, size_t> BuildNodeIndex(
    const std::vector<ResourceTreeNode>& tree_nodes) {
    std::unordered_map<uint32_t, size_t> out;
    out.reserve(tree_nodes.size());
    for (size_t i = 0; i < tree_nodes.size(); ++i) {
        out[tree_nodes[i].node_id] = i;
    }
    return out;
}

std::vector<CardId> FilterCardsByNodeVisibility(
    const std::vector<CardId>& visible_idle_cards,
    const std::vector<CardId>& node_cards) {

    if (node_cards.empty()) {
        return visible_idle_cards;
    }

    std::unordered_set<CardId> allowed(node_cards.begin(), node_cards.end());
    std::vector<CardId> out;
    out.reserve(visible_idle_cards.size());
    for (const CardId card_id : visible_idle_cards) {
        if (allowed.find(card_id) != allowed.end()) {
            out.push_back(card_id);
        }
    }
    return out;
}

} // namespace

HierarchicalScheduler::HierarchicalScheduler(
    FixedTreeKind tree_kind,
    uint32_t num_pools)
    : tree_kind_(tree_kind),
      num_pools_(num_pools == 0 ? 1 : num_pools),
      tree_(BuildFixedTree(num_pools_)) {

    node_index_ = BuildNodeIndex(tree_);
    root_node_id_ = tree_.empty() ? 0 : tree_.front().node_id;
}

void HierarchicalScheduler::OnRequestArrival(const Request& req) {
    queue_.push_back(req);
}

uint32_t HierarchicalScheduler::DecideCardCount(const Request& req) const {
    return DecideCardCountForRequest(req);
}

std::vector<ResourceTreeNode> HierarchicalScheduler::BuildFixedTree(uint32_t num_pools) const {
    std::vector<ResourceTreeNode> tree;

    const uint32_t pool0 = 0;
    const uint32_t pool1 = (num_pools > 1) ? 1U : 0U;

    switch (tree_kind_) {
    case FixedTreeKind::TreeA_Shared: {
        ResourceTreeNode root;
        root.node_id = 0;
        root.node_name = "root";
        root.type = TreeNodeType::Temporal;
        root.children = {1};

        ResourceTreeNode leaf;
        leaf.node_id = 1;
        leaf.node_name = "shared_leaf";
        leaf.type = TreeNodeType::Leaf;
        leaf.pool_id = std::nullopt;
        leaf.affinity_preferred = false;

        tree = {root, leaf};
        break;
    }

    case FixedTreeKind::TreeB_TwoPools: {
        ResourceTreeNode root;
        root.node_id = 0;
        root.node_name = "root";
        root.type = TreeNodeType::Spatial;
        root.children = {1, 2};
        root.spatial_policy = SpatialRoutePolicy::ByLatencyClass;

        ResourceTreeNode latency_leaf;
        latency_leaf.node_id = 1;
        latency_leaf.node_name = "latency_leaf";
        latency_leaf.type = TreeNodeType::Leaf;
        latency_leaf.pool_id = pool0;
        latency_leaf.affinity_preferred = false;

        ResourceTreeNode batch_leaf;
        batch_leaf.node_id = 2;
        batch_leaf.node_name = "batch_leaf";
        batch_leaf.type = TreeNodeType::Leaf;
        batch_leaf.pool_id = pool1;
        batch_leaf.affinity_preferred = false;

        tree = {root, latency_leaf, batch_leaf};
        break;
    }

    case FixedTreeKind::TreeC_UserPinned: {
        ResourceTreeNode root;
        root.node_id = 0;
        root.node_name = "root";
        root.type = TreeNodeType::Spatial;
        root.spatial_policy = SpatialRoutePolicy::ByUserHash;

        tree.push_back(root);
        uint32_t next_node_id = 1;
        for (uint32_t pool_id = 0; pool_id < num_pools; ++pool_id) {
            tree.front().children.push_back(next_node_id);

            ResourceTreeNode leaf;
            leaf.node_id = next_node_id;
            leaf.node_name = "user_pinned_leaf_" + std::to_string(pool_id);
            leaf.type = TreeNodeType::Leaf;
            leaf.pool_id = pool_id;
            leaf.affinity_preferred = false;
            tree.push_back(leaf);

            ++next_node_id;
        }
        break;
    }

    case FixedTreeKind::TreeD_TwoPoolsAffinity: {
        ResourceTreeNode root;
        root.node_id = 0;
        root.node_name = "root";
        root.type = TreeNodeType::Spatial;
        root.children = {1, 2};
        root.spatial_policy = SpatialRoutePolicy::ByLatencyClass;

        ResourceTreeNode latency_leaf;
        latency_leaf.node_id = 1;
        latency_leaf.node_name = "latency_affinity_leaf";
        latency_leaf.type = TreeNodeType::Leaf;
        latency_leaf.pool_id = pool0;
        latency_leaf.affinity_preferred = true;

        ResourceTreeNode batch_leaf;
        batch_leaf.node_id = 2;
        batch_leaf.node_name = "batch_affinity_leaf";
        batch_leaf.type = TreeNodeType::Leaf;
        batch_leaf.pool_id = pool1;
        batch_leaf.affinity_preferred = true;

        tree = {root, latency_leaf, batch_leaf};
        break;
    }
    }

    return tree;
}

bool HierarchicalScheduler::UserAllowed(const ResourceTreeNode& node, UserId user_id) const {
    if (node.allowed_users.empty()) {
        return true;
    }
    return std::find(
        node.allowed_users.begin(),
        node.allowed_users.end(),
        user_id) != node.allowed_users.end();
}

std::vector<uint32_t> HierarchicalScheduler::ResolveSpatialChildren(
    const ResourceTreeNode& node,
    const Request& req) const {

    if (node.children.empty()) {
        return {};
    }

    switch (node.spatial_policy) {
    case SpatialRoutePolicy::ByLatencyClass: {
        if (node.children.size() == 1) {
            return {node.children.front()};
        }
        return req.latency_sensitive
            ? std::vector<uint32_t>{node.children[0]}
            : std::vector<uint32_t>{node.children[1]};
    }

    case SpatialRoutePolicy::ByUserHash: {
        const size_t idx = static_cast<size_t>(req.user_id) % node.children.size();
        return {node.children[idx]};
    }

    case SpatialRoutePolicy::FirstFit:
    default:
        return node.children;
    }
}

std::vector<CardId> HierarchicalScheduler::CollectLeafCandidates(
    const ResourceTreeNode& node,
    const Request& req,
    const SystemState& state,
    const std::vector<CardId>& visible_idle_cards) const {

    std::vector<CardId> preferred;
    std::vector<CardId> fallback;
    preferred.reserve(visible_idle_cards.size());
    fallback.reserve(visible_idle_cards.size());

    for (const CardId card_id : visible_idle_cards) {
        const auto& card = state.cards.at(card_id);

        if (node.pool_id.has_value() && card.pool_id != node.pool_id.value()) {
            continue;
        }

        if (node.affinity_preferred
            && card.resident_user.has_value()
            && card.resident_user.value() == req.user_id) {
            preferred.push_back(card_id);
        } else {
            fallback.push_back(card_id);
        }
    }

    std::vector<CardId> out;
    out.reserve(preferred.size() + fallback.size());
    out.insert(out.end(), preferred.begin(), preferred.end());
    out.insert(out.end(), fallback.begin(), fallback.end());
    return out;
}

std::optional<std::vector<CardId>> HierarchicalScheduler::SelectCardsFromNode(
    const std::vector<ResourceTreeNode>& tree_nodes,
    const std::unordered_map<uint32_t, size_t>& node_index,
    uint32_t node_id,
    const Request& req,
    const SystemState& state,
    const std::vector<CardId>& visible_idle_cards,
    uint32_t required_cards) const {

    const auto it = node_index.find(node_id);
    if (it == node_index.end()) {
        return std::nullopt;
    }

    const ResourceTreeNode& node = tree_nodes.at(it->second);

    if (!UserAllowed(node, req.user_id)) {
        return std::nullopt;
    }

    const std::vector<CardId> node_visible_cards =
        FilterCardsByNodeVisibility(visible_idle_cards, node.cards);
    if (node_visible_cards.empty()) {
        return std::nullopt;
    }

    if (node.type == TreeNodeType::Leaf) {
        std::vector<CardId> candidates =
            CollectLeafCandidates(node, req, state, node_visible_cards);
        if (candidates.size() < required_cards) {
            return std::nullopt;
        }

        std::vector<CardId> assigned;
        assigned.insert(
            assigned.end(),
            candidates.begin(),
            candidates.begin()
                + static_cast<std::vector<CardId>::difference_type>(required_cards));
        return assigned;
    }

    if (node.type == TreeNodeType::Spatial) {
        const auto children = ResolveSpatialChildren(node, req);
        for (const uint32_t child_id : children) {
            auto result = SelectCardsFromNode(
                tree_nodes,
                node_index,
                child_id,
                req,
                state,
                node_visible_cards,
                required_cards);
            if (result.has_value()) {
                return result;
            }
        }
        return std::nullopt;
    }

    // Temporal node: the same visible card domain is time-shared among children,
    // so we try children in order.
    for (const uint32_t child_id : node.children) {
        auto result = SelectCardsFromNode(
            tree_nodes,
            node_index,
            child_id,
            req,
            state,
            node_visible_cards,
            required_cards);
        if (result.has_value()) {
            return result;
        }
    }

    return std::nullopt;
}

std::optional<ExecutionPlan> HierarchicalScheduler::TrySchedule(
    const SystemState& state,
    const ExecutionBackend& /*backend*/) {

    if (queue_.empty()) {
        return std::nullopt;
    }

    std::vector<CardId> idle_cards;
    idle_cards.reserve(state.cards.size());
    for (const auto& card : state.cards) {
        if (!card.busy && state.now >= card.available_time) {
            idle_cards.push_back(card.card_id);
        }
    }
    if (idle_cards.empty()) {
        return std::nullopt;
    }

    const bool has_external_tree = !state.resource_tree.empty();
    const std::vector<ResourceTreeNode>& active_tree =
        has_external_tree ? state.resource_tree : tree_;
    const uint32_t active_root =
        has_external_tree ? state.resource_tree_root : root_node_id_;
    const std::unordered_map<uint32_t, size_t> active_index = BuildNodeIndex(active_tree);

    if (active_index.find(active_root) == active_index.end()) {
        return std::nullopt;
    }

    for (size_t req_idx = 0; req_idx < queue_.size(); ++req_idx) {
        const Request& req = queue_[req_idx];
        const uint32_t required_cards = DecideCardCount(req);

        auto assigned_cards_opt = SelectCardsFromNode(
            active_tree,
            active_index,
            active_root,
            req,
            state,
            idle_cards,
            required_cards);

        if (!assigned_cards_opt.has_value()) {
            continue;
        }

        ExecutionPlan plan;
        plan.request_id = req.request_id;
        plan.assigned_cards = std::move(assigned_cards_opt.value());

        queue_.erase(
            queue_.begin()
            + static_cast<std::deque<Request>::difference_type>(req_idx));
        return plan;
    }

    return std::nullopt;
}

void HierarchicalScheduler::OnTaskFinished(
    const Request&,
    const ExecutionPlan&,
    const ExecutionResult&) {}

bool HierarchicalScheduler::Empty() const {
    return queue_.empty();
}
