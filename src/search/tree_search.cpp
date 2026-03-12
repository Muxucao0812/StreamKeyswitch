#include "search/tree_search.h"

#include "backend/analytical_backend.h"
#include "backend/execution_backend.h"
#include "backend/table_backend.h"
#include "scheduler/hierarchical_scheduler.h"
#include "sim/simulator.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <random>
#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {

struct CandidateTree {
    std::vector<ResourceTreeNode> tree;
    uint32_t root_id = 0;
    std::string op_name;
};

struct EvaluatedCandidate {
    CandidateTree candidate;
    SimulationMetrics metrics;
    double objective = 0.0;
};

bool BuildNodeIndex(
    const std::vector<ResourceTreeNode>& tree,
    std::unordered_map<uint32_t, size_t>* out_index) {

    out_index->clear();
    out_index->reserve(tree.size());
    for (size_t i = 0; i < tree.size(); ++i) {
        const auto [it, inserted] = out_index->emplace(tree[i].node_id, i);
        if (!inserted) {
            return false;
        }
    }
    return true;
}

bool BuildParentMap(
    const std::vector<ResourceTreeNode>& tree,
    const std::unordered_map<uint32_t, size_t>& node_index,
    std::unordered_map<uint32_t, uint32_t>* out_parent) {

    out_parent->clear();
    out_parent->reserve(tree.size());

    for (const auto& node : tree) {
        for (const uint32_t child_id : node.children) {
            if (node_index.find(child_id) == node_index.end()) {
                return false;
            }
            const auto [it, inserted] = out_parent->emplace(child_id, node.node_id);
            if (!inserted) {
                return false;
            }
        }
    }

    return true;
}

template <typename T>
void SortAndUnique(std::vector<T>* values) {
    std::sort(values->begin(), values->end());
    values->erase(std::unique(values->begin(), values->end()), values->end());
}

bool ReindexToConnected(
    std::vector<ResourceTreeNode>* tree,
    uint32_t* root_id) {

    std::unordered_map<uint32_t, size_t> node_index;
    if (!BuildNodeIndex(*tree, &node_index)) {
        return false;
    }

    if (node_index.find(*root_id) == node_index.end()) {
        return false;
    }

    std::vector<uint32_t> order;
    order.reserve(tree->size());

    std::unordered_set<uint32_t> visiting;
    std::unordered_set<uint32_t> visited;

    std::function<bool(uint32_t)> dfs = [&](uint32_t node_id) -> bool {
        if (visiting.find(node_id) != visiting.end()) {
            return false;
        }
        if (visited.find(node_id) != visited.end()) {
            return true;
        }

        const auto it = node_index.find(node_id);
        if (it == node_index.end()) {
            return false;
        }

        visiting.insert(node_id);
        order.push_back(node_id);
        const ResourceTreeNode& node = tree->at(it->second);
        for (const uint32_t child_id : node.children) {
            if (!dfs(child_id)) {
                return false;
            }
        }
        visiting.erase(node_id);
        visited.insert(node_id);
        return true;
    };

    if (!dfs(*root_id)) {
        return false;
    }

    std::unordered_map<uint32_t, uint32_t> id_remap;
    id_remap.reserve(order.size());
    for (uint32_t i = 0; i < order.size(); ++i) {
        id_remap.emplace(order[i], i);
    }

    std::vector<ResourceTreeNode> new_tree;
    new_tree.reserve(order.size());

    for (uint32_t old_id : order) {
        const ResourceTreeNode& old_node = tree->at(node_index.at(old_id));
        ResourceTreeNode node = old_node;
        node.node_id = id_remap.at(old_id);
        if (node.node_name.empty()) {
            node.node_name = "node_" + std::to_string(node.node_id);
        }

        std::vector<uint32_t> remapped_children;
        remapped_children.reserve(node.children.size());
        for (const uint32_t child_id : node.children) {
            const auto map_it = id_remap.find(child_id);
            if (map_it != id_remap.end()) {
                remapped_children.push_back(map_it->second);
            }
        }
        node.children = std::move(remapped_children);
        new_tree.push_back(std::move(node));
    }

    *root_id = id_remap.at(*root_id);
    *tree = std::move(new_tree);
    return true;
}

bool NormalizeTree(
    std::vector<ResourceTreeNode>* tree,
    uint32_t* root_id,
    uint32_t num_cards) {

    if (tree->empty()) {
        return false;
    }

    std::unordered_map<uint32_t, size_t> node_index;
    if (!BuildNodeIndex(*tree, &node_index)) {
        return false;
    }

    if (node_index.find(*root_id) == node_index.end()) {
        return false;
    }

    std::unordered_set<uint32_t> visiting;
    std::unordered_set<uint32_t> visited;

    std::function<bool(uint32_t)> dfs = [&](uint32_t node_id) -> bool {
        if (visiting.find(node_id) != visiting.end()) {
            return false;
        }
        if (visited.find(node_id) != visited.end()) {
            return true;
        }

        const auto index_it = node_index.find(node_id);
        if (index_it == node_index.end()) {
            return false;
        }

        visiting.insert(node_id);
        ResourceTreeNode& node = tree->at(index_it->second);

        SortAndUnique(&node.allowed_users);

        if (node.type == TreeNodeType::Leaf) {
            SortAndUnique(&node.cards);
        } else {
            std::vector<CardId> merged_cards;
            for (const uint32_t child_id : node.children) {
                if (!dfs(child_id)) {
                    return false;
                }
                const auto child_it = node_index.find(child_id);
                if (child_it == node_index.end()) {
                    return false;
                }
                const auto& child = tree->at(child_it->second);
                merged_cards.insert(
                    merged_cards.end(),
                    child.cards.begin(),
                    child.cards.end());
            }
            SortAndUnique(&merged_cards);
            node.cards = std::move(merged_cards);
        }

        for (const CardId card_id : node.cards) {
            if (card_id >= num_cards) {
                return false;
            }
        }

        visiting.erase(node_id);
        visited.insert(node_id);
        return true;
    };

    if (!dfs(*root_id)) {
        return false;
    }

    for (auto& node : *tree) {
        if (node.node_name.empty()) {
            node.node_name = "node_" + std::to_string(node.node_id);
        }
    }

    return ReindexToConnected(tree, root_id);
}

bool ValidateTree(
    const std::vector<ResourceTreeNode>& tree,
    uint32_t root_id,
    uint32_t num_cards) {

    if (tree.empty()) {
        return false;
    }

    std::unordered_map<uint32_t, size_t> node_index;
    if (!BuildNodeIndex(tree, &node_index)) {
        return false;
    }

    if (node_index.find(root_id) == node_index.end()) {
        return false;
    }

    std::unordered_map<uint32_t, uint32_t> parent_of;
    if (!BuildParentMap(tree, node_index, &parent_of)) {
        return false;
    }

    std::unordered_set<uint32_t> visiting;
    std::unordered_set<uint32_t> visited;

    std::function<bool(uint32_t)> dfs = [&](uint32_t node_id) -> bool {
        if (visiting.find(node_id) != visiting.end()) {
            return false;
        }
        if (visited.find(node_id) != visited.end()) {
            return true;
        }

        visiting.insert(node_id);
        const ResourceTreeNode& node = tree.at(node_index.at(node_id));

        if (node.cards.empty()) {
            return false;
        }
        for (const CardId card_id : node.cards) {
            if (card_id >= num_cards) {
                return false;
            }
        }

        if (node.type == TreeNodeType::Leaf) {
            if (!node.children.empty()) {
                return false;
            }
        } else if (node.children.empty()) {
            return false;
        }

        for (const uint32_t child_id : node.children) {
            if (!dfs(child_id)) {
                return false;
            }
        }

        visiting.erase(node_id);
        visited.insert(node_id);
        return true;
    };

    if (!dfs(root_id)) {
        return false;
    }

    if (visited.size() != tree.size()) {
        return false;
    }

    std::unordered_map<uint32_t, std::unordered_set<CardId>> subtree_cards;
    subtree_cards.reserve(tree.size());

    std::function<const std::unordered_set<CardId>&(uint32_t)> collect =
        [&](uint32_t node_id) -> const std::unordered_set<CardId>& {
            const auto cache_it = subtree_cards.find(node_id);
            if (cache_it != subtree_cards.end()) {
                return cache_it->second;
            }

            const ResourceTreeNode& node = tree.at(node_index.at(node_id));
            std::unordered_set<CardId> cards;
            if (node.type == TreeNodeType::Leaf) {
                cards.insert(node.cards.begin(), node.cards.end());
            } else {
                for (const uint32_t child_id : node.children) {
                    const auto& child_cards = collect(child_id);
                    cards.insert(child_cards.begin(), child_cards.end());
                }
            }

            auto [it, inserted] = subtree_cards.emplace(node_id, std::move(cards));
            return it->second;
        };

    for (const auto& node : tree) {
        if (node.type != TreeNodeType::Spatial) {
            continue;
        }

        std::unordered_set<CardId> seen;
        for (const uint32_t child_id : node.children) {
            const auto& child_cards = collect(child_id);
            for (const CardId card_id : child_cards) {
                if (seen.find(card_id) != seen.end()) {
                    return false;
                }
                seen.insert(card_id);
            }
        }
    }

    return true;
}

FixedTreeKind ToFixedTreeKind(SchedulerKind scheduler) {
    switch (scheduler) {
    case SchedulerKind::HierarchicalA:
        return FixedTreeKind::TreeA_Shared;
    case SchedulerKind::HierarchicalB:
        return FixedTreeKind::TreeB_TwoPools;
    case SchedulerKind::HierarchicalC:
        return FixedTreeKind::TreeC_UserPinned;
    case SchedulerKind::HierarchicalD:
    default:
        return FixedTreeKind::TreeD_TwoPoolsAffinity;
    }
}

std::unique_ptr<ExecutionBackend> BuildBackendForEval(const ExperimentConfig& config) {
    if (config.backend == BackendKind::Table) {
        if (config.profile_table_path.empty()) {
            return std::make_unique<TableBackend>();
        }
        return std::make_unique<TableBackend>(config.profile_table_path);
    }
    return std::make_unique<AnalyticalBackend>();
}

SimulationMetrics EvaluateCandidateTree(
    const ExperimentConfig& config,
    const SystemState& base_state,
    const std::vector<Request>& workload,
    const std::vector<ResourceTreeNode>& tree,
    uint32_t root_id) {

    SystemState eval_state = base_state;
    eval_state.now = 0;
    eval_state.resource_tree = tree;
    eval_state.resource_tree_root = root_id;

    auto scheduler = std::make_unique<HierarchicalScheduler>(
        ToFixedTreeKind(config.scheduler),
        config.num_pools);
    auto backend = BuildBackendForEval(config);

    Simulator sim(
        std::move(eval_state),
        std::move(scheduler),
        std::move(backend));

    sim.LoadWorkload(workload);
    sim.Run();
    return sim.CollectMetrics();
}

double ComputeObjective(
    const SimulationMetrics& metrics,
    const TreeSearchWeights& weights,
    size_t total_requests) {

    const double fairness_penalty = std::max(0.0, 1.0 - metrics.jain_fairness_index);
    const double reload_penalty = static_cast<double>(metrics.total_reload_count);
    const double incomplete_penalty =
        static_cast<double>((total_requests > metrics.completed_requests)
            ? (total_requests - metrics.completed_requests)
            : 0);

    return weights.mean_latency * metrics.mean_latency
        + weights.p99_latency * static_cast<double>(metrics.p99_latency)
        + weights.fairness_penalty * fairness_penalty
        + weights.reload_penalty * reload_penalty
        + weights.incomplete_penalty * incomplete_penalty;
}

uint32_t NextNodeId(const std::vector<ResourceTreeNode>& tree) {
    uint32_t max_id = 0;
    for (const auto& node : tree) {
        max_id = std::max(max_id, node.node_id);
    }
    return max_id + 1;
}

template <typename T>
T* ChooseRandom(std::vector<T>* values, std::mt19937_64* rng) {
    if (values->empty()) {
        return nullptr;
    }
    std::uniform_int_distribution<size_t> dist(0, values->size() - 1);
    return &values->at(dist(*rng));
}

template <typename T>
const T* ChooseRandomConst(const std::vector<T>& values, std::mt19937_64* rng) {
    if (values.empty()) {
        return nullptr;
    }
    std::uniform_int_distribution<size_t> dist(0, values.size() - 1);
    return &values.at(dist(*rng));
}

bool ApplySplitLeaf(
    CandidateTree* candidate,
    std::mt19937_64* rng) {

    std::unordered_map<uint32_t, size_t> node_index;
    if (!BuildNodeIndex(candidate->tree, &node_index)) {
        return false;
    }

    std::unordered_map<uint32_t, uint32_t> parent_of;
    if (!BuildParentMap(candidate->tree, node_index, &parent_of)) {
        return false;
    }

    std::vector<uint32_t> eligible;
    for (const auto& node : candidate->tree) {
        if (node.type != TreeNodeType::Leaf) {
            continue;
        }
        if (node.cards.size() < 2) {
            continue;
        }
        if (parent_of.find(node.node_id) == parent_of.end()) {
            continue;
        }
        eligible.push_back(node.node_id);
    }

    const uint32_t* leaf_id = ChooseRandomConst(eligible, rng);
    if (leaf_id == nullptr) {
        return false;
    }

    const uint32_t parent_id = parent_of.at(*leaf_id);
    ResourceTreeNode& leaf = candidate->tree.at(node_index.at(*leaf_id));
    ResourceTreeNode& parent = candidate->tree.at(node_index.at(parent_id));

    const size_t split_at = leaf.cards.size() / 2;
    if (split_at == 0 || split_at >= leaf.cards.size()) {
        return false;
    }

    ResourceTreeNode new_leaf = leaf;
    new_leaf.node_id = NextNodeId(candidate->tree);
    new_leaf.node_name = leaf.node_name + "_split_" + std::to_string(new_leaf.node_id);

    new_leaf.cards.assign(leaf.cards.begin() + static_cast<std::ptrdiff_t>(split_at), leaf.cards.end());
    leaf.cards.erase(leaf.cards.begin() + static_cast<std::ptrdiff_t>(split_at), leaf.cards.end());

    if (leaf.allowed_users.size() >= 2) {
        const size_t users_split = leaf.allowed_users.size() / 2;
        new_leaf.allowed_users.assign(
            leaf.allowed_users.begin() + static_cast<std::ptrdiff_t>(users_split),
            leaf.allowed_users.end());
        leaf.allowed_users.erase(
            leaf.allowed_users.begin() + static_cast<std::ptrdiff_t>(users_split),
            leaf.allowed_users.end());
    }

    const auto pos = std::find(parent.children.begin(), parent.children.end(), *leaf_id);
    if (pos == parent.children.end()) {
        return false;
    }
    parent.children.insert(pos + 1, new_leaf.node_id);

    candidate->tree.push_back(std::move(new_leaf));
    candidate->op_name = "split_leaf";
    return true;
}

bool ApplyMergeSiblingLeaves(
    CandidateTree* candidate,
    std::mt19937_64* rng) {

    std::unordered_map<uint32_t, size_t> node_index;
    if (!BuildNodeIndex(candidate->tree, &node_index)) {
        return false;
    }

    struct MergeTarget {
        uint32_t parent_id = 0;
        uint32_t left_id = 0;
        uint32_t right_id = 0;
    };

    std::vector<MergeTarget> targets;

    for (const auto& node : candidate->tree) {
        if (node.children.size() < 2) {
            continue;
        }

        for (size_t i = 0; i < node.children.size(); ++i) {
            for (size_t j = i + 1; j < node.children.size(); ++j) {
                const uint32_t left_id = node.children[i];
                const uint32_t right_id = node.children[j];
                const auto left_it = node_index.find(left_id);
                const auto right_it = node_index.find(right_id);
                if (left_it == node_index.end() || right_it == node_index.end()) {
                    continue;
                }

                const auto& left = candidate->tree[left_it->second];
                const auto& right = candidate->tree[right_it->second];
                if (left.type != TreeNodeType::Leaf || right.type != TreeNodeType::Leaf) {
                    continue;
                }
                if (left.pool_id != right.pool_id) {
                    continue;
                }

                targets.push_back({node.node_id, left_id, right_id});
            }
        }
    }

    const MergeTarget* picked = ChooseRandomConst(targets, rng);
    if (picked == nullptr) {
        return false;
    }

    ResourceTreeNode& parent = candidate->tree.at(node_index.at(picked->parent_id));
    ResourceTreeNode& left = candidate->tree.at(node_index.at(picked->left_id));
    const ResourceTreeNode& right = candidate->tree.at(node_index.at(picked->right_id));

    left.cards.insert(left.cards.end(), right.cards.begin(), right.cards.end());
    SortAndUnique(&left.cards);

    if (left.allowed_users.empty() || right.allowed_users.empty()) {
        left.allowed_users.clear();
    } else {
        left.allowed_users.insert(
            left.allowed_users.end(),
            right.allowed_users.begin(),
            right.allowed_users.end());
        SortAndUnique(&left.allowed_users);
    }

    left.affinity_preferred = left.affinity_preferred || right.affinity_preferred;

    parent.children.erase(
        std::remove(parent.children.begin(), parent.children.end(), picked->right_id),
        parent.children.end());

    candidate->op_name = "merge_siblings";
    return true;
}

bool ApplyRebalanceCards(
    CandidateTree* candidate,
    std::mt19937_64* rng) {

    std::unordered_map<uint32_t, size_t> node_index;
    if (!BuildNodeIndex(candidate->tree, &node_index)) {
        return false;
    }

    struct MoveTarget {
        uint32_t src_id = 0;
        uint32_t dst_id = 0;
    };

    std::vector<MoveTarget> targets;

    for (const auto& parent : candidate->tree) {
        if (parent.children.size() < 2) {
            continue;
        }
        for (size_t i = 0; i < parent.children.size(); ++i) {
            for (size_t j = 0; j < parent.children.size(); ++j) {
                if (i == j) {
                    continue;
                }

                const uint32_t src_id = parent.children[i];
                const uint32_t dst_id = parent.children[j];

                const auto src_it = node_index.find(src_id);
                const auto dst_it = node_index.find(dst_id);
                if (src_it == node_index.end() || dst_it == node_index.end()) {
                    continue;
                }

                const auto& src = candidate->tree[src_it->second];
                const auto& dst = candidate->tree[dst_it->second];
                if (src.type != TreeNodeType::Leaf || dst.type != TreeNodeType::Leaf) {
                    continue;
                }
                if (src.pool_id != dst.pool_id) {
                    continue;
                }
                if (src.cards.size() < 2) {
                    continue;
                }
                targets.push_back({src_id, dst_id});
            }
        }
    }

    const MoveTarget* picked = ChooseRandomConst(targets, rng);
    if (picked == nullptr) {
        return false;
    }

    ResourceTreeNode& src = candidate->tree.at(node_index.at(picked->src_id));
    ResourceTreeNode& dst = candidate->tree.at(node_index.at(picked->dst_id));

    std::uniform_int_distribution<size_t> dist(0, src.cards.size() - 1);
    const size_t card_pos = dist(*rng);
    const CardId moved = src.cards[card_pos];

    if (std::find(dst.cards.begin(), dst.cards.end(), moved) != dst.cards.end()) {
        return false;
    }

    src.cards.erase(src.cards.begin() + static_cast<std::ptrdiff_t>(card_pos));
    dst.cards.push_back(moved);

    candidate->op_name = "rebalance_cards";
    return true;
}

bool ApplyMoveUserGroup(
    CandidateTree* candidate,
    const std::vector<UserId>& users,
    std::mt19937_64* rng) {

    if (users.empty()) {
        return false;
    }

    std::unordered_map<uint32_t, size_t> node_index;
    if (!BuildNodeIndex(candidate->tree, &node_index)) {
        return false;
    }

    struct MoveTarget {
        uint32_t src_id = 0;
        uint32_t dst_id = 0;
    };

    std::vector<MoveTarget> targets;

    for (const auto& parent : candidate->tree) {
        if (parent.children.size() < 2) {
            continue;
        }
        for (size_t i = 0; i < parent.children.size(); ++i) {
            for (size_t j = 0; j < parent.children.size(); ++j) {
                if (i == j) {
                    continue;
                }

                const auto src_it = node_index.find(parent.children[i]);
                const auto dst_it = node_index.find(parent.children[j]);
                if (src_it == node_index.end() || dst_it == node_index.end()) {
                    continue;
                }

                const auto& src = candidate->tree[src_it->second];
                const auto& dst = candidate->tree[dst_it->second];
                if (src.type != TreeNodeType::Leaf || dst.type != TreeNodeType::Leaf) {
                    continue;
                }
                if (src.allowed_users.size() < 2) {
                    continue;
                }

                targets.push_back({src.node_id, dst.node_id});
            }
        }
    }

    const MoveTarget* picked = ChooseRandomConst(targets, rng);
    if (picked == nullptr) {
        return false;
    }

    ResourceTreeNode& src = candidate->tree.at(node_index.at(picked->src_id));
    ResourceTreeNode& dst = candidate->tree.at(node_index.at(picked->dst_id));

    std::uniform_int_distribution<size_t> dist(0, src.allowed_users.size() - 1);
    const size_t idx = dist(*rng);
    const UserId moved_user = src.allowed_users[idx];

    src.allowed_users.erase(src.allowed_users.begin() + static_cast<std::ptrdiff_t>(idx));
    dst.allowed_users.push_back(moved_user);
    SortAndUnique(&dst.allowed_users);

    candidate->op_name = "move_user_group";
    return true;
}

bool ApplyReorderTemporal(
    CandidateTree* candidate,
    std::mt19937_64* rng) {

    std::vector<uint32_t> temporal_ids;
    for (const auto& node : candidate->tree) {
        if (node.type == TreeNodeType::Temporal && node.children.size() >= 2) {
            temporal_ids.push_back(node.node_id);
        }
    }

    const uint32_t* node_id = ChooseRandomConst(temporal_ids, rng);
    if (node_id == nullptr) {
        return false;
    }

    std::unordered_map<uint32_t, size_t> node_index;
    if (!BuildNodeIndex(candidate->tree, &node_index)) {
        return false;
    }

    ResourceTreeNode& node = candidate->tree.at(node_index.at(*node_id));

    std::uniform_int_distribution<size_t> dist(0, node.children.size() - 1);
    const size_t i = dist(*rng);
    size_t j = dist(*rng);
    if (i == j) {
        j = (j + 1) % node.children.size();
    }

    std::swap(node.children[i], node.children[j]);
    candidate->op_name = "reorder_temporal";
    return true;
}

bool ApplyChangeLeafAllowedUsers(
    CandidateTree* candidate,
    const std::vector<UserId>& users,
    std::mt19937_64* rng) {

    if (users.empty()) {
        return false;
    }

    std::vector<uint32_t> leaf_ids;
    for (const auto& node : candidate->tree) {
        if (node.type == TreeNodeType::Leaf) {
            leaf_ids.push_back(node.node_id);
        }
    }

    const uint32_t* leaf_id = ChooseRandomConst(leaf_ids, rng);
    if (leaf_id == nullptr) {
        return false;
    }

    std::unordered_map<uint32_t, size_t> node_index;
    if (!BuildNodeIndex(candidate->tree, &node_index)) {
        return false;
    }

    ResourceTreeNode& leaf = candidate->tree.at(node_index.at(*leaf_id));
    const UserId* user = ChooseRandomConst(users, rng);
    if (user == nullptr) {
        return false;
    }

    if (leaf.allowed_users.empty()) {
        leaf.allowed_users.push_back(*user);
        candidate->op_name = "change_leaf_allowed_users";
        return true;
    }

    const auto it = std::find(leaf.allowed_users.begin(), leaf.allowed_users.end(), *user);
    if (it != leaf.allowed_users.end()) {
        if (leaf.allowed_users.size() <= 1) {
            return false;
        }
        leaf.allowed_users.erase(it);
    } else {
        leaf.allowed_users.push_back(*user);
        SortAndUnique(&leaf.allowed_users);
    }

    candidate->op_name = "change_leaf_allowed_users";
    return true;
}

bool ApplyRandomNeighbor(
    CandidateTree* candidate,
    const std::vector<UserId>& users,
    uint32_t num_cards,
    std::mt19937_64* rng) {

    std::vector<int> ops = {0, 1, 2, 3, 4, 5};
    std::shuffle(ops.begin(), ops.end(), *rng);

    for (const int op : ops) {
        CandidateTree working = *candidate;
        bool changed = false;

        switch (op) {
        case 0:
            changed = ApplySplitLeaf(&working, rng);
            break;
        case 1:
            changed = ApplyMergeSiblingLeaves(&working, rng);
            break;
        case 2:
            changed = ApplyRebalanceCards(&working, rng);
            break;
        case 3:
            changed = ApplyMoveUserGroup(&working, users, rng);
            break;
        case 4:
            changed = ApplyReorderTemporal(&working, rng);
            break;
        case 5:
            changed = ApplyChangeLeafAllowedUsers(&working, users, rng);
            break;
        default:
            changed = false;
            break;
        }

        if (!changed) {
            continue;
        }

        if (!NormalizeTree(&working.tree, &working.root_id, num_cards)) {
            continue;
        }

        if (!ValidateTree(working.tree, working.root_id, num_cards)) {
            continue;
        }

        *candidate = std::move(working);
        return true;
    }

    return false;
}

std::vector<UserId> CollectUsers(const std::vector<Request>& workload) {
    std::vector<UserId> users;
    users.reserve(workload.size());
    for (const auto& req : workload) {
        users.push_back(req.user_id);
    }
    SortAndUnique(&users);
    return users;
}

} // namespace

HierarchicalTreeSearcher::HierarchicalTreeSearcher(TreeSearchOptions options)
    : options_(std::move(options)) {}

bool HierarchicalTreeSearcher::IsHierarchicalSchedulerKind(SchedulerKind kind) {
    return kind == SchedulerKind::HierarchicalA
        || kind == SchedulerKind::HierarchicalB
        || kind == SchedulerKind::HierarchicalC
        || kind == SchedulerKind::HierarchicalD;
}

std::vector<ResourceTreeNode> HierarchicalTreeSearcher::BuildDefaultInitialTree(
    const SystemState& state,
    const std::vector<Request>& workload,
    uint32_t* root_node_id) {

    std::vector<ResourceTreeNode> tree;
    *root_node_id = 0;

    std::vector<UserId> latency_users;
    std::vector<UserId> batch_users;
    {
        std::unordered_map<UserId, bool> user_latency;
        for (const auto& req : workload) {
            user_latency[req.user_id] = req.latency_sensitive;
        }
        for (const auto& it : user_latency) {
            if (it.second) {
                latency_users.push_back(it.first);
            } else {
                batch_users.push_back(it.first);
            }
        }
        SortAndUnique(&latency_users);
        SortAndUnique(&batch_users);
    }

    if (state.pools.size() >= 2) {
        ResourceTreeNode root;
        root.node_id = 0;
        root.node_name = "root";
        root.type = TreeNodeType::Spatial;
        root.children = {1, 2};
        root.spatial_policy = SpatialRoutePolicy::ByLatencyClass;

        ResourceTreeNode left;
        left.node_id = 1;
        left.node_name = "latency_leaf";
        left.type = TreeNodeType::Leaf;
        left.pool_id = state.pools[0].pool_id;
        left.affinity_preferred = true;
        left.allowed_users = latency_users;
        left.cards = state.pools[0].card_ids;

        ResourceTreeNode right;
        right.node_id = 2;
        right.node_name = "batch_leaf";
        right.type = TreeNodeType::Leaf;
        right.pool_id = state.pools[1].pool_id;
        right.affinity_preferred = true;
        right.allowed_users = batch_users;
        right.cards = state.pools[1].card_ids;

        root.cards = left.cards;
        root.cards.insert(root.cards.end(), right.cards.begin(), right.cards.end());
        SortAndUnique(&root.cards);

        tree = {root, left, right};
        return tree;
    }

    ResourceTreeNode root;
    root.node_id = 0;
    root.node_name = "root";
    root.type = TreeNodeType::Temporal;
    root.children = {1};

    ResourceTreeNode leaf;
    leaf.node_id = 1;
    leaf.node_name = "shared_leaf";
    leaf.type = TreeNodeType::Leaf;
    leaf.affinity_preferred = true;
    leaf.cards.reserve(state.cards.size());
    for (const auto& card : state.cards) {
        leaf.cards.push_back(card.card_id);
    }

    root.cards = leaf.cards;

    tree = {root, leaf};
    return tree;
}

TreeSearchResult HierarchicalTreeSearcher::Search(
    const ExperimentConfig& config,
    const SystemState& base_state,
    const std::vector<Request>& workload,
    const std::vector<ResourceTreeNode>& initial_tree,
    uint32_t initial_root_id,
    std::ostream& log) const {

    TreeSearchResult result;

    if (initial_tree.empty()) {
        result.error_message = "Tree search error: initial tree is empty";
        return result;
    }

    if (workload.empty()) {
        result.error_message = "Tree search error: workload is empty";
        return result;
    }

    CandidateTree current;
    current.tree = initial_tree;
    current.root_id = initial_root_id;
    current.op_name = "initial";

    if (!NormalizeTree(&current.tree, &current.root_id, config.num_cards)
        || !ValidateTree(current.tree, current.root_id, config.num_cards)) {
        result.error_message = "Tree search error: initial tree is invalid";
        return result;
    }

    SimulationMetrics current_metrics;
    try {
        current_metrics = EvaluateCandidateTree(
            config,
            base_state,
            workload,
            current.tree,
            current.root_id);
    } catch (const std::exception& ex) {
        result.error_message =
            std::string("Tree search error: failed to evaluate initial tree: ") + ex.what();
        return result;
    }

    const TreeSearchWeights weights = options_.weights;
    double current_objective = ComputeObjective(current_metrics, weights, workload.size());

    result.best_tree = current.tree;
    result.best_root_id = current.root_id;
    result.best_metrics = current_metrics;
    result.best_objective = current_objective;
    result.evaluated_candidates = 1;

    const std::vector<UserId> users = CollectUsers(workload);
    std::mt19937_64 rng(options_.seed);

    log << "TreeSearchInitialObjective=" << current_objective
        << " mean_latency=" << current_metrics.mean_latency
        << " p99=" << current_metrics.p99_latency
        << " fairness=" << current_metrics.jain_fairness_index
        << " reloads=" << current_metrics.total_reload_count
        << "\n";

    for (uint32_t step = 1; step <= options_.steps; ++step) {
        std::optional<EvaluatedCandidate> best_step_candidate;

        for (uint32_t k = 1; k <= options_.neighbors_per_step; ++k) {
            CandidateTree neighbor = current;
            if (!ApplyRandomNeighbor(&neighbor, users, config.num_cards, &rng)) {
                continue;
            }

            SimulationMetrics metrics;
            try {
                metrics = EvaluateCandidateTree(
                    config,
                    base_state,
                    workload,
                    neighbor.tree,
                    neighbor.root_id);
            } catch (const std::exception&) {
                continue;
            }

            const double objective = ComputeObjective(metrics, weights, workload.size());
            ++result.evaluated_candidates;

            log << "TreeSearchStep=" << step
                << " Candidate=" << k
                << " Op=" << neighbor.op_name
                << " Objective=" << objective
                << " mean_latency=" << metrics.mean_latency
                << " p99=" << metrics.p99_latency
                << " fairness=" << metrics.jain_fairness_index
                << " reloads=" << metrics.total_reload_count
                << "\n";

            if (!best_step_candidate.has_value()
                || objective < best_step_candidate->objective) {
                best_step_candidate = EvaluatedCandidate{
                    neighbor,
                    metrics,
                    objective};
            }

            if (objective < result.best_objective) {
                result.best_objective = objective;
                result.best_tree = neighbor.tree;
                result.best_root_id = neighbor.root_id;
                result.best_metrics = metrics;
            }
        }

        result.executed_steps = step;

        if (!best_step_candidate.has_value()) {
            log << "TreeSearchStep=" << step << " NoValidCandidates\n";
            continue;
        }

        if (best_step_candidate->objective + 1e-9 < current_objective) {
            current = best_step_candidate->candidate;
            current_objective = best_step_candidate->objective;
            log << "TreeSearchStep=" << step
                << " AcceptedOp=" << current.op_name
                << " Objective=" << current_objective
                << "\n";
        } else {
            log << "TreeSearchStep=" << step
                << " NoImprovement"
                << " CurrentObjective=" << current_objective
                << "\n";
        }
    }

    result.ok = true;
    return result;
}
