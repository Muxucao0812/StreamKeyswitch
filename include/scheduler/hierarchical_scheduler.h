#pragma once

#include "model/resource_tree.h"
#include "scheduler/scheduler.h"

#include <cstdint>
#include <deque>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

enum class FixedTreeKind {
    TreeA_Shared,
    TreeB_TwoPools,
    TreeC_UserPinned,
    TreeD_TwoPoolsAffinity
};

class HierarchicalScheduler : public Scheduler {
public:
    explicit HierarchicalScheduler(
        FixedTreeKind tree_kind = FixedTreeKind::TreeA_Shared,
        uint32_t num_pools = 2);

    void OnRequestArrival(const Request& req) override;

    std::optional<ExecutionPlan> TrySchedule(
        const SystemState& state,
        const ExecutionBackend& backend) override;

    void OnTaskFinished(
        const Request& req,
        const ExecutionPlan& plan,
        const ExecutionResult& result) override;

    bool Empty() const override;

private:
    uint32_t DecideCardCount(const Request& req) const;

    std::vector<ResourceTreeNode> BuildFixedTree(uint32_t num_pools) const;

    std::vector<uint32_t> ResolveSpatialChildren(
        const ResourceTreeNode& node,
        const Request& req) const;

    std::vector<CardId> CollectLeafCandidates(
        const ResourceTreeNode& node,
        const Request& req,
        const SystemState& state,
        const std::vector<CardId>& visible_idle_cards) const;

    std::optional<std::vector<CardId>> SelectCardsFromNode(
        const std::vector<ResourceTreeNode>& tree_nodes,
        const std::unordered_map<uint32_t, size_t>& node_index,
        uint32_t node_id,
        const Request& req,
        const SystemState& state,
        const std::vector<CardId>& visible_idle_cards,
        uint32_t required_cards) const;

    bool UserAllowed(const ResourceTreeNode& node, UserId user_id) const;

private:
    FixedTreeKind tree_kind_ = FixedTreeKind::TreeA_Shared;
    uint32_t num_pools_ = 1;

    std::vector<ResourceTreeNode> tree_;
    uint32_t root_node_id_ = 0;
    std::unordered_map<uint32_t, size_t> node_index_;

    std::deque<Request> queue_;
};
