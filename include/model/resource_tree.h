#pragma once

#include "common/types.h"

#include <optional>
#include <string>
#include <vector>

enum class TreeNodeType {
    Temporal,
    Spatial,
    Leaf
};

enum class SpatialRoutePolicy {
    FirstFit,
    ByLatencyClass,
    ByUserHash
};

struct ResourceTreeNode {
    uint32_t node_id = 0;
    std::string node_name;
    TreeNodeType type = TreeNodeType::Leaf;

    std::vector<uint32_t> children;

    // Cards visible at this node. Empty means inherit from parent/whole state.
    std::vector<CardId> cards;

    // Optional user guard: if non-empty, only these users can enter this subtree.
    std::vector<UserId> allowed_users;

    // For leaf node: if empty, this leaf can use all pools.
    std::optional<uint32_t> pool_id;

    // For leaf node: whether to prioritize resident-user matching cards.
    bool affinity_preferred = false;

    // For spatial node.
    SpatialRoutePolicy spatial_policy = SpatialRoutePolicy::FirstFit;
};
