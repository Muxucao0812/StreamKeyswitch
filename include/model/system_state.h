#pragma once

#include "model/card.h"
#include "model/resource_tree.h"
#include <string>
#include <vector>

struct ResourcePool {
    uint32_t pool_id = 0;
    std::string name;
    bool latency_sensitive_pool = false;
    bool batch_pool = false;
    std::vector<CardId> card_ids;
};

struct SystemState {
    Time now = 0;
    std::vector<CardState> cards;
    std::vector<ResourcePool> pools;
    std::vector<ResourceTreeNode> resource_tree;
    uint32_t resource_tree_root = 0;
};
