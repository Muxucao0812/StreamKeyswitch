#pragma once

#include "model/resource_tree.h"
#include "model/system_state.h"

#include <cstdint>
#include <string>
#include <vector>

struct PoolConfigLoadResult {
    bool ok = false;
    std::vector<ResourcePool> pools;
    std::string error_message;
};

struct TreeConfigLoadResult {
    bool ok = false;
    std::vector<ResourceTreeNode> resource_tree;
    uint32_t root_node_id = 0;
    std::string error_message;
};

struct TreeConfigSaveResult {
    bool ok = false;
    std::string error_message;
};

PoolConfigLoadResult LoadPoolsFromFile(
    const std::string& path,
    uint32_t num_cards);

TreeConfigLoadResult LoadTreeFromFile(
    const std::string& path,
    uint32_t num_cards);

TreeConfigSaveResult SaveTreeToFile(
    const std::string& path,
    const std::vector<ResourceTreeNode>& resource_tree,
    uint32_t root_node_id);
