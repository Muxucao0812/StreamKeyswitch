#include "common/config_loader.h"

#include <algorithm>
#include <cctype>
#include <functional>
#include <fstream>
#include <sstream>
#include <stack>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {

std::string Trim(const std::string& text) {
    size_t begin = 0;
    while (begin < text.size() && std::isspace(static_cast<unsigned char>(text[begin]))) {
        ++begin;
    }

    if (begin == text.size()) {
        return "";
    }

    size_t end = text.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(text[end - 1]))) {
        --end;
    }
    return text.substr(begin, end - begin);
}

std::vector<std::string> SplitBySpace(const std::string& line) {
    std::istringstream iss(line);
    std::vector<std::string> tokens;
    std::string token;
    while (iss >> token) {
        tokens.push_back(token);
    }
    return tokens;
}

bool ParseUInt32(const std::string& text, uint32_t* value) {
    if (text.empty()) {
        return false;
    }

    uint64_t out = 0;
    for (const char ch : text) {
        if (!std::isdigit(static_cast<unsigned char>(ch))) {
            return false;
        }
        out = out * 10 + static_cast<uint64_t>(ch - '0');
        if (out > UINT32_MAX) {
            return false;
        }
    }

    *value = static_cast<uint32_t>(out);
    return true;
}

bool ParseBool(const std::string& text, bool* value) {
    if (text == "1" || text == "true" || text == "TRUE" || text == "True") {
        *value = true;
        return true;
    }
    if (text == "0" || text == "false" || text == "FALSE" || text == "False") {
        *value = false;
        return true;
    }
    return false;
}

bool ParseCsvUInt32(const std::string& text, std::vector<uint32_t>* values) {
    values->clear();
    if (text.empty()) {
        return true;
    }

    std::stringstream ss(text);
    std::string item;
    while (std::getline(ss, item, ',')) {
        const std::string trimmed = Trim(item);
        if (trimmed.empty()) {
            continue;
        }
        uint32_t value = 0;
        if (!ParseUInt32(trimmed, &value)) {
            return false;
        }
        values->push_back(value);
    }
    return true;
}

bool ParseTreeNodeType(const std::string& text, TreeNodeType* type) {
    if (text == "Temporal" || text == "temporal") {
        *type = TreeNodeType::Temporal;
        return true;
    }
    if (text == "Spatial" || text == "spatial") {
        *type = TreeNodeType::Spatial;
        return true;
    }
    if (text == "Leaf" || text == "leaf") {
        *type = TreeNodeType::Leaf;
        return true;
    }
    return false;
}

bool ParseSpatialRoutePolicy(const std::string& text, SpatialRoutePolicy* policy) {
    if (text == "FirstFit" || text == "firstfit") {
        *policy = SpatialRoutePolicy::FirstFit;
        return true;
    }
    if (text == "ByLatencyClass" || text == "bylatencyclass") {
        *policy = SpatialRoutePolicy::ByLatencyClass;
        return true;
    }
    if (text == "ByUserHash" || text == "byuserhash") {
        *policy = SpatialRoutePolicy::ByUserHash;
        return true;
    }
    return false;
}

const char* TreeNodeTypeToText(TreeNodeType type) {
    switch (type) {
    case TreeNodeType::Temporal:
        return "Temporal";
    case TreeNodeType::Spatial:
        return "Spatial";
    case TreeNodeType::Leaf:
        return "Leaf";
    }
    return "Leaf";
}

const char* SpatialRoutePolicyToText(SpatialRoutePolicy policy) {
    switch (policy) {
    case SpatialRoutePolicy::FirstFit:
        return "FirstFit";
    case SpatialRoutePolicy::ByLatencyClass:
        return "ByLatencyClass";
    case SpatialRoutePolicy::ByUserHash:
        return "ByUserHash";
    }
    return "FirstFit";
}

template <typename T>
std::string JoinCsv(const std::vector<T>& values) {
    std::ostringstream oss;
    for (size_t i = 0; i < values.size(); ++i) {
        if (i != 0) {
            oss << ",";
        }
        oss << values[i];
    }
    return oss.str();
}

template <typename T>
void SortAndUnique(std::vector<T>* values) {
    std::sort(values->begin(), values->end());
    values->erase(std::unique(values->begin(), values->end()), values->end());
}

} // namespace

PoolConfigLoadResult LoadPoolsFromFile(
    const std::string& path,
    uint32_t num_cards) {

    PoolConfigLoadResult result;
    std::ifstream in(path);
    if (!in.is_open()) {
        result.error_message = "Failed to open pool config file: " + path;
        return result;
    }

    std::unordered_map<uint32_t, ResourcePool> pools_by_id;
    std::unordered_set<uint32_t> assigned_cards;

    std::string line;
    uint32_t line_no = 0;
    while (std::getline(in, line)) {
        ++line_no;
        const std::string trimmed = Trim(line);
        if (trimmed.empty() || trimmed[0] == '#') {
            continue;
        }

        const auto tokens = SplitBySpace(trimmed);
        if (tokens.empty()) {
            continue;
        }

        if (tokens[0] != "POOL") {
            result.error_message =
                "Pool config parse error at line " + std::to_string(line_no)
                + ": expected 'POOL'";
            return result;
        }

        if (tokens.size() < 3) {
            result.error_message =
                "Pool config parse error at line " + std::to_string(line_no)
                + ": expected 'POOL <pool_id> cards=...'";
            return result;
        }

        uint32_t pool_id = 0;
        if (!ParseUInt32(tokens[1], &pool_id)) {
            result.error_message =
                "Pool config parse error at line " + std::to_string(line_no)
                + ": invalid pool_id";
            return result;
        }

        ResourcePool pool;
        pool.pool_id = pool_id;

        bool has_cards = false;
        for (size_t i = 2; i < tokens.size(); ++i) {
            const auto pos = tokens[i].find('=');
            if (pos == std::string::npos || pos == 0 || pos + 1 >= tokens[i].size()) {
                result.error_message =
                    "Pool config parse error at line " + std::to_string(line_no)
                    + ": expected key=value token";
                return result;
            }

            const std::string key = tokens[i].substr(0, pos);
            const std::string value = tokens[i].substr(pos + 1);

            if (key == "cards") {
                std::vector<uint32_t> parsed_cards;
                if (!ParseCsvUInt32(value, &parsed_cards)) {
                    result.error_message =
                        "Pool config parse error at line " + std::to_string(line_no)
                        + ": invalid cards list";
                    return result;
                }
                pool.card_ids = std::move(parsed_cards);
                has_cards = true;
                continue;
            }

            if (key == "name") {
                pool.name = value;
                continue;
            }

            if (key == "tag") {
                if (value == "latency") {
                    pool.latency_sensitive_pool = true;
                } else if (value == "batch") {
                    pool.batch_pool = true;
                } else {
                    pool.name = value;
                }
                continue;
            }

            if (key == "latency_sensitive") {
                bool flag = false;
                if (!ParseBool(value, &flag)) {
                    result.error_message =
                        "Pool config parse error at line " + std::to_string(line_no)
                        + ": invalid latency_sensitive bool";
                    return result;
                }
                pool.latency_sensitive_pool = flag;
                continue;
            }

            if (key == "batch") {
                bool flag = false;
                if (!ParseBool(value, &flag)) {
                    result.error_message =
                        "Pool config parse error at line " + std::to_string(line_no)
                        + ": invalid batch bool";
                    return result;
                }
                pool.batch_pool = flag;
                continue;
            }

            result.error_message =
                "Pool config parse error at line " + std::to_string(line_no)
                + ": unknown key '" + key + "'";
            return result;
        }

        if (!has_cards) {
            result.error_message =
                "Pool config parse error at line " + std::to_string(line_no)
                + ": missing cards=...";
            return result;
        }

        SortAndUnique(&pool.card_ids);
        for (const CardId card_id : pool.card_ids) {
            if (card_id >= num_cards) {
                result.error_message =
                    "Pool config parse error at line " + std::to_string(line_no)
                    + ": card id out of range";
                return result;
            }
            if (assigned_cards.find(card_id) != assigned_cards.end()) {
                result.error_message =
                    "Pool config parse error at line " + std::to_string(line_no)
                    + ": duplicated card id " + std::to_string(card_id);
                return result;
            }
            assigned_cards.insert(card_id);
        }

        auto [it, inserted] = pools_by_id.emplace(pool_id, pool);
        if (!inserted) {
            result.error_message =
                "Pool config parse error at line " + std::to_string(line_no)
                + ": duplicated pool id " + std::to_string(pool_id);
            return result;
        }
    }

    if (pools_by_id.empty()) {
        result.error_message = "Pool config error: no POOL entries found";
        return result;
    }

    if (assigned_cards.size() != static_cast<size_t>(num_cards)) {
        result.error_message =
            "Pool config error: cards assigned="
            + std::to_string(assigned_cards.size())
            + ", expected=" + std::to_string(num_cards);
        return result;
    }

    result.pools.reserve(pools_by_id.size());
    for (auto& [pool_id, pool] : pools_by_id) {
        if (pool.name.empty()) {
            pool.name = "pool" + std::to_string(pool_id);
        }
        result.pools.push_back(pool);
    }

    std::sort(
        result.pools.begin(),
        result.pools.end(),
        [](const ResourcePool& a, const ResourcePool& b) {
            return a.pool_id < b.pool_id;
        });

    result.ok = true;
    return result;
}

TreeConfigLoadResult LoadTreeFromFile(
    const std::string& path,
    uint32_t num_cards) {

    TreeConfigLoadResult result;
    std::ifstream in(path);
    if (!in.is_open()) {
        result.error_message = "Failed to open tree config file: " + path;
        return result;
    }

    struct NodeDraft {
        ResourceTreeNode node;
        bool has_parent = false;
        std::string parent_name;
    };

    std::vector<NodeDraft> drafts;
    std::unordered_map<std::string, size_t> name_to_idx;
    std::string explicit_root_name;

    std::string line;
    uint32_t line_no = 0;
    while (std::getline(in, line)) {
        ++line_no;
        const std::string trimmed = Trim(line);
        if (trimmed.empty() || trimmed[0] == '#') {
            continue;
        }

        const auto tokens = SplitBySpace(trimmed);
        if (tokens.empty()) {
            continue;
        }

        if (tokens[0] == "ROOT") {
            if (tokens.size() != 2) {
                result.error_message =
                    "Tree config parse error at line " + std::to_string(line_no)
                    + ": ROOT expects one node name";
                return result;
            }
            explicit_root_name = tokens[1];
            continue;
        }

        if (tokens[0] != "NODE") {
            result.error_message =
                "Tree config parse error at line " + std::to_string(line_no)
                + ": expected NODE or ROOT";
            return result;
        }

        if (tokens.size() < 3) {
            result.error_message =
                "Tree config parse error at line " + std::to_string(line_no)
                + ": expected NODE <name> key=value...";
            return result;
        }

        const std::string node_name = tokens[1];
        if (name_to_idx.find(node_name) != name_to_idx.end()) {
            result.error_message =
                "Tree config parse error at line " + std::to_string(line_no)
                + ": duplicated node name '" + node_name + "'";
            return result;
        }

        NodeDraft draft;
        draft.node.node_id = static_cast<uint32_t>(drafts.size());
        draft.node.node_name = node_name;

        bool has_type = false;
        bool has_cards = false;
        for (size_t i = 2; i < tokens.size(); ++i) {
            const auto pos = tokens[i].find('=');
            if (pos == std::string::npos || pos == 0 || pos + 1 >= tokens[i].size()) {
                result.error_message =
                    "Tree config parse error at line " + std::to_string(line_no)
                    + ": expected key=value token";
                return result;
            }

            const std::string key = tokens[i].substr(0, pos);
            const std::string value = tokens[i].substr(pos + 1);

            if (key == "type") {
                if (!ParseTreeNodeType(value, &draft.node.type)) {
                    result.error_message =
                        "Tree config parse error at line " + std::to_string(line_no)
                        + ": invalid type '" + value + "'";
                    return result;
                }
                has_type = true;
                continue;
            }

            if (key == "cards") {
                std::vector<uint32_t> parsed_cards;
                if (!ParseCsvUInt32(value, &parsed_cards)) {
                    result.error_message =
                        "Tree config parse error at line " + std::to_string(line_no)
                        + ": invalid cards list";
                    return result;
                }
                draft.node.cards.assign(parsed_cards.begin(), parsed_cards.end());
                has_cards = true;
                continue;
            }

            if (key == "allowed_users") {
                std::vector<uint32_t> parsed_users;
                if (!ParseCsvUInt32(value, &parsed_users)) {
                    result.error_message =
                        "Tree config parse error at line " + std::to_string(line_no)
                        + ": invalid allowed_users list";
                    return result;
                }
                draft.node.allowed_users.assign(parsed_users.begin(), parsed_users.end());
                continue;
            }

            if (key == "parent") {
                draft.has_parent = true;
                draft.parent_name = value;
                continue;
            }

            if (key == "pool") {
                uint32_t pool_id = 0;
                if (!ParseUInt32(value, &pool_id)) {
                    result.error_message =
                        "Tree config parse error at line " + std::to_string(line_no)
                        + ": invalid pool id";
                    return result;
                }
                draft.node.pool_id = pool_id;
                continue;
            }

            if (key == "affinity") {
                bool affinity = false;
                if (!ParseBool(value, &affinity)) {
                    result.error_message =
                        "Tree config parse error at line " + std::to_string(line_no)
                        + ": invalid affinity bool";
                    return result;
                }
                draft.node.affinity_preferred = affinity;
                continue;
            }

            if (key == "route") {
                if (!ParseSpatialRoutePolicy(value, &draft.node.spatial_policy)) {
                    result.error_message =
                        "Tree config parse error at line " + std::to_string(line_no)
                        + ": invalid route '" + value + "'";
                    return result;
                }
                continue;
            }

            result.error_message =
                "Tree config parse error at line " + std::to_string(line_no)
                + ": unknown key '" + key + "'";
            return result;
        }

        if (!has_type) {
            result.error_message =
                "Tree config parse error at line " + std::to_string(line_no)
                + ": missing type=...";
            return result;
        }
        if (!has_cards) {
            result.error_message =
                "Tree config parse error at line " + std::to_string(line_no)
                + ": missing cards=...";
            return result;
        }

        SortAndUnique(&draft.node.cards);
        for (const CardId card_id : draft.node.cards) {
            if (card_id >= num_cards) {
                result.error_message =
                    "Tree config parse error at line " + std::to_string(line_no)
                    + ": card id out of range";
                return result;
            }
        }
        SortAndUnique(&draft.node.allowed_users);

        name_to_idx.emplace(node_name, drafts.size());
        drafts.push_back(std::move(draft));
    }

    if (drafts.empty()) {
        result.error_message = "Tree config error: no NODE entries found";
        return result;
    }

    for (auto& draft : drafts) {
        if (!draft.has_parent) {
            continue;
        }

        const auto parent_it = name_to_idx.find(draft.parent_name);
        if (parent_it == name_to_idx.end()) {
            result.error_message =
                "Tree config error: parent node '" + draft.parent_name + "' not found";
            return result;
        }

        NodeDraft& parent = drafts[parent_it->second];
        parent.node.children.push_back(draft.node.node_id);
    }

    uint32_t root_id = 0;
    if (!explicit_root_name.empty()) {
        const auto root_it = name_to_idx.find(explicit_root_name);
        if (root_it == name_to_idx.end()) {
            result.error_message =
                "Tree config error: ROOT node '" + explicit_root_name + "' not found";
            return result;
        }
        root_id = drafts[root_it->second].node.node_id;
    } else {
        std::vector<uint32_t> roots;
        for (const auto& draft : drafts) {
            if (!draft.has_parent) {
                roots.push_back(draft.node.node_id);
            }
        }
        if (roots.size() != 1) {
            result.error_message =
                "Tree config error: expected exactly one implicit root, got "
                + std::to_string(roots.size());
            return result;
        }
        root_id = roots.front();
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
        const auto& node = drafts.at(node_id).node;
        for (const uint32_t child_id : node.children) {
            if (child_id >= drafts.size()) {
                return false;
            }
            if (!dfs(child_id)) {
                return false;
            }
        }
        visiting.erase(node_id);
        visited.insert(node_id);
        return true;
    };

    if (!dfs(root_id)) {
        result.error_message = "Tree config error: cycle detected or invalid child link";
        return result;
    }

    if (visited.size() != drafts.size()) {
        result.error_message =
            "Tree config error: tree is disconnected from root";
        return result;
    }

    result.resource_tree.reserve(drafts.size());
    for (auto& draft : drafts) {
        result.resource_tree.push_back(std::move(draft.node));
    }
    result.root_node_id = root_id;
    result.ok = true;
    return result;
}

TreeConfigSaveResult SaveTreeToFile(
    const std::string& path,
    const std::vector<ResourceTreeNode>& resource_tree,
    uint32_t root_node_id) {

    TreeConfigSaveResult result;

    if (resource_tree.empty()) {
        result.error_message = "Tree save error: empty resource_tree";
        return result;
    }

    std::unordered_map<uint32_t, size_t> node_index;
    node_index.reserve(resource_tree.size());
    for (size_t i = 0; i < resource_tree.size(); ++i) {
        node_index[resource_tree[i].node_id] = i;
    }

    const auto root_it = node_index.find(root_node_id);
    if (root_it == node_index.end()) {
        result.error_message =
            "Tree save error: root_node_id not found";
        return result;
    }

    std::unordered_map<uint32_t, uint32_t> parent_of;
    for (const auto& node : resource_tree) {
        for (const uint32_t child_id : node.children) {
            if (node_index.find(child_id) == node_index.end()) {
                result.error_message =
                    "Tree save error: child node id " + std::to_string(child_id) + " not found";
                return result;
            }
            const auto [it, inserted] =
                parent_of.emplace(child_id, node.node_id);
            if (!inserted) {
                result.error_message =
                    "Tree save error: node " + std::to_string(child_id)
                    + " has multiple parents";
                return result;
            }
        }
    }

    std::vector<const ResourceTreeNode*> ordered;
    ordered.reserve(resource_tree.size());
    for (const auto& node : resource_tree) {
        ordered.push_back(&node);
    }
    std::sort(
        ordered.begin(),
        ordered.end(),
        [](const ResourceTreeNode* a, const ResourceTreeNode* b) {
            return a->node_id < b->node_id;
        });

    std::ofstream out(path);
    if (!out.is_open()) {
        result.error_message = "Tree save error: failed to open " + path;
        return result;
    }

    const ResourceTreeNode& root = resource_tree[root_it->second];
    const std::string root_name = root.node_name.empty()
        ? ("node_" + std::to_string(root.node_id))
        : root.node_name;
    out << "ROOT " << root_name << "\n";

    for (const ResourceTreeNode* node_ptr : ordered) {
        const ResourceTreeNode& node = *node_ptr;
        const std::string node_name = node.node_name.empty()
            ? ("node_" + std::to_string(node.node_id))
            : node.node_name;

        out << "NODE " << node_name
            << " type=" << TreeNodeTypeToText(node.type)
            << " cards=" << JoinCsv(node.cards);

        if (!node.allowed_users.empty()) {
            out << " allowed_users=" << JoinCsv(node.allowed_users);
        }

        const auto parent_it = parent_of.find(node.node_id);
        if (parent_it != parent_of.end()) {
            const ResourceTreeNode& parent = resource_tree.at(node_index.at(parent_it->second));
            const std::string parent_name = parent.node_name.empty()
                ? ("node_" + std::to_string(parent.node_id))
                : parent.node_name;
            out << " parent=" << parent_name;
        }

        if (node.pool_id.has_value()) {
            out << " pool=" << node.pool_id.value();
        }

        out << " affinity=" << (node.affinity_preferred ? "true" : "false");

        if (node.type == TreeNodeType::Spatial) {
            out << " route=" << SpatialRoutePolicyToText(node.spatial_policy);
        }

        out << "\n";
    }

    result.ok = true;
    return result;
}
