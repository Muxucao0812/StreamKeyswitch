#pragma once

#include "common/experiment_config.h"
#include "model/request.h"
#include "model/resource_tree.h"
#include "model/system_state.h"
#include "sim/metrics.h"

#include <cstdint>
#include <iosfwd>
#include <string>
#include <vector>

struct TreeSearchWeights {
    double mean_latency = 1.0;
    double p99_latency = 1.0;
    double fairness_penalty = 10000.0;
    double reload_penalty = 10.0;
    double incomplete_penalty = 1000000.0;
};

struct TreeSearchOptions {
    uint32_t steps = 30;
    uint32_t neighbors_per_step = 8;
    uint64_t seed = 20260312ULL;
    TreeSearchWeights weights;
};

struct TreeSearchResult {
    bool ok = false;

    std::vector<ResourceTreeNode> best_tree;
    uint32_t best_root_id = 0;

    SimulationMetrics best_metrics;
    double best_objective = 0.0;

    uint64_t evaluated_candidates = 0;
    uint32_t executed_steps = 0;

    std::string error_message;
};

class HierarchicalTreeSearcher {
public:
    explicit HierarchicalTreeSearcher(TreeSearchOptions options);

    TreeSearchResult Search(
        const ExperimentConfig& config,
        const SystemState& base_state,
        const std::vector<Request>& workload,
        const std::vector<ResourceTreeNode>& initial_tree,
        uint32_t initial_root_id,
        std::ostream& log) const;

    static bool IsHierarchicalSchedulerKind(SchedulerKind kind);

    static std::vector<ResourceTreeNode> BuildDefaultInitialTree(
        const SystemState& state,
        const std::vector<Request>& workload,
        uint32_t* root_node_id);

private:
    TreeSearchOptions options_;
};
