#pragma once

#include "common/types.h"

#include <cstdint>
#include <iosfwd>
#include <string>

enum class SchedulerKind {
    FIFO,
    Affinity,
    StaticPartition,
    Score,
    Pool,
    HierarchicalA,
    HierarchicalB,
    HierarchicalC,
    HierarchicalD
};

enum class BackendKind {
    Analytical,
    Table,
    CycleStub,
    Hybrid
};

enum class WorkloadKind {
    Synthetic,
    Burst
};

struct ExperimentConfig {
    // Core mode switches.
    SchedulerKind scheduler = SchedulerKind::FIFO;
    BackendKind backend = BackendKind::Analytical;
    WorkloadKind workload = WorkloadKind::Synthetic;

    // Global experiment controls.
    uint64_t seed = 20260312ULL;
    uint32_t num_cards = 8;
    bool enable_multi_card = true;
    bool hybrid_use_table_coarse = false;

    // Workload size controls.
    uint32_t num_users = 8;

    // Synthetic workload parameters.
    uint32_t synthetic_requests_per_user = 4;
    Time synthetic_inter_arrival = 300;
    Time synthetic_start_time = 0;

    // Burst workload parameters.
    uint32_t burst_bursts = 4;
    uint32_t burst_requests_per_user_per_burst = 2;
    Time burst_intra_gap = 20;
    Time burst_inter_gap = 1200;
    Time burst_start_time = 0;
    uint32_t burst_level = 1;

    // Built-in pool layout parameters.
    uint32_t num_pools = 2;

    // External input/output files.
    std::string pool_config_path;
    std::string tree_config_path;
    std::string profile_table_path;
    // Optional HE parameter file used to derive key size and related workload fields.
    std::string he_params_path;
    std::string csv_output_path;

    // Tree-search controls.
    bool enable_tree_search = false;
    uint32_t tree_search_steps = 30;
    uint32_t tree_search_neighbors = 8;
    std::string tree_search_output_path;

    // Tree-search objective weights.
    double objective_w_mean_latency = 1.0;
    double objective_w_p99_latency = 1.0;
    double objective_w_fairness_penalty = 10000.0;
    double objective_w_reload_penalty = 10.0;
    double objective_w_incomplete_penalty = 1000000.0;
};

struct ParseExperimentConfigResult {
    bool ok = false;
    bool show_help = false;
    ExperimentConfig config;
    std::string error_message;
};

const char* ToString(SchedulerKind scheduler);
const char* ToString(BackendKind backend);
const char* ToString(WorkloadKind workload);

std::string BuildUsageText(const std::string& program_name);

ParseExperimentConfigResult ParseExperimentConfig(int argc, char** argv);

void PrintExperimentConfig(std::ostream& os, const ExperimentConfig& config);
