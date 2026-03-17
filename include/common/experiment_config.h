#pragma once

#include "common/types.h"
#include "model/request.h"

#include <chrono>
#include <cstdint>
#include <iosfwd>
#include <string>

#ifndef KEYAWARE_ALWAYS_DUMP_LOGICAL_GRAPH
#define KEYAWARE_ALWAYS_DUMP_LOGICAL_GRAPH 0
#endif

#ifndef KEYAWARE_ALWAYS_DUMP_RUNTIME_PLAN
#define KEYAWARE_ALWAYS_DUMP_RUNTIME_PLAN 0
#endif

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
    CycleStub
};

enum class WorkloadKind {
    Synthetic,
    Burst
};

struct ExperimentConfig {
    // Core mode switches.
    SchedulerKind scheduler = SchedulerKind::FIFO;
    BackendKind backend = BackendKind::CycleStub;
    WorkloadKind workload = WorkloadKind::Synthetic;

    // Global experiment controls.
    uint64_t seed = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count()
        );
    uint32_t num_cards = 1;
    bool enable_multi_card = true;

    // Workload size controls.
    uint32_t num_users = 1;

    // Synthetic workload parameters.
    uint32_t synthetic_requests_per_user = 1;
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
    uint32_t num_pools = 1;

    // External input/output files.
    std::string pool_config_path;
    std::string tree_config_path;
    // Optional HE parameter file used to derive key size and related workload fields.
    std::string he_params_path;
    std::string csv_output_path;

    bool dump_logical_graph = (KEYAWARE_ALWAYS_DUMP_LOGICAL_GRAPH != 0);
    bool dump_runtime_plan = (KEYAWARE_ALWAYS_DUMP_RUNTIME_PLAN != 0);

    // Keyswitch execution method selection.
    KeySwitchMethod keyswitch_method = KeySwitchMethod::Poseidon;
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
