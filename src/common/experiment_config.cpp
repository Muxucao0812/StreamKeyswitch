#include "common/experiment_config.h"

#include <climits>
#include <cctype>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <sstream>
#include <unordered_map>
#include <vector>

namespace {

bool ParseUint64(const std::string& text, uint64_t* value) {
    if (text.empty()) {
        return false;
    }

    uint64_t out = 0;
    for (const char ch : text) {
        if (!std::isdigit(static_cast<unsigned char>(ch))) {
            return false;
        }
        const uint64_t digit = static_cast<uint64_t>(ch - '0');
        const uint64_t max_u64 = std::numeric_limits<uint64_t>::max();
        if (out > (max_u64 - digit) / 10) {
            return false;
        }
        out = out * 10 + digit;
    }
    *value = out;
    return true;
}

bool ParseUint32(const std::string& text, uint32_t* value) {
    uint64_t wide = 0;
    if (!ParseUint64(text, &wide)) {
        return false;
    }
    if (wide > static_cast<uint64_t>(UINT32_MAX)) {
        return false;
    }
    *value = static_cast<uint32_t>(wide);
    return true;
}

bool ParseDouble(const std::string& text, double* value) {
    if (text.empty()) {
        return false;
    }

    char* end = nullptr;
    const double out = std::strtod(text.c_str(), &end);
    if (end == text.c_str() || *end != '\0') {
        return false;
    }

    *value = out;
    return true;
}

bool ParseSchedulerKind(
    const std::string& text,
    SchedulerKind* value) {
    static const std::unordered_map<std::string, SchedulerKind> mapping = {
        {"fifo", SchedulerKind::FIFO},
        {"affinity", SchedulerKind::Affinity},
        {"static_partition", SchedulerKind::StaticPartition},
        {"score", SchedulerKind::Score},
        {"pool", SchedulerKind::Pool},
        {"hierarchical_a", SchedulerKind::HierarchicalA},
        {"hierarchical_b", SchedulerKind::HierarchicalB},
        {"hierarchical_c", SchedulerKind::HierarchicalC},
        {"hierarchical_d", SchedulerKind::HierarchicalD},
        {"hierarchical", SchedulerKind::HierarchicalD}};

    const auto it = mapping.find(text);
    if (it == mapping.end()) {
        return false;
    }
    *value = it->second;
    return true;
}

bool ParseBackendKind(
    const std::string& text,
    BackendKind* value) {
    static const std::unordered_map<std::string, BackendKind> mapping = {
        {"cycle", BackendKind::CycleStub},
        {"cycle_stub", BackendKind::CycleStub},
    };

    const auto it = mapping.find(text);
    if (it == mapping.end()) {
        return false;
    }
    *value = it->second;
    return true;
}

bool ParseWorkloadKind(
    const std::string& text,
    WorkloadKind* value) {
    static const std::unordered_map<std::string, WorkloadKind> mapping = {
        {"synthetic", WorkloadKind::Synthetic},
        {"burst", WorkloadKind::Burst}};

    const auto it = mapping.find(text);
    if (it == mapping.end()) {
        return false;
    }
    *value = it->second;
    return true;
}

bool ParseKeySwitchMethod(
    const std::string& text,
    KeySwitchMethod* value) {
    static const std::unordered_map<std::string, KeySwitchMethod> mapping = {
        {"poseidon", KeySwitchMethod::Poseidon},
        {"fab", KeySwitchMethod::FAB},
        {"fast", KeySwitchMethod::FAST},
        {"ola", KeySwitchMethod::OLA},
        {"hera", KeySwitchMethod::HERA},
        {"cinnamon", KeySwitchMethod::Cinnamon}};

    const auto it = mapping.find(text);
    if (it == mapping.end()) {
        return false;
    }
    *value = it->second;
    return true;
}

const char* ToString(KeySwitchMethod method) {
    switch (method) {
    case KeySwitchMethod::Auto:
        return "auto";
    case KeySwitchMethod::Poseidon:
        return "poseidon";
    case KeySwitchMethod::FAB:
        return "fab";
    case KeySwitchMethod::FAST:
        return "fast";
    case KeySwitchMethod::OLA:
        return "ola";
    case KeySwitchMethod::HERA:
        return "hera";
    case KeySwitchMethod::Cinnamon:
        return "cinnamon";
    case KeySwitchMethod::SingleBoardClassic:
        return "single_board_classic";
    case KeySwitchMethod::SingleBoardFused:
        return "single_board_fused";
    case KeySwitchMethod::ScaleOutLimb:
        return "scaleout_limb";
    case KeySwitchMethod::ScaleOutDigit:
        return "scaleout_digit";
    case KeySwitchMethod::ScaleOutCiphertext:
        return "scaleout_ciphertext";
    }
    return "poseidon";
}

} // namespace

const char* ToString(SchedulerKind scheduler) {
    switch (scheduler) {
    case SchedulerKind::FIFO:
        return "fifo";
    case SchedulerKind::Affinity:
        return "affinity";
    case SchedulerKind::StaticPartition:
        return "static_partition";
    case SchedulerKind::Score:
        return "score";
    case SchedulerKind::Pool:
        return "pool";
    case SchedulerKind::HierarchicalA:
        return "hierarchical_a";
    case SchedulerKind::HierarchicalB:
        return "hierarchical_b";
    case SchedulerKind::HierarchicalC:
        return "hierarchical_c";
    case SchedulerKind::HierarchicalD:
        return "hierarchical_d";
    }
    return "fifo";
}

const char* ToString(BackendKind backend) {
    switch (backend) {
    case BackendKind::CycleStub:
        return "cycle_stub";
    }
    return "cycle_stub";
}

const char* ToString(WorkloadKind workload) {
    switch (workload) {
    case WorkloadKind::Synthetic:
        return "synthetic";
    case WorkloadKind::Burst:
        return "burst";
    }
    return "synthetic";
}

std::string BuildUsageText(const std::string& program_name) {
    std::ostringstream oss;
    oss
        << "Usage:\n"
        << "  " << program_name << " [scheduler] [workload|backend] [workload|backend] [options]\n"
        << "  " << program_name << " [options]\n\n"
        << "Schedulers:\n"
        << "  fifo | affinity | static_partition | score | pool |\n"
        << "  hierarchical_a | hierarchical_b | hierarchical_c | hierarchical_d | hierarchical\n\n"
        << "Backends:\n"
        << "  cycle | cycle_stub\n\n"
        << "Workloads:\n"
        << "  synthetic | burst\n\n"
        << "Options:\n"
        << "  --scheduler <name>                    Select scheduler policy.\n"
        << "  --backend <name>                      Select backend runtime (cycle/cycle_stub).\n"
        << "  --workload <name>                     Select workload generator mode.\n"
        << "  --seed <uint64>                       RNG seed for reproducible runs.\n"
        << "  --num-cards <uint32>                  Number of accelerator cards.\n"
        << "  --num-users <uint32>                  Number of logical users.\n"
        << "  --requests-per-user <uint32>          Synthetic: requests per user.\n"
        << "  --inter-arrival <uint64>              Synthetic: inter-arrival gap.\n"
        << "  --synthetic-start-time <uint64>       Synthetic: first arrival time.\n"
        << "  --bursts <uint32>                     Burst: number of bursts.\n"
        << "  --requests-per-user-per-burst <uint32> Burst: requests per user in each burst.\n"
        << "  --intra-burst-gap <uint64>            Burst: gap between requests in one burst.\n"
        << "  --inter-burst-gap <uint64>            Burst: gap between bursts.\n"
        << "  --burst-start-time <uint64>           Burst: first burst start time.\n"
        << "  --burst-level <uint32>                Burst intensity multiplier.\n"
        << "  --num-pools <uint32>                  Number of pools for built-in pool layout.\n"
        << "  --enable-multi-card                   Allow requests to use multiple cards.\n"
        << "  --disable-multi-card                  Force single-card requests only.\n"
        << "  --pool-config <path>                  External pool config file.\n"
        << "  --tree-config <path>                  External resource-tree config file.\n"
        << "  --he-params <path>                    HE parameter file for workload derivation.\n"
        << "  --ks-method <poseidon|fab|fast|ola|hera|cinnamon>\n"
        << "                                        Keyswitch execution method for generated requests.\n"
        << "  --csv-output <path>                   Append run metrics to CSV file.\n"
        << "  --search-tree                         Enable tree search before simulation.\n"
        << "  --search-steps <uint32>               Tree search optimization steps.\n"
        << "  --search-neighbors <uint32>           Tree neighbors sampled per step.\n"
        << "  --search-output-tree <path>           Output path of optimized tree.\n"
        << "  --obj-w-mean-latency <double>         Objective weight: mean latency.\n"
        << "  --obj-w-p99-latency <double>          Objective weight: p99 latency.\n"
        << "  --obj-w-fairness-penalty <double>     Objective weight: fairness penalty.\n"
        << "  --obj-w-reload-penalty <double>       Objective weight: reload penalty.\n"
        << "  --obj-w-incomplete-penalty <double>   Objective weight: incomplete penalty.\n"
        << "  --help | -h                           Show help and exit.\n";
    return oss.str();
}

ParseExperimentConfigResult ParseExperimentConfig(int argc, char** argv) {
    ParseExperimentConfigResult result;
    result.ok = false;
    result.show_help = false;

    ExperimentConfig config;

    bool scheduler_set = false;
    bool backend_set = false;
    bool workload_set = false;

    // We support both:
    // 1) positional style: <scheduler> <workload/backend> <workload/backend>
    // 2) explicit style: --scheduler/--backend/--workload ...
    // Explicit options are strictly validated and must not conflict with positional values.
    std::vector<std::string> positional;
    positional.reserve(3);

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];

        if (arg == "--help" || arg == "-h") {
            result.show_help = true;
            result.config = config;
            return result;
        }

        if (!arg.empty() && arg[0] == '-') {
            auto require_value = [&](const char* option_name, std::string* out) -> bool {
                if (i + 1 >= argc) {
                    result.error_message =
                        std::string("Missing value for option ") + option_name;
                    return false;
                }
                *out = argv[++i];
                return true;
            };

            if (arg == "--scheduler") {
                std::string value_text;
                if (!require_value("--scheduler", &value_text)) {
                    return result;
                }
                SchedulerKind scheduler;
                if (!ParseSchedulerKind(value_text, &scheduler)) {
                    result.error_message =
                        "Invalid scheduler: " + value_text;
                    return result;
                }
                config.scheduler = scheduler;
                scheduler_set = true;
                continue;
            }

            if (arg == "--backend") {
                std::string value_text;
                if (!require_value("--backend", &value_text)) {
                    return result;
                }
                BackendKind backend;
                if (!ParseBackendKind(value_text, &backend)) {
                    result.error_message =
                        "Invalid backend: " + value_text;
                    return result;
                }
                config.backend = backend;
                backend_set = true;
                continue;
            }

            if (arg == "--workload") {
                std::string value_text;
                if (!require_value("--workload", &value_text)) {
                    return result;
                }
                WorkloadKind workload;
                if (!ParseWorkloadKind(value_text, &workload)) {
                    result.error_message =
                        "Invalid workload: " + value_text;
                    return result;
                }
                config.workload = workload;
                workload_set = true;
                continue;
            }

            if (arg == "--seed") {
                std::string value_text;
                if (!require_value("--seed", &value_text)) {
                    return result;
                }
                uint64_t seed = 0;
                if (!ParseUint64(value_text, &seed)) {
                    result.error_message = "Invalid seed: " + value_text;
                    return result;
                }
                config.seed = seed;
                continue;
            }

            if (arg == "--num-cards") {
                std::string value_text;
                if (!require_value("--num-cards", &value_text)) {
                    return result;
                }
                uint32_t num_cards = 0;
                if (!ParseUint32(value_text, &num_cards) || num_cards == 0) {
                    result.error_message = "Invalid --num-cards: " + value_text;
                    return result;
                }
                config.num_cards = num_cards;
                continue;
            }

            if (arg == "--num-users") {
                std::string value_text;
                if (!require_value("--num-users", &value_text)) {
                    return result;
                }
                uint32_t num_users = 0;
                if (!ParseUint32(value_text, &num_users) || num_users == 0) {
                    result.error_message = "Invalid --num-users: " + value_text;
                    return result;
                }
                config.num_users = num_users;
                continue;
            }

            if (arg == "--requests-per-user") {
                std::string value_text;
                if (!require_value("--requests-per-user", &value_text)) {
                    return result;
                }
                uint32_t requests_per_user = 0;
                if (!ParseUint32(value_text, &requests_per_user) || requests_per_user == 0) {
                    result.error_message = "Invalid --requests-per-user: " + value_text;
                    return result;
                }
                config.synthetic_requests_per_user = requests_per_user;
                continue;
            }

            if (arg == "--inter-arrival") {
                std::string value_text;
                if (!require_value("--inter-arrival", &value_text)) {
                    return result;
                }
                uint64_t value = 0;
                if (!ParseUint64(value_text, &value)) {
                    result.error_message = "Invalid --inter-arrival: " + value_text;
                    return result;
                }
                config.synthetic_inter_arrival = value;
                continue;
            }

            if (arg == "--synthetic-start-time") {
                std::string value_text;
                if (!require_value("--synthetic-start-time", &value_text)) {
                    return result;
                }
                uint64_t value = 0;
                if (!ParseUint64(value_text, &value)) {
                    result.error_message = "Invalid --synthetic-start-time: " + value_text;
                    return result;
                }
                config.synthetic_start_time = value;
                continue;
            }

            if (arg == "--bursts") {
                std::string value_text;
                if (!require_value("--bursts", &value_text)) {
                    return result;
                }
                uint32_t bursts = 0;
                if (!ParseUint32(value_text, &bursts) || bursts == 0) {
                    result.error_message = "Invalid --bursts: " + value_text;
                    return result;
                }
                config.burst_bursts = bursts;
                continue;
            }

            if (arg == "--requests-per-user-per-burst") {
                std::string value_text;
                if (!require_value("--requests-per-user-per-burst", &value_text)) {
                    return result;
                }
                uint32_t value = 0;
                if (!ParseUint32(value_text, &value) || value == 0) {
                    result.error_message = "Invalid --requests-per-user-per-burst: " + value_text;
                    return result;
                }
                config.burst_requests_per_user_per_burst = value;
                continue;
            }

            if (arg == "--intra-burst-gap") {
                std::string value_text;
                if (!require_value("--intra-burst-gap", &value_text)) {
                    return result;
                }
                uint64_t value = 0;
                if (!ParseUint64(value_text, &value)) {
                    result.error_message = "Invalid --intra-burst-gap: " + value_text;
                    return result;
                }
                config.burst_intra_gap = value;
                continue;
            }

            if (arg == "--inter-burst-gap") {
                std::string value_text;
                if (!require_value("--inter-burst-gap", &value_text)) {
                    return result;
                }
                uint64_t value = 0;
                if (!ParseUint64(value_text, &value)) {
                    result.error_message = "Invalid --inter-burst-gap: " + value_text;
                    return result;
                }
                config.burst_inter_gap = value;
                continue;
            }

            if (arg == "--burst-start-time") {
                std::string value_text;
                if (!require_value("--burst-start-time", &value_text)) {
                    return result;
                }
                uint64_t value = 0;
                if (!ParseUint64(value_text, &value)) {
                    result.error_message = "Invalid --burst-start-time: " + value_text;
                    return result;
                }
                config.burst_start_time = value;
                continue;
            }

            if (arg == "--burst-level") {
                std::string value_text;
                if (!require_value("--burst-level", &value_text)) {
                    return result;
                }
                uint32_t level = 0;
                if (!ParseUint32(value_text, &level) || level == 0) {
                    result.error_message = "Invalid --burst-level: " + value_text;
                    return result;
                }
                config.burst_level = level;
                continue;
            }

            if (arg == "--num-pools") {
                std::string value_text;
                if (!require_value("--num-pools", &value_text)) {
                    return result;
                }
                uint32_t num_pools = 0;
                if (!ParseUint32(value_text, &num_pools) || num_pools == 0) {
                    result.error_message = "Invalid --num-pools: " + value_text;
                    return result;
                }
                config.num_pools = num_pools;
                continue;
            }

            if (arg == "--enable-multi-card") {
                config.enable_multi_card = true;
                continue;
            }

            if (arg == "--disable-multi-card") {
                config.enable_multi_card = false;
                continue;
            }

            if (arg == "--pool-config") {
                std::string value_text;
                if (!require_value("--pool-config", &value_text)) {
                    return result;
                }
                config.pool_config_path = value_text;
                continue;
            }

            if (arg == "--tree-config") {
                std::string value_text;
                if (!require_value("--tree-config", &value_text)) {
                    return result;
                }
                config.tree_config_path = value_text;
                continue;
            }

            if (arg == "--he-params") {
                std::string value_text;
                if (!require_value("--he-params", &value_text)) {
                    return result;
                }
                config.he_params_path = value_text;
                continue;
            }

            if (arg == "--ks-method") {
                std::string value_text;
                if (!require_value("--ks-method", &value_text)) {
                    return result;
                }
                KeySwitchMethod method = KeySwitchMethod::Poseidon;
                if (!ParseKeySwitchMethod(value_text, &method)) {
                    result.error_message =
                        "Invalid --ks-method: " + value_text
                        + " (expected poseidon|fab|fast|ola|hera|cinnamon)";
                    return result;
                }
                config.keyswitch_method = method;
                continue;
            }

            if (arg == "--csv-output") {
                std::string value_text;
                if (!require_value("--csv-output", &value_text)) {
                    return result;
                }
                if (value_text.empty()) {
                    result.error_message = "Invalid --csv-output: empty path";
                    return result;
                }
                config.csv_output_path = value_text;
                continue;
            }

            if (arg == "--search-tree") {
                config.enable_tree_search = true;
                continue;
            }

            if (arg == "--search-steps") {
                std::string value_text;
                if (!require_value("--search-steps", &value_text)) {
                    return result;
                }
                uint32_t value = 0;
                if (!ParseUint32(value_text, &value) || value == 0) {
                    result.error_message = "Invalid --search-steps: " + value_text;
                    return result;
                }
                config.tree_search_steps = value;
                continue;
            }

            if (arg == "--search-neighbors") {
                std::string value_text;
                if (!require_value("--search-neighbors", &value_text)) {
                    return result;
                }
                uint32_t value = 0;
                if (!ParseUint32(value_text, &value) || value == 0) {
                    result.error_message = "Invalid --search-neighbors: " + value_text;
                    return result;
                }
                config.tree_search_neighbors = value;
                continue;
            }

            if (arg == "--search-output-tree") {
                std::string value_text;
                if (!require_value("--search-output-tree", &value_text)) {
                    return result;
                }
                if (value_text.empty()) {
                    result.error_message = "Invalid --search-output-tree: empty path";
                    return result;
                }
                config.tree_search_output_path = value_text;
                continue;
            }

            if (arg == "--obj-w-mean-latency") {
                std::string value_text;
                if (!require_value("--obj-w-mean-latency", &value_text)) {
                    return result;
                }
                double value = 0.0;
                if (!ParseDouble(value_text, &value) || value < 0.0) {
                    result.error_message = "Invalid --obj-w-mean-latency: " + value_text;
                    return result;
                }
                config.objective_w_mean_latency = value;
                continue;
            }

            if (arg == "--obj-w-p99-latency") {
                std::string value_text;
                if (!require_value("--obj-w-p99-latency", &value_text)) {
                    return result;
                }
                double value = 0.0;
                if (!ParseDouble(value_text, &value) || value < 0.0) {
                    result.error_message = "Invalid --obj-w-p99-latency: " + value_text;
                    return result;
                }
                config.objective_w_p99_latency = value;
                continue;
            }

            if (arg == "--obj-w-fairness-penalty") {
                std::string value_text;
                if (!require_value("--obj-w-fairness-penalty", &value_text)) {
                    return result;
                }
                double value = 0.0;
                if (!ParseDouble(value_text, &value) || value < 0.0) {
                    result.error_message = "Invalid --obj-w-fairness-penalty: " + value_text;
                    return result;
                }
                config.objective_w_fairness_penalty = value;
                continue;
            }

            if (arg == "--obj-w-reload-penalty") {
                std::string value_text;
                if (!require_value("--obj-w-reload-penalty", &value_text)) {
                    return result;
                }
                double value = 0.0;
                if (!ParseDouble(value_text, &value) || value < 0.0) {
                    result.error_message = "Invalid --obj-w-reload-penalty: " + value_text;
                    return result;
                }
                config.objective_w_reload_penalty = value;
                continue;
            }

            if (arg == "--obj-w-incomplete-penalty") {
                std::string value_text;
                if (!require_value("--obj-w-incomplete-penalty", &value_text)) {
                    return result;
                }
                double value = 0.0;
                if (!ParseDouble(value_text, &value) || value < 0.0) {
                    result.error_message = "Invalid --obj-w-incomplete-penalty: " + value_text;
                    return result;
                }
                config.objective_w_incomplete_penalty = value;
                continue;
            }

            // Strict mode: unknown options are rejected to avoid silent misconfiguration.
            result.error_message = "Unknown option: " + arg;
            return result;
        }

        positional.push_back(arg);
    }

    if (positional.size() > 3) {
        result.error_message = "Too many positional arguments.";
        return result;
    }

    auto set_scheduler = [&](SchedulerKind value) -> bool {
        if (scheduler_set && config.scheduler != value) {
            result.error_message =
                "Conflicting scheduler options.";
            return false;
        }
        config.scheduler = value;
        scheduler_set = true;
        return true;
    };

    auto set_backend = [&](BackendKind value) -> bool {
        if (backend_set && config.backend != value) {
            result.error_message =
                "Conflicting backend options.";
            return false;
        }
        config.backend = value;
        backend_set = true;
        return true;
    };

    auto set_workload = [&](WorkloadKind value) -> bool {
        if (workload_set && config.workload != value) {
            result.error_message =
                "Conflicting workload options.";
            return false;
        }
        config.workload = value;
        workload_set = true;
        return true;
    };

    // Positional arg #1: scheduler
    if (!positional.empty()) {
        SchedulerKind scheduler;
        if (!ParseSchedulerKind(positional[0], &scheduler)) {
            result.error_message = "Invalid scheduler: " + positional[0];
            return result;
        }
        if (!set_scheduler(scheduler)) {
            return result;
        }
    }

    // Positional arg #2: either workload or backend
    if (positional.size() >= 2) {
        BackendKind backend;
        WorkloadKind workload;
        const std::string& token = positional[1];

        if (ParseWorkloadKind(token, &workload)) {
            if (!set_workload(workload)) {
                return result;
            }
        } else if (ParseBackendKind(token, &backend)) {
            if (!set_backend(backend)) {
                return result;
            }
        } else {
            result.error_message =
                "Second positional argument must be workload or backend: " + token;
            return result;
        }
    }

    // Positional arg #3: remaining one (workload/backend).
    if (positional.size() >= 3) {
        BackendKind backend;
        WorkloadKind workload;
        const std::string& token = positional[2];

        const bool workload_ok = ParseWorkloadKind(token, &workload);
        const bool backend_ok = ParseBackendKind(token, &backend);

        if (workload_ok && backend_ok) {
            result.error_message =
                "Ambiguous positional argument: " + token;
            return result;
        }

        if (workload_ok) {
            if (!set_workload(workload)) {
                return result;
            }
        } else if (backend_ok) {
            if (!set_backend(backend)) {
                return result;
            }
        } else {
            result.error_message =
                "Third positional argument must be workload or backend: " + token;
            return result;
        }
    }

    result.ok = true;
    result.config = config;
    return result;
}

void PrintExperimentConfig(std::ostream& os, const ExperimentConfig& config) {
    os << "=== Experiment Config ===\n";
    os << "Scheduler: " << ToString(config.scheduler) << "\n";
    os << "Backend: " << ToString(config.backend) << "\n";
    os << "Workload: " << ToString(config.workload) << "\n";
    os << "KeySwitchMethod: " << ToString(config.keyswitch_method) << "\n";
    os << "Seed: " << config.seed << "\n";
    os << "NumCards: " << config.num_cards << "\n";
    os << "NumUsers: " << config.num_users << "\n";
    os << "EnableMultiCard: " << (config.enable_multi_card ? "true" : "false") << "\n";
    os << "SyntheticRequestsPerUser: " << config.synthetic_requests_per_user << "\n";
    os << "SyntheticInterArrival: " << config.synthetic_inter_arrival << "\n";
    os << "SyntheticStartTime: " << config.synthetic_start_time << "\n";
    os << "BurstBursts: " << config.burst_bursts << "\n";
    os << "BurstRequestsPerUserPerBurst: " << config.burst_requests_per_user_per_burst << "\n";
    os << "BurstIntraGap: " << config.burst_intra_gap << "\n";
    os << "BurstInterGap: " << config.burst_inter_gap << "\n";
    os << "BurstStartTime: " << config.burst_start_time << "\n";
    os << "BurstLevel: " << config.burst_level << "\n";
    os << "NumPools: " << config.num_pools << "\n";
    os << "PoolConfigSource: "
       << (config.pool_config_path.empty() ? "built-in default" : config.pool_config_path)
       << "\n";
    os << "TreeConfigSource: "
       << (config.tree_config_path.empty() ? "built-in default" : config.tree_config_path)
       << "\n";
    os << "HEParamsSource: "
       << (config.he_params_path.empty() ? "built-in default" : config.he_params_path)
       << "\n";
    os << "CSVOutput: "
       << (config.csv_output_path.empty() ? "disabled" : config.csv_output_path)
       << "\n";
    os << "EnableTreeSearch: " << (config.enable_tree_search ? "true" : "false") << "\n";
    os << "TreeSearchSteps: " << config.tree_search_steps << "\n";
    os << "TreeSearchNeighbors: " << config.tree_search_neighbors << "\n";
    os << "TreeSearchOutputPath: "
       << (config.tree_search_output_path.empty() ? "auto" : config.tree_search_output_path)
       << "\n";
    os << "ObjectiveWeights: "
       << "mean_latency=" << config.objective_w_mean_latency
       << ", p99_latency=" << config.objective_w_p99_latency
       << ", fairness_penalty=" << config.objective_w_fairness_penalty
       << ", reload_penalty=" << config.objective_w_reload_penalty
       << ", incomplete_penalty=" << config.objective_w_incomplete_penalty
       << "\n";
    os << "=========================\n";
}
