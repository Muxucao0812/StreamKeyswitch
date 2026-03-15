#include "backend/cycle_backend.h"
#include "backend/execution_backend.h"
#include "common/config_loader.h"
#include "common/experiment_config.h"
#include "common/he_params_loader.h"
#include "model/he_params.h"
#include "model/request_sizing.h"
#include "model/system_state.h"
#include "model/workload.h"
#include "scheduler/affinity_scheduler.h"
#include "scheduler/fifo_scheduler.h"
#include "scheduler/hierarchical_scheduler.h"
#include "scheduler/pool_scheduler.h"
#include "scheduler/score_scheduler.h"
#include "scheduler/scheduler.h"
#include "scheduler/static_partition_scheduler.h"
#include "search/tree_search.h"
#include "sim/simulator.h"

#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

std::unique_ptr<Scheduler> BuildScheduler(
    SchedulerKind scheduler_kind,
    uint32_t num_pools) {
    if (scheduler_kind == SchedulerKind::Affinity) {
        return std::make_unique<AffinityScheduler>();
    }
    if (scheduler_kind == SchedulerKind::StaticPartition) {
        return std::make_unique<StaticPartitionScheduler>(num_pools);
    }
    if (scheduler_kind == SchedulerKind::Score) {
        ScoreWeights weights;
        weights.waiting_time = 1.0;
        weights.switch_cost = 1.0;
        weights.queue_pressure = 5.0;
        weights.priority = 100.0;
        return std::make_unique<ScoreScheduler>(weights);
    }
    if (scheduler_kind == SchedulerKind::Pool) {
        return std::make_unique<PoolScheduler>();
    }
    if (scheduler_kind == SchedulerKind::HierarchicalA) {
        return std::make_unique<HierarchicalScheduler>(
            FixedTreeKind::TreeA_Shared,
            num_pools);
    }
    if (scheduler_kind == SchedulerKind::HierarchicalB) {
        return std::make_unique<HierarchicalScheduler>(
            FixedTreeKind::TreeB_TwoPools,
            num_pools);
    }
    if (scheduler_kind == SchedulerKind::HierarchicalC) {
        return std::make_unique<HierarchicalScheduler>(
            FixedTreeKind::TreeC_UserPinned,
            num_pools);
    }
    if (scheduler_kind == SchedulerKind::HierarchicalD) {
        return std::make_unique<HierarchicalScheduler>(
            FixedTreeKind::TreeD_TwoPoolsAffinity,
            num_pools);
    }
    return std::make_unique<FIFOScheduler>();
}

std::unique_ptr<ExecutionBackend> BuildBackend(const ExperimentConfig& config) {
    (void)config;
    return std::make_unique<CycleBackend>();
}

std::vector<ResourcePool> BuildDefaultPools(uint32_t num_cards, uint32_t num_pools) {
    std::vector<ResourcePool> pools;
    pools.reserve(num_pools);
    for (uint32_t pool_id = 0; pool_id < num_pools; ++pool_id) {
        ResourcePool pool;
        pool.pool_id = pool_id;
        pool.name = "pool" + std::to_string(pool_id);
        // pool 0: latency-sensitive, pool 1: batch
        pool.latency_sensitive_pool = (pool_id == 0);
        pool.batch_pool = (pool_id != 0);
        pools.push_back(pool);
    }

    for (CardId card_id = 0; card_id < num_cards; ++card_id) {
        const uint32_t pool_id = (num_pools == 0) ? 0 : (card_id % num_pools);
        pools[pool_id].card_ids.push_back(card_id);
    }
    return pools;
}

std::vector<ResourcePool> BuildPools(
    const ExperimentConfig& config) {
    if (config.pool_config_path.empty()) {
        return BuildDefaultPools(config.num_cards, config.num_pools);
    }

    const PoolConfigLoadResult load =
        LoadPoolsFromFile(config.pool_config_path, config.num_cards);
    if (!load.ok) {
        throw std::runtime_error(load.error_message);
    }
    return load.pools;
}

SystemState BuildInitialState(const ExperimentConfig& config) {
    SystemState state;
    state.now = 0;
    state.pools = BuildPools(config);

    std::vector<std::optional<uint32_t>> card_to_pool(config.num_cards, std::nullopt);
    for (const auto& pool : state.pools) {
        for (const CardId card_id : pool.card_ids) {
            if (card_id >= config.num_cards) {
                throw std::runtime_error(
                    "Pool mapping out of range for card " + std::to_string(card_id));
            }
            card_to_pool[card_id] = pool.pool_id;
        }
    }

    for (CardId card_id = 0; card_id < config.num_cards; ++card_id) {
        if (!card_to_pool[card_id].has_value()) {
            throw std::runtime_error(
                "Card " + std::to_string(card_id) + " is not assigned to any pool");
        }
    }

    state.cards.reserve(config.num_cards);
    for (CardId card_id = 0; card_id < config.num_cards; ++card_id) {
        CardState card;
        card.card_id = card_id;
        card.pool_id = card_to_pool[card_id].value();
        card.memory_capacity_bytes = kAlveoU280HbmBytes;
        card.bram_capacity_bytes = kAlveoU280BramBytes;
        state.cards.push_back(card);
    }

    if (!config.tree_config_path.empty()) {
        const TreeConfigLoadResult load =
            LoadTreeFromFile(config.tree_config_path, config.num_cards);
        if (!load.ok) {
            throw std::runtime_error(load.error_message);
        }
        state.resource_tree = load.resource_tree;
        state.resource_tree_root = load.root_node_id;
    }

    return state;
}

std::vector<Request> BuildWorkload(
    WorkloadBuilder& builder,
    const ExperimentConfig& config) {
    builder.SetDefaultKeySwitchMethod(config.keyswitch_method);
    if (config.workload == WorkloadKind::Burst) {
        const uint32_t reqs_per_user_per_burst =
            config.burst_requests_per_user_per_burst * config.burst_level;
        return builder.GenerateBurst(
            config.num_users,
            config.burst_bursts,
            reqs_per_user_per_burst,
            config.burst_intra_gap,
            config.burst_inter_gap,
            config.burst_start_time);
    }

    return builder.GenerateSynthetic(
        config.num_users,
        config.synthetic_requests_per_user,
        config.synthetic_inter_arrival,
        config.synthetic_start_time);
}

HEParams BuildHEParams(const ExperimentConfig& config) {
    if (config.he_params_path.empty()) {
        return HEParams::BuiltInDefault();
    }

    const HEParamsLoadResult load = LoadHEParamsFromFile(config.he_params_path);
    if (!load.ok) {
        throw std::runtime_error(load.error_message);
    }
    return load.params;
}

void ApplyMultiCardConfig(
    std::vector<Request>* requests,
    bool enable_multi_card) {
    if (enable_multi_card) {
        return;
    }
    for (auto& req : *requests) {
        req.ks_profile.preferred_cards = 1;
        req.ks_profile.max_cards = 1;
    }
}

std::string CsvEscape(const std::string& value) {
    bool need_quote = false;
    for (const char ch : value) {
        if (ch == ',' || ch == '"' || ch == '\n') {
            need_quote = true;
            break;
        }
    }

    if (!need_quote) {
        return value;
    }

    std::string out;
    out.reserve(value.size() + 4);
    out.push_back('"');
    for (const char ch : value) {
        if (ch == '"') {
            out.push_back('"');
        }
        out.push_back(ch);
    }
    out.push_back('"');
    return out;
}

bool FileIsEmpty(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        return true;
    }
    in.seekg(0, std::ios::end);
    return in.tellg() <= 0;
}

void AppendMetricsCsvRow(
    const std::string& path,
    const ExperimentConfig& config,
    const SimulationMetrics& metrics) {

    const bool write_header = FileIsEmpty(path);
    std::ofstream out(path, std::ios::app);
    if (!out.is_open()) {
        throw std::runtime_error("Failed to open CSV output file: " + path);
    }

    if (write_header) {
        out
            << "scheduler,backend,workload,seed,num_users,num_cards,enable_multi_card,"
            << "completed_requests,mean_latency,p95_latency,p99_latency,max_latency,"
            << "total_throughput,total_reload_count,fairness_index,"
            << "pool_config_source,tree_config_source,he_params_source,"
            << "pool_config_mode,tree_config_mode,he_params_mode\n";
    }

    const std::string pool_source =
        config.pool_config_path.empty() ? "built-in default" : config.pool_config_path;
    const std::string tree_source =
        config.tree_config_path.empty() ? "built-in default" : config.tree_config_path;
    const std::string he_params_source =
        config.he_params_path.empty() ? "built-in default" : config.he_params_path;
    const std::string pool_mode = config.pool_config_path.empty() ? "built-in" : "external";
    const std::string tree_mode = config.tree_config_path.empty() ? "built-in" : "external";
    const std::string he_params_mode = config.he_params_path.empty() ? "built-in" : "external";

    out
        << CsvEscape(ToString(config.scheduler)) << ","
        << CsvEscape(ToString(config.backend)) << ","
        << CsvEscape(ToString(config.workload)) << ","
        << config.seed << ","
        << config.num_users << ","
        << config.num_cards << ","
        << (config.enable_multi_card ? "true" : "false") << ","
        << metrics.completed_requests << ","
        << metrics.mean_latency << ","
        << metrics.p95_latency << ","
        << metrics.p99_latency << ","
        << metrics.max_latency << ","
        << metrics.total_throughput << ","
        << metrics.total_reload_count << ","
        << metrics.jain_fairness_index << ","
        << CsvEscape(pool_source) << ","
        << CsvEscape(tree_source) << ","
        << CsvEscape(he_params_source) << ","
        << pool_mode << ","
        << tree_mode << ","
        << he_params_mode
        << "\n";
}

TreeSearchOptions BuildTreeSearchOptions(const ExperimentConfig& config) {
    TreeSearchOptions options;
    options.steps = config.tree_search_steps;
    options.neighbors_per_step = config.tree_search_neighbors;
    options.seed = config.seed;
    options.weights.mean_latency = config.objective_w_mean_latency;
    options.weights.p99_latency = config.objective_w_p99_latency;
    options.weights.fairness_penalty = config.objective_w_fairness_penalty;
    options.weights.reload_penalty = config.objective_w_reload_penalty;
    options.weights.incomplete_penalty = config.objective_w_incomplete_penalty;
    return options;
}

} // namespace

int main(int argc, char** argv) {
    const ParseExperimentConfigResult parse = ParseExperimentConfig(argc, argv);
   
    if (parse.show_help) {
        std::cout << BuildUsageText(argv[0]);
        return 0;
    }
    if (!parse.ok) {
        std::cerr << "Argument error: " << parse.error_message << "\n\n";
        std::cerr << BuildUsageText(argv[0]);
        return 1;
    }

    ExperimentConfig config = parse.config;
 
    HEParams he_params;
    try {
        he_params = BuildHEParams(config);
    } catch (const std::exception& ex) {
        std::cerr << "HE params load error: " << ex.what() << "\n";
        return 1;
    }

    WorkloadBuilder workload_builder(config.seed, he_params);
    auto requests = BuildWorkload(workload_builder, config);
    ApplyMultiCardConfig(&requests, config.enable_multi_card);

    SystemState initial_state;
    try {
        initial_state = BuildInitialState(config);
    } catch (const std::exception& ex) {
        std::cerr << "Config load error: " << ex.what() << "\n";
        return 1;
    }

    bool tree_search_executed = false;
    std::string tree_search_initial_source;
    std::string tree_search_output_path;
    double tree_search_best_objective = 0.0;
    uint32_t tree_search_steps = 0;

    if (config.enable_tree_search) {
        if (!HierarchicalTreeSearcher::IsHierarchicalSchedulerKind(config.scheduler)) {
            std::cerr << "Tree search requires a hierarchical scheduler "
                      << "(hierarchical_a/b/c/d).\n";
            return 1;
        }

        tree_search_initial_source = config.tree_config_path.empty()
            ? "built-in default"
            : config.tree_config_path;

        std::vector<ResourceTreeNode> initial_tree = initial_state.resource_tree;
        uint32_t initial_root_id = initial_state.resource_tree_root;
        if (initial_tree.empty()) {
            initial_tree = HierarchicalTreeSearcher::BuildDefaultInitialTree(
                initial_state,
                requests,
                &initial_root_id);
        }

        const TreeSearchOptions options = BuildTreeSearchOptions(config);
        HierarchicalTreeSearcher searcher(options);

        const TreeSearchResult search_result = searcher.Search(
            config,
            initial_state,
            requests,
            initial_tree,
            initial_root_id,
            std::cout);
        if (!search_result.ok) {
            std::cerr << search_result.error_message << "\n";
            return 1;
        }

        tree_search_output_path = config.tree_search_output_path.empty()
            ? "optimized_tree_generated.cfg"
            : config.tree_search_output_path;
        const TreeConfigSaveResult saved = SaveTreeToFile(
            tree_search_output_path,
            search_result.best_tree,
            search_result.best_root_id);
        if (!saved.ok) {
            std::cerr << saved.error_message << "\n";
            return 1;
        }

        initial_state.resource_tree = search_result.best_tree;
        initial_state.resource_tree_root = search_result.best_root_id;

        tree_search_executed = true;
        tree_search_best_objective = search_result.best_objective;
        tree_search_steps = search_result.executed_steps;

        // Use optimized tree as the runtime tree source metadata.
        config.tree_config_path = tree_search_output_path;
    }

    PrintExperimentConfig(std::cout, config);

    std::unique_ptr<Scheduler> scheduler;
    std::unique_ptr<ExecutionBackend> backend;
    try {
        scheduler = BuildScheduler(config.scheduler, config.num_pools);
        backend = BuildBackend(config);
    } catch (const std::exception& ex) {
        std::cerr << "Backend initialization error: " << ex.what() << "\n";
        return 1;
    }

    CycleBackend* cycle_backend = dynamic_cast<CycleBackend*>(backend.get());

    Simulator sim(std::move(initial_state), std::move(scheduler), std::move(backend));

    sim.LoadWorkload(requests);
    sim.Run();
    const SimulationMetrics metrics = sim.CollectMetrics();

    std::cout << "Scheduler=" << ToString(config.scheduler) << "\n";
    std::cout << "Backend=" << ToString(config.backend) << "\n";
    std::cout << "RuntimeBackend=cycle_stub\n";
    std::cout << "Workload=" << ToString(config.workload) << "\n";
    std::cout << "Seed=" << config.seed << "\n";
    std::cout << "NumCards=" << config.num_cards << "\n";
    PrintMetrics(std::cout, metrics);
    std::cout << "PoolConfigSource="
              << (config.pool_config_path.empty() ? "built-in default" : config.pool_config_path)
              << "\n";
    std::cout << "TreeConfigSource="
              << (config.tree_config_path.empty() ? "built-in default" : config.tree_config_path)
              << "\n";
    std::cout << "HEParamsSource="
              << (config.he_params_path.empty() ? "built-in default" : config.he_params_path)
              << "\n";
    if (cycle_backend != nullptr) {
        cycle_backend->PrintStats(std::cout);
    }
    if (tree_search_executed) {
        std::cout << "TreeSearchInitialSource=" << tree_search_initial_source << "\n";
        std::cout << "TreeSearchSteps=" << tree_search_steps << "\n";
        std::cout << "TreeSearchBestObjective=" << tree_search_best_objective << "\n";
        std::cout << "TreeSearchBestTreePath=" << tree_search_output_path << "\n";
    }
    if (!config.csv_output_path.empty()) {
        try {
            AppendMetricsCsvRow(config.csv_output_path, config, metrics);
            std::cout << "CSVOutput=" << config.csv_output_path << "\n";
        } catch (const std::exception& ex) {
            std::cerr << "CSV export error: " << ex.what() << "\n";
            return 1;
        }
    }
    return 0;
}
