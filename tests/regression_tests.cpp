#include "backend/analytical_backend.h"
#include "backend/execution_backend.h"
#include "backend/table_backend.h"
#include "model/system_state.h"
#include "model/workload.h"
#include "scheduler/affinity_scheduler.h"
#include "scheduler/fifo_scheduler.h"
#include "scheduler/hierarchical_scheduler.h"
#include "scheduler/scheduler.h"
#include "sim/simulator.h"
#include "test_framework.h"

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace {

enum class LocalWorkloadKind {
    Synthetic,
    Burst
};

SystemState BuildDefaultState(uint32_t num_cards, uint32_t num_pools) {
    SystemState state;
    state.now = 0;
    state.pools.clear();
    state.pools.reserve(num_pools);
    for (uint32_t pool_id = 0; pool_id < num_pools; ++pool_id) {
        ResourcePool pool;
        pool.pool_id = pool_id;
        pool.name = "pool" + std::to_string(pool_id);
        pool.latency_sensitive_pool = (pool_id == 0);
        pool.batch_pool = (pool_id != 0);
        state.pools.push_back(pool);
    }

    state.cards.clear();
    state.cards.reserve(num_cards);
    for (CardId card_id = 0; card_id < num_cards; ++card_id) {
        CardState card;
        card.card_id = card_id;
        card.pool_id = card_id % num_pools;
        card.memory_capacity_bytes = 16ULL * 1024ULL * 1024ULL * 1024ULL;
        state.cards.push_back(card);
        state.pools[card.pool_id].card_ids.push_back(card_id);
    }

    return state;
}

std::vector<Request> BuildWorkload(LocalWorkloadKind kind, uint64_t seed) {
    WorkloadBuilder builder(seed);
    if (kind == LocalWorkloadKind::Burst) {
        return builder.GenerateBurst(
            /*num_users=*/8,
            /*bursts=*/4,
            /*requests_per_user_per_burst=*/2,
            /*intra_burst_gap=*/20,
            /*inter_burst_gap=*/1200,
            /*start_time=*/0);
    }
    return builder.GenerateSynthetic(
        /*num_users=*/8,
        /*requests_per_user=*/4,
        /*inter_arrival=*/300,
        /*start_time=*/0);
}

SimulationMetrics RunScenario(
    std::unique_ptr<Scheduler> scheduler,
    std::unique_ptr<ExecutionBackend> backend,
    LocalWorkloadKind workload_kind,
    uint64_t seed) {

    Simulator sim(
        BuildDefaultState(/*num_cards=*/8, /*num_pools=*/2),
        std::move(scheduler),
        std::move(backend));
    sim.LoadWorkload(BuildWorkload(workload_kind, seed));
    sim.Run();
    return sim.CollectMetrics();
}

void CheckRegressionCase(
    testfw::TestContext& ctx,
    const std::string& case_name,
    const SimulationMetrics& metrics,
    size_t expected_completed,
    uint64_t expected_reload,
    double expected_mean_latency,
    double mean_latency_tolerance) {

    std::cout << "[REG] " << case_name
              << " completed=" << metrics.completed_requests
              << " reload=" << metrics.total_reload_count
              << " mean_latency=" << metrics.mean_latency << "\n";

    EXPECT_EQ(ctx, metrics.completed_requests, expected_completed);
    EXPECT_EQ(ctx, metrics.total_reload_count, expected_reload);
    EXPECT_NEAR(ctx, metrics.mean_latency, expected_mean_latency, mean_latency_tolerance);
}

} // namespace

int main() {
    testfw::TestContext ctx;
    const uint64_t seed = 42;

    {
        auto metrics = RunScenario(
            std::make_unique<FIFOScheduler>(),
            std::make_unique<AnalyticalBackend>(),
            LocalWorkloadKind::Synthetic,
            seed);
        CheckRegressionCase(
            ctx,
            "fifo+synthetic+analytical",
            metrics,
            /*completed=*/32,
            /*reload=*/82,
            /*mean_latency=*/2599.78125,
            /*tol=*/1.0);
    }

    {
        auto metrics = RunScenario(
            std::make_unique<AffinityScheduler>(),
            std::make_unique<TableBackend>(),
            LocalWorkloadKind::Synthetic,
            seed);
        CheckRegressionCase(
            ctx,
            "affinity+synthetic+table",
            metrics,
            /*completed=*/32,
            /*reload=*/77,
            /*mean_latency=*/6267.125,
            /*tol=*/1.0);
    }

    {
        auto metrics = RunScenario(
            std::make_unique<HierarchicalScheduler>(FixedTreeKind::TreeD_TwoPoolsAffinity, 2),
            std::make_unique<TableBackend>(),
            LocalWorkloadKind::Burst,
            seed);
        CheckRegressionCase(
            ctx,
            "hierarchical_d+burst+table",
            metrics,
            /*completed=*/64,
            /*reload=*/128,
            /*mean_latency=*/5721.796875,
            /*tol=*/1.0);
    }

    std::cout << "Regression assertions: " << ctx.assertions
              << ", failures: " << ctx.failures << "\n";
    return (ctx.failures == 0) ? 0 : 1;
}
