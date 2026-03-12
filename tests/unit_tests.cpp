#include "backend/analytical_backend.h"
#include "backend/execution_backend.h"
#include "model/system_state.h"
#include "model/workload.h"
#include "scheduler/affinity_scheduler.h"
#include "scheduler/fifo_scheduler.h"
#include "scheduler/hierarchical_scheduler.h"
#include "scheduler/pool_scheduler.h"
#include "scheduler/static_partition_scheduler.h"
#include "sim/event.h"
#include "sim/metrics.h"
#include "sim/simulator.h"
#include "test_framework.h"

#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

namespace {

Request MakeRequest(
    RequestId request_id,
    UserId user_id,
    uint64_t input_bytes,
    uint32_t max_cards,
    bool latency_sensitive = false) {
    Request req;
    req.request_id = request_id;
    req.user_id = user_id;
    req.arrival_time = 0;
    req.latency_sensitive = latency_sensitive;
    req.priority = latency_sensitive ? 1 : 0;

    req.user_profile.user_id = user_id;
    req.user_profile.key_bytes = 8192;
    req.user_profile.key_load_time = 500;
    req.user_profile.latency_sensitive = latency_sensitive;
    req.user_profile.weight = 1;

    req.ks_profile.num_ciphertexts = 2;
    req.ks_profile.num_polys = 2;
    req.ks_profile.num_digits = 3;
    req.ks_profile.num_rns_limbs = 4;
    req.ks_profile.input_bytes = input_bytes;
    req.ks_profile.output_bytes = input_bytes;
    req.ks_profile.key_bytes = req.user_profile.key_bytes;
    req.ks_profile.preferred_cards = max_cards;
    req.ks_profile.max_cards = max_cards;
    return req;
}

SystemState MakeState(uint32_t num_cards, uint32_t num_pools) {
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
        card.busy = false;
        card.available_time = 0;
        card.memory_capacity_bytes = 16ULL * 1024ULL * 1024ULL * 1024ULL;
        state.cards.push_back(card);
        state.pools[card.pool_id].card_ids.push_back(card_id);
    }
    return state;
}

bool SequenceEquals(
    const std::vector<StageType>& actual,
    const std::vector<StageType>& expected) {
    if (actual.size() != expected.size()) {
        return false;
    }
    for (size_t i = 0; i < actual.size(); ++i) {
        if (actual[i] != expected[i]) {
            return false;
        }
    }
    return true;
}

void TestStageSequence(testfw::TestContext& ctx) {
    AnalyticalBackend backend;

    {
        SystemState state = MakeState(2, 2);
        Request req = MakeRequest(/*id=*/1, /*user=*/7, /*input_bytes=*/4096, /*max_cards=*/1);
        state.cards[0].resident_user = req.user_id;

        ExecutionPlan plan;
        plan.request_id = req.request_id;
        plan.assigned_cards = {0};

        const auto sequence = backend.StageSequenceForTest(req, plan, state);
        const std::vector<StageType> expected = {
            StageType::Dispatch,
            StageType::Decompose,
            StageType::Multiply,
            StageType::BasisConvert};
        EXPECT_TRUE(ctx, SequenceEquals(sequence, expected));
    }

    {
        SystemState state = MakeState(2, 2);
        Request req = MakeRequest(/*id=*/2, /*user=*/3, /*input_bytes=*/4096, /*max_cards=*/1);

        ExecutionPlan plan;
        plan.request_id = req.request_id;
        plan.assigned_cards = {0};

        const auto sequence = backend.StageSequenceForTest(req, plan, state);
        const std::vector<StageType> expected = {
            StageType::KeyLoad,
            StageType::Dispatch,
            StageType::Decompose,
            StageType::Multiply,
            StageType::BasisConvert};
        EXPECT_TRUE(ctx, SequenceEquals(sequence, expected));
    }

    {
        SystemState state = MakeState(4, 2);
        Request req = MakeRequest(/*id=*/3, /*user=*/2, /*input_bytes=*/12288, /*max_cards=*/4);

        ExecutionPlan plan;
        plan.request_id = req.request_id;
        plan.assigned_cards = {0, 1};

        const auto sequence = backend.StageSequenceForTest(req, plan, state);
        const std::vector<StageType> expected = {
            StageType::KeyLoad,
            StageType::Dispatch,
            StageType::Decompose,
            StageType::Multiply,
            StageType::BasisConvert,
            StageType::Merge};
        EXPECT_TRUE(ctx, SequenceEquals(sequence, expected));
    }
}

void TestSchedulerConstraints(testfw::TestContext& ctx) {
    AnalyticalBackend backend;

    {
        SystemState state = MakeState(2, 2);
        StaticPartitionScheduler scheduler(/*num_pools=*/2);
        Request req = MakeRequest(/*id=*/10, /*user=*/1, /*input_bytes=*/4096, /*max_cards=*/1);
        scheduler.OnRequestArrival(req);

        const auto plan_opt = scheduler.TrySchedule(state, backend);
        EXPECT_TRUE(ctx, plan_opt.has_value());
        EXPECT_EQ(ctx, state.cards[plan_opt->assigned_cards[0]].pool_id, 1u);
    }

    {
        SystemState state = MakeState(2, 2);
        PoolScheduler scheduler;
        Request req = MakeRequest(
            /*id=*/11, /*user=*/0, /*input_bytes=*/4096, /*max_cards=*/1, /*latency_sensitive=*/true);
        scheduler.OnRequestArrival(req);

        const auto plan_opt = scheduler.TrySchedule(state, backend);
        EXPECT_TRUE(ctx, plan_opt.has_value());
        EXPECT_EQ(ctx, state.cards[plan_opt->assigned_cards[0]].pool_id, 0u);
    }

    {
        SystemState state = MakeState(2, 2);
        state.resource_tree.clear();

        ResourceTreeNode root;
        root.node_id = 0;
        root.node_name = "root";
        root.type = TreeNodeType::Spatial;
        root.spatial_policy = SpatialRoutePolicy::FirstFit;
        root.children = {1, 2};
        root.cards = {0, 1};

        ResourceTreeNode leaf_a;
        leaf_a.node_id = 1;
        leaf_a.node_name = "leaf_a";
        leaf_a.type = TreeNodeType::Leaf;
        leaf_a.cards = {0};
        leaf_a.allowed_users = {0};

        ResourceTreeNode leaf_b;
        leaf_b.node_id = 2;
        leaf_b.node_name = "leaf_b";
        leaf_b.type = TreeNodeType::Leaf;
        leaf_b.cards = {1};
        leaf_b.allowed_users = {1};

        state.resource_tree = {root, leaf_a, leaf_b};
        state.resource_tree_root = 0;

        HierarchicalScheduler scheduler(FixedTreeKind::TreeA_Shared, 2);
        Request req = MakeRequest(/*id=*/12, /*user=*/2, /*input_bytes=*/4096, /*max_cards=*/1);
        scheduler.OnRequestArrival(req);

        const auto plan_opt = scheduler.TrySchedule(state, backend);
        EXPECT_TRUE(ctx, !plan_opt.has_value());
    }

    {
        SystemState state = MakeState(2, 1);
        AffinityScheduler scheduler;
        Request req = MakeRequest(/*id=*/13, /*user=*/0, /*input_bytes=*/20000, /*max_cards=*/4);
        scheduler.OnRequestArrival(req);

        const auto plan_opt = scheduler.TrySchedule(state, backend);
        EXPECT_TRUE(ctx, !plan_opt.has_value());
    }

    {
        SystemState state = MakeState(4, 1);
        state.resource_tree.clear();

        ResourceTreeNode root;
        root.node_id = 0;
        root.node_name = "root";
        root.type = TreeNodeType::Temporal;
        root.children = {1};
        root.cards = {0, 1, 2, 3};

        ResourceTreeNode leaf;
        leaf.node_id = 1;
        leaf.node_name = "leaf";
        leaf.type = TreeNodeType::Leaf;
        leaf.cards = {0, 1};

        state.resource_tree = {root, leaf};
        state.resource_tree_root = 0;

        HierarchicalScheduler scheduler(FixedTreeKind::TreeA_Shared, 1);
        Request req = MakeRequest(/*id=*/14, /*user=*/0, /*input_bytes=*/20000, /*max_cards=*/4);
        scheduler.OnRequestArrival(req);

        const auto plan_opt = scheduler.TrySchedule(state, backend);
        EXPECT_TRUE(ctx, !plan_opt.has_value());
    }
}

void TestMetricsBoundaries(testfw::TestContext& ctx) {
    {
        const auto metrics = ComputeMetrics({}, {}, {});
        EXPECT_EQ(ctx, metrics.completed_requests, static_cast<size_t>(0));
        EXPECT_EQ(ctx, metrics.p95_latency, static_cast<Time>(0));
        EXPECT_EQ(ctx, metrics.p99_latency, static_cast<Time>(0));
        EXPECT_NEAR(ctx, metrics.mean_latency, 0.0, 1e-9);
        EXPECT_NEAR(ctx, metrics.jain_fairness_index, 0.0, 1e-9);
    }

    {
        RequestMetricsSample sample;
        sample.request_id = 1;
        sample.user_id = 9;
        sample.arrival_time = 0;
        sample.start_time = 20;
        sample.finish_time = 120;
        sample.latency = 100;
        sample.queue_time = 20;
        sample.service_time = 100;
        sample.reload_count = 1;

        const auto metrics = ComputeMetrics({sample}, {9}, {});
        EXPECT_EQ(ctx, metrics.completed_requests, static_cast<size_t>(1));
        EXPECT_EQ(ctx, metrics.p95_latency, static_cast<Time>(100));
        EXPECT_EQ(ctx, metrics.p99_latency, static_cast<Time>(100));
        EXPECT_NEAR(ctx, metrics.mean_queue_time, 20.0, 1e-9);
        EXPECT_NEAR(ctx, metrics.mean_service_time, 100.0, 1e-9);
    }

    {
        RequestMetricsSample a;
        a.request_id = 1;
        a.user_id = 1;
        a.arrival_time = 0;
        a.finish_time = 10;
        a.latency = 10;
        a.queue_time = 0;
        a.service_time = 10;

        RequestMetricsSample b = a;
        b.request_id = 2;
        b.finish_time = 20;
        b.latency = 20;
        b.service_time = 20;

        const auto metrics = ComputeMetrics({a, b}, {1}, {});
        EXPECT_EQ(ctx, metrics.completed_requests, static_cast<size_t>(2));
        EXPECT_EQ(ctx, metrics.p95_latency, static_cast<Time>(20));
        EXPECT_EQ(ctx, metrics.p99_latency, static_cast<Time>(20));
        EXPECT_EQ(ctx, metrics.per_user.size(), static_cast<size_t>(1));
        EXPECT_EQ(ctx, metrics.per_user[0].completed_requests, static_cast<size_t>(2));
    }

    {
        const auto metrics = ComputeMetrics({}, {1, 2}, {});
        EXPECT_EQ(ctx, metrics.per_user.size(), static_cast<size_t>(2));
        EXPECT_EQ(ctx, metrics.per_user[0].completed_requests, static_cast<size_t>(0));
        EXPECT_EQ(ctx, metrics.per_user[1].completed_requests, static_cast<size_t>(0));
        EXPECT_NEAR(ctx, metrics.jain_fairness_index, 0.0, 1e-9);
    }

    {
        RequestMetricsSample u1;
        u1.request_id = 1;
        u1.user_id = 1;
        u1.arrival_time = 0;
        u1.finish_time = 100;
        u1.latency = 100;
        u1.service_time = 100;

        RequestMetricsSample u2 = u1;
        u2.request_id = 2;
        u2.user_id = 2;

        const auto metrics = ComputeMetrics({u1, u2}, {1, 2}, {});
        EXPECT_NEAR(ctx, metrics.jain_fairness_index, 1.0, 1e-9);
    }
}

void TestHEParamsDrivenWorkload(testfw::TestContext& ctx) {
    HEParams params;
    params.name = "test_he_profile";
    params.poly_modulus_degree = 65536;
    params.num_digits = 3;
    params.num_rns_limbs = 6;
    params.num_polys = 23;
    params.key_component_count = 2;
    params.bytes_per_coeff = 8;
    params.key_load_base_time = 144;
    params.key_load_bandwidth_bytes_per_ns = 32;

    WorkloadBuilder builder(/*seed=*/123, params);
    const auto requests = builder.GenerateSynthetic(
        /*num_users=*/1,
        /*requests_per_user=*/1,
        /*inter_arrival=*/100,
        /*start_time=*/0);

    EXPECT_EQ(ctx, requests.size(), static_cast<size_t>(1));

    const Request& req = requests.front();
    EXPECT_EQ(ctx, req.user_profile.key_bytes, params.ComputeKeyBytes());
    EXPECT_EQ(ctx, req.user_profile.key_load_time, params.ComputeKeyLoadTime());
    EXPECT_EQ(ctx, req.ks_profile.num_polys, params.num_polys);
    EXPECT_TRUE(ctx, req.ks_profile.num_digits >= params.num_digits - 1);
    EXPECT_TRUE(ctx, req.ks_profile.num_digits <= params.num_digits + 1);
    EXPECT_TRUE(
        ctx,
        req.ks_profile.num_rns_limbs == params.num_rns_limbs
            || req.ks_profile.num_rns_limbs == params.num_rns_limbs + 2);
}

SimulationMetrics RunOneSimulation(uint64_t seed) {
    SystemState state = MakeState(8, 2);

    auto scheduler = std::make_unique<FIFOScheduler>();
    auto backend = std::make_unique<AnalyticalBackend>();

    WorkloadBuilder builder(seed);
    const auto requests = builder.GenerateSynthetic(
        /*num_users=*/8,
        /*requests_per_user=*/4,
        /*inter_arrival=*/300,
        /*start_time=*/0);

    Simulator sim(
        state,
        std::move(scheduler),
        std::move(backend));
    sim.LoadWorkload(requests);
    sim.Run();
    return sim.CollectMetrics();
}

void TestDeterministicOrderingAndSeedStability(testfw::TestContext& ctx) {
    {
        std::priority_queue<Event, std::vector<Event>, EventCompare> queue;

        Event e1;
        e1.event_id = 2;
        e1.type = EventType::RequestArrival;
        e1.timestamp = 100;

        Event e2;
        e2.event_id = 1;
        e2.type = EventType::TaskFinish;
        e2.timestamp = 100;

        Event e3;
        e3.event_id = 3;
        e3.type = EventType::TaskFinish;
        e3.timestamp = 100;

        queue.push(e1);
        queue.push(e2);
        queue.push(e3);

        EXPECT_EQ(ctx, queue.top().event_id, static_cast<EventId>(1));
        queue.pop();
        EXPECT_EQ(ctx, queue.top().event_id, static_cast<EventId>(3));
        queue.pop();
        EXPECT_EQ(ctx, queue.top().event_id, static_cast<EventId>(2));
        queue.pop();
    }

    {
        const auto m1 = RunOneSimulation(/*seed=*/1234);
        const auto m2 = RunOneSimulation(/*seed=*/1234);

        EXPECT_EQ(ctx, m1.completed_requests, m2.completed_requests);
        EXPECT_EQ(ctx, m1.total_reload_count, m2.total_reload_count);
        EXPECT_EQ(ctx, m1.p95_latency, m2.p95_latency);
        EXPECT_EQ(ctx, m1.p99_latency, m2.p99_latency);
        EXPECT_NEAR(ctx, m1.mean_latency, m2.mean_latency, 1e-12);
    }
}

} // namespace

int main() {
    testfw::TestContext ctx;

    const std::vector<std::pair<std::string, std::function<void(testfw::TestContext&)>>> tests = {
        {"StageSequence", TestStageSequence},
        {"SchedulerConstraints", TestSchedulerConstraints},
        {"MetricsBoundaries", TestMetricsBoundaries},
        {"HEParamsDrivenWorkload", TestHEParamsDrivenWorkload},
        {"DeterministicOrderingAndSeed", TestDeterministicOrderingAndSeedStability},
    };

    for (const auto& test : tests) {
        const int failures_before = ctx.failures;
        test.second(ctx);
        if (ctx.failures == failures_before) {
            std::cout << "[PASS] " << test.first << "\n";
        } else {
            std::cout << "[FAIL] " << test.first << "\n";
        }
    }

    std::cout << "Assertions: " << ctx.assertions
              << ", Failures: " << ctx.failures << "\n";
    return (ctx.failures == 0) ? 0 : 1;
}
