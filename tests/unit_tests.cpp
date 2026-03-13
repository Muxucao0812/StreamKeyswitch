#include "backend/analytical_backend.h"
#include "backend/cycle_backend.h"
#include "backend/execution_backend.h"
#include "backend/model/keyswitch_execution_model.h"
#include "backend/primitive_simulator.h"
#include "model/request_sizing.h"
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
        card.memory_capacity_bytes = kAlveoU280HbmBytes;
        card.bram_capacity_bytes = kAlveoU280BramBytes;
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

void ConfigureKeySwitchProblem(
    Request* req,
    uint32_t ciphertexts,
    uint32_t digits,
    uint32_t limbs,
    uint32_t polys,
    uint32_t poly_degree,
    uint64_t input_bytes,
    uint64_t output_bytes,
    uint64_t key_bytes) {
    req->ks_profile.num_ciphertexts = ciphertexts;
    req->ks_profile.num_digits = digits;
    req->ks_profile.num_rns_limbs = limbs;
    req->ks_profile.num_polys = polys;
    req->ks_profile.poly_modulus_degree = poly_degree;
    req->ks_profile.input_bytes = input_bytes;
    req->ks_profile.output_bytes = output_bytes;
    req->ks_profile.key_bytes = key_bytes;
    req->user_profile.key_bytes = key_bytes;
}

size_t CountSteps(
    const KeySwitchExecution& execution,
    TileExecutionStepType step_type) {
    size_t count = 0;
    for (const auto& step : execution.steps) {
        if (step.type == step_type) {
            ++count;
        }
    }
    return count;
}

const PrimitiveTypeBreakdown* FindPrimitiveBreakdownEntry(
    const PrimitiveResult& result,
    PrimitiveType primitive_type) {
    for (const auto& entry : result.primitive_breakdown) {
        if (entry.primitive_type == primitive_type) {
            return &entry;
        }
    }
    return nullptr;
}

Time SumStageBreakdown(const ExecutionResult& result) {
    return result.breakdown.key_load_time
        + result.breakdown.dispatch_time
        + result.breakdown.decompose_time
        + result.breakdown.multiply_time
        + result.breakdown.basis_convert_time
        + result.breakdown.merge_time;
}

void TestCycleBackendFormulaConsistency(testfw::TestContext& ctx) {
    CycleBackend backend;

    SystemState state = MakeState(/*num_cards=*/1, /*num_pools=*/1);
    state.cards[0].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;

    Request req = MakeRequest(
        /*id=*/100,
        /*user=*/9,
        /*input_bytes=*/4096,
        /*max_cards=*/1);
    req.ks_profile.output_bytes = req.ks_profile.input_bytes;

    ExecutionPlan plan;
    plan.request_id = req.request_id;
    plan.assigned_cards = {0};

    const ExecutionResult result = backend.Estimate(req, plan, state);
    EXPECT_TRUE(ctx, result.tiled_execution);
    EXPECT_TRUE(ctx, !result.fallback_used);
    EXPECT_TRUE(ctx, result.tile_count >= 1);
    EXPECT_TRUE(ctx, result.breakdown.key_load_time > 0);
    EXPECT_TRUE(ctx, result.breakdown.dispatch_time > 0);
    EXPECT_TRUE(ctx, result.breakdown.decompose_time > 0);
    EXPECT_TRUE(ctx, result.breakdown.multiply_time > 0);
    EXPECT_TRUE(ctx, result.breakdown.basis_convert_time > 0);
    EXPECT_EQ(ctx, result.breakdown.merge_time, static_cast<Time>(0));
    EXPECT_EQ(
        ctx,
        result.total_latency,
        result.breakdown.key_load_time
            + result.breakdown.dispatch_time
            + result.breakdown.decompose_time
            + result.breakdown.multiply_time
            + result.breakdown.basis_convert_time
            + result.breakdown.merge_time);

    SystemState tiled_state = state;
    tiled_state.cards[0].bram_capacity_bytes = 1ULL * 1024ULL * 1024ULL;

    Request tiled_req = req;
    tiled_req.request_id = 101;
    tiled_req.ks_profile.input_bytes = 300000;
    tiled_req.ks_profile.output_bytes = 300000;

    ExecutionPlan tiled_plan = plan;
    tiled_plan.request_id = tiled_req.request_id;

    const ExecutionResult tiled_result = backend.Estimate(tiled_req, tiled_plan, tiled_state);
    EXPECT_TRUE(ctx, tiled_result.tiled_execution || tiled_result.fallback_used);
    EXPECT_TRUE(ctx, tiled_result.tile_count >= 1);
    if (tiled_result.tiled_execution) {
        EXPECT_TRUE(ctx, tiled_result.peak_bram_bytes <= tiled_state.cards[0].bram_capacity_bytes);
    }
    EXPECT_TRUE(ctx, tiled_result.ct_hbm_to_bram_bytes >= tiled_req.ks_profile.input_bytes);

    SystemState mc_state = MakeState(/*num_cards=*/2, /*num_pools=*/1);
    mc_state.cards[0].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;
    mc_state.cards[1].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;

    Request mc_req = MakeRequest(
        /*id=*/102,
        /*user=*/7,
        /*input_bytes=*/4096,
        /*max_cards=*/2);
    mc_req.ks_profile.output_bytes = mc_req.ks_profile.input_bytes;
    mc_req.ks_profile.method = KeySwitchMethod::ScaleOutLimb;

    ExecutionPlan mc_plan;
    mc_plan.request_id = mc_req.request_id;
    mc_plan.assigned_cards = {0, 1};

    const ExecutionResult mc_result = backend.Estimate(mc_req, mc_plan, mc_state);
    EXPECT_TRUE(ctx, mc_result.tiled_execution);
    EXPECT_TRUE(ctx, !mc_result.fallback_used);
    EXPECT_TRUE(ctx, mc_result.breakdown.merge_time > 0);
    EXPECT_TRUE(ctx, mc_result.tile_count >= 1);
    EXPECT_TRUE(ctx, mc_result.key_hbm_to_bram_bytes >= mc_req.ks_profile.key_bytes);
    EXPECT_TRUE(
        ctx,
        mc_result.ct_hbm_to_bram_bytes
            >= (mc_req.ks_profile.input_bytes / static_cast<uint64_t>(2)));
}

void TestKeySwitchTilePlanningBasic(testfw::TestContext& ctx) {
    SystemState state = MakeState(/*num_cards=*/1, /*num_pools=*/1);
    state.cards[0].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;

    Request req = MakeRequest(
        /*id=*/200,
        /*user=*/1,
        /*input_bytes=*/1ULL * 1024ULL * 1024ULL,
        /*max_cards=*/1);
    ConfigureKeySwitchProblem(
        &req,
        /*ciphertexts=*/8,
        /*digits=*/4,
        /*limbs=*/6,
        /*polys=*/2,
        /*poly_degree=*/1,
        /*input_bytes=*/1ULL * 1024ULL * 1024ULL,
        /*output_bytes=*/1ULL * 1024ULL * 1024ULL,
        /*key_bytes=*/64ULL * 1024ULL);

    ExecutionPlan plan;
    plan.request_id = req.request_id;
    plan.assigned_cards = {0};

    KeySwitchExecutionModel model;
    const KeySwitchExecution execution = model.Build(req, plan, state);

    EXPECT_TRUE(ctx, execution.valid);
    EXPECT_TRUE(ctx, execution.tiled_execution);
    EXPECT_TRUE(ctx, !execution.fallback_used);
    EXPECT_TRUE(ctx, execution.tile_plan.valid);
    EXPECT_TRUE(ctx, execution.tile_count >= 1);
    EXPECT_EQ(ctx, execution.tile_count, execution.tile_plan.total_tile_count);
    EXPECT_TRUE(ctx, execution.tile_plan.ct_tile >= 1);
    EXPECT_TRUE(ctx, execution.tile_plan.ct_tile <= execution.problem.ciphertexts);
    EXPECT_TRUE(ctx, execution.tile_plan.limb_tile >= 1);
    EXPECT_TRUE(ctx, execution.tile_plan.limb_tile <= execution.problem.limbs);
    EXPECT_TRUE(ctx, execution.tile_plan.digit_tile >= 1);
    EXPECT_TRUE(ctx, execution.tile_plan.digit_tile <= execution.problem.digits);
    EXPECT_TRUE(ctx, execution.peak_bram_bytes <= execution.problem.bram_budget_bytes);
    EXPECT_TRUE(ctx, !execution.tile_plan.per_tile_buffer_usage.empty());

    for (const TileBufferUsage& tile_usage : execution.tile_plan.per_tile_buffer_usage) {
        EXPECT_EQ(
            ctx,
            tile_usage.peak_live_bytes,
            tile_usage.persistent_bytes + tile_usage.static_bytes + tile_usage.dynamic_working_bytes);
    }
}

void TestKeySwitchAutoTilingOnSmallBram(testfw::TestContext& ctx) {
    Request req = MakeRequest(
        /*id=*/201,
        /*user=*/2,
        /*input_bytes=*/4ULL * 1024ULL * 1024ULL,
        /*max_cards=*/1);
    ConfigureKeySwitchProblem(
        &req,
        /*ciphertexts=*/12,
        /*digits=*/4,
        /*limbs=*/6,
        /*polys=*/2,
        /*poly_degree=*/1,
        /*input_bytes=*/4ULL * 1024ULL * 1024ULL,
        /*output_bytes=*/4ULL * 1024ULL * 1024ULL,
        /*key_bytes=*/128ULL * 1024ULL);

    ExecutionPlan plan;
    plan.request_id = req.request_id;
    plan.assigned_cards = {0};

    SystemState large_state = MakeState(/*num_cards=*/1, /*num_pools=*/1);
    large_state.cards[0].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;

    SystemState small_state = large_state;
    small_state.cards[0].bram_capacity_bytes = 2ULL * 1024ULL * 1024ULL;

    KeySwitchExecutionModel model;
    const KeySwitchExecution large_exec = model.Build(req, plan, large_state);
    const KeySwitchExecution small_exec = model.Build(req, plan, small_state);

    EXPECT_TRUE(ctx, large_exec.valid);
    EXPECT_TRUE(ctx, small_exec.valid);
    EXPECT_TRUE(ctx, !large_exec.fallback_used);
    EXPECT_TRUE(ctx, !small_exec.fallback_used);
    EXPECT_TRUE(ctx, small_exec.tile_count > large_exec.tile_count);
    EXPECT_TRUE(ctx, small_exec.tile_plan.ct_tile <= large_exec.tile_plan.ct_tile);
    EXPECT_TRUE(ctx, small_exec.peak_bram_bytes <= small_exec.problem.bram_budget_bytes);
}

void TestKeySwitchPersistentKeyTransferDelta(testfw::TestContext& ctx) {
    Request req = MakeRequest(
        /*id=*/202,
        /*user=*/3,
        /*input_bytes=*/8ULL * 1024ULL * 1024ULL,
        /*max_cards=*/1);
    ConfigureKeySwitchProblem(
        &req,
        /*ciphertexts=*/16,
        /*digits=*/6,
        /*limbs=*/8,
        /*polys=*/2,
        /*poly_degree=*/1,
        /*input_bytes=*/8ULL * 1024ULL * 1024ULL,
        /*output_bytes=*/8ULL * 1024ULL * 1024ULL,
        /*key_bytes=*/256ULL * 1024ULL);

    SystemState state = MakeState(/*num_cards=*/1, /*num_pools=*/1);
    state.cards[0].bram_capacity_bytes = 4ULL * 1024ULL * 1024ULL;

    ExecutionPlan plan;
    plan.request_id = req.request_id;
    plan.assigned_cards = {0};

    KeySwitchExecutionModelParams persistent_params;
    persistent_params.tile_planner.allow_key_persistent = true;
    persistent_params.tile_planner.w_key_transfer = 4.0;
    KeySwitchExecutionModel persistent_model(persistent_params);

    KeySwitchExecutionModelParams nonpersistent_params;
    nonpersistent_params.tile_planner.allow_key_persistent = false;
    nonpersistent_params.tile_planner.w_key_transfer = 4.0;
    KeySwitchExecutionModel nonpersistent_model(nonpersistent_params);

    const KeySwitchExecution persistent_exec = persistent_model.Build(req, plan, state);
    const KeySwitchExecution nonpersistent_exec = nonpersistent_model.Build(req, plan, state);

    EXPECT_TRUE(ctx, persistent_exec.valid);
    EXPECT_TRUE(ctx, nonpersistent_exec.valid);
    EXPECT_TRUE(ctx, persistent_exec.key_persistent_bram);
    EXPECT_TRUE(ctx, !nonpersistent_exec.key_persistent_bram);
    EXPECT_TRUE(
        ctx,
        nonpersistent_exec.key_hbm_to_bram_bytes > persistent_exec.key_hbm_to_bram_bytes);
    EXPECT_TRUE(
        ctx,
        CountSteps(nonpersistent_exec, TileExecutionStepType::KeyLoadHBMToBRAM)
            > CountSteps(persistent_exec, TileExecutionStepType::KeyLoadHBMToBRAM));
}

void TestKeySwitchMethodSelection(testfw::TestContext& ctx) {
    SystemState state = MakeState(/*num_cards=*/2, /*num_pools=*/1);
    state.cards[0].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;
    state.cards[1].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;

    Request base = MakeRequest(
        /*id=*/206,
        /*user=*/11,
        /*input_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*max_cards=*/2);
    ConfigureKeySwitchProblem(
        &base,
        /*ciphertexts=*/8,
        /*digits=*/5,
        /*limbs=*/12,
        /*polys=*/2,
        /*poly_degree=*/1,
        /*input_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*output_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*key_bytes=*/512ULL * 1024ULL);

    ExecutionPlan plan;
    plan.request_id = base.request_id;
    plan.assigned_cards = {0, 1};

    KeySwitchExecutionModel model;

    Request single_req = base;
    single_req.ks_profile.method = KeySwitchMethod::SingleBoardClassic;
    const KeySwitchExecution single_exec = model.Build(single_req, plan, state);

    Request scale_req = base;
    scale_req.request_id = base.request_id + 1;
    scale_req.ks_profile.method = KeySwitchMethod::ScaleOutLimb;
    const KeySwitchExecution scale_exec = model.Build(scale_req, plan, state);

    EXPECT_TRUE(ctx, single_exec.valid);
    EXPECT_TRUE(ctx, scale_exec.valid);
    EXPECT_EQ(ctx, single_exec.problem.cards, static_cast<uint32_t>(1));
    EXPECT_EQ(ctx, scale_exec.problem.cards, static_cast<uint32_t>(2));
    EXPECT_TRUE(ctx, scale_exec.problem.limbs <= single_exec.problem.limbs);
    EXPECT_TRUE(ctx, scale_exec.key_hbm_to_bram_bytes >= single_exec.key_hbm_to_bram_bytes);
    EXPECT_EQ(ctx, CountSteps(single_exec, TileExecutionStepType::InterCardSendStep), static_cast<size_t>(0));
    EXPECT_EQ(ctx, CountSteps(single_exec, TileExecutionStepType::InterCardRecvStep), static_cast<size_t>(0));
    EXPECT_EQ(ctx, CountSteps(single_exec, TileExecutionStepType::InterCardReduceStep), static_cast<size_t>(0));
    EXPECT_EQ(ctx, CountSteps(single_exec, TileExecutionStepType::BarrierStep), static_cast<size_t>(0));
    EXPECT_TRUE(ctx, CountSteps(scale_exec, TileExecutionStepType::InterCardSendStep) > 0);
    EXPECT_TRUE(ctx, CountSteps(scale_exec, TileExecutionStepType::InterCardRecvStep) > 0);
    EXPECT_TRUE(ctx, CountSteps(scale_exec, TileExecutionStepType::InterCardReduceStep) > 0);
    EXPECT_TRUE(ctx, CountSteps(scale_exec, TileExecutionStepType::BarrierStep) > 0);
}

void TestUnsupportedMethodDoesNotSilentlyDegrade(testfw::TestContext& ctx) {
    SystemState state = MakeState(/*num_cards=*/2, /*num_pools=*/1);
    state.cards[0].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;
    state.cards[1].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;

    Request req = MakeRequest(
        /*id=*/2061,
        /*user=*/11,
        /*input_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*max_cards=*/2);
    ConfigureKeySwitchProblem(
        &req,
        /*ciphertexts=*/8,
        /*digits=*/5,
        /*limbs=*/12,
        /*polys=*/2,
        /*poly_degree=*/1,
        /*input_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*output_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*key_bytes=*/512ULL * 1024ULL);
    req.ks_profile.method = KeySwitchMethod::SingleBoardFused;

    ExecutionPlan plan;
    plan.request_id = req.request_id;
    plan.assigned_cards = {0, 1};

    KeySwitchExecutionModel model;
    const KeySwitchExecution exec = model.Build(req, plan, state);

    EXPECT_TRUE(ctx, !exec.valid);
    EXPECT_TRUE(ctx, exec.fallback_used);
    EXPECT_TRUE(ctx, !exec.tiled_execution);
    EXPECT_TRUE(ctx, exec.method == KeySwitchMethod::SingleBoardFused);
    EXPECT_TRUE(ctx, exec.requested_method == KeySwitchMethod::SingleBoardFused);
    EXPECT_TRUE(ctx, exec.effective_method == KeySwitchMethod::SingleBoardFused);
    EXPECT_TRUE(ctx, !exec.method_degraded);
    EXPECT_TRUE(ctx, exec.fallback_reason == KeySwitchFallbackReason::UnsupportedMethod);
    EXPECT_EQ(ctx, exec.steps.size(), static_cast<size_t>(0));

    CycleBackend backend;
    const ExecutionResult cycle_result = backend.Estimate(req, plan, state);
    EXPECT_TRUE(ctx, cycle_result.fallback_used);
    EXPECT_TRUE(ctx, cycle_result.requested_method == KeySwitchMethod::SingleBoardFused);
    EXPECT_TRUE(ctx, cycle_result.effective_method == KeySwitchMethod::SingleBoardFused);
    EXPECT_TRUE(ctx, !cycle_result.method_degraded);
    EXPECT_TRUE(ctx, cycle_result.fallback_reason == KeySwitchFallbackReason::UnsupportedMethod);
    EXPECT_TRUE(ctx, !cycle_result.fallback_reason_message.empty());
}

void TestScaleOutConfigSemanticsAreEnforced(testfw::TestContext& ctx) {
    SystemState state = MakeState(/*num_cards=*/2, /*num_pools=*/1);
    state.cards[0].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;
    state.cards[1].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;

    Request base = MakeRequest(
        /*id=*/2062,
        /*user=*/11,
        /*input_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*max_cards=*/2);
    ConfigureKeySwitchProblem(
        &base,
        /*ciphertexts=*/8,
        /*digits=*/5,
        /*limbs=*/12,
        /*polys=*/2,
        /*poly_degree=*/1,
        /*input_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*output_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*key_bytes=*/512ULL * 1024ULL);
    base.ks_profile.method = KeySwitchMethod::ScaleOutLimb;
    base.ks_profile.scale_out_cards = 2;

    ExecutionPlan plan;
    plan.request_id = base.request_id;
    plan.assigned_cards = {0, 1};

    KeySwitchExecutionModel model;

    {
        Request req = base;
        req.ks_profile.partition = PartitionStrategy::ByDigit;
        const KeySwitchExecution exec = model.Build(req, plan, state);
        EXPECT_TRUE(ctx, !exec.valid);
        EXPECT_TRUE(ctx, exec.fallback_reason == KeySwitchFallbackReason::UnsupportedConfig);
    }

    {
        Request req = base;
        req.ks_profile.collective = CollectiveStrategy::TreeReduce;
        const KeySwitchExecution exec = model.Build(req, plan, state);
        EXPECT_TRUE(ctx, !exec.valid);
        EXPECT_TRUE(ctx, exec.fallback_reason == KeySwitchFallbackReason::UnsupportedConfig);
    }

    {
        Request req = base;
        req.ks_profile.key_placement = KeyPlacement::StreamFromHBM;
        const KeySwitchExecution exec = model.Build(req, plan, state);
        EXPECT_TRUE(ctx, !exec.valid);
        EXPECT_TRUE(ctx, exec.fallback_reason == KeySwitchFallbackReason::UnsupportedConfig);
    }

    {
        Request req = base;
        req.ks_profile.enable_inter_card_merge = false;
        const KeySwitchExecution exec = model.Build(req, plan, state);
        EXPECT_TRUE(ctx, !exec.valid);
        EXPECT_TRUE(ctx, exec.fallback_reason == KeySwitchFallbackReason::UnsupportedConfig);
    }

    {
        Request req = base;
        req.ks_profile.allow_cross_card_reduce = false;
        const KeySwitchExecution exec = model.Build(req, plan, state);
        EXPECT_TRUE(ctx, !exec.valid);
        EXPECT_TRUE(ctx, exec.fallback_reason == KeySwitchFallbackReason::UnsupportedConfig);
    }

    {
        Request req = base;
        req.ks_profile.allow_local_moddown = false;
        const KeySwitchExecution exec = model.Build(req, plan, state);
        EXPECT_TRUE(ctx, !exec.valid);
        EXPECT_TRUE(ctx, exec.fallback_reason == KeySwitchFallbackReason::UnsupportedConfig);
    }

    {
        Request req = base;
        req.ks_profile.partition = PartitionStrategy::ByLimb;
        req.ks_profile.collective = CollectiveStrategy::GatherToRoot;
        req.ks_profile.key_placement = KeyPlacement::ReplicatedPerCard;
        req.ks_profile.enable_inter_card_merge = true;
        req.ks_profile.allow_cross_card_reduce = true;
        const KeySwitchExecution exec = model.Build(req, plan, state);
        EXPECT_TRUE(ctx, exec.valid);
        EXPECT_TRUE(ctx, !exec.fallback_used);
    }
}

void TestSingleBoardRejectsDisabledLocalModdown(testfw::TestContext& ctx) {
    SystemState state = MakeState(/*num_cards=*/1, /*num_pools=*/1);
    state.cards[0].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;

    Request req = MakeRequest(
        /*id=*/2063,
        /*user=*/11,
        /*input_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*max_cards=*/1);
    ConfigureKeySwitchProblem(
        &req,
        /*ciphertexts=*/8,
        /*digits=*/5,
        /*limbs=*/12,
        /*polys=*/2,
        /*poly_degree=*/1,
        /*input_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*output_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*key_bytes=*/512ULL * 1024ULL);
    req.ks_profile.method = KeySwitchMethod::SingleBoardClassic;
    req.ks_profile.allow_local_moddown = false;

    ExecutionPlan plan;
    plan.request_id = req.request_id;
    plan.assigned_cards = {0};

    KeySwitchExecutionModel model;
    const KeySwitchExecution exec = model.Build(req, plan, state);

    EXPECT_TRUE(ctx, !exec.valid);
    EXPECT_TRUE(ctx, exec.fallback_used);
    EXPECT_TRUE(ctx, exec.fallback_reason == KeySwitchFallbackReason::UnsupportedConfig);

    CycleBackend backend;
    const ExecutionResult cycle_result = backend.Estimate(req, plan, state);
    EXPECT_TRUE(ctx, cycle_result.fallback_used);
    EXPECT_TRUE(ctx, cycle_result.fallback_reason == KeySwitchFallbackReason::UnsupportedConfig);
    EXPECT_TRUE(ctx, !cycle_result.fallback_reason_message.empty());
}

void TestSingleBoardClassicLocalPrimitiveFlow(testfw::TestContext& ctx) {
    SystemState state = MakeState(/*num_cards=*/2, /*num_pools=*/1);
    state.cards[0].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;
    state.cards[1].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;

    Request req = MakeRequest(
        /*id=*/207,
        /*user=*/12,
        /*input_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*max_cards=*/2);
    ConfigureKeySwitchProblem(
        &req,
        /*ciphertexts=*/6,
        /*digits=*/4,
        /*limbs=*/8,
        /*polys=*/2,
        /*poly_degree=*/1,
        /*input_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*output_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*key_bytes=*/256ULL * 1024ULL);
    req.ks_profile.method = KeySwitchMethod::SingleBoardClassic;
    req.ks_profile.partition = PartitionStrategy::None;
    req.ks_profile.key_placement = KeyPlacement::StreamFromHBM;
    req.ks_profile.collective = CollectiveStrategy::None;
    req.ks_profile.scale_out_cards = 1;

    ExecutionPlan plan;
    plan.request_id = req.request_id;
    plan.assigned_cards = {0, 1};

    KeySwitchExecutionModel model;
    const KeySwitchExecution exec = model.Build(req, plan, state);

    EXPECT_TRUE(ctx, exec.valid);
    EXPECT_TRUE(ctx, !exec.fallback_used);
    EXPECT_TRUE(ctx, exec.method == KeySwitchMethod::SingleBoardClassic);
    EXPECT_EQ(ctx, exec.problem.cards, static_cast<uint32_t>(1));

    EXPECT_TRUE(ctx, CountSteps(exec, TileExecutionStepType::KeyLoadHostToHBM) > 0);
    EXPECT_TRUE(ctx, CountSteps(exec, TileExecutionStepType::KeyLoadHBMToBRAM) > 0);
    EXPECT_TRUE(ctx, CountSteps(exec, TileExecutionStepType::InputHBMToBRAM) > 0);
    EXPECT_TRUE(ctx, CountSteps(exec, TileExecutionStepType::DecomposeTile) > 0);
    EXPECT_TRUE(ctx, CountSteps(exec, TileExecutionStepType::NttTile) > 0);
    EXPECT_TRUE(ctx, CountSteps(exec, TileExecutionStepType::KSInnerProdTile) > 0);
    EXPECT_TRUE(ctx, CountSteps(exec, TileExecutionStepType::InttTile) > 0);
    EXPECT_TRUE(ctx, CountSteps(exec, TileExecutionStepType::AccumulateSubtractTile) > 0);
    EXPECT_TRUE(ctx, CountSteps(exec, TileExecutionStepType::BasisConvertTile) > 0);
    EXPECT_TRUE(ctx, CountSteps(exec, TileExecutionStepType::OutputBRAMToHBM) > 0);

    EXPECT_EQ(ctx, CountSteps(exec, TileExecutionStepType::InterCardSendStep), static_cast<size_t>(0));
    EXPECT_EQ(ctx, CountSteps(exec, TileExecutionStepType::InterCardRecvStep), static_cast<size_t>(0));
    EXPECT_EQ(ctx, CountSteps(exec, TileExecutionStepType::InterCardReduceStep), static_cast<size_t>(0));
    EXPECT_EQ(ctx, CountSteps(exec, TileExecutionStepType::BarrierStep), static_cast<size_t>(0));
}

void TestScaleOutLimbIncludesInterCardFlow(testfw::TestContext& ctx) {
    SystemState state = MakeState(/*num_cards=*/4, /*num_pools=*/1);
    for (CardId card_id = 0; card_id < 4; ++card_id) {
        state.cards[card_id].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;
    }

    Request req = MakeRequest(
        /*id=*/208,
        /*user=*/13,
        /*input_bytes=*/4ULL * 1024ULL * 1024ULL,
        /*max_cards=*/4);
    ConfigureKeySwitchProblem(
        &req,
        /*ciphertexts=*/4,
        /*digits=*/4,
        /*limbs=*/16,
        /*polys=*/2,
        /*poly_degree=*/1,
        /*input_bytes=*/4ULL * 1024ULL * 1024ULL,
        /*output_bytes=*/4ULL * 1024ULL * 1024ULL,
        /*key_bytes=*/512ULL * 1024ULL);
    req.ks_profile.method = KeySwitchMethod::ScaleOutLimb;
    req.ks_profile.partition = PartitionStrategy::ByLimb;
    req.ks_profile.key_placement = KeyPlacement::ReplicatedPerCard;
    req.ks_profile.collective = CollectiveStrategy::GatherToRoot;
    req.ks_profile.scale_out_cards = 4;

    ExecutionPlan plan;
    plan.request_id = req.request_id;
    plan.assigned_cards = {0, 1, 2, 3};

    KeySwitchExecutionModel model;
    const KeySwitchExecution exec = model.Build(req, plan, state);

    EXPECT_TRUE(ctx, exec.valid);
    EXPECT_TRUE(ctx, !exec.fallback_used);
    EXPECT_TRUE(ctx, exec.method == KeySwitchMethod::ScaleOutLimb);
    EXPECT_EQ(ctx, exec.problem.cards, static_cast<uint32_t>(4));
    EXPECT_TRUE(ctx, CountSteps(exec, TileExecutionStepType::InputHBMToBRAM) > 0);

    EXPECT_TRUE(ctx, CountSteps(exec, TileExecutionStepType::InterCardSendStep) > 0);
    EXPECT_TRUE(ctx, CountSteps(exec, TileExecutionStepType::InterCardRecvStep) > 0);
    EXPECT_TRUE(ctx, CountSteps(exec, TileExecutionStepType::InterCardReduceStep) > 0);
    EXPECT_TRUE(ctx, CountSteps(exec, TileExecutionStepType::BarrierStep) > 0);
    EXPECT_EQ(ctx, exec.out_bram_to_hbm_bytes, req.ks_profile.output_bytes);
}

void TestScaleOutWithSingleAssignedCardDegradesToSingleBoard(testfw::TestContext& ctx) {
    SystemState state = MakeState(/*num_cards=*/1, /*num_pools=*/1);
    state.cards[0].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;

    Request req = MakeRequest(
        /*id=*/209,
        /*user=*/14,
        /*input_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*max_cards=*/4);
    ConfigureKeySwitchProblem(
        &req,
        /*ciphertexts=*/4,
        /*digits=*/4,
        /*limbs=*/8,
        /*polys=*/2,
        /*poly_degree=*/1,
        /*input_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*output_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*key_bytes=*/256ULL * 1024ULL);
    req.ks_profile.method = KeySwitchMethod::ScaleOutLimb;
    req.ks_profile.partition = PartitionStrategy::ByLimb;
    req.ks_profile.key_placement = KeyPlacement::ReplicatedPerCard;
    req.ks_profile.collective = CollectiveStrategy::GatherToRoot;
    req.ks_profile.scale_out_cards = 4;

    ExecutionPlan plan;
    plan.request_id = req.request_id;
    plan.assigned_cards = {0};

    KeySwitchExecutionModel model;
    const KeySwitchExecution exec = model.Build(req, plan, state);

    EXPECT_TRUE(ctx, exec.valid);
    EXPECT_TRUE(ctx, !exec.fallback_used);
    EXPECT_TRUE(ctx, exec.method == KeySwitchMethod::SingleBoardClassic);
    EXPECT_TRUE(ctx, exec.requested_method == KeySwitchMethod::ScaleOutLimb);
    EXPECT_TRUE(ctx, exec.effective_method == KeySwitchMethod::SingleBoardClassic);
    EXPECT_TRUE(ctx, exec.method_degraded);
    EXPECT_TRUE(ctx, exec.degraded_reason == KeySwitchFallbackReason::DegradedToSingleBoard);
    EXPECT_TRUE(ctx, exec.fallback_reason == KeySwitchFallbackReason::None);
    EXPECT_EQ(ctx, exec.problem.cards, static_cast<uint32_t>(1));
    EXPECT_EQ(ctx, CountSteps(exec, TileExecutionStepType::InterCardSendStep), static_cast<size_t>(0));
    EXPECT_EQ(ctx, CountSteps(exec, TileExecutionStepType::InterCardRecvStep), static_cast<size_t>(0));
    EXPECT_EQ(ctx, CountSteps(exec, TileExecutionStepType::InterCardReduceStep), static_cast<size_t>(0));
    EXPECT_EQ(ctx, CountSteps(exec, TileExecutionStepType::BarrierStep), static_cast<size_t>(0));

    CycleBackend backend;
    const ExecutionResult cycle_result = backend.Estimate(req, plan, state);
    EXPECT_TRUE(ctx, !cycle_result.fallback_used);
    EXPECT_TRUE(ctx, cycle_result.requested_method == KeySwitchMethod::ScaleOutLimb);
    EXPECT_TRUE(ctx, cycle_result.effective_method == KeySwitchMethod::SingleBoardClassic);
    EXPECT_TRUE(ctx, cycle_result.method_degraded);
    EXPECT_TRUE(ctx, cycle_result.degraded_reason == KeySwitchFallbackReason::DegradedToSingleBoard);
    EXPECT_TRUE(ctx, cycle_result.fallback_reason == KeySwitchFallbackReason::None);
    EXPECT_TRUE(ctx, !cycle_result.degraded_reason_message.empty());
}

void TestPrimitiveSimulatorBreakdownForLocalAndInterCard(testfw::TestContext& ctx) {
    PrimitiveTrace trace;

    PrimitiveOp input_dma;
    input_dma.type = PrimitiveType::InputHBMToBRAM;
    input_dma.stage_type = StageType::Dispatch;
    input_dma.bytes = 32ULL * 1024ULL;
    input_dma.assigned_cards = {0};
    trace.ops.push_back(input_dma);

    PrimitiveOp ntt;
    ntt.type = PrimitiveType::NTT;
    ntt.stage_type = StageType::BasisConvert;
    ntt.work_units = 40000;
    ntt.assigned_cards = {0};
    trace.ops.push_back(ntt);

    PrimitiveOp inner;
    inner.type = PrimitiveType::KSInnerProd;
    inner.stage_type = StageType::Multiply;
    inner.work_units = 60000;
    inner.assigned_cards = {0};
    trace.ops.push_back(inner);

    PrimitiveOp send;
    send.type = PrimitiveType::InterCardSend;
    send.stage_type = StageType::Merge;
    send.bytes = 16ULL * 1024ULL;
    send.src_card = 1;
    send.dst_card = 0;
    send.fan_in = 2;
    send.assigned_cards = {0, 1};
    trace.ops.push_back(send);

    PrimitiveOp recv = send;
    recv.type = PrimitiveType::InterCardRecv;
    trace.ops.push_back(recv);

    PrimitiveOp reduce;
    reduce.type = PrimitiveType::InterCardReduce;
    reduce.stage_type = StageType::Merge;
    reduce.bytes = 64ULL * 1024ULL;
    reduce.work_units = 1024;
    reduce.fan_in = 4;
    reduce.assigned_cards = {0, 1, 2, 3};
    trace.ops.push_back(reduce);

    PrimitiveOp barrier;
    barrier.type = PrimitiveType::Barrier;
    barrier.stage_type = StageType::Merge;
    barrier.fan_in = 4;
    barrier.assigned_cards = {0, 1, 2, 3};
    trace.ops.push_back(barrier);

    SystemState state = MakeState(/*num_cards=*/4, /*num_pools=*/1);
    PrimitiveSimulatorStub simulator;
    const PrimitiveResult result = simulator.Simulate(trace, state);

    const PrimitiveTypeBreakdown* ntt_breakdown =
        FindPrimitiveBreakdownEntry(result, PrimitiveType::NTT);
    const PrimitiveTypeBreakdown* inner_breakdown =
        FindPrimitiveBreakdownEntry(result, PrimitiveType::KSInnerProd);
    const PrimitiveTypeBreakdown* send_breakdown =
        FindPrimitiveBreakdownEntry(result, PrimitiveType::InterCardSend);
    const PrimitiveTypeBreakdown* recv_breakdown =
        FindPrimitiveBreakdownEntry(result, PrimitiveType::InterCardRecv);
    const PrimitiveTypeBreakdown* reduce_breakdown =
        FindPrimitiveBreakdownEntry(result, PrimitiveType::InterCardReduce);
    const PrimitiveTypeBreakdown* barrier_breakdown =
        FindPrimitiveBreakdownEntry(result, PrimitiveType::Barrier);

    EXPECT_TRUE(ctx, result.total_latency_ns > 0);
    EXPECT_TRUE(ctx, ntt_breakdown != nullptr);
    EXPECT_TRUE(ctx, inner_breakdown != nullptr);
    EXPECT_TRUE(ctx, send_breakdown != nullptr);
    EXPECT_TRUE(ctx, recv_breakdown != nullptr);
    EXPECT_TRUE(ctx, reduce_breakdown != nullptr);
    EXPECT_TRUE(ctx, barrier_breakdown != nullptr);

    EXPECT_TRUE(ctx, ntt_breakdown->latency_ns > 0);
    EXPECT_TRUE(ctx, inner_breakdown->latency_ns > 0);
    EXPECT_TRUE(ctx, send_breakdown->latency_ns > 0);
    EXPECT_TRUE(ctx, recv_breakdown->latency_ns > 0);
    EXPECT_TRUE(ctx, reduce_breakdown->latency_ns > 0);
    EXPECT_TRUE(ctx, barrier_breakdown->latency_ns > 0);

    EXPECT_EQ(ctx, send_breakdown->bytes, send.bytes);
    EXPECT_EQ(ctx, recv_breakdown->bytes, recv.bytes);
    EXPECT_EQ(ctx, reduce_breakdown->bytes, reduce.bytes);
}

void TestMethodConfigProducesDifferentTraceStructure(testfw::TestContext& ctx) {
    SystemState state = MakeState(/*num_cards=*/2, /*num_pools=*/1);
    state.cards[0].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;
    state.cards[1].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;

    Request base = MakeRequest(
        /*id=*/210,
        /*user=*/15,
        /*input_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*max_cards=*/2);
    ConfigureKeySwitchProblem(
        &base,
        /*ciphertexts=*/6,
        /*digits=*/4,
        /*limbs=*/10,
        /*polys=*/2,
        /*poly_degree=*/1,
        /*input_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*output_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*key_bytes=*/256ULL * 1024ULL);

    ExecutionPlan plan;
    plan.request_id = base.request_id;
    plan.assigned_cards = {0, 1};

    KeySwitchExecutionModel model;

    Request single_req = base;
    single_req.ks_profile.method = KeySwitchMethod::SingleBoardClassic;
    const KeySwitchExecution single_exec = model.Build(single_req, plan, state);

    Request scale_req = base;
    scale_req.request_id += 1;
    scale_req.ks_profile.method = KeySwitchMethod::ScaleOutLimb;
    scale_req.ks_profile.partition = PartitionStrategy::ByLimb;
    scale_req.ks_profile.key_placement = KeyPlacement::ReplicatedPerCard;
    scale_req.ks_profile.collective = CollectiveStrategy::GatherToRoot;
    scale_req.ks_profile.scale_out_cards = 2;
    const KeySwitchExecution scale_exec = model.Build(scale_req, plan, state);

    EXPECT_TRUE(ctx, single_exec.valid);
    EXPECT_TRUE(ctx, scale_exec.valid);

    EXPECT_EQ(ctx, CountSteps(single_exec, TileExecutionStepType::InterCardSendStep), static_cast<size_t>(0));
    EXPECT_EQ(ctx, CountSteps(single_exec, TileExecutionStepType::InterCardRecvStep), static_cast<size_t>(0));
    EXPECT_EQ(ctx, CountSteps(single_exec, TileExecutionStepType::InterCardReduceStep), static_cast<size_t>(0));
    EXPECT_TRUE(ctx, CountSteps(scale_exec, TileExecutionStepType::InterCardSendStep) > 0);
    EXPECT_TRUE(ctx, CountSteps(scale_exec, TileExecutionStepType::InterCardRecvStep) > 0);
    EXPECT_TRUE(ctx, CountSteps(scale_exec, TileExecutionStepType::InterCardReduceStep) > 0);
    EXPECT_TRUE(ctx, CountSteps(scale_exec, TileExecutionStepType::BarrierStep) > 0);
    EXPECT_TRUE(ctx, scale_exec.steps.size() > single_exec.steps.size());
}

void TestCycleBackendScaleOutCommunicationBreakdown(testfw::TestContext& ctx) {
    SystemState state = MakeState(/*num_cards=*/4, /*num_pools=*/1);
    for (CardId card_id = 0; card_id < 4; ++card_id) {
        state.cards[card_id].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;
    }

    Request req = MakeRequest(
        /*id=*/211,
        /*user=*/16,
        /*input_bytes=*/4ULL * 1024ULL * 1024ULL,
        /*max_cards=*/4);
    ConfigureKeySwitchProblem(
        &req,
        /*ciphertexts=*/4,
        /*digits=*/4,
        /*limbs=*/16,
        /*polys=*/2,
        /*poly_degree=*/1,
        /*input_bytes=*/4ULL * 1024ULL * 1024ULL,
        /*output_bytes=*/4ULL * 1024ULL * 1024ULL,
        /*key_bytes=*/512ULL * 1024ULL);
    req.ks_profile.method = KeySwitchMethod::ScaleOutLimb;
    req.ks_profile.partition = PartitionStrategy::ByLimb;
    req.ks_profile.key_placement = KeyPlacement::ReplicatedPerCard;
    req.ks_profile.collective = CollectiveStrategy::GatherToRoot;
    req.ks_profile.scale_out_cards = 4;

    ExecutionPlan plan;
    plan.request_id = req.request_id;
    plan.assigned_cards = {0, 1, 2, 3};

    CycleBackend backend;
    const ExecutionResult result = backend.Estimate(req, plan, state);

    EXPECT_TRUE(ctx, !result.fallback_used);
    EXPECT_TRUE(ctx, result.transfer_breakdown.input_hbm_to_bram_time > 0);
    EXPECT_TRUE(ctx, result.compute_breakdown.inner_product_time > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_send_time > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_recv_time > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_reduce_time > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_barrier_time > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_send_bytes > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_reduce_bytes > 0);
}

void TestCycleBackendSingleBoardTraceShape(testfw::TestContext& ctx) {
    SystemState state = MakeState(/*num_cards=*/2, /*num_pools=*/1);
    state.cards[0].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;
    state.cards[1].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;

    Request req = MakeRequest(
        /*id=*/212,
        /*user=*/17,
        /*input_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*max_cards=*/2);
    ConfigureKeySwitchProblem(
        &req,
        /*ciphertexts=*/4,
        /*digits=*/4,
        /*limbs=*/8,
        /*polys=*/2,
        /*poly_degree=*/1,
        /*input_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*output_bytes=*/2ULL * 1024ULL * 1024ULL,
        /*key_bytes=*/256ULL * 1024ULL);
    req.ks_profile.method = KeySwitchMethod::SingleBoardClassic;
    req.ks_profile.partition = PartitionStrategy::None;
    req.ks_profile.key_placement = KeyPlacement::StreamFromHBM;
    req.ks_profile.collective = CollectiveStrategy::None;
    req.ks_profile.scale_out_cards = 1;
    req.ks_profile.enable_inter_card_merge = false;
    req.ks_profile.allow_cross_card_reduce = false;
    req.ks_profile.allow_local_moddown = true;

    ExecutionPlan plan;
    plan.request_id = req.request_id;
    plan.assigned_cards = {0, 1};

    CycleBackend backend;
    const ExecutionResult result = backend.Estimate(req, plan, state);

    EXPECT_TRUE(ctx, !result.fallback_used);
    EXPECT_TRUE(ctx, !result.method_degraded);
    EXPECT_TRUE(ctx, result.requested_method == KeySwitchMethod::SingleBoardClassic);
    EXPECT_TRUE(ctx, result.effective_method == KeySwitchMethod::SingleBoardClassic);
    EXPECT_TRUE(ctx, result.normal_execution);
    EXPECT_TRUE(ctx, result.primitive_breakdown_primary);
    EXPECT_TRUE(ctx, result.stage_breakdown_compat_only);
    EXPECT_TRUE(ctx, !result.unsupported_method);
    EXPECT_TRUE(ctx, !result.unsupported_config);
    EXPECT_TRUE(ctx, !result.compatibility_fallback);
    EXPECT_TRUE(ctx, result.total_latency > 0);
    EXPECT_EQ(ctx, result.total_latency, SumStageBreakdown(result));

    EXPECT_EQ(ctx, result.communication_breakdown.inter_card_send_time, static_cast<Time>(0));
    EXPECT_EQ(ctx, result.communication_breakdown.inter_card_recv_time, static_cast<Time>(0));
    EXPECT_EQ(ctx, result.communication_breakdown.inter_card_reduce_time, static_cast<Time>(0));
    EXPECT_EQ(ctx, result.communication_breakdown.inter_card_barrier_time, static_cast<Time>(0));
    EXPECT_EQ(ctx, result.communication_breakdown.inter_card_send_bytes, static_cast<uint64_t>(0));
    EXPECT_EQ(ctx, result.communication_breakdown.inter_card_recv_bytes, static_cast<uint64_t>(0));
    EXPECT_EQ(ctx, result.communication_breakdown.inter_card_reduce_bytes, static_cast<uint64_t>(0));
}

void TestCycleBackendScaleOutTraceShape(testfw::TestContext& ctx) {
    SystemState state = MakeState(/*num_cards=*/4, /*num_pools=*/1);
    for (CardId card_id = 0; card_id < 4; ++card_id) {
        state.cards[card_id].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;
    }

    Request req = MakeRequest(
        /*id=*/213,
        /*user=*/18,
        /*input_bytes=*/4ULL * 1024ULL * 1024ULL,
        /*max_cards=*/4);
    ConfigureKeySwitchProblem(
        &req,
        /*ciphertexts=*/4,
        /*digits=*/4,
        /*limbs=*/16,
        /*polys=*/2,
        /*poly_degree=*/1,
        /*input_bytes=*/4ULL * 1024ULL * 1024ULL,
        /*output_bytes=*/4ULL * 1024ULL * 1024ULL,
        /*key_bytes=*/512ULL * 1024ULL);
    req.ks_profile.method = KeySwitchMethod::ScaleOutLimb;
    req.ks_profile.partition = PartitionStrategy::ByLimb;
    req.ks_profile.key_placement = KeyPlacement::ReplicatedPerCard;
    req.ks_profile.collective = CollectiveStrategy::GatherToRoot;
    req.ks_profile.scale_out_cards = 4;
    req.ks_profile.enable_inter_card_merge = true;
    req.ks_profile.allow_cross_card_reduce = true;
    req.ks_profile.allow_local_moddown = true;

    ExecutionPlan plan;
    plan.request_id = req.request_id;
    plan.assigned_cards = {0, 1, 2, 3};

    CycleBackend backend;
    const ExecutionResult result = backend.Estimate(req, plan, state);

    EXPECT_TRUE(ctx, !result.fallback_used);
    EXPECT_TRUE(ctx, !result.method_degraded);
    EXPECT_TRUE(ctx, result.requested_method == KeySwitchMethod::ScaleOutLimb);
    EXPECT_TRUE(ctx, result.effective_method == KeySwitchMethod::ScaleOutLimb);
    EXPECT_TRUE(ctx, result.normal_execution);
    EXPECT_TRUE(ctx, result.primitive_breakdown_primary);
    EXPECT_TRUE(ctx, result.stage_breakdown_compat_only);
    EXPECT_TRUE(ctx, result.total_latency > 0);
    EXPECT_EQ(ctx, result.total_latency, SumStageBreakdown(result));

    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_send_time > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_recv_time > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_reduce_time > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_barrier_time > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_send_bytes > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_recv_bytes > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_reduce_bytes > 0);
}

void TestCycleBackendMethodReasonVisibility(testfw::TestContext& ctx) {
    {
        SystemState state = MakeState(/*num_cards=*/1, /*num_pools=*/1);
        state.cards[0].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;

        Request req = MakeRequest(
            /*id=*/214,
            /*user=*/19,
            /*input_bytes=*/2ULL * 1024ULL * 1024ULL,
            /*max_cards=*/4);
        ConfigureKeySwitchProblem(
            &req,
            /*ciphertexts=*/4,
            /*digits=*/4,
            /*limbs=*/8,
            /*polys=*/2,
            /*poly_degree=*/1,
            /*input_bytes=*/2ULL * 1024ULL * 1024ULL,
            /*output_bytes=*/2ULL * 1024ULL * 1024ULL,
            /*key_bytes=*/256ULL * 1024ULL);
        req.ks_profile.method = KeySwitchMethod::ScaleOutLimb;
        req.ks_profile.partition = PartitionStrategy::ByLimb;
        req.ks_profile.key_placement = KeyPlacement::ReplicatedPerCard;
        req.ks_profile.collective = CollectiveStrategy::GatherToRoot;
        req.ks_profile.scale_out_cards = 4;
        req.ks_profile.enable_inter_card_merge = true;
        req.ks_profile.allow_cross_card_reduce = true;
        req.ks_profile.allow_local_moddown = true;

        ExecutionPlan plan;
        plan.request_id = req.request_id;
        plan.assigned_cards = {0};

        CycleBackend backend;
        const ExecutionResult degraded = backend.Estimate(req, plan, state);
        EXPECT_TRUE(ctx, !degraded.fallback_used);
        EXPECT_TRUE(ctx, degraded.method_degraded);
        EXPECT_TRUE(ctx, degraded.degraded_to_single_board);
        EXPECT_TRUE(ctx, degraded.degraded_reason == KeySwitchFallbackReason::DegradedToSingleBoard);
        EXPECT_TRUE(ctx, degraded.requested_method == KeySwitchMethod::ScaleOutLimb);
        EXPECT_TRUE(ctx, degraded.effective_method == KeySwitchMethod::SingleBoardClassic);
        EXPECT_TRUE(ctx, !degraded.normal_execution);
        EXPECT_TRUE(ctx, !degraded.unsupported_method);
        EXPECT_TRUE(ctx, !degraded.unsupported_config);
    }

    {
        SystemState state = MakeState(/*num_cards=*/1, /*num_pools=*/1);
        state.cards[0].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;

        Request req = MakeRequest(
            /*id=*/215,
            /*user=*/20,
            /*input_bytes=*/4096,
            /*max_cards=*/1);
        ConfigureKeySwitchProblem(
            &req,
            /*ciphertexts=*/2,
            /*digits=*/3,
            /*limbs=*/4,
            /*polys=*/2,
            /*poly_degree=*/1,
            /*input_bytes=*/4096,
            /*output_bytes=*/4096,
            /*key_bytes=*/8192);
        req.ks_profile.method = KeySwitchMethod::SingleBoardFused;

        ExecutionPlan plan;
        plan.request_id = req.request_id;
        plan.assigned_cards = {0};

        CycleBackend backend;
        const ExecutionResult unsupported = backend.Estimate(req, plan, state);
        EXPECT_TRUE(ctx, unsupported.fallback_used);
        EXPECT_TRUE(ctx, unsupported.fallback_reason == KeySwitchFallbackReason::UnsupportedMethod);
        EXPECT_TRUE(ctx, unsupported.unsupported_method);
        EXPECT_TRUE(ctx, !unsupported.unsupported_config);
        EXPECT_TRUE(ctx, !unsupported.normal_execution);
        EXPECT_TRUE(ctx, unsupported.requested_method == KeySwitchMethod::SingleBoardFused);
        EXPECT_TRUE(ctx, unsupported.effective_method == KeySwitchMethod::SingleBoardFused);
        EXPECT_TRUE(ctx, unsupported.primitive_breakdown_primary);
        EXPECT_TRUE(ctx, unsupported.stage_breakdown_compat_only);
    }
}

void TestBackendsSharedModelConsistency(testfw::TestContext& ctx) {
    SystemState state = MakeState(/*num_cards=*/2, /*num_pools=*/1);
    state.cards[0].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;
    state.cards[1].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;

    Request req = MakeRequest(
        /*id=*/203,
        /*user=*/4,
        /*input_bytes=*/4096,
        /*max_cards=*/2);
    ConfigureKeySwitchProblem(
        &req,
        /*ciphertexts=*/2,
        /*digits=*/3,
        /*limbs=*/4,
        /*polys=*/2,
        /*poly_degree=*/1,
        /*input_bytes=*/4096,
        /*output_bytes=*/4096,
        /*key_bytes=*/8192);

    ExecutionPlan plan;
    plan.request_id = req.request_id;
    plan.assigned_cards = {0, 1};

    AnalyticalBackend analytical;
    CycleBackend cycle;
    KeySwitchExecutionModel shared_model;

    const ExecutionResult analytical_result = analytical.Estimate(req, plan, state);
    const ExecutionResult cycle_result = cycle.Estimate(req, plan, state);
    const KeySwitchExecution shared_exec = shared_model.Build(req, plan, state);

    EXPECT_TRUE(ctx, !analytical_result.fallback_used);
    EXPECT_TRUE(ctx, !cycle_result.fallback_used);
    EXPECT_TRUE(ctx, shared_exec.valid);

    EXPECT_EQ(ctx, analytical_result.tile_count, cycle_result.tile_count);
    EXPECT_EQ(ctx, analytical_result.tile_count, shared_exec.tile_count);
    EXPECT_EQ(ctx, analytical_result.key_persistent_bram, cycle_result.key_persistent_bram);
    EXPECT_EQ(ctx, analytical_result.key_resident_hit, cycle_result.key_resident_hit);
    EXPECT_EQ(ctx, analytical_result.key_hbm_to_bram_bytes, cycle_result.key_hbm_to_bram_bytes);
    EXPECT_EQ(ctx, analytical_result.ct_hbm_to_bram_bytes, cycle_result.ct_hbm_to_bram_bytes);
    EXPECT_EQ(ctx, analytical_result.out_bram_to_hbm_bytes, cycle_result.out_bram_to_hbm_bytes);
}

void TestFallbackFlagVisibility(testfw::TestContext& ctx) {
    SystemState state = MakeState(/*num_cards=*/1, /*num_pools=*/1);
    state.cards[0].bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;

    Request req = MakeRequest(
        /*id=*/204,
        /*user=*/5,
        /*input_bytes=*/4096,
        /*max_cards=*/1);
    ConfigureKeySwitchProblem(
        &req,
        /*ciphertexts=*/2,
        /*digits=*/3,
        /*limbs=*/4,
        /*polys=*/2,
        /*poly_degree=*/1,
        /*input_bytes=*/4096,
        /*output_bytes=*/4096,
        /*key_bytes=*/8192);

    ExecutionPlan no_card_plan;
    no_card_plan.request_id = req.request_id;
    no_card_plan.assigned_cards = {};

    AnalyticalBackend analytical;
    CycleBackend cycle;
    KeySwitchExecutionModel shared_model;

    const ExecutionResult analytical_fallback = analytical.Estimate(req, no_card_plan, state);
    const ExecutionResult cycle_fallback = cycle.Estimate(req, no_card_plan, state);
    const KeySwitchExecution shared_fallback = shared_model.Build(req, no_card_plan, state);

    EXPECT_TRUE(ctx, analytical_fallback.fallback_used);
    EXPECT_TRUE(ctx, cycle_fallback.fallback_used);
    EXPECT_TRUE(ctx, shared_fallback.fallback_used);
    EXPECT_TRUE(ctx, shared_fallback.fallback_reason == KeySwitchFallbackReason::NoAssignedCard);
    EXPECT_TRUE(ctx, cycle_fallback.fallback_reason == KeySwitchFallbackReason::NoAssignedCard);
    EXPECT_TRUE(ctx, !cycle_fallback.fallback_reason_message.empty());
    EXPECT_TRUE(ctx, !analytical_fallback.tiled_execution);
    EXPECT_TRUE(ctx, !cycle_fallback.tiled_execution);
    EXPECT_TRUE(ctx, analytical_fallback.hbm_read_bytes > 0);
    EXPECT_TRUE(ctx, cycle_fallback.hbm_read_bytes > 0);

    ExecutionPlan ok_plan;
    ok_plan.request_id = req.request_id;
    ok_plan.assigned_cards = {0};
    const ExecutionResult analytical_ok = analytical.Estimate(req, ok_plan, state);
    const ExecutionResult cycle_ok = cycle.Estimate(req, ok_plan, state);
    EXPECT_TRUE(ctx, !analytical_ok.fallback_used);
    EXPECT_TRUE(ctx, !cycle_ok.fallback_used);
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
        Request req = MakeRequest(
            /*id=*/13,
            /*user=*/0,
            /*input_bytes=*/3ULL * kAlveoU280HbmBytes,
            /*max_cards=*/4);
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
        Request req = MakeRequest(
            /*id=*/14,
            /*user=*/0,
            /*input_bytes=*/3ULL * kAlveoU280HbmBytes,
            /*max_cards=*/4);
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
    params.key_storage_divisor = 1;
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
    EXPECT_EQ(
        ctx,
        req.user_profile.ct_bytes,
        params.ComputeCiphertextBytes(
            /*ciphertext_count=*/1,
            params.num_polys,
            params.num_rns_limbs));
    EXPECT_EQ(ctx, req.ks_profile.num_polys, params.num_polys);
    EXPECT_TRUE(ctx, req.ks_profile.num_digits >= params.num_digits - 1);
    EXPECT_TRUE(ctx, req.ks_profile.num_digits <= params.num_digits + 1);
    EXPECT_TRUE(
        ctx,
        req.ks_profile.num_rns_limbs == params.num_rns_limbs
            || req.ks_profile.num_rns_limbs == params.num_rns_limbs + 2);
    EXPECT_EQ(
        ctx,
        req.ks_profile.input_bytes,
        params.ComputeCiphertextBytes(
            req.ks_profile.num_ciphertexts,
            req.ks_profile.num_polys,
            req.ks_profile.num_rns_limbs));
    EXPECT_EQ(ctx, req.ks_profile.output_bytes, req.ks_profile.input_bytes);
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
        {"CycleBackendFormulaConsistency", TestCycleBackendFormulaConsistency},
        {"KeySwitchTilePlanningBasic", TestKeySwitchTilePlanningBasic},
        {"KeySwitchAutoTilingOnSmallBram", TestKeySwitchAutoTilingOnSmallBram},
        {"KeySwitchPersistentKeyTransferDelta", TestKeySwitchPersistentKeyTransferDelta},
        {"KeySwitchMethodSelection", TestKeySwitchMethodSelection},
        {"UnsupportedMethodDoesNotSilentlyDegrade", TestUnsupportedMethodDoesNotSilentlyDegrade},
        {"ScaleOutConfigSemanticsAreEnforced", TestScaleOutConfigSemanticsAreEnforced},
        {"SingleBoardRejectsDisabledLocalModdown", TestSingleBoardRejectsDisabledLocalModdown},
        {"SingleBoardClassicLocalPrimitiveFlow", TestSingleBoardClassicLocalPrimitiveFlow},
        {"ScaleOutLimbIncludesInterCardFlow", TestScaleOutLimbIncludesInterCardFlow},
        {"ScaleOutSingleCardDegrades", TestScaleOutWithSingleAssignedCardDegradesToSingleBoard},
        {"PrimitiveSimulatorLocalAndCommBreakdown", TestPrimitiveSimulatorBreakdownForLocalAndInterCard},
        {"MethodConfigDifferentTraceStructure", TestMethodConfigProducesDifferentTraceStructure},
        {"CycleBackendScaleOutCommunicationBreakdown", TestCycleBackendScaleOutCommunicationBreakdown},
        {"CycleBackendSingleBoardTraceShape", TestCycleBackendSingleBoardTraceShape},
        {"CycleBackendScaleOutTraceShape", TestCycleBackendScaleOutTraceShape},
        {"CycleBackendMethodReasonVisibility", TestCycleBackendMethodReasonVisibility},
        {"BackendsSharedModelConsistency", TestBackendsSharedModelConsistency},
        {"FallbackFlagVisibility", TestFallbackFlagVisibility},
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
