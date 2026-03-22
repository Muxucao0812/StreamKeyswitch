#include "backend/hw/hardware_model.h"
#include "backend/model/keyswitch_execution_model.h"
#include "backend/runtime_planner.h"
#include "model/request_sizing.h"
#include "test_framework.h"

#include <algorithm>
#include <array>
#include <functional>
#include <iostream>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace {

Request MakeRequest(
    RequestId request_id,
    UserId user_id,
    KeySwitchMethod method) {

    Request req;
    req.request_id = request_id;
    req.user_id = user_id;
    req.user_profile.user_id = user_id;
    req.user_profile.key_bytes = 8192;
    req.user_profile.key_load_time = 500;
    req.user_profile.weight = 1;

    req.ks_profile.method = method;
    req.ks_profile.num_ciphertexts = 2;
    req.ks_profile.num_digits = 3;
    req.ks_profile.num_rns_limbs = 4;
    req.ks_profile.num_polys = 2;
    req.ks_profile.poly_modulus_degree = 4096;
    req.ks_profile.input_bytes = 4ULL * 1024ULL * 1024ULL;
    req.ks_profile.output_bytes = 4ULL * 1024ULL * 1024ULL;
    req.ks_profile.key_bytes = 64ULL * 1024ULL * 1024ULL;
    req.ks_profile.preferred_cards = 1;
    req.ks_profile.max_cards = 1;
    req.ks_profile.scale_out_cards = 1;
    req.ks_profile.enable_inter_card_merge = true;
    req.ks_profile.allow_cross_card_reduce = true;
    req.ks_profile.allow_local_moddown = true;
    return req;
}

SystemState MakeState(uint32_t num_cards) {
    SystemState state;
    state.now = 0;

    ResourcePool pool;
    pool.pool_id = 0;
    pool.name = "pool0";
    state.pools.push_back(pool);

    state.cards.reserve(num_cards);
    for (CardId card_id = 0; card_id < num_cards; ++card_id) {
        CardState card;
        card.card_id = card_id;
        card.pool_id = 0;
        card.memory_capacity_bytes = kAlveoU280HbmBytes;
        card.bram_capacity_bytes = kAlveoU280BramBytes;
        state.cards.push_back(card);
        state.pools[0].card_ids.push_back(card_id);
    }
    return state;
}

ExecutionPlan MakePlan(RequestId request_id, uint32_t num_cards) {
    ExecutionPlan plan;
    plan.request_id = request_id;
    plan.assigned_cards.reserve(num_cards);
    for (CardId card_id = 0; card_id < num_cards; ++card_id) {
        plan.assigned_cards.push_back(card_id);
    }
    return plan;
}

RuntimePlan BuildRuntimePlanForMethod(
    KeySwitchMethod method,
    const KeySwitchExecutionModel& model,
    const SystemState& state) {

    const Request req = MakeRequest(/*request_id=*/700, /*user_id=*/11, method);
    const ExecutionPlan plan = MakePlan(req.request_id, /*num_cards=*/1);
    const KeySwitchExecution execution = model.Build(req, plan, state);
    if (!execution.valid) {
        return RuntimePlan{};
    }

    const HardwareModel hardware;
    RuntimePlanner planner(hardware, model.TilePlannerParams());
    return planner.Plan(execution);
}

uint64_t CountDependencyEdges(const RuntimePlan& plan) {
    uint64_t edges = 0;
    for (const TileExecutionStep& step : plan.steps) {
        edges += static_cast<uint64_t>(step.depends_on.size());
    }
    return edges;
}

uint64_t SumStageCounts(const RuntimePlanSummary& summary) {
    uint64_t total = 0;
    for (uint64_t count : summary.stage_step_counts) {
        total += count;
    }
    return total;
}

bool ContainsText(const std::string& text, const std::string& pattern) {
    return text.find(pattern) != std::string::npos;
}

uint32_t SumDigitShardCounts(const std::vector<DigitShard>& shards) {
    uint32_t total = 0;
    for (const DigitShard& shard : shards) {
        total += shard.count;
    }
    return total;
}

void TestRuntimePlanSummarySelfConsistent(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
    KeySwitchExecutionModel model;
    const RuntimePlan plan = BuildRuntimePlanForMethod(KeySwitchMethod::Poseidon, model, state);

    EXPECT_TRUE(ctx, plan.valid);
    const RuntimePlanValidationResult validation = ValidateRuntimePlanDataflow(plan);
    EXPECT_TRUE(ctx, validation.valid);

    const RuntimePlanSummary summary = SummarizeRuntimePlan(plan);
    EXPECT_TRUE(ctx, summary.valid);
    EXPECT_EQ(ctx, summary.total_steps, static_cast<uint64_t>(plan.steps.size()));
    EXPECT_EQ(ctx, summary.dependency_edges, CountDependencyEdges(plan));
    EXPECT_EQ(ctx, SumStageCounts(summary), summary.total_steps);
    EXPECT_TRUE(ctx, summary.critical_chain_depth > 0);
    EXPECT_TRUE(ctx, summary.hbm_read_bytes > 0);
    EXPECT_TRUE(ctx, summary.hbm_write_bytes > 0);
    EXPECT_TRUE(ctx, summary.spill_count > 0);
    EXPECT_TRUE(ctx, summary.reload_count > 0);
    EXPECT_EQ(ctx, summary.spill_count, summary.reload_count);
}

void TestRuntimePlanDeltaSelfIsZero(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
    KeySwitchExecutionModel model;
    const RuntimePlan plan = BuildRuntimePlanForMethod(KeySwitchMethod::Poseidon, model, state);
    EXPECT_TRUE(ctx, plan.valid);

    const RuntimePlanSummary summary = SummarizeRuntimePlan(plan);
    const RuntimePlanDelta delta = DiffRuntimePlanSummary(summary, summary);

    EXPECT_TRUE(ctx, !delta.changed);
    EXPECT_EQ(ctx, delta.total_steps, static_cast<int64_t>(0));
    EXPECT_EQ(ctx, delta.dependency_edges, static_cast<int64_t>(0));
    EXPECT_EQ(ctx, delta.hbm_read_bytes, static_cast<int64_t>(0));
    EXPECT_EQ(ctx, delta.hbm_write_bytes, static_cast<int64_t>(0));
    EXPECT_EQ(ctx, delta.spill_count, static_cast<int64_t>(0));
    EXPECT_EQ(ctx, delta.reload_count, static_cast<int64_t>(0));
    EXPECT_EQ(ctx, delta.critical_chain_depth, static_cast<int64_t>(0));

    const std::string formatted = FormatRuntimePlanDelta(summary, summary, delta);
    EXPECT_TRUE(ctx, ContainsText(formatted, "changed=false"));
}

void TestPoseidonVsHeraDirection(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
    KeySwitchExecutionModel model;
    const RuntimePlan poseidon = BuildRuntimePlanForMethod(KeySwitchMethod::Poseidon, model, state);
    const RuntimePlan hera = BuildRuntimePlanForMethod(KeySwitchMethod::HERA, model, state);

    EXPECT_TRUE(ctx, poseidon.valid);
    EXPECT_TRUE(ctx, hera.valid);

    const RuntimePlanSummary poseidon_summary = SummarizeRuntimePlan(poseidon);
    const RuntimePlanSummary hera_summary = SummarizeRuntimePlan(hera);
    const RuntimePlanDelta delta = DiffRuntimePlanSummary(poseidon_summary, hera_summary);

    EXPECT_TRUE(ctx, poseidon_summary.spill_count > hera_summary.spill_count);
    EXPECT_TRUE(ctx, poseidon_summary.reload_count > hera_summary.reload_count);
    EXPECT_TRUE(ctx, delta.spill_count < 0);
    EXPECT_TRUE(ctx, delta.reload_count < 0);
}

void TestValidationRejectsDuplicateStepId(testfw::TestContext& ctx) {
    RuntimePlan plan;
    plan.valid = true;

    TileExecutionStep input;
    input.step_id = 1;
    input.type = TileExecutionStepType::InputHBMToBRAM;
    input.stage_type = StageType::Dispatch;
    input.input_storage = IntermediateStorageLevel::HBM;
    input.output_storage = IntermediateStorageLevel::BRAM;
    input.input_bytes = 256;
    input.output_bytes = 256;
    input.bytes = 256;

    TileExecutionStep duplicate = input;
    duplicate.depends_on = {1};
    duplicate.input_storage = IntermediateStorageLevel::BRAM;
    duplicate.output_storage = IntermediateStorageLevel::BRAM;

    plan.steps = {input, duplicate};

    const RuntimePlanValidationResult validation = ValidateRuntimePlanDataflow(plan);
    EXPECT_TRUE(ctx, !validation.valid);
    EXPECT_TRUE(ctx, ContainsText(validation.reason, "duplicate_step_id"));
}

void TestValidationRejectsReloadWithoutSpill(testfw::TestContext& ctx) {
    RuntimePlan plan;
    plan.valid = true;

    TileExecutionStep input;
    input.step_id = 1;
    input.type = TileExecutionStepType::InputHBMToBRAM;
    input.stage_type = StageType::Dispatch;
    input.input_storage = IntermediateStorageLevel::HBM;
    input.output_storage = IntermediateStorageLevel::BRAM;
    input.input_bytes = 512;
    input.output_bytes = 512;
    input.bytes = 512;

    TileExecutionStep reload;
    reload.step_id = 2;
    reload.type = TileExecutionStepType::IntermediateHBMToBRAM;
    reload.stage_type = StageType::Dispatch;
    reload.input_storage = IntermediateStorageLevel::HBM;
    reload.output_storage = IntermediateStorageLevel::BRAM;
    reload.input_bytes = 512;
    reload.output_bytes = 512;
    reload.bytes = 512;
    reload.depends_on = {1};

    plan.steps = {input, reload};

    const RuntimePlanValidationResult validation = ValidateRuntimePlanDataflow(plan);
    EXPECT_TRUE(ctx, !validation.valid);
    EXPECT_TRUE(ctx, ContainsText(validation.reason, "reload_dep_not_spill"));
}

void TestDegradedCinnamonMatchesSingleBoardRuntimeSummary(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
    KeySwitchExecutionModel model;

    Request cinnamon_req = MakeRequest(/*request_id=*/701, /*user_id=*/31, KeySwitchMethod::Cinnamon);
    cinnamon_req.ks_profile.scale_out_cards = 4;
    const ExecutionPlan cinnamon_plan = MakePlan(cinnamon_req.request_id, /*num_cards=*/1);
    const KeySwitchExecution degraded = model.Build(cinnamon_req, cinnamon_plan, state);

    EXPECT_TRUE(ctx, degraded.valid);
    EXPECT_TRUE(ctx, degraded.method_degraded);
    EXPECT_TRUE(ctx, degraded.effective_method == KeySwitchMethod::Poseidon);

    RuntimePlan degraded_runtime;
    degraded_runtime.valid = degraded.valid;
    degraded_runtime.problem = degraded.problem;
    degraded_runtime.policy = degraded.policy;
    degraded_runtime.tile_plan = degraded.tile_plan;
    degraded_runtime.steps = degraded.steps;
    degraded_runtime.tile_count = degraded.tile_count;
    degraded_runtime.key_resident_hit = degraded.key_resident_hit;
    degraded_runtime.key_persistent_bram = degraded.key_persistent_bram;
    degraded_runtime.working_set_bytes = degraded.working_set_bytes;
    degraded_runtime.key_host_to_hbm_bytes = degraded.key_host_to_hbm_bytes;
    degraded_runtime.key_hbm_to_bram_bytes = degraded.key_hbm_to_bram_bytes;
    degraded_runtime.ct_hbm_to_bram_bytes = degraded.ct_hbm_to_bram_bytes;
    degraded_runtime.out_bram_to_hbm_bytes = degraded.out_bram_to_hbm_bytes;

    const RuntimePlanSummary degraded_summary = SummarizeRuntimePlan(degraded_runtime);

    Request poseidon_req = cinnamon_req;
    poseidon_req.ks_profile.method = KeySwitchMethod::Poseidon;
    poseidon_req.ks_profile.partition = PartitionStrategy::None;
    poseidon_req.ks_profile.collective = CollectiveStrategy::None;
    poseidon_req.ks_profile.key_placement = KeyPlacement::StreamFromHBM;
    poseidon_req.ks_profile.scale_out_cards = 1;
    poseidon_req.ks_profile.enable_inter_card_merge = false;
    poseidon_req.ks_profile.allow_cross_card_reduce = false;

    const ExecutionPlan single_plan = MakePlan(poseidon_req.request_id, /*num_cards=*/1);
    const KeySwitchExecution single_exec = model.Build(poseidon_req, single_plan, state);
    EXPECT_TRUE(ctx, single_exec.valid);

    const HardwareModel hardware;
    RuntimePlanner planner(hardware, model.TilePlannerParams());
    const RuntimePlan single_runtime = planner.Plan(single_exec);
    EXPECT_TRUE(ctx, single_runtime.valid);

    const RuntimePlanSummary single_summary = SummarizeRuntimePlan(single_runtime);
    const RuntimePlanDelta delta = DiffRuntimePlanSummary(single_summary, degraded_summary);

    EXPECT_TRUE(ctx, !delta.changed);
    EXPECT_EQ(ctx, single_summary.total_steps, degraded_summary.total_steps);
    EXPECT_EQ(ctx, single_summary.dependency_edges, degraded_summary.dependency_edges);
    EXPECT_EQ(ctx, single_summary.hbm_read_bytes, degraded_summary.hbm_read_bytes);
    EXPECT_EQ(ctx, single_summary.hbm_write_bytes, degraded_summary.hbm_write_bytes);
}

void TestBuildProblemCinnamonBuildsEvenDigitShards(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/4);
    KeySwitchExecutionModel model;

    Request req = MakeRequest(/*request_id=*/702, /*user_id=*/41, KeySwitchMethod::CinnamonOA);
    req.ks_profile.num_digits = 8;
    req.ks_profile.scale_out_cards = 4;
    const ExecutionPlan plan = MakePlan(req.request_id, /*num_cards=*/4);

    const KeySwitchProblem problem = model.BuildProblem(req, plan, state);
    EXPECT_TRUE(ctx, problem.valid);
    EXPECT_EQ(ctx, problem.active_cards, static_cast<uint32_t>(4));
    EXPECT_EQ(ctx, problem.digit_shards.size(), static_cast<std::size_t>(4));
    EXPECT_EQ(ctx, SumDigitShardCounts(problem.digit_shards), static_cast<uint32_t>(8));
    EXPECT_EQ(ctx, problem.digit_shards[0].count, static_cast<uint32_t>(2));
    EXPECT_EQ(ctx, problem.digit_shards[1].count, static_cast<uint32_t>(2));
    EXPECT_EQ(ctx, problem.digit_shards[2].count, static_cast<uint32_t>(2));
    EXPECT_EQ(ctx, problem.digit_shards[3].count, static_cast<uint32_t>(2));
}

void TestBuildProblemCinnamonBuildsUnevenDigitShards(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/3);
    KeySwitchExecutionModel model;

    Request req = MakeRequest(/*request_id=*/703, /*user_id=*/42, KeySwitchMethod::CinnamonOA);
    req.ks_profile.num_digits = 10;
    req.ks_profile.scale_out_cards = 3;
    const ExecutionPlan plan = MakePlan(req.request_id, /*num_cards=*/3);

    const KeySwitchProblem problem = model.BuildProblem(req, plan, state);
    EXPECT_TRUE(ctx, problem.valid);
    EXPECT_EQ(ctx, problem.active_cards, static_cast<uint32_t>(3));
    EXPECT_EQ(ctx, problem.digit_shards.size(), static_cast<std::size_t>(3));
    EXPECT_EQ(ctx, SumDigitShardCounts(problem.digit_shards), static_cast<uint32_t>(10));
    EXPECT_EQ(ctx, problem.digit_shards[0].begin, static_cast<uint32_t>(0));
    EXPECT_EQ(ctx, problem.digit_shards[0].count, static_cast<uint32_t>(3));
    EXPECT_EQ(ctx, problem.digit_shards[1].begin, static_cast<uint32_t>(3));
    EXPECT_EQ(ctx, problem.digit_shards[1].count, static_cast<uint32_t>(3));
    EXPECT_EQ(ctx, problem.digit_shards[2].begin, static_cast<uint32_t>(6));
    EXPECT_EQ(ctx, problem.digit_shards[2].count, static_cast<uint32_t>(4));
}

void TestBuildProblemCapsActiveCardsByDigits(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/8);
    KeySwitchExecutionModel model;

    Request req = MakeRequest(/*request_id=*/704, /*user_id=*/43, KeySwitchMethod::CinnamonOA);
    req.ks_profile.num_digits = 2;
    req.ks_profile.scale_out_cards = 8;
    const ExecutionPlan plan = MakePlan(req.request_id, /*num_cards=*/8);

    const KeySwitchProblem problem = model.BuildProblem(req, plan, state);
    EXPECT_TRUE(ctx, problem.valid);
    EXPECT_EQ(ctx, problem.active_cards, static_cast<uint32_t>(2));
    EXPECT_EQ(ctx, problem.cards, static_cast<uint32_t>(2));
    EXPECT_EQ(ctx, problem.digit_shards.size(), static_cast<std::size_t>(2));
    EXPECT_EQ(ctx, SumDigitShardCounts(problem.digit_shards), static_cast<uint32_t>(2));
}

void TestBuildProblemSingleBoardLeavesDigitShardsEmpty(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
    KeySwitchExecutionModel model;

    Request req = MakeRequest(/*request_id=*/705, /*user_id=*/44, KeySwitchMethod::Poseidon);
    req.ks_profile.num_digits = 4;
    req.ks_profile.scale_out_cards = 4;
    const ExecutionPlan plan = MakePlan(req.request_id, /*num_cards=*/1);

    const KeySwitchProblem problem = model.BuildProblem(req, plan, state);
    EXPECT_TRUE(ctx, problem.valid);
    EXPECT_EQ(ctx, problem.active_cards, static_cast<uint32_t>(1));
    EXPECT_EQ(ctx, problem.cards, static_cast<uint32_t>(1));
    EXPECT_TRUE(ctx, problem.digit_shards.empty());
}

} // namespace

int main() {
    testfw::TestContext ctx;

    const std::vector<std::pair<std::string, std::function<void(testfw::TestContext&)>>> tests = {
        {"RuntimePlanSummarySelfConsistent", TestRuntimePlanSummarySelfConsistent},
        {"RuntimePlanDeltaSelfIsZero", TestRuntimePlanDeltaSelfIsZero},
        {"PoseidonVsHeraDirection", TestPoseidonVsHeraDirection},
        {"ValidationRejectsDuplicateStepId", TestValidationRejectsDuplicateStepId},
        {"ValidationRejectsReloadWithoutSpill", TestValidationRejectsReloadWithoutSpill},
        {"DegradedCinnamonMatchesSingleBoardRuntimeSummary", TestDegradedCinnamonMatchesSingleBoardRuntimeSummary},
        {"BuildProblemCinnamonBuildsEvenDigitShards", TestBuildProblemCinnamonBuildsEvenDigitShards},
        {"BuildProblemCinnamonBuildsUnevenDigitShards", TestBuildProblemCinnamonBuildsUnevenDigitShards},
        {"BuildProblemCapsActiveCardsByDigits", TestBuildProblemCapsActiveCardsByDigits},
        {"BuildProblemSingleBoardLeavesDigitShardsEmpty", TestBuildProblemSingleBoardLeavesDigitShardsEmpty},
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
