#include "backend/cycle_backend.h"
#include "backend/cycle_sim/arch.h"
#include "backend/cycle_sim/driver.h"
#include "backend/cycle_sim/lowering.h"
#include "backend/model/keyswitch_execution_model.h"
#include "model/he_params.h"
#include "model/request_sizing.h"
#include "model/system_state.h"
#include "test_framework.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

namespace {

constexpr std::array<KeySwitchMethod, 5> kSharedSingleBoardMethods = {
    KeySwitchMethod::Poseidon,
    KeySwitchMethod::FAB,
    KeySwitchMethod::FAST,
    KeySwitchMethod::OLA,
    KeySwitchMethod::HERA,
};

Request MakePoseidonRequest(
    RequestId request_id,
    UserId user_id,
    uint32_t ciphertexts,
    uint32_t digits,
    uint32_t limbs,
    uint32_t polys,
    uint32_t poly_degree,
    uint64_t input_bytes,
    uint64_t output_bytes,
    uint64_t key_bytes) {

    Request req;
    req.request_id = request_id;
    req.user_id = user_id;
    req.user_profile.user_id = user_id;
    req.user_profile.key_bytes = key_bytes;
    req.user_profile.key_load_time = 500;
    req.user_profile.weight = 1;

    req.ks_profile.method = KeySwitchMethod::Poseidon;
    req.ks_profile.num_ciphertexts = ciphertexts;
    req.ks_profile.num_digits = digits;
    req.ks_profile.num_rns_limbs = limbs;
    req.ks_profile.num_polys = polys;
    req.ks_profile.poly_modulus_degree = poly_degree;
    req.ks_profile.input_bytes = input_bytes;
    req.ks_profile.output_bytes = output_bytes;
    req.ks_profile.key_bytes = key_bytes;
    req.ks_profile.preferred_cards = 1;
    req.ks_profile.max_cards = 1;
    return req;
}

SystemState MakeState(
    uint32_t num_cards,
    uint64_t bram_capacity_bytes = kAlveoU280BramBytes,
    std::optional<UserId> resident_user = std::nullopt) {

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
        card.bram_capacity_bytes = bram_capacity_bytes;
        card.resident_user = resident_user;
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

const CycleComponentStats* FindComponentStats(
    const std::vector<CycleComponentStats>& stats,
    CycleInstructionKind kind) {

    for (const auto& entry : stats) {
        if (entry.kind == kind) {
            return &entry;
        }
    }
    return nullptr;
}

const CycleGroupTiming* FindGroupTiming(
    const std::vector<CycleGroupTiming>& timings,
    uint32_t group_id) {

    for (const auto& timing : timings) {
        if (timing.group_id == group_id) {
            return &timing;
        }
    }
    return nullptr;
}

bool HasDependency(
    const CycleInstructionGroup& group,
    uint32_t dependency) {

    return std::find(
               group.dependencies.begin(),
               group.dependencies.end(),
               dependency)
        != group.dependencies.end();
}

bool HasInterCardSteps(const std::vector<TileExecutionStep>& steps) {
    for (const TileExecutionStep& step : steps) {
        switch (step.type) {
        case TileExecutionStepType::InterCardSendStep:
        case TileExecutionStepType::InterCardRecvStep:
        case TileExecutionStepType::InterCardReduceStep:
        case TileExecutionStepType::BarrierStep:
        case TileExecutionStepType::InterCardCommTile:
        case TileExecutionStepType::InterCardBarrier:
        case TileExecutionStepType::Merge:
            return true;
        default:
            break;
        }
    }
    return false;
}

Time StageSum(const ExecutionResult& result) {
    return result.breakdown.key_load_time
        + result.breakdown.dispatch_time
        + result.breakdown.decompose_time
        + result.breakdown.multiply_time
        + result.breakdown.basis_convert_time
        + result.breakdown.merge_time;
}

Time TransferSum(const ExecutionResult& result) {
    return result.transfer_breakdown.key_host_to_hbm_time
        + result.transfer_breakdown.key_hbm_to_bram_time
        + result.transfer_breakdown.input_hbm_to_bram_time
        + result.transfer_breakdown.output_bram_to_hbm_time;
}

Time ComputeSum(const ExecutionResult& result) {
    return result.compute_breakdown.transform_time
        + result.compute_breakdown.ntt_time
        + result.compute_breakdown.intt_time
        + result.compute_breakdown.bconv_time
        + result.compute_breakdown.inner_product_time
        + result.compute_breakdown.accumulate_time
        + result.compute_breakdown.subtract_time;
}

uint64_t SimulateTotalCycles(
    const Request& req,
    const SystemState& state,
    const HardwareModel& hardware) {

    const ExecutionPlan plan = MakePlan(req.request_id, 1);
    KeySwitchExecutionModel model;
    const KeySwitchExecution execution = model.BuildSingleBoard(req, plan, state);
    if (!execution.valid) {
        return 0;
    }

    PoseidonCycleLowerer lowerer(hardware);
    const CycleLoweringResult lowering = lowerer.Lower(execution);
    if (!lowering.valid) {
        return 0;
    }

    CycleDriver driver(hardware);
    return driver.Run(lowering.program).total_cycles;
}

CycleSimStats SimulateStats(
    const Request& req,
    const SystemState& state,
    const HardwareModel& hardware) {

    const ExecutionPlan plan = MakePlan(req.request_id, 1);
    KeySwitchExecutionModel model;
    const KeySwitchExecution execution = model.BuildSingleBoard(req, plan, state);
    if (!execution.valid) {
        return CycleSimStats{};
    }

    PoseidonCycleLowerer lowerer(hardware);
    const CycleLoweringResult lowering = lowerer.Lower(execution);
    if (!lowering.valid) {
        return CycleSimStats{};
    }

    CycleDriver driver(hardware);
    return driver.Run(lowering.program);
}

Time SimulateLatencyNs(
    const Request& req,
    const SystemState& state,
    const HardwareModel& hardware) {

    const uint64_t cycles = SimulateTotalCycles(req, state, hardware);
    return hardware.CyclesToNs(cycles);
}

void TestPoseidonLoweringSequence(testfw::TestContext& ctx) {
    const Request req = MakePoseidonRequest(
        /*request_id=*/1,
        /*user_id=*/7,
        /*ciphertexts=*/1,
        /*digits=*/1,
        /*limbs=*/1,
        /*polys=*/1,
        /*poly_degree=*/1024,
        /*input_bytes=*/4096,
        /*output_bytes=*/4096,
        /*key_bytes=*/1024);
    const SystemState state = MakeState(/*num_cards=*/1);
    const ExecutionPlan plan = MakePlan(req.request_id, 1);

    KeySwitchExecutionModel model;
    const KeySwitchExecution execution = model.BuildSingleBoard(req, plan, state);
    EXPECT_TRUE(ctx, execution.valid);
    EXPECT_TRUE(ctx, execution.tile_plan.key_persistent);

    auto count_steps = [](const KeySwitchExecution& ex, TileExecutionStepType type) {
        size_t count = 0;
        for (const TileExecutionStep& step : ex.steps) {
            if (step.type == type) {
                ++count;
            }
        }
        return count;
    };

    EXPECT_TRUE(ctx, count_steps(execution, TileExecutionStepType::ModUpInttTile) > 0);
    EXPECT_TRUE(ctx, count_steps(execution, TileExecutionStepType::ModUpBConvTile) > 0);
    EXPECT_TRUE(ctx, count_steps(execution, TileExecutionStepType::ModUpNttTile) > 0);
    EXPECT_TRUE(ctx, count_steps(execution, TileExecutionStepType::CrossDigitReduceTile) > 0);
    EXPECT_TRUE(ctx, count_steps(execution, TileExecutionStepType::ModDownInttTile) > 0);
    EXPECT_TRUE(ctx, count_steps(execution, TileExecutionStepType::ModDownNttTile) > 0);
    EXPECT_TRUE(ctx, count_steps(execution, TileExecutionStepType::FinalSubtractTile) > 0);
    EXPECT_TRUE(ctx, count_steps(execution, TileExecutionStepType::IntermediateBRAMToHBM) > 0);
    EXPECT_TRUE(ctx, count_steps(execution, TileExecutionStepType::IntermediateHBMToBRAM) > 0);

    HardwareModel hardware;
    PoseidonCycleLowerer lowerer(hardware);
    const CycleLoweringResult lowering = lowerer.Lower(execution);
    EXPECT_TRUE(ctx, lowering.valid);
    EXPECT_TRUE(ctx, !lowering.program.groups.empty());
}

void TestPoseidonLoweringAllowsDigitOverlap(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
    const ExecutionPlan plan = MakePlan(/*request_id=*/33, /*num_cards=*/1);
    KeySwitchExecutionModel model;

    Request poseidon = MakePoseidonRequest(
        /*request_id=*/33,
        /*user_id=*/7,
        /*ciphertexts=*/1,
        /*digits=*/2,
        /*limbs=*/2,
        /*polys=*/1,
        /*poly_degree=*/1024,
        /*input_bytes=*/8192,
        /*output_bytes=*/8192,
        /*key_bytes=*/2048);
    poseidon.ks_profile.method = KeySwitchMethod::Poseidon;

    Request hera = poseidon;
    hera.ks_profile.method = KeySwitchMethod::HERA;

    const KeySwitchExecution poseidon_exec = model.Build(poseidon, plan, state);
    const KeySwitchExecution hera_exec = model.Build(hera, plan, state);

    auto count_steps = [](const KeySwitchExecution& ex, TileExecutionStepType type) {
        size_t count = 0;
        for (const TileExecutionStep& step : ex.steps) {
            if (step.type == type) {
                ++count;
            }
        }
        return count;
    };

    EXPECT_TRUE(ctx, poseidon_exec.valid);
    EXPECT_TRUE(ctx, hera_exec.valid);
    EXPECT_TRUE(ctx, count_steps(poseidon_exec, TileExecutionStepType::IntermediateBRAMToHBM) > 0);
    EXPECT_EQ(ctx, count_steps(hera_exec, TileExecutionStepType::IntermediateBRAMToHBM), static_cast<std::size_t>(0));
    EXPECT_TRUE(ctx, hera_exec.policy.supports_moddown_shortcut);
    EXPECT_TRUE(ctx, poseidon_exec.policy.requires_large_rf == false);
}

void TestMethodAwareDataflowTraits(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
    const ExecutionPlan plan = MakePlan(/*request_id=*/34, /*num_cards=*/1);
    KeySwitchExecutionModel model;

    auto make_req = [](KeySwitchMethod method) {
        Request req = MakePoseidonRequest(
            /*request_id=*/34,
            /*user_id=*/8,
            /*ciphertexts=*/1,
            /*digits=*/3,
            /*limbs=*/3,
            /*polys=*/2,
            /*poly_degree=*/1024,
            /*input_bytes=*/12288,
            /*output_bytes=*/12288,
            /*key_bytes=*/3072);
        req.ks_profile.method = method;
        return req;
    };

    const KeySwitchExecution fab = model.Build(make_req(KeySwitchMethod::FAB), plan, state);
    const KeySwitchExecution fast = model.Build(make_req(KeySwitchMethod::FAST), plan, state);
    const KeySwitchExecution hera = model.Build(make_req(KeySwitchMethod::HERA), plan, state);

    auto count_steps = [](const KeySwitchExecution& ex, TileExecutionStepType type) {
        size_t count = 0;
        for (const TileExecutionStep& step : ex.steps) {
            if (step.type == type) {
                ++count;
            }
        }
        return count;
    };

    EXPECT_TRUE(ctx, fab.valid);
    EXPECT_TRUE(ctx, fast.valid);
    EXPECT_TRUE(ctx, hera.valid);

    EXPECT_TRUE(ctx, fab.policy.granularity == KeySwitchProcessingGranularity::Digit);
    EXPECT_TRUE(ctx, fab.policy.requires_large_rf);
    EXPECT_TRUE(ctx, fast.policy.granularity == KeySwitchProcessingGranularity::Limb);
    EXPECT_TRUE(ctx, count_steps(fast, TileExecutionStepType::IntermediateBRAMToHBM) > 0);
    EXPECT_TRUE(ctx, count_steps(hera, TileExecutionStepType::IntermediateBRAMToHBM) == 0);
    EXPECT_TRUE(ctx, hera.policy.supports_moddown_shortcut);
}

void TestCycleDriverEmptyProgram(testfw::TestContext& ctx) {
    HardwareModel hardware;
    CycleDriver driver(hardware);
    const CycleProgram program;
    const CycleSimStats stats = driver.Run(program);

    EXPECT_EQ(ctx, stats.total_cycles, static_cast<uint64_t>(0));
    EXPECT_TRUE(ctx, stats.group_timings.empty());
    EXPECT_TRUE(ctx, stats.component_stats.empty());
}

void TestCycleArchPipelineAndStall(testfw::TestContext& ctx) {
    HardwareConfig config;
    config.ewe_mul_unit_count = 1;
    config.ewe_pipeline_depth = 1;
    config.ewe_full_pipeline = false;
    config.ewe_mul_delay_cycles = 4;

    HardwareModel hardware(config);
    CycleArch arch(hardware);

    CycleInstruction first;
    first.id = 0;
    first.kind = CycleInstructionKind::EweMul;
    first.latency_cycles = 4;

    CycleInstruction second = first;
    second.id = 1;

    std::vector<uint64_t> completed;
    arch.BeginCycle(/*cycle=*/0, &completed);
    EXPECT_TRUE(ctx, completed.empty());
    EXPECT_TRUE(ctx, arch.CanIssue(CycleInstructionKind::EweMul, 0));
    arch.Issue(first, 0);
    EXPECT_TRUE(ctx, !arch.CanIssue(CycleInstructionKind::EweMul, 0));
    arch.RecordStall(CycleInstructionKind::EweMul);
    arch.EndCycle();

    for (uint64_t cycle = 1; cycle < 4; ++cycle) {
        completed.clear();
        arch.BeginCycle(cycle, &completed);
        EXPECT_TRUE(ctx, completed.empty());
        EXPECT_TRUE(ctx, !arch.CanIssue(CycleInstructionKind::EweMul, cycle));
        arch.RecordStall(CycleInstructionKind::EweMul);
        arch.EndCycle();
    }

    completed.clear();
    arch.BeginCycle(/*cycle=*/4, &completed);
    EXPECT_EQ(ctx, completed.size(), static_cast<std::size_t>(1));
    EXPECT_EQ(ctx, completed[0], static_cast<uint64_t>(0));
    EXPECT_TRUE(ctx, arch.CanIssue(CycleInstructionKind::EweMul, 4));
    arch.Issue(second, 4);
    arch.EndCycle();

    for (uint64_t cycle = 5; cycle < 8; ++cycle) {
        completed.clear();
        arch.BeginCycle(cycle, &completed);
        EXPECT_TRUE(ctx, completed.empty());
        arch.EndCycle();
    }

    completed.clear();
    arch.BeginCycle(/*cycle=*/8, &completed);
    EXPECT_EQ(ctx, completed.size(), static_cast<std::size_t>(1));
    EXPECT_EQ(ctx, completed[0], static_cast<uint64_t>(1));
    EXPECT_TRUE(ctx, !arch.HasInflight());
    arch.EndCycle();

    const auto component_stats = arch.GetComponentStats();
    const CycleComponentStats* mul_stats =
        FindComponentStats(component_stats, CycleInstructionKind::EweMul);
    EXPECT_TRUE(ctx, mul_stats != nullptr);
    if (mul_stats != nullptr) {
        EXPECT_EQ(ctx, mul_stats->issued_instructions, static_cast<uint64_t>(2));
        EXPECT_EQ(ctx, mul_stats->completed_instructions, static_cast<uint64_t>(2));
        EXPECT_EQ(ctx, mul_stats->stall_cycles, static_cast<uint64_t>(4));
        EXPECT_EQ(ctx, mul_stats->max_inflight, static_cast<uint64_t>(1));
        EXPECT_EQ(ctx, mul_stats->busy_cycles, static_cast<uint64_t>(8));
    }
}

void TestCycleDriverRoundRobinOverlap(testfw::TestContext& ctx) {
    HardwareConfig config;
    config.ewe_mul_unit_count = 2;
    config.ewe_pipeline_depth = 4;
    config.ewe_full_pipeline = true;

    HardwareModel hardware(config);
    CycleProgram program;

    CycleInstructionGroup group0;
    group0.id = 0;
    group0.kind = CycleInstructionKind::EweMul;
    group0.name = "g0";
    for (uint64_t idx = 0; idx < 2; ++idx) {
        CycleInstruction instruction;
        instruction.id = idx;
        instruction.group_id = group0.id;
        instruction.kind = group0.kind;
        instruction.latency_cycles = 4;
        group0.instructions.push_back(instruction);
    }

    CycleInstructionGroup group1;
    group1.id = 1;
    group1.kind = CycleInstructionKind::EweMul;
    group1.name = "g1";
    for (uint64_t idx = 0; idx < 2; ++idx) {
        CycleInstruction instruction;
        instruction.id = 2 + idx;
        instruction.group_id = group1.id;
        instruction.kind = group1.kind;
        instruction.latency_cycles = 4;
        group1.instructions.push_back(instruction);
    }

    program.groups.push_back(group0);
    program.groups.push_back(group1);
    program.instruction_count = 4;

    CycleDriver driver(hardware);
    const CycleSimStats stats = driver.Run(program);

    EXPECT_EQ(ctx, stats.group_timings.size(), static_cast<std::size_t>(2));
    const CycleGroupTiming* timing0 = FindGroupTiming(stats.group_timings, 0);
    const CycleGroupTiming* timing1 = FindGroupTiming(stats.group_timings, 1);
    EXPECT_TRUE(ctx, timing0 != nullptr);
    EXPECT_TRUE(ctx, timing1 != nullptr);
    if (timing0 != nullptr && timing1 != nullptr) {
        EXPECT_EQ(ctx, timing0->start_cycle, static_cast<uint64_t>(0));
        EXPECT_EQ(ctx, timing1->start_cycle, static_cast<uint64_t>(0));
        EXPECT_EQ(ctx, timing0->finish_cycle, static_cast<uint64_t>(5));
        EXPECT_EQ(ctx, timing1->finish_cycle, static_cast<uint64_t>(5));
    }
}

void TestCycleDriverPeakTracking(testfw::TestContext& ctx) {
    HardwareModel hardware;
    CycleProgram program;

    CycleInstructionGroup group0;
    group0.id = 0;
    group0.kind = CycleInstructionKind::LoadHBM;
    group0.name = "load";
    group0.live_bytes_delta_on_issue = 8;
    group0.live_bytes_delta_on_complete = 4;
    CycleInstruction load_instruction;
    load_instruction.id = 0;
    load_instruction.group_id = group0.id;
    load_instruction.kind = group0.kind;
    load_instruction.latency_cycles = 1;
    group0.instructions.push_back(load_instruction);

    CycleInstructionGroup group1;
    group1.id = 1;
    group1.kind = CycleInstructionKind::Decompose;
    group1.name = "compute";
    group1.dependencies.push_back(0);
    group1.live_bytes_delta_on_issue = 5;
    group1.live_bytes_delta_on_complete = -9;
    CycleInstruction compute_instruction;
    compute_instruction.id = 1;
    compute_instruction.group_id = group1.id;
    compute_instruction.kind = group1.kind;
    compute_instruction.latency_cycles = 1;
    group1.instructions.push_back(compute_instruction);

    program.groups.push_back(group0);
    program.groups.push_back(group1);
    program.instruction_count = 2;

    CycleDriver driver(hardware);
    const CycleSimStats stats = driver.Run(program);

    EXPECT_EQ(ctx, stats.peak_on_chip_live_bytes, static_cast<uint64_t>(17));
}

void TestEstimatePoseidonCyclePath(testfw::TestContext& ctx) {
    const Request req = MakePoseidonRequest(
        /*request_id=*/2,
        /*user_id=*/9,
        /*ciphertexts=*/2,
        /*digits=*/3,
        /*limbs=*/4,
        /*polys=*/2,
        /*poly_degree=*/65536,
        /*input_bytes=*/4096,
        /*output_bytes=*/4096,
        /*key_bytes=*/8192);
    const SystemState state = MakeState(/*num_cards=*/1);
    const ExecutionPlan plan = MakePlan(req.request_id, 1);
    HardwareModel hardware;

    CycleBackend backend;
    const ExecutionResult result = backend.Estimate(req, plan, state);
    const CycleSimStats sim_stats = SimulateStats(req, state, hardware);

    EXPECT_TRUE(ctx, !result.fallback_used);
    EXPECT_TRUE(ctx, result.tiled_execution);
    EXPECT_TRUE(ctx, result.tile_count >= 1);
    EXPECT_TRUE(ctx, result.total_latency > 0);
    EXPECT_EQ(ctx, result.total_latency, StageSum(result));
    EXPECT_TRUE(ctx, TransferSum(result) > 0);
    EXPECT_TRUE(ctx, ComputeSum(result) > 0);
    EXPECT_EQ(
        ctx,
        result.hbm_read_bytes,
        result.key_host_to_hbm_bytes
            + result.key_hbm_to_bram_bytes
            + result.ct_hbm_to_bram_bytes);
    EXPECT_EQ(ctx, result.hbm_write_bytes, result.out_bram_to_hbm_bytes);
    EXPECT_TRUE(ctx, sim_stats.peak_on_chip_live_bytes > 0);
    EXPECT_EQ(ctx, result.peak_bram_bytes, sim_stats.peak_on_chip_live_bytes);
}

void TestExecutionModelSharedSingleBoardMethods(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
    const ExecutionPlan plan = MakePlan(/*request_id=*/21, /*num_cards=*/1);
    KeySwitchExecutionModel model;

    for (KeySwitchMethod method : kSharedSingleBoardMethods) {
        Request req = MakePoseidonRequest(
            /*request_id=*/21,
            /*user_id=*/17,
            /*ciphertexts=*/1,
            /*digits=*/1,
            /*limbs=*/1,
            /*polys=*/1,
            /*poly_degree=*/1024,
            /*input_bytes=*/4096,
            /*output_bytes=*/4096,
            /*key_bytes=*/1024);
        req.ks_profile.method = method;

        const KeySwitchExecution execution = model.Build(req, plan, state);

        EXPECT_TRUE(ctx, execution.valid);
        EXPECT_TRUE(ctx, !execution.fallback_used);
        EXPECT_TRUE(ctx, execution.method == method);
        EXPECT_TRUE(ctx, execution.requested_method == method);
        EXPECT_TRUE(ctx, execution.effective_method == method);
        EXPECT_TRUE(ctx, execution.problem.method == method);
        EXPECT_EQ(ctx, execution.problem.cards, static_cast<uint32_t>(1));
        EXPECT_TRUE(ctx, !HasInterCardSteps(execution.steps));
    }
}

void TestCycleLowererSelectorDispatch(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
    const ExecutionPlan plan = MakePlan(/*request_id=*/24, /*num_cards=*/1);
    KeySwitchExecutionModel model;
    HardwareModel hardware;
    CycleLowererSelector selector(hardware);

    for (KeySwitchMethod method : kSharedSingleBoardMethods) {
        Request req = MakePoseidonRequest(
            /*request_id=*/24,
            /*user_id=*/29,
            /*ciphertexts=*/1,
            /*digits=*/1,
            /*limbs=*/1,
            /*polys=*/1,
            /*poly_degree=*/1024,
            /*input_bytes=*/4096,
            /*output_bytes=*/4096,
            /*key_bytes=*/1024);
        req.ks_profile.method = method;

        const KeySwitchExecution execution = model.Build(req, plan, state);
        const CycleLoweringResult lowering = selector.Lower(execution);

        EXPECT_TRUE(ctx, lowering.valid);
        EXPECT_TRUE(ctx, lowering.program.method == method);
        EXPECT_TRUE(ctx, !lowering.program.name.empty());
        EXPECT_TRUE(ctx, !lowering.program.empty());
    }
}

void TestCycleBackendSharedSingleBoardMethods(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
    const ExecutionPlan plan = MakePlan(/*request_id=*/22, /*num_cards=*/1);
    CycleBackend backend;

    for (KeySwitchMethod method : kSharedSingleBoardMethods) {
        Request req = MakePoseidonRequest(
            /*request_id=*/22,
            /*user_id=*/19,
            /*ciphertexts=*/1,
            /*digits=*/1,
            /*limbs=*/1,
            /*polys=*/1,
            /*poly_degree=*/1024,
            /*input_bytes=*/4096,
            /*output_bytes=*/4096,
            /*key_bytes=*/1024);
        req.ks_profile.method = method;

        const ExecutionResult result = backend.Estimate(req, plan, state);

        EXPECT_TRUE(ctx, !result.fallback_used);
        EXPECT_TRUE(ctx, !result.method_degraded);
        EXPECT_TRUE(ctx, result.requested_method == method);
        EXPECT_TRUE(ctx, result.effective_method == method);
        EXPECT_TRUE(ctx, result.total_latency > 0);
        EXPECT_EQ(ctx, result.total_latency, StageSum(result));
        EXPECT_TRUE(ctx, TransferSum(result) > 0);
        EXPECT_TRUE(ctx, ComputeSum(result) > 0);
    }
}

void TestAutoSingleCardStillMapsToPoseidon(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
    const ExecutionPlan plan = MakePlan(/*request_id=*/25, /*num_cards=*/1);

    Request req = MakePoseidonRequest(
        /*request_id=*/25,
        /*user_id=*/31,
        /*ciphertexts=*/1,
        /*digits=*/1,
        /*limbs=*/1,
        /*polys=*/1,
        /*poly_degree=*/1024,
        /*input_bytes=*/4096,
        /*output_bytes=*/4096,
        /*key_bytes=*/1024);
    req.ks_profile.method = KeySwitchMethod::Auto;

    CycleBackend backend;
    const ExecutionResult result = backend.Estimate(req, plan, state);

    EXPECT_TRUE(ctx, !result.fallback_used);
    EXPECT_TRUE(ctx, result.requested_method == KeySwitchMethod::Auto);
    EXPECT_TRUE(ctx, result.effective_method == KeySwitchMethod::Poseidon);
    EXPECT_TRUE(ctx, result.total_latency > 0);
}

void TestCinnamonRemainsUnsupported(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
    const ExecutionPlan plan = MakePlan(/*request_id=*/23, /*num_cards=*/1);

    Request req = MakePoseidonRequest(
        /*request_id=*/23,
        /*user_id=*/23,
        /*ciphertexts=*/1,
        /*digits=*/1,
        /*limbs=*/1,
        /*polys=*/1,
        /*poly_degree=*/1024,
        /*input_bytes=*/4096,
        /*output_bytes=*/4096,
        /*key_bytes=*/1024);
    req.ks_profile.method = KeySwitchMethod::Cinnamon;

    KeySwitchExecutionModel model;
    const KeySwitchExecution execution = model.Build(req, plan, state);
    EXPECT_TRUE(ctx, !execution.valid);
    EXPECT_TRUE(ctx, execution.fallback_used);
    EXPECT_TRUE(ctx, execution.fallback_reason == KeySwitchFallbackReason::UnsupportedMethod);
    EXPECT_TRUE(ctx, execution.method == KeySwitchMethod::Cinnamon);
    EXPECT_TRUE(ctx, execution.requested_method == KeySwitchMethod::Cinnamon);
    EXPECT_TRUE(ctx, execution.effective_method == KeySwitchMethod::Cinnamon);

    CycleBackend backend;
    const ExecutionResult result = backend.Estimate(req, plan, state);
    EXPECT_TRUE(ctx, result.fallback_used);
    EXPECT_TRUE(ctx, !result.method_degraded);
    EXPECT_TRUE(ctx, result.fallback_reason == KeySwitchFallbackReason::UnsupportedMethod);
    EXPECT_TRUE(ctx, result.requested_method == KeySwitchMethod::Cinnamon);
    EXPECT_TRUE(ctx, result.effective_method == KeySwitchMethod::Cinnamon);
    EXPECT_TRUE(ctx, !result.fallback_reason_message.empty());
}

void TestBuiltInScaleSingleBoardNoLongerFallsBack(testfw::TestContext& ctx) {
    const HEParams he = HEParams::BuiltInDefault();
    Request req = MakePoseidonRequest(
        /*request_id=*/26,
        /*user_id=*/37,
        /*ciphertexts=*/4,
        /*digits=*/he.num_digits,
        /*limbs=*/he.num_rns_limbs,
        /*polys=*/he.num_polys,
        /*poly_degree=*/he.poly_modulus_degree,
        /*input_bytes=*/he.ComputeCiphertextBytes(
            /*ciphertext_count=*/4,
            he.num_polys,
            he.num_rns_limbs),
        /*output_bytes=*/he.ComputeCiphertextBytes(
            /*ciphertext_count=*/4,
            he.num_polys,
            he.num_rns_limbs),
        /*key_bytes=*/he.ComputeKeyBytes());
    req.ks_profile.method = KeySwitchMethod::Poseidon;

    const SystemState state = MakeState(/*num_cards=*/1);
    const ExecutionPlan plan = MakePlan(req.request_id, /*num_cards=*/1);

    KeySwitchExecutionModel model;
    const KeySwitchExecution execution = model.BuildSingleBoard(req, plan, state);
    EXPECT_TRUE(ctx, execution.valid);
    EXPECT_TRUE(ctx, execution.tile_plan.valid);
    EXPECT_TRUE(ctx, !execution.fallback_used);
    EXPECT_TRUE(ctx, execution.tile_count > 1);
    EXPECT_TRUE(ctx, execution.tile_plan.limb_tile >= 1);
    EXPECT_TRUE(ctx, execution.tile_plan.ct_tile >= 1);

    CycleBackend backend;
    const ExecutionResult result = backend.Estimate(req, plan, state);
    EXPECT_TRUE(ctx, !result.fallback_used);
    EXPECT_TRUE(ctx, result.total_latency > 0);
    EXPECT_TRUE(ctx, result.tile_count > 1);
}

void TestProblemSizeMonotonicity(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
    HardwareModel hardware;

    const Request small = MakePoseidonRequest(
        /*request_id=*/3,
        /*user_id=*/11,
        /*ciphertexts=*/1,
        /*digits=*/1,
        /*limbs=*/1,
        /*polys=*/1,
        /*poly_degree=*/1024,
        /*input_bytes=*/4096,
        /*output_bytes=*/4096,
        /*key_bytes=*/1024);
    const Request large = MakePoseidonRequest(
        /*request_id=*/4,
        /*user_id=*/11,
        /*ciphertexts=*/2,
        /*digits=*/3,
        /*limbs=*/4,
        /*polys=*/2,
        /*poly_degree=*/65536,
        /*input_bytes=*/32768,
        /*output_bytes=*/32768,
        /*key_bytes=*/32768);

    const uint64_t small_cycles = SimulateTotalCycles(small, state, hardware);
    const uint64_t large_cycles = SimulateTotalCycles(large, state, hardware);

    EXPECT_TRUE(ctx, small_cycles > 0);
    EXPECT_TRUE(ctx, large_cycles > 0);
    EXPECT_TRUE(ctx, large_cycles >= small_cycles);
}

void TestHardwareScalingMonotonicity(testfw::TestContext& ctx) {
    const Request req = MakePoseidonRequest(
        /*request_id=*/5,
        /*user_id=*/13,
        /*ciphertexts=*/2,
        /*digits=*/3,
        /*limbs=*/4,
        /*polys=*/2,
        /*poly_degree=*/65536,
        /*input_bytes=*/32768,
        /*output_bytes=*/32768,
        /*key_bytes=*/32768);
    const SystemState state = MakeState(/*num_cards=*/1);

    HardwareConfig weak_config;
    weak_config.clock_mhz = 300.0;
    weak_config.pcie_write_bytes_per_ns = 12.0;
    weak_config.hbm_bytes_per_ns = 32.0;
    weak_config.decompose_unit_count = 1;
    weak_config.ntt_unit_count = 1;
    weak_config.ewe_mul_unit_count = 4;
    weak_config.ewe_add_unit_count = 2;
    weak_config.ewe_sub_unit_count = 2;
    weak_config.bconv_unit_count = 12;

    HardwareConfig strong_config = weak_config;
    strong_config.clock_mhz = 600.0;
    strong_config.pcie_write_bytes_per_ns = 24.0;
    strong_config.hbm_bytes_per_ns = 64.0;
    strong_config.decompose_unit_count = 2;
    strong_config.ntt_unit_count = 2;
    strong_config.ewe_mul_unit_count = 8;
    strong_config.ewe_add_unit_count = 4;
    strong_config.ewe_sub_unit_count = 4;
    strong_config.bconv_unit_count = 24;

    const HardwareModel weak_hw(weak_config);
    const HardwareModel strong_hw(strong_config);

    const Time weak_latency_ns = SimulateLatencyNs(req, state, weak_hw);
    const Time strong_latency_ns = SimulateLatencyNs(req, state, strong_hw);

    EXPECT_TRUE(ctx, weak_latency_ns > 0);
    EXPECT_TRUE(ctx, strong_latency_ns > 0);
    EXPECT_TRUE(ctx, strong_latency_ns <= weak_latency_ns);
}

} // namespace

int main() {
    testfw::TestContext ctx;

    TestPoseidonLoweringSequence(ctx);
    TestPoseidonLoweringAllowsDigitOverlap(ctx);
    TestMethodAwareDataflowTraits(ctx);
    TestCycleDriverEmptyProgram(ctx);
    TestCycleArchPipelineAndStall(ctx);
    TestCycleDriverRoundRobinOverlap(ctx);
    TestCycleDriverPeakTracking(ctx);
    TestEstimatePoseidonCyclePath(ctx);
    TestExecutionModelSharedSingleBoardMethods(ctx);
    TestCycleLowererSelectorDispatch(ctx);
    TestCycleBackendSharedSingleBoardMethods(ctx);
    TestAutoSingleCardStillMapsToPoseidon(ctx);
    TestCinnamonRemainsUnsupported(ctx);
    TestBuiltInScaleSingleBoardNoLongerFallsBack(ctx);
    TestProblemSizeMonotonicity(ctx);
    TestHardwareScalingMonotonicity(ctx);

    std::cout << "Cycle sim assertions: " << ctx.assertions
              << ", failures: " << ctx.failures << "\n";
    return (ctx.failures == 0) ? 0 : 1;
}
