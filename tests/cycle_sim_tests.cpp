#include "backend/cycle_backend/cycle_backend.h"
#include "backend/cycle_backend/cycle_backend_cinnamon_ib.h"
#include "backend/cycle_backend/cycle_backend_cinnamon_oa.h"
#include "backend/cycle_backend/cycle_backend_ola.h"
#include "backend/cycle_backend/cycle_backend_poseidon.h"
#include "backend/cycle_backend/cycle_backend_primitives.h"
#include "backend/cycle_sim/arch.h"
#include "backend/cycle_sim/driver.h"
#include "backend/cycle_sim/lowering.h"
#include "backend/model/keyswitch_execution_model.h"
#include "backend/runtime_planner.h"
#include "backend/step_graph_runtime.h"
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

std::size_t CountStepType(
    const std::vector<TileExecutionStep>& steps,
    TileExecutionStepType target_type) {

    std::size_t count = 0;
    for (const TileExecutionStep& step : steps) {
        if (step.type == target_type) {
            ++count;
        }
    }
    return count;
}

struct LiveReplayResult {
    bool underflow = false;
    uint64_t peak_live = 0;
    uint64_t live_end = 0;
};

uint64_t ApplyLiveDeltaForTest(uint64_t live_bytes, int64_t delta) {
    if (delta >= 0) {
        return live_bytes + static_cast<uint64_t>(delta);
    }
    const uint64_t released = static_cast<uint64_t>(-delta);
    return (released >= live_bytes) ? 0 : (live_bytes - released);
}

LiveReplayResult ReplayProgramLiveBytes(const CycleProgram& program) {
    LiveReplayResult replay;
    for (const CycleInstructionGroup& group : program.groups) {
        if (group.live_bytes_delta_on_issue < 0) {
            const uint64_t released =
                static_cast<uint64_t>(-group.live_bytes_delta_on_issue);
            if (released > replay.live_end) {
                replay.underflow = true;
            }
        }
        replay.live_end = ApplyLiveDeltaForTest(
            replay.live_end,
            group.live_bytes_delta_on_issue);
        replay.peak_live = std::max<uint64_t>(replay.peak_live, replay.live_end);

        if (group.live_bytes_delta_on_complete < 0) {
            const uint64_t released =
                static_cast<uint64_t>(-group.live_bytes_delta_on_complete);
            if (released > replay.live_end) {
                replay.underflow = true;
            }
        }
        replay.live_end = ApplyLiveDeltaForTest(
            replay.live_end,
            group.live_bytes_delta_on_complete);
        replay.peak_live = std::max<uint64_t>(replay.peak_live, replay.live_end);
    }
    return replay;
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

RuntimePlan BuildRuntimePlan(
    const Request& req,
    const SystemState& state,
    const HardwareModel& hardware) {

    const ExecutionPlan plan = MakePlan(req.request_id, 1);
    KeySwitchExecutionModel model;
    const KeySwitchExecution execution = model.Build(req, plan, state);
    if (!execution.valid) {
        return RuntimePlan{};
    }

    RuntimePlanner planner(hardware, model.TilePlannerParams());
    return planner.Plan(execution);
}

KeySwitchExecution BuildPhysicalExecution(
    const Request& req,
    const SystemState& state,
    const HardwareModel& hardware) {

    const ExecutionPlan plan = MakePlan(req.request_id, 1);
    KeySwitchExecutionModel model;
    KeySwitchExecution execution = model.Build(req, plan, state);
    if (!execution.valid) {
        return execution;
    }

    const RuntimePlan runtime_plan = BuildRuntimePlan(req, state, hardware);
    if (!runtime_plan.valid) {
        execution.valid = false;
        execution.fallback_used = true;
        execution.fallback_reason = KeySwitchFallbackReason::TilePlanInvalid;
        return execution;
    }

    execution.tile_plan = runtime_plan.tile_plan;
    execution.steps = runtime_plan.steps;
    execution.tile_count = runtime_plan.tile_count;
    execution.key_persistent_bram = runtime_plan.key_persistent_bram;
    execution.key_host_to_hbm_bytes = runtime_plan.key_host_to_hbm_bytes;
    execution.key_hbm_to_bram_bytes = runtime_plan.key_hbm_to_bram_bytes;
    execution.ct_hbm_to_bram_bytes = runtime_plan.ct_hbm_to_bram_bytes;
    execution.out_bram_to_hbm_bytes = runtime_plan.out_bram_to_hbm_bytes;
    execution.valid = runtime_plan.valid;
    return execution;
}

uint64_t SimulateTotalCycles(
    const Request& req,
    const SystemState& state,
    const HardwareModel& hardware) {

    const KeySwitchExecution execution = BuildPhysicalExecution(req, state, hardware);
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

    const KeySwitchExecution execution = BuildPhysicalExecution(req, state, hardware);
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

RuntimeState SimulateRuntimeState(
    const Request& req,
    const SystemState& state,
    const HardwareModel& hardware) {

    const RuntimePlan runtime_plan = BuildRuntimePlan(req, state, hardware);
    if (!runtime_plan.valid) {
        return RuntimeState{};
    }

    StepGraphRuntimeExecutor executor(hardware);
    return executor.ExecuteStepGraph(runtime_plan);
}

const StepRuntimeRecord* FindFirstRecord(
    const RuntimeState& state,
    TileExecutionStepType type) {

    for (const StepRuntimeRecord& record : state.step_records) {
        if (record.step_type == type) {
            return &record;
        }
    }
    return nullptr;
}

uint64_t SumStepCycles(const RuntimeState& state) {
    uint64_t total = 0;
    for (const StepRuntimeRecord& record : state.step_records) {
        total += record.total_cycles;
    }
    return total;
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
    const HardwareModel hardware;
    const KeySwitchExecution execution = BuildPhysicalExecution(req, state, hardware);
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
    PoseidonCycleLowerer lowerer(hardware);
    const CycleLoweringResult lowering = lowerer.Lower(execution);
    EXPECT_TRUE(ctx, lowering.valid);
    EXPECT_TRUE(ctx, !lowering.program.groups.empty());
}

void TestPoseidonLoweringAllowsDigitOverlap(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
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

    const HardwareModel hardware;
    const KeySwitchExecution poseidon_exec = BuildPhysicalExecution(poseidon, state, hardware);
    const KeySwitchExecution hera_exec = BuildPhysicalExecution(hera, state, hardware);

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
    EXPECT_TRUE(ctx, poseidon_exec.policy.requires_large_bram == false);
}

void TestMethodAwareDataflowTraits(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
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

    const HardwareModel hardware;
    const KeySwitchExecution fab = BuildPhysicalExecution(make_req(KeySwitchMethod::FAB), state, hardware);
    const KeySwitchExecution fast = BuildPhysicalExecution(make_req(KeySwitchMethod::FAST), state, hardware);
    const KeySwitchExecution hera = BuildPhysicalExecution(make_req(KeySwitchMethod::HERA), state, hardware);

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
    EXPECT_TRUE(ctx, fab.policy.requires_large_bram);
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

    EXPECT_EQ(ctx, stats.peak_bram_live_bytes, static_cast<uint64_t>(17));
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
    EXPECT_TRUE(ctx, result.hbm_write_bytes >= result.out_bram_to_hbm_bytes);
    EXPECT_TRUE(ctx, sim_stats.peak_bram_live_bytes > 0);
    EXPECT_TRUE(ctx, result.peak_bram_bytes > 0);
}

void TestPoseidonProgramMemoryAccountingConsistency(testfw::TestContext& ctx) {
    Request req = MakePoseidonRequest(
        /*request_id=*/30,
        /*user_id=*/31,
        /*ciphertexts=*/2,
        /*digits=*/3,
        /*limbs=*/4,
        /*polys=*/2,
        /*poly_degree=*/65536,
        /*input_bytes=*/4096,
        /*output_bytes=*/4096,
        /*key_bytes=*/8192);
    req.ks_profile.method = KeySwitchMethod::Poseidon;

    const SystemState state = MakeState(/*num_cards=*/1);
    const HardwareModel hardware;
    const KeySwitchExecution execution = BuildPhysicalExecution(req, state, hardware);
    EXPECT_TRUE(ctx, execution.valid);
    if (!execution.valid) {
        return;
    }

    const CycleProgram program = BuildPoseidonProgram(execution.problem, hardware);
    EXPECT_TRUE(ctx, !program.empty());
    if (program.empty()) {
        return;
    }

    const LiveReplayResult replay = ReplayProgramLiveBytes(program);
    EXPECT_EQ(ctx, program.estimated_peak_live_bytes, replay.peak_live);

    CycleDriver driver(hardware);
    const CycleSimStats stats = driver.Run(program);
    EXPECT_EQ(ctx, stats.peak_bram_live_bytes, replay.peak_live);
}

void TestOLAProgramMemoryAccountingConsistency(testfw::TestContext& ctx) {
    Request req = MakePoseidonRequest(
        /*request_id=*/31,
        /*user_id=*/32,
        /*ciphertexts=*/2,
        /*digits=*/3,
        /*limbs=*/4,
        /*polys=*/2,
        /*poly_degree=*/65536,
        /*input_bytes=*/4096,
        /*output_bytes=*/4096,
        /*key_bytes=*/8192);
    req.ks_profile.method = KeySwitchMethod::OLA;

    const SystemState state = MakeState(/*num_cards=*/1);
    const HardwareModel hardware;
    const KeySwitchExecution execution = BuildPhysicalExecution(req, state, hardware);
    EXPECT_TRUE(ctx, execution.valid);
    if (!execution.valid) {
        return;
    }

    const CycleProgram program = BuildOLAProgram(execution.problem, hardware);
    EXPECT_TRUE(ctx, !program.empty());
    if (program.empty()) {
        return;
    }

    const LiveReplayResult replay = ReplayProgramLiveBytes(program);
    EXPECT_TRUE(ctx, !replay.underflow);
    EXPECT_EQ(ctx, program.estimated_peak_live_bytes, replay.peak_live);

    CycleDriver driver(hardware);
    const CycleSimStats stats = driver.Run(program);
    EXPECT_EQ(ctx, stats.peak_bram_live_bytes, replay.peak_live);
}

void TestOLASpillReloadAccountingNoUnderflow(testfw::TestContext& ctx) {
    Request req = MakePoseidonRequest(
        /*request_id=*/32,
        /*user_id=*/33,
        /*ciphertexts=*/2,
        /*digits=*/4,
        /*limbs=*/4,
        /*polys=*/2,
        /*poly_degree=*/65536,
        /*input_bytes=*/4096,
        /*output_bytes=*/4096,
        /*key_bytes=*/8192);
    req.ks_profile.method = KeySwitchMethod::OLA;

    const SystemState state = MakeState(/*num_cards=*/1);
    const HardwareModel hardware;
    const KeySwitchExecution execution = BuildPhysicalExecution(req, state, hardware);
    EXPECT_TRUE(ctx, execution.valid);
    if (!execution.valid) {
        return;
    }

    const CycleProgram program = BuildOLAProgram(execution.problem, hardware);
    EXPECT_TRUE(ctx, !program.empty());
    if (program.empty()) {
        return;
    }

    uint64_t spill_groups = 0;
    uint64_t reload_groups = 0;
    for (const CycleInstructionGroup& group : program.groups) {
        if (group.name.find("spill") != std::string::npos) {
            ++spill_groups;
        }
        if (group.name.find("reload") != std::string::npos) {
            ++reload_groups;
        }
    }

    const LiveReplayResult replay = ReplayProgramLiveBytes(program);
    EXPECT_TRUE(ctx, !replay.underflow);
    EXPECT_TRUE(ctx, spill_groups > 0);
    EXPECT_TRUE(ctx, reload_groups > 0);
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
        EXPECT_TRUE(ctx, execution.logical_graph.valid);
        EXPECT_TRUE(ctx, !execution.logical_graph.nodes.empty());
        EXPECT_TRUE(ctx, execution.steps.empty());
    }
}

void TestCycleLowererSelectorDispatch(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
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

        const KeySwitchExecution execution = BuildPhysicalExecution(req, state, hardware);
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
        EXPECT_TRUE(ctx, result.compute_cycles > 0);
        EXPECT_TRUE(ctx, result.transfer_cycles > 0);
        EXPECT_EQ(ctx, result.dependency_stall_cycles, static_cast<uint64_t>(0));
        EXPECT_EQ(ctx, result.resource_stall_cycles, static_cast<uint64_t>(0));
    }
}

void TestDynamicRuntimeShortcutAffectsBytesAndWork(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
    const HardwareModel hardware;

    Request fast = MakePoseidonRequest(
        /*request_id=*/41,
        /*user_id=*/21,
        /*ciphertexts=*/1,
        /*digits=*/3,
        /*limbs=*/3,
        /*polys=*/2,
        /*poly_degree=*/1024,
        /*input_bytes=*/12288,
        /*output_bytes=*/12288,
        /*key_bytes=*/4096);
    fast.ks_profile.method = KeySwitchMethod::FAST;

    Request hera = fast;
    hera.request_id = 42;
    hera.ks_profile.method = KeySwitchMethod::HERA;

    const RuntimeState fast_runtime = SimulateRuntimeState(fast, state, hardware);
    const RuntimeState hera_runtime = SimulateRuntimeState(hera, state, hardware);

    EXPECT_TRUE(ctx, fast_runtime.valid);
    EXPECT_TRUE(ctx, hera_runtime.valid);

    const StepRuntimeRecord* fast_moddown_intt =
        FindFirstRecord(fast_runtime, TileExecutionStepType::ModDownInttTile);
    const StepRuntimeRecord* hera_moddown_intt =
        FindFirstRecord(hera_runtime, TileExecutionStepType::ModDownInttTile);
    const StepRuntimeRecord* fast_moddown_bconv =
        FindFirstRecord(fast_runtime, TileExecutionStepType::ModDownBConvTile);
    const StepRuntimeRecord* hera_moddown_bconv =
        FindFirstRecord(hera_runtime, TileExecutionStepType::ModDownBConvTile);

    EXPECT_TRUE(ctx, fast_moddown_intt != nullptr);
    EXPECT_TRUE(ctx, hera_moddown_intt != nullptr);
    EXPECT_TRUE(ctx, fast_moddown_bconv != nullptr);
    EXPECT_TRUE(ctx, hera_moddown_bconv != nullptr);

    if (fast_moddown_intt != nullptr && hera_moddown_intt != nullptr) {
        EXPECT_TRUE(ctx, hera_moddown_intt->input_bytes < fast_moddown_intt->input_bytes);
        EXPECT_EQ(ctx, hera_moddown_intt->compute_cycles, fast_moddown_intt->compute_cycles);
    }
    if (fast_moddown_bconv != nullptr && hera_moddown_bconv != nullptr) {
        EXPECT_EQ(ctx, hera_moddown_bconv->compute_cycles, fast_moddown_bconv->compute_cycles);
    }
}

void TestDynamicRuntimeStoragePathsDifferByMethod(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
    const ExecutionPlan plan = MakePlan(/*request_id=*/43, /*num_cards=*/1);
    const HardwareModel hardware;

    auto make_req = [](RequestId id, KeySwitchMethod method) {
        Request req = MakePoseidonRequest(
            /*request_id=*/id,
            /*user_id=*/22,
            /*ciphertexts=*/1,
            /*digits=*/2,
            /*limbs=*/2,
            /*polys=*/2,
            /*poly_degree=*/1024,
            /*input_bytes=*/8192,
            /*output_bytes=*/8192,
            /*key_bytes=*/2048);
        req.ks_profile.method = method;
        return req;
    };

    const Request poseidon_req = make_req(43, KeySwitchMethod::Poseidon);
    const Request fab_req = make_req(44, KeySwitchMethod::FAB);
    const Request fast_req = make_req(45, KeySwitchMethod::FAST);
    const Request hera_req = make_req(46, KeySwitchMethod::HERA);

    CycleBackend backend;
    const ExecutionResult poseidon_result = backend.Estimate(poseidon_req, plan, state);
    const ExecutionResult fab_result = backend.Estimate(fab_req, plan, state);
    const ExecutionResult fast_result = backend.Estimate(fast_req, plan, state);
    const ExecutionResult hera_result = backend.Estimate(hera_req, plan, state);

    EXPECT_TRUE(ctx, !poseidon_result.fallback_used);
    EXPECT_TRUE(ctx, !fab_result.fallback_used);
    EXPECT_TRUE(ctx, !fast_result.fallback_used);
    EXPECT_TRUE(ctx, !hera_result.fallback_used);

    EXPECT_TRUE(ctx, poseidon_result.spill_bytes > 0);
    EXPECT_TRUE(ctx, poseidon_result.reload_bytes > 0);
    EXPECT_TRUE(ctx, fab_result.bram_read_bytes > 0 || fab_result.bram_write_bytes > 0);
    EXPECT_TRUE(ctx, fast_result.bram_read_bytes > 0 || fast_result.bram_write_bytes > 0);
    EXPECT_TRUE(ctx, hera_result.direct_forward_count > poseidon_result.direct_forward_count);
    EXPECT_TRUE(ctx, hera_result.transfer_cycles < poseidon_result.transfer_cycles);

    const RuntimeState hera_runtime = SimulateRuntimeState(hera_req, state, hardware);
    EXPECT_TRUE(ctx, hera_runtime.valid);
    EXPECT_TRUE(ctx, hera_runtime.direct_forward_count > 0);
}

void TestDynamicRuntimeLifetimeAccounting(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
    const HardwareModel hardware;
    const ExecutionPlan plan = MakePlan(/*request_id=*/47, /*num_cards=*/1);

    Request req = MakePoseidonRequest(
        /*request_id=*/47,
        /*user_id=*/23,
        /*ciphertexts=*/1,
        /*digits=*/1,
        /*limbs=*/1,
        /*polys=*/1,
        /*poly_degree=*/1024,
        /*input_bytes=*/4096,
        /*output_bytes=*/4096,
        /*key_bytes=*/1024);
    req.ks_profile.method = KeySwitchMethod::HERA;

    KeySwitchExecutionModel model;
    const KeySwitchExecution execution = model.Build(req, plan, state);
    const RuntimePlan runtime_plan = BuildRuntimePlan(req, state, hardware);
    const RuntimeState runtime = SimulateRuntimeState(req, state, hardware);

    EXPECT_TRUE(ctx, execution.valid);
    EXPECT_TRUE(ctx, runtime_plan.valid);
    EXPECT_TRUE(ctx, runtime.valid);
    EXPECT_TRUE(ctx, runtime.peak_bram_bytes >= runtime.live_bram_bytes);
    EXPECT_TRUE(ctx, runtime.peak_hbm_bytes >= runtime.live_hbm_bytes);

    bool has_live_persistent = false;
    bool has_live_non_persistent = false;
    for (const auto& entry : runtime.tensors) {
        if (!entry.second.alive) {
            continue;
        }
        has_live_persistent = has_live_persistent || entry.second.persistent;
        has_live_non_persistent = has_live_non_persistent || !entry.second.persistent;
    }

    if (runtime_plan.key_persistent_bram) {
        EXPECT_TRUE(ctx, has_live_persistent);
    } else {
        EXPECT_TRUE(ctx, !has_live_persistent);
    }
    EXPECT_TRUE(ctx, !has_live_non_persistent);
}

void TestDynamicRuntimePipelineOverlap(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
    const HardwareModel hardware;

    Request req = MakePoseidonRequest(
        /*request_id=*/48,
        /*user_id=*/24,
        /*ciphertexts=*/2,
        /*digits=*/3,
        /*limbs=*/4,
        /*polys=*/2,
        /*poly_degree=*/65536,
        /*input_bytes=*/4096,
        /*output_bytes=*/4096,
        /*key_bytes=*/8192);
    req.ks_profile.method = KeySwitchMethod::Poseidon;

    RuntimePlan runtime_plan = BuildRuntimePlan(req, state, hardware);
    runtime_plan.steps.clear();

    TileExecutionStep load0;
    load0.step_id = 1;
    load0.type = TileExecutionStepType::InputHBMToBRAM;
    load0.stage_type = StageType::Dispatch;
    load0.input_bytes = 4096;
    load0.output_bytes = 4096;
    load0.input_storage = IntermediateStorageLevel::HBM;
    load0.output_storage = IntermediateStorageLevel::BRAM;

    TileExecutionStep modup0;
    modup0.step_id = 2;
    modup0.type = TileExecutionStepType::ModUpInttTile;
    modup0.stage_type = StageType::BasisConvert;
    modup0.input_bytes = 4096;
    modup0.output_bytes = 4096;
    modup0.work_items = 1;
    modup0.input_storage = IntermediateStorageLevel::BRAM;
    modup0.output_storage = IntermediateStorageLevel::BRAM;
    modup0.depends_on = {1};

    TileExecutionStep load1 = load0;
    load1.step_id = 3;

    TileExecutionStep modup1 = modup0;
    modup1.step_id = 4;
    modup1.depends_on = {3};

    runtime_plan.steps.push_back(load0);
    runtime_plan.steps.push_back(modup0);
    runtime_plan.steps.push_back(load1);
    runtime_plan.steps.push_back(modup1);

    StepGraphRuntimeExecutor executor(hardware);
    const RuntimeState runtime = executor.ExecuteStepGraph(runtime_plan);
    const uint64_t serial_step_cycles = SumStepCycles(runtime);

    EXPECT_TRUE(ctx, runtime_plan.valid);
    EXPECT_TRUE(ctx, runtime.valid);
    EXPECT_TRUE(ctx, !runtime.step_records.empty());
    EXPECT_TRUE(ctx, runtime.total_cycles > 0);
    EXPECT_TRUE(ctx, serial_step_cycles >= runtime.total_cycles);
    EXPECT_TRUE(ctx, runtime.compute_cycles + runtime.transfer_cycles >= runtime.total_cycles);
    EXPECT_TRUE(ctx, runtime.compute_cycles + runtime.transfer_cycles >= serial_step_cycles);
    EXPECT_TRUE(ctx, serial_step_cycles > runtime.total_cycles);
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

void TestCinnamonByDigitOutputAggregation(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/3);
    const ExecutionPlan plan = MakePlan(/*request_id=*/23, /*num_cards=*/3);

    Request req = MakePoseidonRequest(
        /*request_id=*/23,
        /*user_id=*/23,
        /*ciphertexts=*/2,
        /*digits=*/3,
        /*limbs=*/6,
        /*polys=*/2,
        /*poly_degree=*/1024,
        /*input_bytes=*/24576,
        /*output_bytes=*/24576,
        /*key_bytes=*/12288);
    req.ks_profile.method = KeySwitchMethod::Cinnamon;
    req.ks_profile.partition = PartitionStrategy::ByDigit;
    req.ks_profile.collective = CollectiveStrategy::GatherToRoot;
    req.ks_profile.key_placement = KeyPlacement::ShardedByPartition;
    req.ks_profile.multi_board_mode = MultiBoardMode::OutputAggregation;
    req.ks_profile.scale_out_cards = 3;
    req.ks_profile.enable_inter_card_merge = true;
    req.ks_profile.allow_cross_card_reduce = true;
    req.ks_profile.allow_local_moddown = true;

    KeySwitchExecutionModel model;
    const KeySwitchExecution execution = model.Build(req, plan, state);
    EXPECT_TRUE(ctx, execution.valid);
    EXPECT_TRUE(ctx, !execution.fallback_used);
    EXPECT_TRUE(ctx, !execution.method_degraded);
    EXPECT_TRUE(ctx, execution.method == KeySwitchMethod::Cinnamon);
    EXPECT_TRUE(ctx, execution.requested_method == KeySwitchMethod::Cinnamon);
    EXPECT_TRUE(ctx, execution.effective_method == KeySwitchMethod::Cinnamon);
    EXPECT_TRUE(ctx, execution.partition_strategy == PartitionStrategy::ByDigit);
    EXPECT_TRUE(ctx, execution.key_placement == KeyPlacement::ShardedByPartition);
    EXPECT_TRUE(ctx, execution.collective_strategy == CollectiveStrategy::GatherToRoot);
    EXPECT_TRUE(ctx, execution.multi_board_mode == MultiBoardMode::OutputAggregation);
    EXPECT_TRUE(ctx, execution.active_cards == 3);
    EXPECT_TRUE(ctx, CountStepType(execution.steps, TileExecutionStepType::InterCardSendStep) > 0);
    EXPECT_TRUE(ctx, CountStepType(execution.steps, TileExecutionStepType::InterCardRecvStep) > 0);
    EXPECT_TRUE(ctx, CountStepType(execution.steps, TileExecutionStepType::InterCardReduceStep) > 0);
    EXPECT_TRUE(ctx, CountStepType(execution.steps, TileExecutionStepType::BarrierStep) > 0);

    CycleBackend backend;
    const ExecutionResult result = backend.Estimate(req, plan, state);
    EXPECT_TRUE(ctx, !result.fallback_used);
    EXPECT_TRUE(ctx, !result.method_degraded);
    EXPECT_TRUE(ctx, result.requested_method == KeySwitchMethod::Cinnamon);
    EXPECT_TRUE(ctx, result.effective_method == KeySwitchMethod::Cinnamon);
    EXPECT_TRUE(ctx, result.total_latency > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_send_time > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_recv_time > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_reduce_time > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_barrier_time > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_send_bytes > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_recv_bytes > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_reduce_bytes > 0);
}

void TestCinnamonByDigitInputBroadcast(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/3);
    const ExecutionPlan plan = MakePlan(/*request_id=*/230, /*num_cards=*/3);

    Request req = MakePoseidonRequest(
        /*request_id=*/230,
        /*user_id=*/230,
        /*ciphertexts=*/2,
        /*digits=*/3,
        /*limbs=*/6,
        /*polys=*/2,
        /*poly_degree=*/1024,
        /*input_bytes=*/24576,
        /*output_bytes=*/24576,
        /*key_bytes=*/12288);
    req.ks_profile.method = KeySwitchMethod::Cinnamon;
    req.ks_profile.partition = PartitionStrategy::ByDigit;
    req.ks_profile.collective = CollectiveStrategy::GatherToRoot;
    req.ks_profile.key_placement = KeyPlacement::ShardedByPartition;
    req.ks_profile.multi_board_mode = MultiBoardMode::InputBroadcast;
    req.ks_profile.scale_out_cards = 3;
    req.ks_profile.enable_inter_card_merge = true;
    req.ks_profile.allow_cross_card_reduce = true;
    req.ks_profile.allow_local_moddown = true;

    KeySwitchExecutionModel model;
    const KeySwitchExecution execution = model.Build(req, plan, state);
    EXPECT_TRUE(ctx, execution.valid);
    EXPECT_TRUE(ctx, !execution.fallback_used);
    EXPECT_TRUE(ctx, !execution.method_degraded);
    EXPECT_TRUE(ctx, execution.method == KeySwitchMethod::Cinnamon);
    EXPECT_TRUE(ctx, execution.requested_method == KeySwitchMethod::Cinnamon);
    EXPECT_TRUE(ctx, execution.effective_method == KeySwitchMethod::Cinnamon);
    EXPECT_TRUE(ctx, execution.partition_strategy == PartitionStrategy::ByDigit);
    EXPECT_TRUE(ctx, execution.key_placement == KeyPlacement::ShardedByPartition);
    EXPECT_TRUE(ctx, execution.collective_strategy == CollectiveStrategy::GatherToRoot);
    EXPECT_TRUE(ctx, execution.multi_board_mode == MultiBoardMode::InputBroadcast);
    EXPECT_TRUE(ctx, execution.active_cards == 3);
    EXPECT_TRUE(ctx, CountStepType(execution.steps, TileExecutionStepType::InterCardSendStep) > 0);
    EXPECT_TRUE(ctx, CountStepType(execution.steps, TileExecutionStepType::InterCardRecvStep) > 0);
    EXPECT_TRUE(ctx, CountStepType(execution.steps, TileExecutionStepType::InterCardReduceStep) > 0);
    EXPECT_TRUE(ctx, CountStepType(execution.steps, TileExecutionStepType::BarrierStep) > 0);

    bool has_broadcast_send = false;
    bool has_broadcast_recv = false;
    const int32_t root_card = static_cast<int32_t>(plan.assigned_cards.front());
    for (const TileExecutionStep& step : execution.steps) {
        if (step.bytes != req.ks_profile.input_bytes) {
            continue;
        }
        if (step.src_card != root_card || step.dst_card == root_card) {
            continue;
        }
        if (step.type == TileExecutionStepType::InterCardSendStep) {
            has_broadcast_send = true;
        } else if (step.type == TileExecutionStepType::InterCardRecvStep) {
            has_broadcast_recv = true;
        }
    }
    EXPECT_TRUE(ctx, has_broadcast_send);
    EXPECT_TRUE(ctx, has_broadcast_recv);

    CycleBackend backend;
    const ExecutionResult result = backend.Estimate(req, plan, state);
    EXPECT_TRUE(ctx, !result.fallback_used);
    EXPECT_TRUE(ctx, !result.method_degraded);
    EXPECT_TRUE(ctx, result.requested_method == KeySwitchMethod::Cinnamon);
    EXPECT_TRUE(ctx, result.effective_method == KeySwitchMethod::Cinnamon);
    EXPECT_TRUE(ctx, result.total_latency > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_send_time > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_recv_time > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_reduce_time > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_barrier_time > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_send_bytes > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_recv_bytes > 0);
    EXPECT_TRUE(ctx, result.communication_breakdown.inter_card_reduce_bytes > 0);
}

void TestCinnamonSingleCardDegradesToOutputCentric(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/1);
    const ExecutionPlan plan = MakePlan(/*request_id=*/24, /*num_cards=*/1);

    Request req = MakePoseidonRequest(
        /*request_id=*/24,
        /*user_id=*/23,
        /*ciphertexts=*/2,
        /*digits=*/3,
        /*limbs=*/6,
        /*polys=*/2,
        /*poly_degree=*/1024,
        /*input_bytes=*/24576,
        /*output_bytes=*/24576,
        /*key_bytes=*/12288);
    req.ks_profile.method = KeySwitchMethod::Cinnamon;
    req.ks_profile.partition = PartitionStrategy::ByDigit;
    req.ks_profile.collective = CollectiveStrategy::GatherToRoot;
    req.ks_profile.key_placement = KeyPlacement::ShardedByPartition;
    req.ks_profile.multi_board_mode = MultiBoardMode::OutputAggregation;
    req.ks_profile.scale_out_cards = 3;
    req.ks_profile.enable_inter_card_merge = true;
    req.ks_profile.allow_cross_card_reduce = true;
    req.ks_profile.allow_local_moddown = true;

    KeySwitchExecutionModel model;
    const KeySwitchExecution execution = model.Build(req, plan, state);
    EXPECT_TRUE(ctx, execution.valid);
    EXPECT_TRUE(ctx, !execution.fallback_used);
    EXPECT_TRUE(ctx, execution.method_degraded);
    EXPECT_TRUE(ctx, execution.degraded_reason == KeySwitchFallbackReason::DegradedToSingleBoard);
    EXPECT_TRUE(ctx, execution.requested_method == KeySwitchMethod::Cinnamon);
    EXPECT_TRUE(ctx, execution.effective_method == KeySwitchMethod::OutputCentric);

    CycleBackend backend;
    const ExecutionResult result = backend.Estimate(req, plan, state);
    EXPECT_TRUE(ctx, !result.fallback_used);
    EXPECT_TRUE(ctx, result.method_degraded);
    EXPECT_TRUE(ctx, result.degraded_to_single_board);
    EXPECT_TRUE(ctx, result.degraded_reason == KeySwitchFallbackReason::DegradedToSingleBoard);
    EXPECT_TRUE(ctx, result.requested_method == KeySwitchMethod::Cinnamon);
    EXPECT_TRUE(ctx, result.effective_method == KeySwitchMethod::OutputCentric);
    EXPECT_TRUE(ctx, result.total_latency > 0);
}

void TestCinnamonRejectsInvalidConfig(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/3);
    const ExecutionPlan plan = MakePlan(/*request_id=*/25, /*num_cards=*/3);

    Request req = MakePoseidonRequest(
        /*request_id=*/25,
        /*user_id=*/29,
        /*ciphertexts=*/2,
        /*digits=*/3,
        /*limbs=*/6,
        /*polys=*/2,
        /*poly_degree=*/1024,
        /*input_bytes=*/24576,
        /*output_bytes=*/24576,
        /*key_bytes=*/12288);
    req.ks_profile.method = KeySwitchMethod::Cinnamon;
    req.ks_profile.partition = PartitionStrategy::ByLimb;
    req.ks_profile.collective = CollectiveStrategy::GatherToRoot;
    req.ks_profile.key_placement = KeyPlacement::ShardedByPartition;
    req.ks_profile.scale_out_cards = 3;
    req.ks_profile.enable_inter_card_merge = true;
    req.ks_profile.allow_cross_card_reduce = true;
    req.ks_profile.allow_local_moddown = true;

    KeySwitchExecutionModel model;
    const KeySwitchExecution execution = model.Build(req, plan, state);
    EXPECT_TRUE(ctx, !execution.valid);
    EXPECT_TRUE(ctx, execution.fallback_used);
    EXPECT_TRUE(ctx, execution.fallback_reason == KeySwitchFallbackReason::UnsupportedConfig);

    CycleBackend backend;
    const ExecutionResult result = backend.Estimate(req, plan, state);
    EXPECT_TRUE(ctx, result.fallback_used);
    EXPECT_TRUE(ctx, result.fallback_reason == KeySwitchFallbackReason::UnsupportedConfig);
}

void TestCinnamonProgramBuildersRequireProblemDigitShards(testfw::TestContext& ctx) {
    const SystemState state = MakeState(/*num_cards=*/3);
    const ExecutionPlan plan = MakePlan(/*request_id=*/251, /*num_cards=*/3);
    const HardwareModel hardware;
    KeySwitchExecutionModel model;

    Request oa_req = MakePoseidonRequest(
        /*request_id=*/251,
        /*user_id=*/30,
        /*ciphertexts=*/2,
        /*digits=*/10,
        /*limbs=*/6,
        /*polys=*/2,
        /*poly_degree=*/1024,
        /*input_bytes=*/24576,
        /*output_bytes=*/24576,
        /*key_bytes=*/12288);
    oa_req.ks_profile.method = KeySwitchMethod::CinnamonOA;
    oa_req.ks_profile.partition = PartitionStrategy::ByDigit;
    oa_req.ks_profile.collective = CollectiveStrategy::GatherToRoot;
    oa_req.ks_profile.key_placement = KeyPlacement::ShardedByPartition;
    oa_req.ks_profile.multi_board_mode = MultiBoardMode::OutputAggregation;
    oa_req.ks_profile.scale_out_cards = 3;
    oa_req.ks_profile.enable_inter_card_merge = true;
    oa_req.ks_profile.allow_cross_card_reduce = true;
    oa_req.ks_profile.allow_local_moddown = true;

    KeySwitchProblem oa_problem = model.BuildProblem(oa_req, plan, state);
    EXPECT_TRUE(ctx, oa_problem.valid);
    EXPECT_EQ(ctx, oa_problem.digit_shards.size(), static_cast<std::size_t>(3));
    EXPECT_TRUE(ctx, !BuildCinnamonOutputAggregationProgram(oa_problem, hardware).empty());
    oa_problem.digit_shards.pop_back();
    EXPECT_TRUE(ctx, BuildCinnamonOutputAggregationProgram(oa_problem, hardware).empty());

    Request ib_req = oa_req;
    ib_req.request_id = 252;
    ib_req.ks_profile.method = KeySwitchMethod::CinnamonIB;
    ib_req.ks_profile.multi_board_mode = MultiBoardMode::InputBroadcast;

    KeySwitchProblem ib_problem = model.BuildProblem(ib_req, plan, state);
    EXPECT_TRUE(ctx, ib_problem.valid);
    EXPECT_EQ(ctx, ib_problem.digit_shards.size(), static_cast<std::size_t>(3));
    EXPECT_TRUE(ctx, !BuildCinnamonInputBroadcastProgram(ib_problem, hardware).empty());
    ib_problem.digit_shards.clear();
    EXPECT_TRUE(ctx, BuildCinnamonInputBroadcastProgram(ib_problem, hardware).empty());
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
    EXPECT_TRUE(ctx, execution.logical_graph.valid);
    EXPECT_TRUE(ctx, !execution.fallback_used);

    const HardwareModel hardware;
    const RuntimePlan runtime_plan = BuildRuntimePlan(req, state, hardware);
    EXPECT_TRUE(ctx, runtime_plan.valid);
    EXPECT_TRUE(ctx, runtime_plan.tile_count > 1);
    EXPECT_TRUE(ctx, runtime_plan.tile_plan.limb_tile >= 1);
    EXPECT_TRUE(ctx, runtime_plan.tile_plan.ct_tile >= 1);

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

void TestCyclePrimitiveEmitterMicroOpRules(testfw::TestContext& ctx) {
    Request req = MakePoseidonRequest(
        /*request_id=*/49,
        /*user_id=*/25,
        /*ciphertexts=*/2,
        /*digits=*/3,
        /*limbs=*/4,
        /*polys=*/2,
        /*poly_degree=*/65536,
        /*input_bytes=*/4096,
        /*output_bytes=*/4096,
        /*key_bytes=*/8192);
    const SystemState state = MakeState(/*num_cards=*/1);
    const HardwareModel hardware;
    const KeySwitchExecution execution = BuildPhysicalExecution(req, state, hardware);
    EXPECT_TRUE(ctx, execution.valid);
    if (!execution.valid) {
        return;
    }

    CyclePrimitiveEmitter emitter(
        execution.problem,
        hardware,
        KeySwitchMethod::Poseidon,
        "primitive_micro_ops");

    CyclePrimitiveDesc desc;
    desc.transfer_path = CycleTransferPath::HBMToSPM;
    desc.type = CycleOpType::DataLoad;

    desc.name = "load";
    desc.bytes = 500;
    desc.input_limbs = 5;
    desc.output_limbs = 0;
    emitter.EmitLoadHBM(desc);
    EXPECT_EQ(
        ctx,
        emitter.Program().groups.back().instructions.size(),
        static_cast<std::size_t>(5));

    desc.name = "store";
    desc.transfer_path = CycleTransferPath::SPMToHBM;
    desc.type = CycleOpType::Spill;
    desc.bytes = 700;
    desc.input_limbs = 0;
    desc.output_limbs = 7;
    emitter.EmitStoreHBM(desc);
    EXPECT_EQ(
        ctx,
        emitter.Program().groups.back().instructions.size(),
        static_cast<std::size_t>(7));

    desc.name = "ntt";
    desc.transfer_path = CycleTransferPath::None;
    desc.type = CycleOpType::NTT;
    desc.bytes = 300;
    desc.input_limbs = 3;
    desc.output_limbs = 0;
    emitter.EmitNTT(desc);
    EXPECT_EQ(
        ctx,
        emitter.Program().groups.back().instructions.size(),
        static_cast<std::size_t>(3));

    desc.name = "intt";
    desc.type = CycleOpType::INTT;
    desc.bytes = 200;
    desc.input_limbs = 2;
    emitter.EmitINTT(desc);
    EXPECT_EQ(
        ctx,
        emitter.Program().groups.back().instructions.size(),
        static_cast<std::size_t>(2));

    desc.name = "mul";
    desc.type = CycleOpType::Multiply;
    desc.bytes = 400;
    desc.input_limbs = 4;
    emitter.EmitEweMul(desc);
    EXPECT_EQ(
        ctx,
        emitter.Program().groups.back().instructions.size(),
        static_cast<std::size_t>(4));

    desc.name = "add";
    desc.type = CycleOpType::Add;
    desc.bytes = 600;
    desc.input_limbs = 6;
    emitter.EmitEweAdd(desc);
    EXPECT_EQ(
        ctx,
        emitter.Program().groups.back().instructions.size(),
        static_cast<std::size_t>(6));

    desc.name = "sub";
    desc.type = CycleOpType::Sub;
    desc.bytes = 800;
    desc.input_limbs = 8;
    emitter.EmitEweSub(desc);
    EXPECT_EQ(
        ctx,
        emitter.Program().groups.back().instructions.size(),
        static_cast<std::size_t>(8));

    desc.name = "bconv";
    desc.type = CycleOpType::BConv;
    desc.bytes = 1200;
    desc.input_limbs = 3;
    desc.output_limbs = 4;
    emitter.EmitBConv(desc);
    EXPECT_EQ(
        ctx,
        emitter.Program().groups.back().instructions.size(),
        static_cast<std::size_t>(12));
}

void TestCyclePrimitiveEmitterMemoryReplayConsistency(testfw::TestContext& ctx) {
    Request req = MakePoseidonRequest(
        /*request_id=*/50,
        /*user_id=*/26,
        /*ciphertexts=*/2,
        /*digits=*/3,
        /*limbs=*/4,
        /*polys=*/2,
        /*poly_degree=*/65536,
        /*input_bytes=*/4096,
        /*output_bytes=*/4096,
        /*key_bytes=*/8192);
    const SystemState state = MakeState(/*num_cards=*/1);
    const HardwareModel hardware;
    const KeySwitchExecution execution = BuildPhysicalExecution(req, state, hardware);
    EXPECT_TRUE(ctx, execution.valid);
    if (!execution.valid) {
        return;
    }

    CyclePrimitiveEmitter emitter(
        execution.problem,
        hardware,
        KeySwitchMethod::Poseidon,
        "primitive_memory");

    CyclePrimitiveDesc desc;
    desc.transfer_path = CycleTransferPath::HBMToSPM;
    desc.type = CycleOpType::DataLoad;

    emitter.Bram().AcquireOnIssue(256);
    desc.name = "load_chunk";
    desc.bytes = 256;
    desc.input_limbs = 4;
    desc.output_limbs = 0;
    emitter.EmitLoadHBM(desc);

    emitter.Bram().AcquireOnComplete(128);
    desc.name = "bconv_chunk";
    desc.transfer_path = CycleTransferPath::None;
    desc.type = CycleOpType::BConv;
    desc.bytes = 384;
    desc.input_limbs = 4;
    desc.output_limbs = 2;
    emitter.EmitBConv(desc);

    emitter.Bram().ReleaseOnComplete(384);
    desc.name = "store_chunk";
    desc.transfer_path = CycleTransferPath::SPMToHBM;
    desc.type = CycleOpType::Spill;
    desc.bytes = 384;
    desc.input_limbs = 0;
    desc.output_limbs = 6;
    emitter.EmitStoreHBM(desc);

    EXPECT_TRUE(ctx, emitter.ValidateMemoryAccounting("PrimitiveTest"));

    CycleProgram& program = emitter.Program();
    program.estimated_peak_live_bytes = emitter.Bram().Peak();
    const LiveReplayResult replay = ReplayProgramLiveBytes(program);
    EXPECT_TRUE(ctx, !replay.underflow);
    EXPECT_EQ(ctx, program.estimated_peak_live_bytes, replay.peak_live);

    CycleDriver driver(hardware);
    const CycleSimStats stats = driver.Run(program);
    EXPECT_EQ(ctx, stats.peak_bram_live_bytes, replay.peak_live);
}

void TestPoseidonAndOLAProgramShapeRegression(testfw::TestContext& ctx) {
    Request req = MakePoseidonRequest(
        /*request_id=*/51,
        /*user_id=*/27,
        /*ciphertexts=*/2,
        /*digits=*/3,
        /*limbs=*/4,
        /*polys=*/2,
        /*poly_degree=*/65536,
        /*input_bytes=*/4096,
        /*output_bytes=*/4096,
        /*key_bytes=*/8192);
    const SystemState state = MakeState(/*num_cards=*/1);
    const HardwareModel hardware;
    const KeySwitchExecution execution = BuildPhysicalExecution(req, state, hardware);
    EXPECT_TRUE(ctx, execution.valid);
    if (!execution.valid) {
        return;
    }

    const CycleProgram poseidon = BuildPoseidonProgram(execution.problem, hardware);
    const CycleProgram ola = BuildOLAProgram(execution.problem, hardware);
    EXPECT_TRUE(ctx, !poseidon.empty());
    EXPECT_TRUE(ctx, !ola.empty());
    if (poseidon.empty() || ola.empty()) {
        return;
    }

    uint64_t poseidon_spill = 0;
    uint64_t poseidon_reload = 0;
    for (const CycleInstructionGroup& group : poseidon.groups) {
        if (group.name.find("spill") != std::string::npos) {
            ++poseidon_spill;
        }
        if (group.name.find("reload") != std::string::npos) {
            ++poseidon_reload;
        }
    }

    uint64_t ola_spill = 0;
    uint64_t ola_reload = 0;
    for (const CycleInstructionGroup& group : ola.groups) {
        if (group.name.find("spill") != std::string::npos) {
            ++ola_spill;
        }
        if (group.name.find("reload") != std::string::npos) {
            ++ola_reload;
        }
    }

    EXPECT_TRUE(ctx, poseidon_spill > 0);
    EXPECT_TRUE(ctx, poseidon_reload > 0);
    EXPECT_TRUE(ctx, ola_spill > 0);
    EXPECT_TRUE(ctx, ola_reload > 0);
    EXPECT_TRUE(ctx, poseidon.estimated_peak_live_bytes > 0);
    EXPECT_TRUE(ctx, ola.estimated_peak_live_bytes > 0);
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
    TestPoseidonProgramMemoryAccountingConsistency(ctx);
    TestOLAProgramMemoryAccountingConsistency(ctx);
    TestOLASpillReloadAccountingNoUnderflow(ctx);
    TestExecutionModelSharedSingleBoardMethods(ctx);
    TestCycleLowererSelectorDispatch(ctx);
    TestCycleBackendSharedSingleBoardMethods(ctx);
    TestDynamicRuntimeShortcutAffectsBytesAndWork(ctx);
    TestDynamicRuntimeStoragePathsDifferByMethod(ctx);
    TestDynamicRuntimeLifetimeAccounting(ctx);
    TestDynamicRuntimePipelineOverlap(ctx);
    TestAutoSingleCardStillMapsToPoseidon(ctx);
    TestCinnamonByDigitOutputAggregation(ctx);
    TestCinnamonByDigitInputBroadcast(ctx);
    TestCinnamonSingleCardDegradesToOutputCentric(ctx);
    TestCinnamonRejectsInvalidConfig(ctx);
    TestCinnamonProgramBuildersRequireProblemDigitShards(ctx);
    TestBuiltInScaleSingleBoardNoLongerFallsBack(ctx);
    TestProblemSizeMonotonicity(ctx);
    TestHardwareScalingMonotonicity(ctx);
    TestCyclePrimitiveEmitterMicroOpRules(ctx);
    TestCyclePrimitiveEmitterMemoryReplayConsistency(ctx);
    TestPoseidonAndOLAProgramShapeRegression(ctx);

    std::cout << "Cycle sim assertions: " << ctx.assertions
              << ", failures: " << ctx.failures << "\n";
    return (ctx.failures == 0) ? 0 : 1;
}
