#include "backend/cycle_backend/cycle_backend.h"
#include "backend/cycle_backend/cycle_backend_ola.h"
#include "backend/cycle_backend/cycle_backend_poseidon.h"
#include "backend/cycle_backend/cycle_backend_fab.h"
#include "backend/cycle_backend/cycle_backend_fast.h"
#include "backend/cycle_backend/cycle_backend_hera.h"
#include "backend/cycle_backend/cycle_backend_digit_centric.h"
#include "backend/cycle_backend/cycle_backend_output_centric.h"
#include "backend/cycle_backend/cycle_backend_max_parallel.h"
#include "backend/cycle_sim/driver.h"

#include <algorithm>
#include <array>
#include <iostream>
#include <limits>
#include <ostream>
#include <string>

namespace {

template <typename T>
void SaturatingAdd(T* dst, T value) {
    const __uint128_t sum =
        static_cast<__uint128_t>(*dst) + static_cast<__uint128_t>(value);
    const __uint128_t limit =
        static_cast<__uint128_t>(std::numeric_limits<T>::max());
    *dst = (sum > limit) ? std::numeric_limits<T>::max() : static_cast<T>(sum);
}

void NormalizeStatusFlags(ExecutionResult* result) {
    if (result->fallback_reason != KeySwitchFallbackReason::None
        && result->fallback_reason_message.empty()) {
        result->fallback_reason_message = ToString(result->fallback_reason);
    }
    if (result->degraded_reason != KeySwitchFallbackReason::None
        && result->degraded_reason_message.empty()) {
        result->degraded_reason_message = ToString(result->degraded_reason);
    }

    result->unsupported_method =
        result->fallback_used
        && result->fallback_reason == KeySwitchFallbackReason::UnsupportedMethod;
    result->unsupported_config =
        result->fallback_used
        && result->fallback_reason == KeySwitchFallbackReason::UnsupportedConfig;
    result->compatibility_fallback =
        result->fallback_used
        && result->fallback_reason == KeySwitchFallbackReason::LegacyStageFallback;
    result->degraded_to_single_board =
        result->method_degraded
        && result->degraded_reason == KeySwitchFallbackReason::DegradedToSingleBoard;
    result->normal_execution = !result->fallback_used && !result->method_degraded;
}

Time TotalLatency(const ExecutionBreakdown& breakdown) {
    return breakdown.key_load_time
        + breakdown.dispatch_time
        + breakdown.decompose_time
        + breakdown.multiply_time
        + breakdown.basis_convert_time
        + breakdown.merge_time;
}

constexpr std::size_t kStageTypeCount = 6;

// 通用 keyswitch 子图模板（给非 Poseidon 方法复用）。
// 当前仅提供流程骨架，不接入 BuildProgram 的执行分支，
// 后续可按各 method 的数据流/内存策略逐步替换内部实现。
struct [[maybe_unused]] GeneralExecutionBuilder {
    const KeySwitchProblem& problem;
    const HardwareModel& hardware;
    CycleProgram program;

    GeneralExecutionBuilder(
        const KeySwitchProblem& p,
        const HardwareModel& hw,
        KeySwitchMethod method)
        : problem(p)
        , hardware(hw) {
        program.method = method;
        program.name = "general_keyswitch";
    }

    uint64_t LoadInput(
        uint32_t ct_now,
        uint32_t poly_now,
        uint32_t limb_now) {
        return static_cast<uint64_t>(ct_now) * poly_now * limb_now * problem.ct_limb_bytes;
    }

    uint64_t ModUp(
        uint32_t ct_now,
        uint64_t input_bram_bytes,
        bool* spilled) {
        (void)ct_now;
        if (spilled != nullptr) {
            *spilled = false;
        }
        return input_bram_bytes;
    }

    uint64_t InnerProd(
        uint32_t ct_now,
        uint64_t modup_output_bytes,
        bool modup_spilled,
        bool is_last_digit,
        bool* accum_in_bram) {
        (void)modup_output_bytes;
        (void)modup_spilled;
        if (accum_in_bram != nullptr) {
            *accum_in_bram = is_last_digit;
        }
        const uint32_t accum_limbs = problem.polys * problem.key_limbs;
        return static_cast<uint64_t>(ct_now) * accum_limbs * problem.ct_limb_bytes;
    }

    uint64_t ModDown(uint32_t ct_now, bool accum_in_bram) {
        (void)accum_in_bram;
        const uint32_t out_limbs = problem.polys * problem.limbs;
        return static_cast<uint64_t>(ct_now) * out_limbs * problem.ct_limb_bytes;
    }
};

[[maybe_unused]] CycleProgram BuildGeneralProgramTemplate(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware,
    KeySwitchMethod method) {
    GeneralExecutionBuilder builder(problem, hardware, method);

    const uint32_t ct_now = problem.ciphertexts;
    const uint32_t digit_limb_now = problem.digit_limbs;
    bool accum_in_bram = false;

    for (uint32_t digit_idx = 0; digit_idx < problem.digits; ++digit_idx) {
        builder.LoadInput(ct_now, /*poly_now=*/1, digit_limb_now);
        bool modup_spilled = false;
        const uint64_t modup_out = builder.ModUp(
            ct_now,
            static_cast<uint64_t>(ct_now) * digit_limb_now * problem.ct_limb_bytes,
            &modup_spilled);
        const bool is_last = (digit_idx == problem.digits - 1);
        builder.InnerProd(ct_now, modup_out, modup_spilled, is_last, &accum_in_bram);
    }

    builder.ModDown(ct_now, accum_in_bram);
    return builder.program;
}

uint8_t MethodKey(KeySwitchMethod method) {
    return static_cast<uint8_t>(method);
}

std::size_t StageIndex(StageType stage_type) {
    switch (stage_type) {
    case StageType::KeyLoad:
        return 0;
    case StageType::Dispatch:
        return 1;
    case StageType::Decompose:
        return 2;
    case StageType::Multiply:
        return 3;
    case StageType::BasisConvert:
        return 4;
    case StageType::Merge:
        return 5;
    }

    return 0;
}

StageType StageTypeOf(
    CycleOpType type,
    CycleInstructionKind kind) {

    if (kind == CycleInstructionKind::Decompose) {
        return StageType::Decompose;
    }

    switch (type) {
    case CycleOpType::KeyLoad:
        return StageType::KeyLoad;
    case CycleOpType::DataLoad:
    case CycleOpType::Spill:
        return StageType::Dispatch;
    case CycleOpType::NTT:
    case CycleOpType::INTT:
    case CycleOpType::BConv:
        return StageType::BasisConvert;
    case CycleOpType::Multiply:
    case CycleOpType::Add:
    case CycleOpType::Sub:
        return StageType::Multiply;
    case CycleOpType::InterCardComm:
        return StageType::Merge;
    }

    return StageType::Dispatch;
}

void AddTransferTime(
    TransferBreakdown* breakdown,
    CycleOpType type,
    CycleTransferPath transfer_path,
    Time value) {

    switch (type) {
    case CycleOpType::KeyLoad:
        if (transfer_path == CycleTransferPath::HostToHBM) {
            SaturatingAdd(&breakdown->key_host_to_hbm_time, value);
        } else {
            SaturatingAdd(&breakdown->key_hbm_to_bram_time, value);
        }
        return;
    case CycleOpType::DataLoad:
        SaturatingAdd(&breakdown->input_hbm_to_bram_time, value);
        return;
    case CycleOpType::Spill:
        SaturatingAdd(&breakdown->output_bram_to_hbm_time, value);
        return;
    default:
        return;
    }
}

void AddCycleComputeTime(
    PrimitiveComputeBreakdown* breakdown,
    const CycleGroupTiming& timing,
    Time value) {

    if (timing.kind == CycleInstructionKind::Decompose) {
        SaturatingAdd(&breakdown->transform_time, value);
        return;
    }

    switch (timing.type) {
    case CycleOpType::NTT:
        SaturatingAdd(&breakdown->ntt_time, value);
        return;
    case CycleOpType::INTT:
        SaturatingAdd(&breakdown->intt_time, value);
        return;
    case CycleOpType::BConv:
        SaturatingAdd(&breakdown->bconv_time, value);
        return;
    case CycleOpType::Multiply:
        SaturatingAdd(&breakdown->inner_product_time, value);
        return;
    case CycleOpType::Add:
        SaturatingAdd(&breakdown->accumulate_time, value);
        return;
    case CycleOpType::Sub:
        SaturatingAdd(&breakdown->subtract_time, value);
        return;
    default:
        return;
    }
}

ExecutionResult MakeFallbackResult(
    const Request& req,
    KeySwitchMethod effective_method,
    KeySwitchFallbackReason reason) {

    ExecutionResult result{};
    result.requested_method = req.ks_profile.method;
    result.effective_method = effective_method;

    result.fallback_used = (reason != KeySwitchFallbackReason::None);
    result.fallback_reason = reason;

    result.method_degraded = false;
    result.degraded_reason = KeySwitchFallbackReason::None;
    result.tiled_execution = false;

    result.primitive_breakdown_primary = true;
    result.stage_breakdown_compat_only = true;

    result.tile_count = 1;
    result.key_host_to_hbm_bytes = req.ks_profile.key_bytes;
    result.key_hbm_to_bram_bytes = 0;
    result.ct_hbm_to_bram_bytes = req.ks_profile.input_bytes;
    result.out_bram_to_hbm_bytes = req.ks_profile.output_bytes;
    result.hbm_read_bytes =
        result.key_host_to_hbm_bytes
        + result.key_hbm_to_bram_bytes
        + result.ct_hbm_to_bram_bytes;
    result.hbm_write_bytes = result.out_bram_to_hbm_bytes;
    result.working_set_bytes = req.ks_profile.input_bytes + req.ks_profile.key_bytes;
    result.peak_hbm_bytes = result.working_set_bytes;
    result.peak_total_bytes = result.peak_hbm_bytes;
    result.primitive_peak_memory_bytes = result.peak_total_bytes;
    result.peak_memory_bytes = result.peak_total_bytes;

    NormalizeStatusFlags(&result);
    return result;
}

ExecutionBreakdown StageBreakdownFromCycles(
    const HardwareModel& hw_model,
    const std::array<uint64_t, kStageTypeCount>& stage_cycles,
    Time target_total_ns) {

    ExecutionBreakdown breakdown;
    breakdown.key_load_time = hw_model.CyclesToNs(stage_cycles[0]);
    breakdown.dispatch_time = hw_model.CyclesToNs(stage_cycles[1]);
    breakdown.decompose_time = hw_model.CyclesToNs(stage_cycles[2]);
    breakdown.multiply_time = hw_model.CyclesToNs(stage_cycles[3]);
    breakdown.basis_convert_time = hw_model.CyclesToNs(stage_cycles[4]);
    breakdown.merge_time = hw_model.CyclesToNs(stage_cycles[5]);

    Time total_ns = TotalLatency(breakdown);
    if (total_ns == target_total_ns) {
        return breakdown;
    }

    std::array<Time*, kStageTypeCount> fields = {
        &breakdown.key_load_time,
        &breakdown.dispatch_time,
        &breakdown.decompose_time,
        &breakdown.multiply_time,
        &breakdown.basis_convert_time,
        &breakdown.merge_time,
    };

    auto pick_largest = [&fields]() {
        return std::max_element(
            fields.begin(),
            fields.end(),
            [](const Time* lhs, const Time* rhs) {
                return *lhs < *rhs;
            });
    };

    if (total_ns < target_total_ns) {
        Time* field = *pick_largest();
        SaturatingAdd(field, target_total_ns - total_ns);
        return breakdown;
    }

    Time delta = total_ns - target_total_ns;
    while (delta > 0) {
        const auto it = pick_largest();
        Time* field = *it;
        if (*field == 0) {
            break;
        }
        const Time reduce = std::min<Time>(*field, delta);
        *field -= reduce;
        delta -= reduce;
    }

    return breakdown;
}

TransferBreakdown TransferBreakdownFromCycles(
    const HardwareModel& hw_model,
    const TransferBreakdown& cycles) {

    TransferBreakdown breakdown;
    breakdown.key_host_to_hbm_time = hw_model.CyclesToNs(cycles.key_host_to_hbm_time);
    breakdown.key_hbm_to_bram_time = hw_model.CyclesToNs(cycles.key_hbm_to_bram_time);
    breakdown.input_hbm_to_bram_time = hw_model.CyclesToNs(cycles.input_hbm_to_bram_time);
    breakdown.output_bram_to_hbm_time = hw_model.CyclesToNs(cycles.output_bram_to_hbm_time);
    return breakdown;
}

PrimitiveComputeBreakdown ComputeBreakdownFromCycles(
    const HardwareModel& hw_model,
    const PrimitiveComputeBreakdown& cycles) {

    PrimitiveComputeBreakdown breakdown;
    breakdown.transform_time = hw_model.CyclesToNs(cycles.transform_time);
    breakdown.ntt_time = hw_model.CyclesToNs(cycles.ntt_time);
    breakdown.intt_time = hw_model.CyclesToNs(cycles.intt_time);
    breakdown.bconv_time = hw_model.CyclesToNs(cycles.bconv_time);
    breakdown.inner_product_time = hw_model.CyclesToNs(cycles.inner_product_time);
    breakdown.accumulate_time = hw_model.CyclesToNs(cycles.accumulate_time);
    breakdown.subtract_time = hw_model.CyclesToNs(cycles.subtract_time);
    return breakdown;
}

void DumpDetailedStats(
    const CycleProgram& program,
    const CycleSimStats& stats,
    const HardwareModel& hw) {

    std::cout << "\n========== Detailed Execution Statistics ==========\n";

    // 1. 总览
    const Time total_ns = hw.CyclesToNs(stats.total_cycles);
    std::cout << "\n--- Summary ---\n";
    std::cout << "Total cycles:           " << stats.total_cycles << "\n";
    std::cout << "Total latency (ns):     " << total_ns << "\n";
    std::cout << "Peak BRAM live (bytes): " << stats.peak_bram_live_bytes << "\n";
    std::cout << "HBM read (bytes):       " << stats.hbm_read_bytes << "\n";
    std::cout << "HBM write (bytes):      " << stats.hbm_write_bytes << "\n";
    std::cout << "Dependency stalls:      " << stats.dependency_stall_cycles << " cycles\n";
    std::cout << "Resource stalls:        " << stats.resource_stall_cycles << " cycles\n";
    std::cout << "Instruction groups:     " << program.groups.size() << "\n";
    std::cout << "Total instructions:     " << program.instruction_count << "\n";

    // 2. 按操作类型统计 cycle 占比
    std::cout << "\n--- Cycle Breakdown by Instruction Kind ---\n";
    struct KindStat { const char* name; uint64_t cycles; uint64_t count; uint64_t bytes; };
    std::vector<KindStat> kind_stats;
    for (std::size_t i = 0; i < kCycleInstructionKindCount; ++i) {
        if (stats.instruction_cycles[i] > 0 || stats.instruction_counts[i] > 0) {
            kind_stats.push_back({
                ToString(static_cast<CycleInstructionKind>(i)),
                stats.instruction_cycles[i],
                stats.instruction_counts[i],
                stats.instruction_bytes[i]
            });
        }
    }
    for (const auto& ks : kind_stats) {
        const double pct = (stats.total_cycles > 0)
            ? (100.0 * static_cast<double>(ks.cycles) / static_cast<double>(stats.total_cycles))
            : 0.0;
        std::cout << "  " << ks.name
                  << ": cycles=" << ks.cycles
                  << " (" << pct << "%)"
                  << "  count=" << ks.count
                  << "  bytes=" << ks.bytes
                  << "\n";
    }

    // 2.1 归一化到“每次输入 1 个 limb”后的操作估算（基于每组指令的单条延迟）。
    struct OneLimbOperationStat {
        std::string operation;
        CycleInstructionKind kind = CycleInstructionKind::LoadHBM;
        uint64_t groups = 0;
        uint64_t instructions = 0;
        uint64_t sample_count = 0;
        uint64_t one_limb_cycle_sum = 0;
        uint64_t one_limb_cycle_min = std::numeric_limits<uint64_t>::max();
        uint64_t one_limb_cycle_max = 0;
    };

    std::vector<OneLimbOperationStat> one_limb_stats;
    one_limb_stats.reserve(program.groups.size());
    for (const CycleInstructionGroup& group : program.groups) {
        if (group.instructions.empty()) {
            continue;
        }

        const uint64_t one_limb_cycles = group.instructions.front().latency_cycles;
        auto it = std::find_if(
            one_limb_stats.begin(),
            one_limb_stats.end(),
            [&](const OneLimbOperationStat& stat) {
                return stat.operation == group.name && stat.kind == group.kind;
            });

        if (it == one_limb_stats.end()) {
            OneLimbOperationStat stat;
            stat.operation = group.name;
            stat.kind = group.kind;
            stat.groups = 1;
            stat.instructions = group.instructions.size();
            stat.sample_count = 1;
            stat.one_limb_cycle_sum = one_limb_cycles;
            stat.one_limb_cycle_min = one_limb_cycles;
            stat.one_limb_cycle_max = one_limb_cycles;
            one_limb_stats.push_back(std::move(stat));
            continue;
        }

        ++it->groups;
        it->instructions += group.instructions.size();
        ++it->sample_count;
        it->one_limb_cycle_sum += one_limb_cycles;
        it->one_limb_cycle_min = std::min(it->one_limb_cycle_min, one_limb_cycles);
        it->one_limb_cycle_max = std::max(it->one_limb_cycle_max, one_limb_cycles);
    }

    if (!one_limb_stats.empty()) {
        std::cout << "\n--- EstimateCycle (1-Limb Input) by Operation ---\n";
        for (const OneLimbOperationStat& stat : one_limb_stats) {
            const uint64_t avg_cycles =
                (stat.sample_count > 0) ? (stat.one_limb_cycle_sum / stat.sample_count) : 0;
            std::cout << "  " << stat.operation
                      << ": kind=" << ToString(stat.kind);
            if (stat.one_limb_cycle_min == stat.one_limb_cycle_max) {
                std::cout << "  one_limb_cycles=" << stat.one_limb_cycle_min;
            } else {
                std::cout << "  one_limb_cycles(avg/min/max)="
                          << avg_cycles << "/"
                          << stat.one_limb_cycle_min << "/"
                          << stat.one_limb_cycle_max;
            }
            std::cout << "  groups=" << stat.groups
                      << "  instrs=" << stat.instructions
                      << "\n";
        }
    }

    // 3. Group 时序表（含 BRAM 占用）
    std::cout << "\n--- Group Timeline ---\n";
    std::cout << "  ID  Name                     Start     End       Duration  BRAM_before  BRAM_after   Bytes\n";
    for (const CycleGroupTiming& t : stats.group_timings) {
        const auto& group = program.groups[t.group_id];
        char buf[256];
        snprintf(buf, sizeof(buf),
            "  %2u  %-24s %7lu   %7lu   %8lu  %11lu  %11lu  %lu\n",
            t.group_id,
            group.name.c_str(),
            static_cast<unsigned long>(t.start_cycle),
            static_cast<unsigned long>(t.finish_cycle),
            static_cast<unsigned long>(t.DurationCycles()),
            static_cast<unsigned long>(t.live_bytes_before),
            static_cast<unsigned long>(t.live_bytes_after),
            static_cast<unsigned long>(t.bytes));
        std::cout << buf;
    }

    // 4. BRAM 占用时间线（每个 group 的 start/end 对应的 BRAM 状态，可用于画图）
    std::cout << "\n--- BRAM Timeline (cycle, live_bytes) ---\n";
    std::cout << "# cycle,bram_bytes\n";
    std::cout << "0,0\n";
    for (const CycleGroupTiming& t : stats.group_timings) {
        std::cout << t.start_cycle << "," << t.live_bytes_before << "\n";
        std::cout << t.finish_cycle << "," << t.live_bytes_after << "\n";
    }

    // 5. 传输 vs 计算统计
    uint64_t transfer_cycles = 0;
    uint64_t compute_cycles = 0;
    for (const CycleGroupTiming& t : stats.group_timings) {
        if (t.transfer_path != CycleTransferPath::None) {
            transfer_cycles += t.DurationCycles();
        } else {
            compute_cycles += t.DurationCycles();
        }
    }
    const double transfer_pct = (stats.total_cycles > 0)
        ? (100.0 * static_cast<double>(transfer_cycles) / static_cast<double>(stats.total_cycles))
        : 0.0;
    const double compute_pct = (stats.total_cycles > 0)
        ? (100.0 * static_cast<double>(compute_cycles) / static_cast<double>(stats.total_cycles))
        : 0.0;
    std::cout << "\n--- Transfer vs Compute ---\n";
    std::cout << "Transfer: " << transfer_cycles << " cycles (" << transfer_pct << "%)\n";
    std::cout << "Compute:  " << compute_cycles << " cycles (" << compute_pct << "%)\n";

    std::cout << "====================================================\n\n";
}

} // namespace

void CycleBackend::SetDebugDumpOptions(bool dump_logical_graph, bool dump_runtime_plan) {
    dump_logical_graph_ = dump_logical_graph;
    dump_runtime_plan_ = dump_runtime_plan;
    dumped_logical_methods_.clear();
    dumped_runtime_methods_.clear();
}

bool CycleBackend::ShouldDumpLogicalGraph(KeySwitchMethod method) const {
    if (!dump_logical_graph_) {
        return false;
    }
    return dumped_logical_methods_.insert(MethodKey(method)).second;
}

bool CycleBackend::ShouldDumpRuntimePlan(KeySwitchMethod method) const {
    if (!dump_runtime_plan_) {
        return false;
    }
    return dumped_runtime_methods_.insert(MethodKey(method)).second;
}

KeySwitchMethod CycleBackend::ResolveKeySwitchMethod(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState&
) const {

    const KeySwitchMethod requested = req.ks_profile.method;
    if (requested != KeySwitchMethod::Auto) {
        return requested;
    }

    return (plan.assigned_cards.size() > 1)
        ? KeySwitchMethod::Cinnamon
        : KeySwitchMethod::Poseidon;
}

ExecutionResult CycleBackend::Estimate(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    ++stats_.estimate_calls;

    const KeySwitchMethod method = ResolveKeySwitchMethod(req, plan, state);

    ExecutionResult result{};
    switch (method) {
    case KeySwitchMethod::Poseidon:
        result = EstimateMethod(req, plan, state, KeySwitchMethod::Poseidon);
        break;

    case KeySwitchMethod::FAB:
        result = EstimateMethod(req, plan, state, KeySwitchMethod::FAB);
        break;

    case KeySwitchMethod::FAST:
        result = EstimateMethod(req, plan, state, KeySwitchMethod::FAST);
        break;

    case KeySwitchMethod::OLA:
        result = EstimateMethod(req, plan, state, KeySwitchMethod::OLA);
        break;

    case KeySwitchMethod::HERA:
        result = EstimateMethod(req, plan, state, KeySwitchMethod::HERA);
        break;

    case KeySwitchMethod::DigitCentric:
        result = EstimateMethod(req, plan, state, KeySwitchMethod::DigitCentric);
        break;

    case KeySwitchMethod::OutputCentric:
        result = EstimateMethod(req, plan, state, KeySwitchMethod::OutputCentric);
        break;

    case KeySwitchMethod::MaxParallel:
        result = EstimateMethod(req, plan, state, KeySwitchMethod::MaxParallel);
        break;

    case KeySwitchMethod::Cinnamon:
        result = EstimateMethod(req, plan, state, KeySwitchMethod::Cinnamon);
        break;

    default:
        result = MakeFallbackResult(req, method, KeySwitchFallbackReason::UnsupportedMethod);
        break;
    }

    NormalizeStatusFlags(&result);
    if (result.fallback_used) {
        ++stats_.fallback_count;
    }
    return result;
}

CycleProgram CycleBackend::BuildProgram(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state,
    KeySwitchMethod method) const {

    Request method_req = req;
    method_req.ks_profile.method = method;
    const KeySwitchProblem problem = execution_model_.BuildProblem(method_req, plan, state);

    if (!problem.valid) {
        return CycleProgram{};
    }

    switch (method) {
    case KeySwitchMethod::Poseidon:
        return BuildPoseidonProgram(problem, hw_model_);
    case KeySwitchMethod::OLA:
        return BuildOLAProgram(problem, hw_model_);
    case KeySwitchMethod::FAB:
        return BuildFABProgram(problem, hw_model_);
    case KeySwitchMethod::FAST:
        return BuildFastProgram(problem, hw_model_);
    case KeySwitchMethod::HERA:
        return BuildHERAProgram(problem, hw_model_);
    case KeySwitchMethod::DigitCentric:
        return BuildDigitalCentricProgram(problem, hw_model_);
    case KeySwitchMethod::OutputCentric:
        return BuildOutputCentricProgram(problem, hw_model_);
    case KeySwitchMethod::MaxParallel:
        return BuildMaxParallelProgram(problem, hw_model_);
    default:
        return CycleProgram{};
    }
}

CycleSimStats CycleBackend::Simulate(
    const CycleProgram& program) const {

    CycleDriver driver(hw_model_);
    return driver.Run(program);
}

ExecutionResult CycleBackend::CollectResult(
    const Request& req,
    KeySwitchMethod effective_method,
    const CycleSimStats& sim_stats) const {

    (void)req;
    (void)effective_method;
    ExecutionResult result{};

    std::array<uint64_t, kStageTypeCount> stage_cycles{};
    TransferBreakdown transfer_cycles;
    PrimitiveComputeBreakdown compute_cycles;

    for (const CycleGroupTiming& timing : sim_stats.group_timings) {
        const uint64_t duration_cycles = timing.DurationCycles();
        SaturatingAdd(
            &stage_cycles[StageIndex(StageTypeOf(timing.type, timing.kind))],
            duration_cycles);
        AddTransferTime(
            &transfer_cycles,
            timing.type,
            timing.transfer_path,
            duration_cycles);
        AddCycleComputeTime(
            &compute_cycles,
            timing,
            duration_cycles);
        if (timing.transfer_path != CycleTransferPath::None) {
            result.energy_nj += hw_model_.EstimateTransferEnergyByBytes(timing.bytes);
        }
    }

    const Time total_latency_ns = hw_model_.CyclesToNs(sim_stats.total_cycles);
    result.breakdown = StageBreakdownFromCycles(hw_model_, stage_cycles, total_latency_ns);
    result.transfer_breakdown = TransferBreakdownFromCycles(hw_model_, transfer_cycles);
    result.compute_breakdown = ComputeBreakdownFromCycles(hw_model_, compute_cycles);

    result.total_latency = total_latency_ns;
    result.hbm_read_bytes = sim_stats.hbm_read_bytes;
    result.hbm_write_bytes = sim_stats.hbm_write_bytes;
    result.hbm_round_trips = sim_stats.hbm_round_trips;
    result.spill_bytes = sim_stats.spill_bytes;
    result.reload_bytes = sim_stats.reload_bytes;
    result.dependency_stall_cycles = sim_stats.dependency_stall_cycles;
    result.resource_stall_cycles = sim_stats.resource_stall_cycles;
    result.fine_step_cycles.assign(kTileExecutionStepTypeCount, 0);
    for (std::size_t idx = 0; idx < kTileExecutionStepTypeCount; ++idx) {
        result.fine_step_cycles[idx] = hw_model_.CyclesToNs(sim_stats.fine_step_cycles[idx]);
    }
    if (sim_stats.peak_bram_live_bytes > 0) {
        result.peak_bram_bytes = sim_stats.peak_bram_live_bytes;
    }
    result.peak_total_bytes = std::max(result.peak_bram_bytes, result.peak_hbm_bytes);
    result.primitive_peak_memory_bytes = result.peak_total_bytes;
    result.peak_memory_bytes = result.peak_total_bytes;
    result.total_latency = TotalLatency(result.breakdown);

    NormalizeStatusFlags(&result);
    return result;
}

ExecutionResult CycleBackend::EstimateMethod(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state,
    KeySwitchMethod method) const {

    // 1. 构建 CycleProgram：memory-aware 调度 + 直接生成硬件指令。
    const CycleProgram program = BuildProgram(req, plan, state, method);
    if (program.empty()) {
        return MakeFallbackResult(req, method, KeySwitchFallbackReason::TilePlanInvalid);
    }

    // 2. 运行 cycle 级仿真。
    const CycleSimStats sim_stats = Simulate(program);

    // 3. 详细统计。
    DumpDetailedStats(program, sim_stats, hw_model_);

    // 4. 汇聚结果。
    return CollectResult(req, method, sim_stats);
}

CycleBackendStats CycleBackend::GetStats() const {
    return stats_;
}

void CycleBackend::PrintStats(std::ostream& os) const {
    const CycleBackendStats s = GetStats();

    os << "=== CycleBackend Method Dispatch Stats ===\n";
    os << "EstimateCalls=" << s.estimate_calls << "\n";
    os << "CycleFallbackCount=" << s.fallback_count << "\n";
}
