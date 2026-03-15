#include "backend/cycle_backend.h"
#include "backend/cycle_sim/driver.h"
#include "backend/cycle_sim/lowering.h"
#include "backend/runtime_planner.h"
#include "backend/step_graph_runtime.h"

#include <algorithm>
#include <array>
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

bool IsSharedSingleBoardMethod(KeySwitchMethod method) {
    switch (method) {
    case KeySwitchMethod::Poseidon:
    case KeySwitchMethod::FAB:
    case KeySwitchMethod::FAST:
    case KeySwitchMethod::OLA:
    case KeySwitchMethod::HERA:
        return true;
    default:
        return false;
    }
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

void AddTransferTime(
    TransferBreakdown* breakdown,
    TileExecutionStepType step_type,
    Time value) {

    switch (step_type) {
    case TileExecutionStepType::KeyLoadHostToHBM:
        SaturatingAdd(&breakdown->key_host_to_hbm_time, value);
        return;
    case TileExecutionStepType::KeyLoadHBMToBRAM:
    case TileExecutionStepType::KeyHBMToBRAM:
        SaturatingAdd(&breakdown->key_hbm_to_bram_time, value);
        return;
    case TileExecutionStepType::InputHBMToBRAM:
    case TileExecutionStepType::IntermediateHBMToBRAM:
        SaturatingAdd(&breakdown->input_hbm_to_bram_time, value);
        return;
    case TileExecutionStepType::IntermediateBRAMToHBM:
    case TileExecutionStepType::OutputBRAMToHBM:
        SaturatingAdd(&breakdown->output_bram_to_hbm_time, value);
        return;
    default:
        return;
    }
}

void AddComputeTime(
    PrimitiveComputeBreakdown* breakdown,
    TileExecutionStepType step_type,
    Time value) {

    switch (step_type) {
    case TileExecutionStepType::DecomposeTile:
        SaturatingAdd(&breakdown->transform_time, value);
        return;
    case TileExecutionStepType::ModUpInttTile:
    case TileExecutionStepType::ModDownInttTile:
        SaturatingAdd(&breakdown->intt_time, value);
        return;
    case TileExecutionStepType::ModUpBConvTile:
    case TileExecutionStepType::ModDownBConvTile:
        SaturatingAdd(&breakdown->bconv_time, value);
        return;
    case TileExecutionStepType::ModUpNttTile:
    case TileExecutionStepType::ModDownNttTile:
        SaturatingAdd(&breakdown->ntt_time, value);
        return;
    case TileExecutionStepType::CrossDigitReduceTile:
        SaturatingAdd(&breakdown->accumulate_time, value);
        return;
    case TileExecutionStepType::FinalSubtractTile:
        SaturatingAdd(&breakdown->subtract_time, value);
        return;
    case TileExecutionStepType::NttTile:
        SaturatingAdd(&breakdown->ntt_time, value);
        return;
    case TileExecutionStepType::KSInnerProdTile:
        SaturatingAdd(&breakdown->inner_product_time, value);
        return;
    case TileExecutionStepType::InttTile:
        SaturatingAdd(&breakdown->intt_time, value);
        return;
    case TileExecutionStepType::AccumulateSubtractTile: {
        const Time accumulate = value / 2;
        const Time subtract = value - accumulate;
        SaturatingAdd(&breakdown->accumulate_time, accumulate);
        SaturatingAdd(&breakdown->subtract_time, subtract);
        return;
    }
    case TileExecutionStepType::BasisConvertTile:
        SaturatingAdd(&breakdown->bconv_time, value);
        return;
    default:
        return;
    }
}

void AddCycleComputeTime(
    PrimitiveComputeBreakdown* breakdown,
    const CycleGroupTiming& timing,
    Time value) {

    if (timing.source_step_type == TileExecutionStepType::AccumulateSubtractTile) {
        if (timing.kind == CycleInstructionKind::EweAdd) {
            SaturatingAdd(&breakdown->accumulate_time, value);
            return;
        }
        if (timing.kind == CycleInstructionKind::EweSub) {
            SaturatingAdd(&breakdown->subtract_time, value);
            return;
        }
    }

    AddComputeTime(breakdown, timing.source_step_type, value);
}

void CopyPeakBuffers(
    PeakBufferBreakdown* dst,
    const PeakBufferUsage& src) {

    dst->persistent_peak_bytes = src.persistent_peak_bytes;
    dst->static_peak_bytes = src.static_peak_bytes;
    dst->dynamic_peak_bytes = src.dynamic_peak_bytes;
    dst->key_peak_bytes = src.key_peak_bytes;
    dst->ct_peak_bytes = src.ct_peak_bytes;
    dst->out_peak_bytes = src.out_peak_bytes;
    dst->temp_peak_bytes = src.temp_peak_bytes;
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

void PopulateResultMetadataFromExecution(
    const Request& req,
    const KeySwitchExecution& execution,
    KeySwitchMethod effective_method,
    ExecutionResult* result) {

    result->requested_method = req.ks_profile.method;
    result->effective_method = effective_method;
    result->method_degraded = execution.method_degraded;
    result->degraded_reason = execution.degraded_reason;
    result->primitive_breakdown_primary = true;
    result->stage_breakdown_compat_only = true;
    result->tiled_execution = execution.tiled_execution;

    if (execution.tile_count > 0) {
        result->tile_count = execution.tile_count;
    }
    if (execution.working_set_bytes > 0) {
        result->working_set_bytes = execution.working_set_bytes;
    }

    result->key_resident_reuse = execution.key_resident_hit;
    result->key_resident_hit = execution.key_resident_hit;
    result->key_persistent_bram = execution.key_persistent_bram;

    if (execution.key_host_to_hbm_bytes > 0 || execution.key_resident_hit) {
        result->key_host_to_hbm_bytes = execution.key_host_to_hbm_bytes;
    }
    if (execution.key_hbm_to_bram_bytes > 0) {
        result->key_hbm_to_bram_bytes = execution.key_hbm_to_bram_bytes;
    }
    if (execution.ct_hbm_to_bram_bytes > 0) {
        result->ct_hbm_to_bram_bytes = execution.ct_hbm_to_bram_bytes;
    }
    if (execution.out_bram_to_hbm_bytes > 0) {
        result->out_bram_to_hbm_bytes = execution.out_bram_to_hbm_bytes;
    }

    if (execution.peak_bram_bytes > 0) {
        result->peak_bram_bytes = execution.peak_bram_bytes;
        CopyPeakBuffers(&result->peak_buffers, execution.peak_buffers);
    }

    const uint64_t hbm_candidate = std::max<uint64_t>(
        result->working_set_bytes,
        execution.problem.output_bytes);
    result->peak_hbm_bytes = hbm_candidate;
    result->peak_total_bytes = std::max(result->peak_bram_bytes, result->peak_hbm_bytes);
    result->primitive_peak_memory_bytes = result->peak_total_bytes;
    result->peak_memory_bytes = result->peak_total_bytes;

    result->hbm_read_bytes =
        result->key_host_to_hbm_bytes
        + result->key_hbm_to_bram_bytes
        + result->ct_hbm_to_bram_bytes;
    result->hbm_write_bytes = result->out_bram_to_hbm_bytes;
}

void PopulateResultMetadataFromRuntimePlan(
    const RuntimePlan& plan,
    ExecutionResult* result) {

    result->tiled_execution = true;
    result->tile_count = plan.tile_count;
    result->working_set_bytes = plan.working_set_bytes;
    result->key_resident_reuse = plan.key_resident_hit;
    result->key_resident_hit = plan.key_resident_hit;
    result->key_persistent_bram = plan.key_persistent_bram;
    result->key_host_to_hbm_bytes = plan.key_host_to_hbm_bytes;
    result->key_hbm_to_bram_bytes = plan.key_hbm_to_bram_bytes;
    result->ct_hbm_to_bram_bytes = plan.ct_hbm_to_bram_bytes;
    result->out_bram_to_hbm_bytes = plan.out_bram_to_hbm_bytes;
    result->hbm_read_bytes =
        result->key_host_to_hbm_bytes
        + result->key_hbm_to_bram_bytes
        + result->ct_hbm_to_bram_bytes;
    result->hbm_write_bytes = result->out_bram_to_hbm_bytes;
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

ExecutionResult MakeExecutionFallbackResult(
    const Request& req,
    const KeySwitchExecution& execution,
    KeySwitchMethod method) {

    ExecutionResult result = MakeFallbackResult(
        req,
        method,
        execution.fallback_reason);
    PopulateResultMetadataFromExecution(
        req,
        execution,
        (execution.effective_method == KeySwitchMethod::Auto)
            ? method
            : execution.effective_method,
        &result);
    return result;
}

} // namespace

KeySwitchMethod CycleBackend::ResolveKeySwitchMethod(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& /*state*/) const {

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

ExecutionResult CycleBackend::EstimateMethod(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state,
    KeySwitchMethod method
) const {

    // 固定本次估算方法，构建可执行的 KeySwitch 执行描述。
    Request method_req = req;
    method_req.ks_profile.method = method;
    const KeySwitchExecution execution = execution_model_.Build(method_req, plan, state);
    
    // 执行模型不可用时，返回带原因的统一兜底结果。
    if (!execution.valid) {
        return MakeExecutionFallbackResult(req, execution, method);
    }

    if (IsSharedSingleBoardMethod(method) && execution.problem.cards <= 1) {
        RuntimePlanner runtime_planner(hw_model_, execution_model_.TilePlannerParams());
        const RuntimePlan runtime_plan = runtime_planner.Plan(execution);
        if (!runtime_plan.valid) {
            ExecutionResult result = MakeFallbackResult(
                req,
                (execution.effective_method == KeySwitchMethod::Auto)
                    ? method
                    : execution.effective_method,
                KeySwitchFallbackReason::TilePlanInvalid);
            PopulateResultMetadataFromExecution(
                req,
                execution,
                (execution.effective_method == KeySwitchMethod::Auto)
                    ? method
                    : execution.effective_method,
                &result);
            result.fallback_reason_message =
                std::string(ToString(result.fallback_reason))
                + ": runtime_planner_failed";
            NormalizeStatusFlags(&result);
            return result;
        }

        StepGraphRuntimeExecutor runtime_executor(hw_model_);
        const RuntimeState runtime = runtime_executor.ExecuteStepGraph(runtime_plan);
        if (!runtime.valid) {
            ExecutionResult result = MakeFallbackResult(
                req,
                (execution.effective_method == KeySwitchMethod::Auto)
                    ? method
                    : execution.effective_method,
                KeySwitchFallbackReason::UnsupportedConfig);
            PopulateResultMetadataFromExecution(
                req,
                execution,
                (execution.effective_method == KeySwitchMethod::Auto)
                    ? method
                    : execution.effective_method,
                &result);
            PopulateResultMetadataFromRuntimePlan(runtime_plan, &result);
            result.fallback_reason_message =
                std::string(ToString(result.fallback_reason))
                + ": dynamic_step_graph_runtime_failed";
            NormalizeStatusFlags(&result);
            return result;
        }

        ExecutionResult result{};
        PopulateResultMetadataFromExecution(
            req,
            execution,
            (execution.effective_method == KeySwitchMethod::Auto)
                ? method
                : execution.effective_method,
            &result);
        PopulateResultMetadataFromRuntimePlan(runtime_plan, &result);

        std::array<uint64_t, kStageTypeCount> stage_cycles{};
        TransferBreakdown transfer_cycles;
        PrimitiveComputeBreakdown compute_cycles;
        for (const StepRuntimeRecord& record : runtime.step_records) {
            SaturatingAdd(
                &stage_cycles[StageIndex(record.stage_type)],
                record.total_cycles);
            AddTransferTime(
                &transfer_cycles,
                record.step_type,
                record.transfer_cycles);
            AddComputeTime(
                &compute_cycles,
                record.step_type,
                record.compute_cycles);
        }

        const Time total_latency_ns = hw_model_.CyclesToNs(runtime.total_cycles);
        result.breakdown = StageBreakdownFromCycles(
            hw_model_,
            stage_cycles,
            total_latency_ns);
        result.transfer_breakdown = TransferBreakdownFromCycles(hw_model_, transfer_cycles);
        result.compute_breakdown = ComputeBreakdownFromCycles(hw_model_, compute_cycles);

        result.total_latency = TotalLatency(result.breakdown);
        result.compute_cycles = runtime.compute_cycles;
        result.transfer_cycles = runtime.transfer_cycles;
        result.hbm_read_bytes = runtime.hbm_read_bytes;
        result.hbm_write_bytes = runtime.hbm_write_bytes;
        result.bram_read_bytes = runtime.bram_read_bytes;
        result.bram_write_bytes = runtime.bram_write_bytes;
        result.hbm_round_trips = std::min(runtime.spill_count, runtime.reload_count);
        result.direct_forward_count = runtime.direct_forward_count;
        result.direct_forward_bytes = runtime.direct_forward_bytes;
        result.spill_count = runtime.spill_count;
        result.reload_count = runtime.reload_count;
        result.spill_bytes = runtime.spill_bytes;
        result.reload_bytes = runtime.reload_bytes;
        result.peak_hbm_bytes = std::max(result.peak_hbm_bytes, runtime.peak_hbm_bytes);
        result.peak_bram_bytes = std::max(result.peak_bram_bytes, runtime.peak_bram_bytes);
        result.peak_total_bytes = std::max(result.peak_bram_bytes, result.peak_hbm_bytes);
        result.primitive_peak_memory_bytes = result.peak_total_bytes;
        result.peak_memory_bytes = result.peak_total_bytes;
        result.energy_nj = hw_model_.EstimateTransferEnergyByBytes(
            runtime.hbm_read_bytes + runtime.hbm_write_bytes);
        result.fine_step_cycles.assign(kTileExecutionStepTypeCount, 0);
        for (std::size_t idx = 0; idx < kTileExecutionStepTypeCount; ++idx) {
            result.fine_step_cycles[idx] = hw_model_.CyclesToNs(runtime.fine_step_cycles[idx]);
        }
        return result;
    }

    // 将高层执行步骤 lower 到 cycle simulator 可运行的程序表示。
    CycleLowererSelector lowerer(hw_model_);
    const CycleLoweringResult lowering = lowerer.Lower(execution);
    
    if (!lowering.valid) {
        // lowering 失败时保留上下文元信息，并补齐可读的失败原因。
        ExecutionResult result = MakeFallbackResult(
            req,
            (execution.effective_method == KeySwitchMethod::Auto)
                ? method
                : execution.effective_method,
            (lowering.fallback_reason == KeySwitchFallbackReason::None)
                ? KeySwitchFallbackReason::UnsupportedConfig
                : lowering.fallback_reason);
        PopulateResultMetadataFromExecution(
            req,
            execution,
            (execution.effective_method == KeySwitchMethod::Auto)
                ? method
                : execution.effective_method,
            &result);
        if (!lowering.fallback_reason_message.empty()) {
            result.fallback_reason_message =
                std::string(ToString(result.fallback_reason))
                + ": " + lowering.fallback_reason_message;
        }
        NormalizeStatusFlags(&result);
        return result;
    }

    // 运行 cycle 级仿真，拿到分组时序与资源统计。
    CycleDriver driver(hw_model_);
    const CycleSimStats sim_stats = driver.Run(lowering.program);

    ExecutionResult result{};
    // 先写入请求/方法等元信息，再填充各类统计字段。
    PopulateResultMetadataFromExecution(
        req,
        execution,
        (execution.effective_method == KeySwitchMethod::Auto)
            ? method
            : execution.effective_method,
        &result);

    std::array<uint64_t, kStageTypeCount> stage_cycles{};
    TransferBreakdown transfer_cycles;
    PrimitiveComputeBreakdown compute_cycles;

    // 汇总每个阶段的周期、传输时间和算子时间，并累计传输能耗。
    for (const CycleGroupTiming& timing : sim_stats.group_timings) {
        const uint64_t duration_cycles = timing.DurationCycles();
        SaturatingAdd(
            &stage_cycles[StageIndex(timing.stage_type)],
            duration_cycles);
        AddTransferTime(
            &transfer_cycles,
            timing.source_step_type,
            duration_cycles);
        AddCycleComputeTime(
            &compute_cycles,
            timing,
            duration_cycles);
        if (timing.transfer_path != CycleTransferPath::None) {
            result.energy_nj += hw_model_.EstimateTransferEnergyByBytes(timing.bytes);
        }
    }

    // 将仿真统计统一换算成结果结构体中的时延/带宽/峰值内存等指标。
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
    return result;
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
