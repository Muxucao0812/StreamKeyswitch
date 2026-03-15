#include "backend/cycle_backend.h"
#include "backend/cycle_sim/driver.h"
#include "backend/cycle_sim/lowering.h"

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
    result->peak_rf_bytes = execution.predicted_rf_peak;
    result->peak_sram_bytes = execution.predicted_sram_peak;

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
        result = EstimatePoseidon(req, plan, state);
        break;

    case KeySwitchMethod::FAB:
        result = EstimateFAB(req, plan, state);
        break;

    case KeySwitchMethod::FAST:
        result = EstimateFAST(req, plan, state);
        break;

    case KeySwitchMethod::OLA:
        result = EstimateOLA(req, plan, state);
        break;

    case KeySwitchMethod::HERA:
        result = EstimateHERA(req, plan, state);
        break;

    case KeySwitchMethod::Cinnamon:
        result = EstimateCinnamon(req, plan, state);
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

ExecutionResult CycleBackend::EstimatePoseidon(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    return EstimateMethod(
        req,
        plan,
        state,
        KeySwitchMethod::Poseidon);
}

ExecutionResult CycleBackend::EstimateMethod(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state,
    KeySwitchMethod method) const {

    Request method_req = req;
    method_req.ks_profile.method = method;
    const KeySwitchExecution execution = execution_model_.Build(method_req, plan, state);
    
    if (!execution.valid) {
        return MakeExecutionFallbackResult(req, execution, method);
    }

    CycleLowererSelector lowerer(hw_model_);
    const CycleLoweringResult lowering = lowerer.Lower(execution);
    if (!lowering.valid) {
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

    CycleDriver driver(hw_model_);
    const CycleSimStats sim_stats = driver.Run(lowering.program);

    ExecutionResult result{};
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

    const Time total_latency_ns = hw_model_.CyclesToNs(sim_stats.total_cycles);
    result.breakdown = StageBreakdownFromCycles(
        hw_model_,
        stage_cycles,
        total_latency_ns);
    result.transfer_breakdown = TransferBreakdownFromCycles(
        hw_model_,
        transfer_cycles);
    result.compute_breakdown = ComputeBreakdownFromCycles(
        hw_model_,
        compute_cycles);
    result.total_latency = total_latency_ns;
    result.hbm_read_bytes = sim_stats.hbm_read_bytes;
    result.hbm_write_bytes = sim_stats.hbm_write_bytes;
    result.hbm_round_trips = sim_stats.hbm_round_trips;
    result.spill_bytes = sim_stats.spill_bytes;
    result.reload_bytes = sim_stats.reload_bytes;
    result.peak_rf_bytes = sim_stats.peak_rf_live_bytes;
    result.peak_sram_bytes = sim_stats.peak_sram_live_bytes;
    result.dependency_stall_cycles = sim_stats.dependency_stall_cycles;
    result.resource_stall_cycles = sim_stats.resource_stall_cycles;
    result.fine_step_cycles.assign(kTileExecutionStepTypeCount, 0);
    for (std::size_t idx = 0; idx < kTileExecutionStepTypeCount; ++idx) {
        result.fine_step_cycles[idx] = hw_model_.CyclesToNs(sim_stats.fine_step_cycles[idx]);
    }
    if (sim_stats.peak_on_chip_live_bytes > 0) {
        result.peak_bram_bytes = sim_stats.peak_on_chip_live_bytes;
    }
    result.peak_total_bytes = std::max(result.peak_bram_bytes, result.peak_hbm_bytes);
    result.primitive_peak_memory_bytes = result.peak_total_bytes;
    result.peak_memory_bytes = result.peak_total_bytes;
    result.total_latency = TotalLatency(result.breakdown);
    return result;
}

ExecutionResult CycleBackend::EstimateFAB(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    return EstimateMethod(
        req,
        plan,
        state,
        KeySwitchMethod::FAB);
}

ExecutionResult CycleBackend::EstimateFAST(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    return EstimateMethod(
        req,
        plan,
        state,
        KeySwitchMethod::FAST);
}

ExecutionResult CycleBackend::EstimateOLA(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    return EstimateMethod(
        req,
        plan,
        state,
        KeySwitchMethod::OLA);
}

ExecutionResult CycleBackend::EstimateHERA(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    return EstimateMethod(
        req,
        plan,
        state,
        KeySwitchMethod::HERA);
}

ExecutionResult CycleBackend::EstimateCinnamon(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    return EstimateMethod(
        req,
        plan,
        state,
        KeySwitchMethod::Cinnamon);
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
