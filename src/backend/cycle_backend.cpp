#include "backend/cycle_backend.h"

#include "backend/analytical_backend.h"

#include <algorithm>
#include <ostream>
#include <unordered_map>

namespace {

Time StageTimeFromBreakdown(
    const ExecutionBreakdown& breakdown,
    StageType stage_type) {

    switch (stage_type) {
    case StageType::KeyLoad:
        return breakdown.key_load_time;
    case StageType::Dispatch:
        return breakdown.dispatch_time;
    case StageType::Decompose:
        return breakdown.decompose_time;
    case StageType::Multiply:
        return breakdown.multiply_time;
    case StageType::BasisConvert:
        return breakdown.basis_convert_time;
    case StageType::Merge:
        return breakdown.merge_time;
    }

    return 0;
}

void AddStageTime(
    ExecutionBreakdown* breakdown,
    StageType stage_type,
    Time value) {

    switch (stage_type) {
    case StageType::KeyLoad:
        breakdown->key_load_time += value;
        break;
    case StageType::Dispatch:
        breakdown->dispatch_time += value;
        break;
    case StageType::Decompose:
        breakdown->decompose_time += value;
        break;
    case StageType::Multiply:
        breakdown->multiply_time += value;
        break;
    case StageType::BasisConvert:
        breakdown->basis_convert_time += value;
        break;
    case StageType::Merge:
        breakdown->merge_time += value;
        break;
    }
}

Time TotalLatency(const ExecutionBreakdown& breakdown) {
    return breakdown.key_load_time
        + breakdown.dispatch_time
        + breakdown.decompose_time
        + breakdown.multiply_time
        + breakdown.basis_convert_time
        + breakdown.merge_time;
}

PrimitiveType PrimitiveTypeForStage(StageType stage_type) {
    switch (stage_type) {
    case StageType::KeyLoad:
        return PrimitiveType::KeyLoadDMA;
    case StageType::Dispatch:
        return PrimitiveType::DispatchDMA;
    case StageType::Decompose:
        return PrimitiveType::DecomposeKernel;
    case StageType::Multiply:
        return PrimitiveType::MultiplyKernel;
    case StageType::BasisConvert:
        return PrimitiveType::BasisConvertKernel;
    case StageType::Merge:
        return PrimitiveType::MergeReduce;
    }

    return PrimitiveType::DispatchDMA;
}

} // namespace

std::vector<Stage> CycleBackend::BuildStages(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    std::vector<Stage> stages;
    const auto& p = req.ks_profile;
    const size_t cards = std::max<size_t>(1, plan.assigned_cards.size());

    bool need_key_load = false;
    for (const CardId card_id : plan.assigned_cards) {
        const auto& card = state.cards.at(card_id);
        if (!card.resident_user.has_value() || card.resident_user.value() != req.user_id) {
            need_key_load = true;
            break;
        }
    }

    if (need_key_load) {
        stages.push_back(Stage{StageType::KeyLoad, p.key_bytes, 1});
    }

    stages.push_back(Stage{StageType::Dispatch, p.input_bytes, 1});
    stages.push_back(Stage{
        StageType::Decompose,
        0,
        p.num_ciphertexts * p.num_digits * p.num_rns_limbs});
    stages.push_back(Stage{
        StageType::Multiply,
        0,
        p.num_ciphertexts * p.num_polys * p.num_digits});
    stages.push_back(Stage{
        StageType::BasisConvert,
        0,
        p.num_ciphertexts * p.num_rns_limbs});

    if (cards > 1) {
        stages.push_back(Stage{
            StageType::Merge,
            p.output_bytes,
            static_cast<uint32_t>(cards)});
    }

    return stages;
}

bool CycleBackend::ResidentKeyHit(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    for (const CardId card_id : plan.assigned_cards) {
        const auto& card = state.cards.at(card_id);
        if (!card.resident_user.has_value() || card.resident_user.value() != req.user_id) {
            return false;
        }
    }

    return true;
}

PrimitiveTrace CycleBackend::BuildPrimitiveTrace(
    const std::vector<Stage>& stages,
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    PrimitiveTrace trace;
    trace.ops.reserve(stages.size());

    const bool key_hit = ResidentKeyHit(req, plan, state);
    for (const Stage& stage : stages) {
        PrimitiveOp op;
        op.type = PrimitiveTypeForStage(stage.type);
        op.stage_type = stage.type;
        op.bytes = stage.bytes;
        op.work_units = stage.work_units;
        op.assigned_cards = plan.assigned_cards;
        op.key_hit = key_hit;
        trace.ops.push_back(std::move(op));
    }

    return trace;
}

ExecutionResult CycleBackend::Estimate(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    ++stats_.estimate_calls;

    ExecutionResult result{};
    const std::vector<Stage> stages = BuildStages(req, plan, state);
    const PrimitiveTrace trace = BuildPrimitiveTrace(stages, req, plan, state);

    ++stats_.primitive_sim_calls;
    stats_.primitive_ops_total += trace.ops.size();

    const PrimitiveResult primitive_result = primitive_simulator_.Simulate(trace, state);

    std::unordered_map<StageType, Time> stage_latencies;
    stage_latencies.reserve(primitive_result.stage_breakdown.size());
    for (const auto& entry : primitive_result.stage_breakdown) {
        stage_latencies[entry.stage_type] += entry.latency_ns;
    }

    AnalyticalBackend analytical_fallback;
    ExecutionResult fallback_result{};
    bool fallback_ready = false;

    for (const Stage& stage : stages) {
        auto it = stage_latencies.find(stage.type);
        if (it != stage_latencies.end()) {
            AddStageTime(&result.breakdown, stage.type, it->second);
            continue;
        }

        if (!fallback_ready) {
            fallback_result = analytical_fallback.Estimate(req, plan, state);
            fallback_ready = true;
        }

        ++stats_.fallback_count;
        const Time fallback_stage = StageTimeFromBreakdown(fallback_result.breakdown, stage.type);
        AddStageTime(&result.breakdown, stage.type, fallback_stage);
        result.energy_nj += static_cast<double>(fallback_stage) * 0.5;
    }

    result.total_latency = TotalLatency(result.breakdown);

    const uint64_t request_working_set = req.ks_profile.input_bytes + req.ks_profile.key_bytes;
    result.peak_memory_bytes = std::max<uint64_t>(
        primitive_result.peak_memory_bytes,
        request_working_set);

    result.energy_nj += primitive_result.total_energy_nj;

    return result;
}

CycleBackendStats CycleBackend::GetStats() const {
    return stats_;
}

void CycleBackend::PrintStats(std::ostream& os) const {
    const CycleBackendStats s = GetStats();

    os << "=== CycleBackend Stub Stats ===\n";
    os << "PrimitiveSimulatorCalls=" << s.primitive_sim_calls << "\n";
    os << "PrimitiveOpsTotal=" << s.primitive_ops_total << "\n";
    os << "CycleFallbackCount=" << s.fallback_count << "\n";
}
