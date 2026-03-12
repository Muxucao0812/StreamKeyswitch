#include "backend/hybrid_backend.h"

#include "backend/analytical_backend.h"
#include "backend/table_backend.h"

#include <algorithm>
#include <ostream>
#include <stdexcept>
#include <unordered_map>

namespace {

Time GetStageTime(
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

void SetStageTime(
    ExecutionBreakdown* breakdown,
    StageType stage_type,
    Time stage_time) {

    switch (stage_type) {
    case StageType::KeyLoad:
        breakdown->key_load_time = stage_time;
        break;
    case StageType::Dispatch:
        breakdown->dispatch_time = stage_time;
        break;
    case StageType::Decompose:
        breakdown->decompose_time = stage_time;
        break;
    case StageType::Multiply:
        breakdown->multiply_time = stage_time;
        break;
    case StageType::BasisConvert:
        breakdown->basis_convert_time = stage_time;
        break;
    case StageType::Merge:
        breakdown->merge_time = stage_time;
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

HybridBackend::HybridBackend(
    bool use_table_coarse,
    std::string profile_table_path)
    : use_table_coarse_(use_table_coarse) {

    if (use_table_coarse_) {
        if (profile_table_path.empty()) {
            coarse_backend_ = std::make_unique<TableBackend>();
            coarse_source_ = "table(built-in default)";
        } else {
            coarse_backend_ = std::make_unique<TableBackend>(profile_table_path);
            coarse_source_ = "table(" + profile_table_path + ")";
        }
    } else {
        coarse_backend_ = std::make_unique<AnalyticalBackend>();
        coarse_source_ = "analytical";
    }

    if (coarse_backend_ == nullptr) {
        throw std::runtime_error("HybridBackend failed to initialize coarse backend");
    }
}

std::vector<Stage> HybridBackend::BuildStages(
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

bool HybridBackend::ResidentKeyHit(
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

bool HybridBackend::UsePrimitiveForStage(
    StageType stage_type,
    const ExecutionPlan& plan) const {

    if (stage_type == StageType::Multiply) {
        return true;
    }

    if (stage_type == StageType::Merge && plan.assigned_cards.size() > 1) {
        return true;
    }

    return false;
}

PrimitiveTrace HybridBackend::BuildPrimitiveTrace(
    const std::vector<Stage>& stages,
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    PrimitiveTrace trace;

    const bool key_hit = ResidentKeyHit(req, plan, state);
    for (const Stage& stage : stages) {
        if (!UsePrimitiveForStage(stage.type, plan)) {
            continue;
        }

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

ExecutionResult HybridBackend::Estimate(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    ++stats_.estimate_calls;
    ++stats_.coarse_estimate_calls;

    ExecutionResult result = coarse_backend_->Estimate(req, plan, state);
    const ExecutionResult coarse_result = result;

    const std::vector<Stage> stages = BuildStages(req, plan, state);
    for (const Stage& stage : stages) {
        if (UsePrimitiveForStage(stage.type, plan)) {
            ++stats_.primitive_stage_attempts;
        }
    }

    const PrimitiveTrace trace = BuildPrimitiveTrace(stages, req, plan, state);
    if (trace.ops.empty()) {
        return result;
    }

    ++stats_.primitive_sim_calls;
    stats_.primitive_ops_total += trace.ops.size();

    const PrimitiveResult primitive_result = primitive_simulator_.Simulate(trace, state);

    std::unordered_map<StageType, Time> primitive_stage_latency;
    primitive_stage_latency.reserve(primitive_result.stage_breakdown.size());
    for (const auto& entry : primitive_result.stage_breakdown) {
        primitive_stage_latency[entry.stage_type] += entry.latency_ns;
    }

    Time coarse_replaced_stage_sum = 0;

    for (const Stage& stage : stages) {
        if (!UsePrimitiveForStage(stage.type, plan)) {
            continue;
        }

        auto it = primitive_stage_latency.find(stage.type);
        if (it == primitive_stage_latency.end()) {
            ++stats_.primitive_stage_fallback;
            continue;
        }

        const Time old_stage_time = GetStageTime(result.breakdown, stage.type);
        coarse_replaced_stage_sum += old_stage_time;

        SetStageTime(&result.breakdown, stage.type, it->second);
        ++stats_.primitive_stage_covered;

        if (stage.type == StageType::Multiply) {
            ++stats_.multiply_primitive_covered;
        } else if (stage.type == StageType::Merge) {
            ++stats_.merge_primitive_covered;
        }
    }

    result.total_latency = TotalLatency(result.breakdown);
    result.peak_memory_bytes = std::max<uint64_t>(
        coarse_result.peak_memory_bytes,
        primitive_result.peak_memory_bytes);

    if (coarse_result.total_latency == 0) {
        result.energy_nj = primitive_result.total_energy_nj;
        return result;
    }

    const Time coarse_non_replaced =
        (coarse_result.total_latency > coarse_replaced_stage_sum)
        ? (coarse_result.total_latency - coarse_replaced_stage_sum)
        : 0;

    const double coarse_non_replaced_ratio =
        static_cast<double>(coarse_non_replaced)
        / static_cast<double>(coarse_result.total_latency);

    result.energy_nj = coarse_result.energy_nj * coarse_non_replaced_ratio
        + primitive_result.total_energy_nj;

    return result;
}

HybridBackendStats HybridBackend::GetStats() const {
    return stats_;
}

void HybridBackend::PrintStats(std::ostream& os) const {
    const HybridBackendStats s = GetStats();

    os << "=== HybridBackend Stats ===\n";
    os << "HybridCoarseBackend=" << (UseTableCoarse() ? "table" : "analytical") << "\n";
    os << "HybridCoarseSource=" << CoarseSource() << "\n";
    os << "PrimitiveSimulatorCalls=" << s.primitive_sim_calls << "\n";
    os << "PrimitiveOpsTotal=" << s.primitive_ops_total << "\n";
    os << "HybridStageAttempts=" << s.primitive_stage_attempts << "\n";
    os << "HybridStageCovered=" << s.primitive_stage_covered << "\n";
    os << "HybridStageFallbackCount=" << s.primitive_stage_fallback << "\n";
    os << "HybridMultiplyPrimitiveCovered=" << s.multiply_primitive_covered << "\n";
    os << "HybridMergePrimitiveCovered=" << s.merge_primitive_covered << "\n";
}

const std::string& HybridBackend::CoarseSource() const {
    return coarse_source_;
}

bool HybridBackend::UseTableCoarse() const {
    return use_table_coarse_;
}
