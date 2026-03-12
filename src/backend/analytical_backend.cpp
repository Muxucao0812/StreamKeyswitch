#include "backend/analytical_backend.h"

#include <algorithm>

std::vector<StageType> AnalyticalBackend::StageSequenceForTest(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {
    std::vector<StageType> sequence;
    const auto stages = BuildStages(req, plan, state);
    sequence.reserve(stages.size());
    for (const auto& stage : stages) {
        sequence.push_back(stage.type);
    }
    return sequence;
}

std::vector<Stage> AnalyticalBackend::BuildStages(
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

    const uint32_t decompose_work =
        p.num_ciphertexts * p.num_digits * p.num_rns_limbs;
    stages.push_back(Stage{StageType::Decompose, 0, decompose_work});

    const uint32_t multiply_work =
        p.num_ciphertexts * p.num_polys * p.num_digits;
    stages.push_back(Stage{StageType::Multiply, 0, multiply_work});

    const uint32_t basis_work = p.num_ciphertexts * p.num_rns_limbs;
    stages.push_back(Stage{StageType::BasisConvert, 0, basis_work});

    if (cards > 1) {
        stages.push_back(Stage{StageType::Merge, p.output_bytes, static_cast<uint32_t>(cards)});
    }

    return stages;
}

Time AnalyticalBackend::EstimateStage(
    const Stage& stage,
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& /*state*/) const {

    const size_t cards = std::max<size_t>(1, plan.assigned_cards.size());
    const auto scaled_time = [cards](Time parallel_part, Time serial_part, Time sync_part_per_card) {
        const Time k = static_cast<Time>(cards);
        const Time a_over_k = (k == 0) ? parallel_part : (parallel_part / k);
        const Time c_k = (cards > 1) ? (sync_part_per_card * static_cast<Time>(cards - 1)) : 0;
        return a_over_k + serial_part + c_k;
    };

    switch (stage.type) {
    case StageType::KeyLoad:
        return (req.user_profile.key_load_time > 0)
            ? req.user_profile.key_load_time
            : (500 + static_cast<Time>(stage.bytes / 1024));

    case StageType::Dispatch:
        return scaled_time(
            /*A=*/static_cast<Time>(stage.bytes / 4096),
            /*B=*/50,
            /*C_per_card=*/0);

    case StageType::Decompose:
        return scaled_time(
            /*A=*/static_cast<Time>(25 * stage.work_units),
            /*B=*/300,
            /*C_per_card=*/8);

    case StageType::Multiply:
        return scaled_time(
            /*A=*/static_cast<Time>(35 * stage.work_units),
            /*B=*/500,
            /*C_per_card=*/12);

    case StageType::BasisConvert:
        return scaled_time(
            /*A=*/static_cast<Time>(20 * stage.work_units),
            /*B=*/200,
            /*C_per_card=*/6);

    case StageType::Merge:
        if (cards <= 1) {
            return 0;
        }
        return 150
            + static_cast<Time>(40 * cards)
            + static_cast<Time>(stage.bytes / 8192);
    }

    return 0;
}

ExecutionResult AnalyticalBackend::Estimate(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    ExecutionResult result{};
    const auto stages = BuildStages(req, plan, state);

    for (const Stage& stage : stages) {
        const Time stage_time = EstimateStage(stage, req, plan, state);

        switch (stage.type) {
        case StageType::KeyLoad:
            result.breakdown.key_load_time += stage_time;
            break;
        case StageType::Dispatch:
            result.breakdown.dispatch_time += stage_time;
            break;
        case StageType::Decompose:
            result.breakdown.decompose_time += stage_time;
            break;
        case StageType::Multiply:
            result.breakdown.multiply_time += stage_time;
            break;
        case StageType::BasisConvert:
            result.breakdown.basis_convert_time += stage_time;
            break;
        case StageType::Merge:
            result.breakdown.merge_time += stage_time;
            break;
        }
    }

    result.total_latency =
        result.breakdown.key_load_time
        + result.breakdown.dispatch_time
        + result.breakdown.decompose_time
        + result.breakdown.multiply_time
        + result.breakdown.basis_convert_time
        + result.breakdown.merge_time;

    result.peak_memory_bytes = req.ks_profile.input_bytes + req.ks_profile.key_bytes;
    result.energy_nj = static_cast<double>(result.total_latency) * 0.5;

    return result;
}
