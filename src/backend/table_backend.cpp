#include "backend/table_backend.h"

#include "backend/analytical_backend.h"

#include <algorithm>
#include <cmath>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace {

double ClampMin(double value, double lower) {
    return (value < lower) ? lower : value;
}

uint32_t ToBytesBucket(uint64_t bytes) {
    constexpr uint64_t kBucket = 4096ULL;
    if (bytes == 0) {
        return 0;
    }
    return static_cast<uint32_t>((bytes + kBucket - 1ULL) / kBucket);
}

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
    Time stage_time) {

    switch (stage_type) {
    case StageType::KeyLoad:
        breakdown->key_load_time += stage_time;
        break;
    case StageType::Dispatch:
        breakdown->dispatch_time += stage_time;
        break;
    case StageType::Decompose:
        breakdown->decompose_time += stage_time;
        break;
    case StageType::Multiply:
        breakdown->multiply_time += stage_time;
        break;
    case StageType::BasisConvert:
        breakdown->basis_convert_time += stage_time;
        break;
    case StageType::Merge:
        breakdown->merge_time += stage_time;
        break;
    }
}

} // namespace

TableBackend::TableBackend()
    : profile_table_(StageProfileTable::BuildBuiltInDefault()),
      profile_source_("built-in default") {}

TableBackend::TableBackend(const std::string& profile_table_csv_path)
    : profile_table_(StageProfileTable::BuildBuiltInDefault()),
      profile_source_("built-in default") {

    const ProfileTableLoadResult loaded =
        LoadStageProfileTableFromCsv(profile_table_csv_path);
    if (!loaded.ok) {
        throw std::runtime_error(loaded.error_message);
    }

    profile_table_.SetEntries(loaded.entries);
    profile_source_ = profile_table_csv_path;
}

std::vector<Stage> TableBackend::BuildStages(
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

bool TableBackend::ResidentKeyHit(
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

Time TableBackend::EstimateStageFromTable(
    const Stage& stage,
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state,
    ProfileLookupMode* lookup_mode) const {

    const uint32_t cards = static_cast<uint32_t>(
        std::max<size_t>(1, plan.assigned_cards.size()));

    StageLatencyLookupQuery query;
    query.stage_type = stage.type;
    query.num_digits = req.ks_profile.num_digits;
    query.num_rns_limbs = req.ks_profile.num_rns_limbs;
    query.card_count = cards;
    query.key_hit = ResidentKeyHit(req, plan, state);
    query.input_bytes_bucket = ToBytesBucket(req.ks_profile.input_bytes);
    query.output_bytes_bucket = ToBytesBucket(req.ks_profile.output_bytes);

    const StageLatencyLookupResult looked_up = profile_table_.Lookup(query);
    *lookup_mode = looked_up.mode;

    if (!looked_up.found) {
        return 0;
    }

    const Time base = looked_up.latency_ns;

    double scale = 1.0;
    switch (stage.type) {
    case StageType::KeyLoad: {
        const double baseline_key_bytes = 8192.0;
        scale = ClampMin(
            static_cast<double>(req.ks_profile.key_bytes) / baseline_key_bytes,
            0.25);
        break;
    }
    case StageType::Dispatch: {
        const double baseline_input_bytes = 4096.0;
        scale = ClampMin(
            static_cast<double>(stage.bytes) / baseline_input_bytes,
            0.25);
        break;
    }
    case StageType::Decompose: {
        const double baseline_work = static_cast<double>(
            std::max<uint32_t>(1, req.ks_profile.num_digits * req.ks_profile.num_rns_limbs));
        scale = ClampMin(static_cast<double>(stage.work_units) / baseline_work, 0.25);
        break;
    }
    case StageType::Multiply: {
        const double baseline_work = static_cast<double>(
            std::max<uint32_t>(1, req.ks_profile.num_digits * req.ks_profile.num_polys));
        scale = ClampMin(static_cast<double>(stage.work_units) / baseline_work, 0.25);
        break;
    }
    case StageType::BasisConvert: {
        const double baseline_work = static_cast<double>(
            std::max<uint32_t>(1, req.ks_profile.num_rns_limbs));
        scale = ClampMin(static_cast<double>(stage.work_units) / baseline_work, 0.25);
        break;
    }
    case StageType::Merge: {
        if (cards <= 1) {
            return 0;
        }
        const double baseline_output_bytes = 4096.0;
        scale = ClampMin(
            static_cast<double>(stage.bytes) / baseline_output_bytes,
            0.25);
        break;
    }
    }

    return static_cast<Time>(std::llround(static_cast<double>(base) * scale));
}

ExecutionResult TableBackend::Estimate(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    ExecutionResult result{};
    const auto stages = BuildStages(req, plan, state);

    AnalyticalBackend analytical_fallback;
    std::optional<ExecutionResult> fallback_estimate;

    for (const Stage& stage : stages) {
        ProfileLookupMode lookup_mode = ProfileLookupMode::NotFound;
        Time stage_time = EstimateStageFromTable(
            stage,
            req,
            plan,
            state,
            &lookup_mode);

        ++stats_.total_queries;

        if (lookup_mode == ProfileLookupMode::NotFound) {
            if (!fallback_estimate.has_value()) {
                fallback_estimate = analytical_fallback.Estimate(req, plan, state);
            }
            stage_time = StageTimeFromBreakdown(fallback_estimate->breakdown, stage.type);
            ++stats_.fallback_hits;
        } else if (lookup_mode == ProfileLookupMode::Exact) {
            ++stats_.exact_hits;
        } else if (lookup_mode == ProfileLookupMode::Interpolated) {
            ++stats_.interpolated_hits;
        } else {
            ++stats_.nearest_hits;
        }

        AddStageTime(&result.breakdown, stage.type, stage_time);
    }

    result.total_latency =
        result.breakdown.key_load_time
        + result.breakdown.dispatch_time
        + result.breakdown.decompose_time
        + result.breakdown.multiply_time
        + result.breakdown.basis_convert_time
        + result.breakdown.merge_time;

    result.peak_memory_bytes = req.ks_profile.input_bytes + req.ks_profile.key_bytes;
    result.energy_nj = static_cast<double>(result.total_latency) * 0.52;

    return result;
}

const std::string& TableBackend::ProfileSource() const {
    return profile_source_;
}

TableLookupStats TableBackend::GetLookupStats() const {
    return stats_;
}

void TableBackend::PrintLookupStats(std::ostream& os) const {
    const TableLookupStats s = GetLookupStats();

    os << "=== TableBackend Profile Stats ===\n";
    os << "ProfileTableSource=" << ProfileSource() << "\n";
    os << "ProfileExactHitCount=" << s.exact_hits << "\n";
    os << "ProfileInterpolatedCount=" << s.interpolated_hits << "\n";
    os << "ProfileNearestCount=" << s.nearest_hits << "\n";
    os << "ProfileNearestOrInterpolatedCount=" << s.nearest_or_interpolated_hits() << "\n";
    os << "ProfileFallbackCount=" << s.fallback_hits << "\n";
    os << "ProfileTotalQueries=" << s.total_queries << "\n";
}

