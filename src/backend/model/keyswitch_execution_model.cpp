#include "backend/model/keyswitch_execution_model.h"
#include "model/keyswitch_method_resolver.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <iostream>

namespace {

uint64_t CeilDivU64(uint64_t a, uint64_t b) {
    return (b == 0) ? 0 : (a + b - 1) / b;
}

uint32_t CeilDivU32(uint32_t a, uint32_t b) {
    return (b == 0) ? 0 : (a + b - 1) / b;
}

uint32_t SaturateU32(uint64_t value) {
    return static_cast<uint32_t>(std::min<uint64_t>(value, std::numeric_limits<uint32_t>::max()));
}

PartitionStrategy ResolvePartition(
    PartitionStrategy requested,
    KeySwitchMethod method) {

    if (requested != PartitionStrategy::Auto) {
        return requested;
    }
    return (method == KeySwitchMethod::ScaleOutLimb)
        ? PartitionStrategy::ByLimb
        : PartitionStrategy::None;
}

CollectiveStrategy ResolveCollective(
    CollectiveStrategy requested,
    KeySwitchMethod method) {

    if (requested != CollectiveStrategy::Auto) {
        return requested;
    }
    return (method == KeySwitchMethod::ScaleOutLimb)
        ? CollectiveStrategy::GatherToRoot
        : CollectiveStrategy::None;
}

KeyPlacement ResolveKeyPlacement(
    KeyPlacement requested,
    KeySwitchMethod method) {

    if (requested != KeyPlacement::Auto) {
        return requested;
    }
    return (method == KeySwitchMethod::ScaleOutLimb)
        ? KeyPlacement::ReplicatedPerCard
        : KeyPlacement::StreamFromHBM;
}

KeySwitchExecution UnsupportedConfig(
    KeySwitchMethod method) {

    KeySwitchExecution unsupported;
    unsupported.method = method;
    unsupported.requested_method = method;
    unsupported.effective_method = method;
    unsupported.fallback_used = true;
    unsupported.fallback_reason = KeySwitchFallbackReason::UnsupportedConfig;
    unsupported.valid = false;
    unsupported.tiled_execution = false;
    return unsupported;
}

uint32_t EffectiveScaleOutCards(
    const Request& req,
    const ExecutionPlan& plan) {

    const uint32_t assigned = static_cast<uint32_t>(plan.assigned_cards.size());
    if (assigned == 0) {
        return 0;
    }
    const uint32_t hint = req.ks_profile.scale_out_cards;
    if (hint == 0) {
        return assigned;
    }
    return std::max<uint32_t>(1, std::min<uint32_t>(hint, assigned));
}

struct LimbShard {
    uint32_t begin = 0;
    uint32_t count = 0;
};

std::vector<LimbShard> BuildLimbShards(
    uint32_t total_limbs,
    uint32_t cards) {

    std::vector<LimbShard> shards;
    const uint32_t safe_limbs = std::max<uint32_t>(1, total_limbs);
    const uint32_t safe_cards = std::max<uint32_t>(1, std::min<uint32_t>(cards, safe_limbs));
    shards.reserve(safe_cards);
    for (uint32_t idx = 0; idx < safe_cards; ++idx) {
        const uint32_t begin = static_cast<uint32_t>(
            (static_cast<uint64_t>(idx) * safe_limbs) / safe_cards);
        const uint32_t end = static_cast<uint32_t>(
            (static_cast<uint64_t>(idx + 1) * safe_limbs) / safe_cards);
        LimbShard shard;
        shard.begin = begin;
        shard.count = end - begin;
        shards.push_back(shard);
    }
    return shards;
}

bool IsInterCardStepType(TileExecutionStepType step_type) {
    switch (step_type) {
    case TileExecutionStepType::InterCardSendStep:
    case TileExecutionStepType::InterCardRecvStep:
    case TileExecutionStepType::InterCardReduceStep:
    case TileExecutionStepType::BarrierStep:
    case TileExecutionStepType::InterCardCommTile:
    case TileExecutionStepType::InterCardBarrier:
    case TileExecutionStepType::Merge:
        return true;
    default:
        return false;
    }
}

bool ContainsInterCardSteps(const std::vector<TileExecutionStep>& steps) {
    for (const TileExecutionStep& step : steps) {
        if (IsInterCardStepType(step.type)) {
            return true;
        }
    }
    return false;
}

uint64_t ScaleBytesByRatio(
    uint64_t total_bytes,
    uint32_t part,
    uint32_t whole) {

    if (total_bytes == 0) {
        return 0;
    }
    const uint32_t safe_whole = std::max<uint32_t>(1, whole);
    const __uint128_t numer =
        static_cast<__uint128_t>(total_bytes) * static_cast<__uint128_t>(part)
        + static_cast<__uint128_t>(safe_whole - 1);
    const __uint128_t scaled_128 = numer / static_cast<__uint128_t>(safe_whole);
    const uint64_t scaled = (scaled_128 > static_cast<__uint128_t>(std::numeric_limits<uint64_t>::max()))
        ? std::numeric_limits<uint64_t>::max()
        : static_cast<uint64_t>(scaled_128);
    return std::max<uint64_t>(1, scaled);
}

void MergePeakBuffers(
    PeakBufferUsage* dst,
    const PeakBufferUsage& src) {

    dst->component_peak.key_buffer_bytes = std::max<uint64_t>(
        dst->component_peak.key_buffer_bytes,
        src.component_peak.key_buffer_bytes);
    dst->component_peak.ciphertext_buffer_bytes = std::max<uint64_t>(
        dst->component_peak.ciphertext_buffer_bytes,
        src.component_peak.ciphertext_buffer_bytes);
    dst->component_peak.temp_working_buffer_bytes = std::max<uint64_t>(
        dst->component_peak.temp_working_buffer_bytes,
        src.component_peak.temp_working_buffer_bytes);
    dst->component_peak.accumulation_buffer_bytes = std::max<uint64_t>(
        dst->component_peak.accumulation_buffer_bytes,
        src.component_peak.accumulation_buffer_bytes);

    dst->persistent_peak_bytes = std::max<uint64_t>(dst->persistent_peak_bytes, src.persistent_peak_bytes);
    dst->static_peak_bytes = std::max<uint64_t>(dst->static_peak_bytes, src.static_peak_bytes);
    dst->dynamic_peak_bytes = std::max<uint64_t>(dst->dynamic_peak_bytes, src.dynamic_peak_bytes);
    dst->key_peak_bytes = std::max<uint64_t>(dst->key_peak_bytes, src.key_peak_bytes);
    dst->ct_peak_bytes = std::max<uint64_t>(dst->ct_peak_bytes, src.ct_peak_bytes);
    dst->out_peak_bytes = std::max<uint64_t>(dst->out_peak_bytes, src.out_peak_bytes);
    dst->temp_peak_bytes = std::max<uint64_t>(dst->temp_peak_bytes, src.temp_peak_bytes);
    dst->total_peak_bytes = std::max<uint64_t>(dst->total_peak_bytes, src.total_peak_bytes);
}

class BufferTracker {
public:
    explicit BufferTracker(uint64_t budget_bytes)
        : budget_bytes_(budget_bytes) {}

    const BufferUsage& Live() const {
        return live_;
    }

    const PeakBufferUsage& Peaks() const {
        return peaks_;
    }

    bool overflowed() const {
        return overflowed_;
    }

    void AcquirePersistentKey(uint64_t bytes) {
        live_.persistent.key_buffer_bytes += bytes;
        UpdateTotals();
    }

    void ReleasePersistentKey(uint64_t bytes) {
        live_.persistent.key_buffer_bytes =
            (bytes > live_.persistent.key_buffer_bytes)
            ? 0
            : (live_.persistent.key_buffer_bytes - bytes);
        UpdateTotals();
    }

    void AcquireStaticAccum(uint64_t bytes) {
        live_.tile_static.accumulation_buffer_bytes += bytes;
        UpdateTotals();
    }

    void ReleaseStaticAccum(uint64_t bytes) {
        live_.tile_static.accumulation_buffer_bytes =
            (bytes > live_.tile_static.accumulation_buffer_bytes)
            ? 0
            : (live_.tile_static.accumulation_buffer_bytes - bytes);
        UpdateTotals();
    }

    void AcquireStaticTemp(uint64_t bytes) {
        live_.tile_static.temp_working_buffer_bytes += bytes;
        UpdateTotals();
    }

    void ReleaseStaticTemp(uint64_t bytes) {
        live_.tile_static.temp_working_buffer_bytes =
            (bytes > live_.tile_static.temp_working_buffer_bytes)
            ? 0
            : (live_.tile_static.temp_working_buffer_bytes - bytes);
        UpdateTotals();
    }

    void AcquireDynamicKey(uint64_t bytes) {
        live_.dynamic.key_buffer_bytes += bytes;
        UpdateTotals();
    }

    void ReleaseDynamicKey(uint64_t bytes) {
        live_.dynamic.key_buffer_bytes =
            (bytes > live_.dynamic.key_buffer_bytes)
            ? 0
            : (live_.dynamic.key_buffer_bytes - bytes);
        UpdateTotals();
    }

    void AcquireDynamicCt(uint64_t bytes) {
        live_.dynamic.ciphertext_buffer_bytes += bytes;
        UpdateTotals();
    }

    void ReleaseDynamicCt(uint64_t bytes) {
        live_.dynamic.ciphertext_buffer_bytes =
            (bytes > live_.dynamic.ciphertext_buffer_bytes)
            ? 0
            : (live_.dynamic.ciphertext_buffer_bytes - bytes);
        UpdateTotals();
    }

    void AcquireDynamicTemp(uint64_t bytes) {
        live_.dynamic.temp_working_buffer_bytes += bytes;
        UpdateTotals();
    }

    void ReleaseDynamicTemp(uint64_t bytes) {
        live_.dynamic.temp_working_buffer_bytes =
            (bytes > live_.dynamic.temp_working_buffer_bytes)
            ? 0
            : (live_.dynamic.temp_working_buffer_bytes - bytes);
        UpdateTotals();
    }

private:
    void UpdateTotals() {
        live_.persistent_bytes =
            live_.persistent.key_buffer_bytes
            + live_.persistent.ciphertext_buffer_bytes
            + live_.persistent.temp_working_buffer_bytes
            + live_.persistent.accumulation_buffer_bytes;
        live_.static_bytes =
            live_.tile_static.key_buffer_bytes
            + live_.tile_static.ciphertext_buffer_bytes
            + live_.tile_static.temp_working_buffer_bytes
            + live_.tile_static.accumulation_buffer_bytes;
        live_.dynamic_working_bytes =
            live_.dynamic.key_buffer_bytes
            + live_.dynamic.ciphertext_buffer_bytes
            + live_.dynamic.temp_working_buffer_bytes
            + live_.dynamic.accumulation_buffer_bytes;

        live_.key_bytes =
            live_.persistent.key_buffer_bytes
            + live_.tile_static.key_buffer_bytes
            + live_.dynamic.key_buffer_bytes;
        live_.ct_bytes =
            live_.persistent.ciphertext_buffer_bytes
            + live_.tile_static.ciphertext_buffer_bytes
            + live_.dynamic.ciphertext_buffer_bytes;
        live_.out_bytes =
            live_.persistent.accumulation_buffer_bytes
            + live_.tile_static.accumulation_buffer_bytes
            + live_.dynamic.accumulation_buffer_bytes;
        live_.temp_bytes =
            live_.persistent.temp_working_buffer_bytes
            + live_.tile_static.temp_working_buffer_bytes
            + live_.dynamic.temp_working_buffer_bytes;
        live_.total_live_bytes =
            live_.persistent_bytes + live_.static_bytes + live_.dynamic_working_bytes;

        peaks_.component_peak.key_buffer_bytes =
            std::max(peaks_.component_peak.key_buffer_bytes, live_.key_bytes);
        peaks_.component_peak.ciphertext_buffer_bytes =
            std::max(peaks_.component_peak.ciphertext_buffer_bytes, live_.ct_bytes);
        peaks_.component_peak.temp_working_buffer_bytes =
            std::max(peaks_.component_peak.temp_working_buffer_bytes, live_.temp_bytes);
        peaks_.component_peak.accumulation_buffer_bytes =
            std::max(peaks_.component_peak.accumulation_buffer_bytes, live_.out_bytes);

        peaks_.persistent_peak_bytes =
            std::max(peaks_.persistent_peak_bytes, live_.persistent_bytes);
        peaks_.static_peak_bytes =
            std::max(peaks_.static_peak_bytes, live_.static_bytes);
        peaks_.dynamic_peak_bytes =
            std::max(peaks_.dynamic_peak_bytes, live_.dynamic_working_bytes);

        peaks_.key_peak_bytes = std::max(peaks_.key_peak_bytes, live_.key_bytes);
        peaks_.ct_peak_bytes = std::max(peaks_.ct_peak_bytes, live_.ct_bytes);
        peaks_.out_peak_bytes = std::max(peaks_.out_peak_bytes, live_.out_bytes);
        peaks_.temp_peak_bytes = std::max(peaks_.temp_peak_bytes, live_.temp_bytes);
        peaks_.total_peak_bytes = std::max(peaks_.total_peak_bytes, live_.total_live_bytes);

        if (budget_bytes_ > 0 && live_.total_live_bytes > budget_bytes_) {
            overflowed_ = true;
        }
    }

private:
    uint64_t budget_bytes_ = 0;
    bool overflowed_ = false;
    BufferUsage live_;
    PeakBufferUsage peaks_;
};

uint64_t TempBufferBytes(uint64_t out_bytes, double temp_ratio) {
    return static_cast<uint64_t>(std::ceil(static_cast<double>(out_bytes) * temp_ratio));
}

uint64_t CoeffBytesPerLimb(const KeySwitchProblem& problem) {
    const uint32_t degree = std::max<uint32_t>(1, problem.poly_modulus_degree);
    return std::max<uint64_t>(1, CeilDivU64(problem.ct_limb_bytes, degree));
}

uint64_t StaticTempBytes(
    const KeySwitchProblem& problem,
    uint32_t ct_now,
    uint64_t out_bytes) {

    const uint64_t ratio_bytes = TempBufferBytes(out_bytes, problem.temp_buffer_ratio);
    const uint64_t dep_bytes =
        static_cast<uint64_t>(ct_now)
        * static_cast<uint64_t>(problem.limbs)
        * CoeffBytesPerLimb(problem);
    return std::max<uint64_t>(ratio_bytes, dep_bytes);
}

uint64_t DynamicWorkingTempBytes(
    const KeySwitchProblem& problem,
    uint32_t ct_now,
    uint32_t limb_now,
    uint32_t digit_now) {

    const uint64_t coeff_bytes = CoeffBytesPerLimb(problem);
    const long double base_ld =
        static_cast<long double>(ct_now)
        * static_cast<long double>(limb_now)
        * static_cast<long double>(digit_now)
        * static_cast<long double>(std::max<uint32_t>(1, problem.poly_modulus_degree))
        * static_cast<long double>(coeff_bytes);
    const uint64_t base = (base_ld >= static_cast<long double>(std::numeric_limits<uint64_t>::max()))
        ? std::numeric_limits<uint64_t>::max()
        : static_cast<uint64_t>(base_ld);
    const uint64_t multiply_factor = static_cast<uint64_t>(std::min<uint32_t>(problem.polys, 2));
    const uint64_t factor = std::max<uint64_t>(1, multiply_factor);
    const uint64_t multiply_bytes = (base > (std::numeric_limits<uint64_t>::max() / factor))
        ? std::numeric_limits<uint64_t>::max()
        : (base * factor);
    return std::max<uint64_t>(base, multiply_bytes);
}

double CostTransfer(
    uint64_t bytes,
    uint64_t transfer_count,
    uint64_t bw_bytes_per_ns,
    Time setup_ns) {

    const uint64_t bw = std::max<uint64_t>(1, bw_bytes_per_ns);
    const double streaming = static_cast<double>(CeilDivU64(bytes, bw));
    const double setup = static_cast<double>(setup_ns) * static_cast<double>(transfer_count);
    return streaming + setup;
}

double CostCompute(
    uint64_t work_items,
    uint64_t launch_count,
    uint64_t throughput_per_ns,
    Time launch_setup_ns) {

    const uint64_t thr = std::max<uint64_t>(1, throughput_per_ns);
    const double streaming = static_cast<double>(CeilDivU64(work_items, thr));
    const double setup = static_cast<double>(launch_setup_ns) * static_cast<double>(launch_count);
    return streaming + setup;
}

struct CandidateEval {
    bool valid = false;
    uint32_t ct_tiles = 0;
    uint32_t limb_tiles = 0;
    uint32_t digit_tiles = 0;
    uint32_t total_tile_count = 0;
    uint64_t estimated_peak_bram_bytes = 0;
    TileCostBreakdown cost;
};

CandidateEval EvaluateCandidate(
    const KeySwitchProblem& problem,
    const TilePlanner::Params& params,
    uint32_t ct_tile,
    uint32_t limb_tile,
    uint32_t digit_tile,
    bool key_persistent) {

    CandidateEval eval;

    if (ct_tile == 0 || limb_tile == 0 || digit_tile == 0) {
        return eval;
    }
    if (ct_tile > problem.ciphertexts
        || limb_tile > problem.limbs
        || digit_tile > problem.digits) {
        return eval;
    }

    eval.ct_tiles = CeilDivU32(problem.ciphertexts, ct_tile);
    eval.limb_tiles = CeilDivU32(problem.limbs, limb_tile);
    eval.digit_tiles = CeilDivU32(problem.digits, digit_tile);
    eval.total_tile_count = SaturateU32(
        static_cast<uint64_t>(eval.ct_tiles)
        * static_cast<uint64_t>(eval.limb_tiles)
        * static_cast<uint64_t>(eval.digit_tiles));

    const uint64_t occupancy_budget =
        (problem.bram_budget_bytes > problem.bram_guard_bytes)
        ? (problem.bram_budget_bytes - problem.bram_guard_bytes)
        : 0;
    BufferTracker tracker(occupancy_budget);

    uint64_t key_bytes = 0;
    uint64_t ct_bytes = 0;
    uint64_t out_bytes_total = 0;
    uint64_t key_transfer_count = 0;
    uint64_t ct_transfer_count = 0;
    uint64_t out_transfer_count = 0;

    uint64_t decompose_work = 0;
    uint64_t ks_inner_work = 0;
    uint64_t accumulate_work = 0;
    uint64_t basis_work = 0;
    uint64_t decompose_launch = 0;
    uint64_t ks_inner_launch = 0;
    uint64_t accumulate_launch = 0;
    uint64_t basis_launch = 0;

    if (key_persistent) {
        tracker.AcquirePersistentKey(problem.key_bytes);
        key_bytes += problem.key_bytes;
        key_transfer_count += 1;
    }

    for (uint32_t ct_idx = 0; ct_idx < eval.ct_tiles; ++ct_idx) {
        const uint32_t ct_remain = problem.ciphertexts - ct_idx * ct_tile;
        const uint32_t ct_now = std::min<uint32_t>(ct_tile, ct_remain);

        const uint64_t out_bytes =
            static_cast<uint64_t>(ct_now) * problem.limbs * problem.out_limb_bytes;
        const uint64_t static_temp_bytes = StaticTempBytes(problem, ct_now, out_bytes);
        tracker.AcquireStaticAccum(out_bytes);
        tracker.AcquireStaticTemp(static_temp_bytes);

        for (uint32_t limb_idx = 0; limb_idx < eval.limb_tiles; ++limb_idx) {
            const uint32_t limb_remain = problem.limbs - limb_idx * limb_tile;
            const uint32_t limb_now = std::min<uint32_t>(limb_tile, limb_remain);

            const uint64_t ct_chunk_bytes =
                static_cast<uint64_t>(ct_now) * limb_now * problem.ct_limb_bytes;
            tracker.AcquireDynamicCt(ct_chunk_bytes);
            ct_bytes += ct_chunk_bytes;
            ct_transfer_count += 1;

            for (uint32_t digit_idx = 0; digit_idx < eval.digit_tiles; ++digit_idx) {
                const uint32_t digit_remain = problem.digits - digit_idx * digit_tile;
                const uint32_t digit_now = std::min<uint32_t>(digit_tile, digit_remain);

                uint64_t key_chunk_bytes = 0;
                if (!key_persistent) {
                    key_chunk_bytes =
                        static_cast<uint64_t>(limb_now) * digit_now * problem.key_digit_limb_bytes;
                    tracker.AcquireDynamicKey(key_chunk_bytes);
                    key_bytes += key_chunk_bytes;
                    key_transfer_count += 1;
                }

                const uint64_t dynamic_temp_bytes =
                    DynamicWorkingTempBytes(problem, ct_now, limb_now, digit_now);
                tracker.AcquireDynamicTemp(dynamic_temp_bytes);

                const uint64_t local_decompose =
                    static_cast<uint64_t>(ct_now)
                    * static_cast<uint64_t>(limb_now)
                    * static_cast<uint64_t>(digit_now)
                    * static_cast<uint64_t>(problem.poly_modulus_degree);
                decompose_work += local_decompose;
                ks_inner_work += local_decompose * static_cast<uint64_t>(problem.polys);
                accumulate_work += local_decompose;
                decompose_launch += 1;
                ks_inner_launch += 1;
                accumulate_launch += 1;

                tracker.ReleaseDynamicTemp(dynamic_temp_bytes);
                if (!key_persistent) {
                    tracker.ReleaseDynamicKey(key_chunk_bytes);
                }
            }

            tracker.ReleaseDynamicCt(ct_chunk_bytes);
        }

        basis_work +=
            static_cast<uint64_t>(ct_now)
            * static_cast<uint64_t>(problem.limbs)
            * static_cast<uint64_t>(problem.polys)
            * static_cast<uint64_t>(problem.poly_modulus_degree);
        basis_launch += 1;

        out_bytes_total += out_bytes;
        out_transfer_count += 1;
        tracker.ReleaseStaticAccum(out_bytes);
        tracker.ReleaseStaticTemp(static_temp_bytes);
    }

    if (key_persistent) {
        tracker.ReleasePersistentKey(problem.key_bytes);
    }

    if (tracker.overflowed()) {
        return eval;
    }

    eval.valid = true;
    eval.estimated_peak_bram_bytes = tracker.Peaks().total_peak_bytes;

    const double tile_overhead_cost =
        static_cast<double>(eval.total_tile_count) * static_cast<double>(params.per_tile_fixed_overhead_ns);
    const double key_transfer_cost = CostTransfer(
        key_bytes,
        key_transfer_count,
        params.hbm_to_bram_bw_bytes_per_ns,
        params.dma_setup_ns);
    const double ct_transfer_cost = CostTransfer(
        ct_bytes,
        ct_transfer_count,
        params.hbm_to_bram_bw_bytes_per_ns,
        params.dma_setup_ns);
    const double output_store_cost = CostTransfer(
        out_bytes_total,
        out_transfer_count,
        params.bram_to_hbm_bw_bytes_per_ns,
        params.dma_setup_ns);
    const double decompose_cost = CostCompute(
        decompose_work,
        decompose_launch,
        params.decompose_work_per_ns,
        params.kernel_launch_ns);
    const double ks_inner_cost = CostCompute(
        ks_inner_work,
        ks_inner_launch,
        params.multiply_work_per_ns,
        params.kernel_launch_ns);
    const double accumulate_cost = CostCompute(
        accumulate_work,
        accumulate_launch,
        params.multiply_work_per_ns,
        params.kernel_launch_ns);
    const double basis_cost = CostCompute(
        basis_work,
        basis_launch,
        params.basis_work_per_ns,
        params.kernel_launch_ns);

    eval.cost.tile_overhead_cost = tile_overhead_cost;
    eval.cost.key_transfer_cost = key_transfer_cost;
    eval.cost.ct_transfer_cost = ct_transfer_cost;
    eval.cost.output_store_cost = output_store_cost;
    eval.cost.decompose_compute_cost = decompose_cost;
    eval.cost.multiply_compute_cost = ks_inner_cost + accumulate_cost;
    eval.cost.basis_convert_cost = basis_cost;
    eval.cost.total_cost =
        params.w_tile_overhead * tile_overhead_cost
        + params.w_key_transfer * key_transfer_cost
        + params.w_ct_transfer * ct_transfer_cost
        + params.w_output_store * output_store_cost
        + params.w_decompose_compute * decompose_cost
        + params.w_multiply_compute * (ks_inner_cost + accumulate_cost)
        + params.w_basis_convert * basis_cost;

    return eval;
}

} // namespace

TilePlanner::TilePlanner()
    : params_(Params{}) {}

TilePlanner::TilePlanner(const Params& params)
    : params_(params) {}

TilePlan TilePlanner::Plan(const KeySwitchProblem& problem) const {
    TilePlan plan;
    if (!problem.valid) {
        return plan;
    }

    const uint64_t budget = problem.bram_budget_bytes;
    const uint64_t guard = problem.bram_guard_bytes;
    if (budget <= guard) {
        return plan;
    }

    TileCandidate best;
    CandidateEval best_eval;
    constexpr double kCostEps = 1e-6;
    const auto cost_less = [kCostEps](double lhs, double rhs) {
        return lhs + kCostEps < rhs;
    };
    const auto cost_equal = [kCostEps](double lhs, double rhs) {
        return std::abs(lhs - rhs) <= kCostEps;
    };
    const auto try_update_best = [&](
                                     bool key_persistent,
                                     uint32_t ct_tile,
                                     uint32_t limb_tile,
                                     uint32_t digit_tile) {
        const CandidateEval eval = EvaluateCandidate(
            problem,
            params_,
            ct_tile,
            limb_tile,
            digit_tile,
            key_persistent);
        if (!eval.valid) {
            return;
        }

        TileCandidate candidate;
        candidate.valid = true;
        candidate.key_persistent = key_persistent;
        candidate.ct_tile = ct_tile;
        candidate.limb_tile = limb_tile;
        candidate.digit_tile = digit_tile;
        candidate.score = static_cast<uint64_t>(ct_tile) * limb_tile * digit_tile;
        candidate.estimated_peak_bram_bytes = eval.estimated_peak_bram_bytes;
        candidate.estimated_total_cost = eval.cost.total_cost;

        bool choose = false;
        if (!best.valid) {
            choose = true;
        } else if (cost_less(candidate.estimated_total_cost, best.estimated_total_cost)) {
            choose = true;
        } else if (cost_equal(candidate.estimated_total_cost, best.estimated_total_cost)) {
            if (candidate.estimated_peak_bram_bytes < best.estimated_peak_bram_bytes) {
                choose = true;
            } else if (
                candidate.estimated_peak_bram_bytes == best.estimated_peak_bram_bytes
                && candidate.score > best.score) {
                choose = true;
            }
        }

        if (choose) {
            best = candidate;
            best_eval = eval;
        }
    };

    for (uint32_t ct_tile = 1; ct_tile <= problem.ciphertexts; ++ct_tile) {
        const uint64_t out_bytes =
            static_cast<uint64_t>(ct_tile) * problem.limbs * problem.out_limb_bytes;
        const uint64_t temp_bytes = TempBufferBytes(out_bytes, problem.temp_buffer_ratio);
        const uint64_t static_bytes = out_bytes + temp_bytes;
        if (static_bytes + guard >= budget) {
            continue;
        }

        const uint64_t free_budget = budget - static_bytes - guard;

        if (params_.allow_key_persistent && free_budget > problem.key_bytes) {
            const uint64_t ct_limb_denom = static_cast<uint64_t>(ct_tile) * problem.ct_limb_bytes;
            const uint32_t max_limb = static_cast<uint32_t>(
                std::min<uint64_t>(
                    problem.limbs,
                    (ct_limb_denom == 0)
                    ? 0
                    : ((free_budget - problem.key_bytes) / ct_limb_denom)));
            for (uint32_t limb_tile = 1; limb_tile <= max_limb; ++limb_tile) {
                try_update_best(
                    /*key_persistent=*/true,
                    ct_tile,
                    limb_tile,
                    problem.digits);
            }
        }

        for (uint32_t digit_tile = 1; digit_tile <= problem.digits; ++digit_tile) {
            const uint64_t denom =
                static_cast<uint64_t>(ct_tile) * problem.ct_limb_bytes
                + static_cast<uint64_t>(digit_tile) * problem.key_digit_limb_bytes;
            if (denom == 0) {
                continue;
            }

            const uint32_t max_limb = static_cast<uint32_t>(
                std::min<uint64_t>(problem.limbs, free_budget / denom));
            for (uint32_t limb_tile = 1; limb_tile <= max_limb; ++limb_tile) {
                try_update_best(
                    /*key_persistent=*/false,
                    ct_tile,
                    limb_tile,
                    digit_tile);
            }
        }
    }

    if (!best.valid) {
        return plan;
    }

    plan.valid = true;
    plan.key_persistent = best.key_persistent;
    plan.ct_tile = best.ct_tile;
    plan.limb_tile = best.limb_tile;
    plan.digit_tile = best.digit_tile;
    plan.ct_tiles = CeilDivU32(problem.ciphertexts, plan.ct_tile);
    plan.limb_tiles = CeilDivU32(problem.limbs, plan.limb_tile);
    plan.digit_tiles = CeilDivU32(problem.digits, plan.digit_tile);
    plan.total_tile_count = best_eval.total_tile_count;
    plan.estimated_peak_bram_bytes = best.estimated_peak_bram_bytes;
    plan.estimated_total_cost = best.estimated_total_cost;
    plan.cost = best_eval.cost;
    plan.per_tile_buffer_usage.reserve(plan.ct_tiles);
    for (uint32_t ct_idx = 0; ct_idx < plan.ct_tiles; ++ct_idx) {
        const uint32_t ct_remain = problem.ciphertexts - ct_idx * plan.ct_tile;
        const uint32_t ct_now = std::min<uint32_t>(plan.ct_tile, ct_remain);
        const uint64_t out_bytes =
            static_cast<uint64_t>(ct_now) * problem.limbs * problem.out_limb_bytes;
        const uint64_t static_temp_bytes = StaticTempBytes(problem, ct_now, out_bytes);

        TileBufferUsage entry;
        entry.ct_tile_index = ct_idx;
        entry.ct_count = ct_now;
        entry.persistent_buffers.key_buffer_bytes = plan.key_persistent ? problem.key_bytes : 0;
        entry.static_buffers.accumulation_buffer_bytes = out_bytes;
        entry.static_buffers.temp_working_buffer_bytes = static_temp_bytes;

        BufferBreakdown dynamic_peak{};
        for (uint32_t limb_idx = 0; limb_idx < plan.limb_tiles; ++limb_idx) {
            const uint32_t limb_remain = problem.limbs - limb_idx * plan.limb_tile;
            const uint32_t limb_now = std::min<uint32_t>(plan.limb_tile, limb_remain);
            const uint64_t ct_chunk_bytes =
                static_cast<uint64_t>(ct_now) * limb_now * problem.ct_limb_bytes;
            dynamic_peak.ciphertext_buffer_bytes = std::max<uint64_t>(
                dynamic_peak.ciphertext_buffer_bytes,
                ct_chunk_bytes);

            for (uint32_t digit_idx = 0; digit_idx < plan.digit_tiles; ++digit_idx) {
                const uint32_t digit_remain = problem.digits - digit_idx * plan.digit_tile;
                const uint32_t digit_now = std::min<uint32_t>(plan.digit_tile, digit_remain);
                const uint64_t key_chunk_bytes = plan.key_persistent
                    ? 0
                    : static_cast<uint64_t>(limb_now) * digit_now * problem.key_digit_limb_bytes;
                const uint64_t dyn_temp_bytes =
                    DynamicWorkingTempBytes(problem, ct_now, limb_now, digit_now);

                dynamic_peak.key_buffer_bytes = std::max<uint64_t>(
                    dynamic_peak.key_buffer_bytes,
                    key_chunk_bytes);
                dynamic_peak.temp_working_buffer_bytes = std::max<uint64_t>(
                    dynamic_peak.temp_working_buffer_bytes,
                    dyn_temp_bytes);
            }
        }

        entry.dynamic_peak_buffers = dynamic_peak;
        entry.persistent_bytes =
            entry.persistent_buffers.key_buffer_bytes
            + entry.persistent_buffers.ciphertext_buffer_bytes
            + entry.persistent_buffers.temp_working_buffer_bytes
            + entry.persistent_buffers.accumulation_buffer_bytes;
        entry.static_bytes =
            entry.static_buffers.key_buffer_bytes
            + entry.static_buffers.ciphertext_buffer_bytes
            + entry.static_buffers.temp_working_buffer_bytes
            + entry.static_buffers.accumulation_buffer_bytes;
        entry.dynamic_working_bytes =
            entry.dynamic_peak_buffers.key_buffer_bytes
            + entry.dynamic_peak_buffers.ciphertext_buffer_bytes
            + entry.dynamic_peak_buffers.temp_working_buffer_bytes
            + entry.dynamic_peak_buffers.accumulation_buffer_bytes;

        entry.key_bytes =
            entry.persistent_buffers.key_buffer_bytes
            + entry.dynamic_peak_buffers.key_buffer_bytes;
        entry.ct_bytes = entry.dynamic_peak_buffers.ciphertext_buffer_bytes;
        entry.out_bytes = out_bytes;
        entry.temp_bytes =
            entry.static_buffers.temp_working_buffer_bytes
            + entry.dynamic_peak_buffers.temp_working_buffer_bytes;
        entry.peak_live_bytes =
            entry.persistent_bytes + entry.static_bytes + entry.dynamic_working_bytes;

        plan.per_tile_buffer_usage.push_back(entry);
        plan.estimated_peak_bram_bytes =
            std::max<uint64_t>(plan.estimated_peak_bram_bytes, entry.peak_live_bytes);
    }

    return plan;
}

TransferModel::TransferModel()
    : params_(Params{}) {}

TransferModel::TransferModel(const Params& params)
    : params_(params) {}

Time TransferModel::EstimateLatency(TransferDirection direction, uint64_t bytes) const {
    uint64_t bw = 1;
    Time setup = 0;

    switch (direction) {
    case TransferDirection::HostToHBM:
        bw = std::max<uint64_t>(1, params_.host_to_hbm_bw_bytes_per_ns);
        setup = params_.host_to_hbm_setup_ns;
        break;
    case TransferDirection::HBMToBRAM:
        bw = std::max<uint64_t>(1, params_.hbm_to_bram_bw_bytes_per_ns);
        setup = params_.dma_setup_ns;
        break;
    case TransferDirection::BRAMToHBM:
        bw = std::max<uint64_t>(1, params_.bram_to_hbm_bw_bytes_per_ns);
        setup = params_.dma_setup_ns;
        break;
    }

    return setup + static_cast<Time>(CeilDivU64(bytes, bw));
}

double TransferModel::EstimateEnergyByBytes(uint64_t bytes) const {
    return static_cast<double>(bytes) * params_.energy_hbm_byte_nj;
}

KeySwitchExecutionModel::KeySwitchExecutionModel()
    : params_(KeySwitchExecutionModelParams{}),
      planner_(params_.tile_planner) {}

KeySwitchExecutionModel::KeySwitchExecutionModel(
    const KeySwitchExecutionModelParams& params)
    : params_(params),
      planner_(params.tile_planner) {}

bool KeySwitchExecutionModel::ResidentKeyHit(
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

KeySwitchProblem KeySwitchExecutionModel::BuildProblem(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    KeySwitchProblem problem;
    problem.working_set_bytes = req.ks_profile.input_bytes + req.ks_profile.key_bytes;
    problem.temp_buffer_ratio = params_.tile_planner.temp_buffer_ratio;
    problem.bram_guard_bytes = params_.tile_planner.bram_guard_bytes;

    if (plan.assigned_cards.empty()) {
        problem.valid = false;
        return problem;
    }

    const uint32_t assigned_cards =
        static_cast<uint32_t>(std::max<size_t>(1, plan.assigned_cards.size()));
    problem.method = ResolveKeySwitchMethodForAssignedCards(req.ks_profile.method, assigned_cards);
    problem.cards = (problem.method == KeySwitchMethod::SingleBoardClassic)
        ? 1
        : assigned_cards;

    if (problem.method == KeySwitchMethod::ScaleOutLimb) {
        problem.ciphertexts = std::max<uint32_t>(1, req.ks_profile.num_ciphertexts);
    } else {
        problem.ciphertexts = std::max<uint32_t>(1, req.ks_profile.num_ciphertexts);
    }
    problem.digits = std::max<uint32_t>(1, req.ks_profile.num_digits);
    if (problem.method == KeySwitchMethod::ScaleOutLimb) {
        problem.limbs = std::max<uint32_t>(
            1,
            static_cast<uint32_t>(CeilDivU64(req.ks_profile.num_rns_limbs, problem.cards)));
    } else {
        problem.limbs = std::max<uint32_t>(1, req.ks_profile.num_rns_limbs);
    }
    problem.polys = std::max<uint32_t>(1, req.ks_profile.num_polys);
    problem.poly_modulus_degree = std::max<uint32_t>(1, req.ks_profile.poly_modulus_degree);

    if (problem.method == KeySwitchMethod::ScaleOutLimb) {
        problem.input_bytes = std::max<uint64_t>(
            1,
            CeilDivU64(req.ks_profile.input_bytes, problem.cards));
        problem.output_bytes = std::max<uint64_t>(
            1,
            CeilDivU64(req.ks_profile.output_bytes, problem.cards));
        problem.key_bytes = std::max<uint64_t>(
            1,
            CeilDivU64(req.ks_profile.key_bytes, problem.cards));
    } else {
        problem.input_bytes = std::max<uint64_t>(1, req.ks_profile.input_bytes);
        problem.output_bytes = std::max<uint64_t>(1, req.ks_profile.output_bytes);
        problem.key_bytes = std::max<uint64_t>(1, req.ks_profile.key_bytes);
    }

    const uint64_t ct_limb_denom = static_cast<uint64_t>(problem.ciphertexts) * problem.limbs;
    const uint64_t out_limb_denom = static_cast<uint64_t>(problem.ciphertexts) * problem.limbs;
    const uint64_t key_denom = static_cast<uint64_t>(problem.digits) * problem.limbs;
    problem.ct_limb_bytes = std::max<uint64_t>(1, CeilDivU64(problem.input_bytes, ct_limb_denom));
    problem.out_limb_bytes = std::max<uint64_t>(1, CeilDivU64(problem.output_bytes, out_limb_denom));
    problem.key_digit_limb_bytes = std::max<uint64_t>(1, CeilDivU64(problem.key_bytes, key_denom));

    uint64_t min_bram_capacity = 0;
    const size_t card_limit = std::min<size_t>(plan.assigned_cards.size(), problem.cards);
    for (size_t idx = 0; idx < card_limit; ++idx) {
        const CardId card_id = plan.assigned_cards[idx];
        if (card_id >= state.cards.size()) {
            continue;
        }
        const uint64_t cap = state.cards[card_id].bram_capacity_bytes;
        if (cap == 0) {
            continue;
        }
        if (min_bram_capacity == 0 || cap < min_bram_capacity) {
            min_bram_capacity = cap;
        }
    }
    if (min_bram_capacity == 0) {
        min_bram_capacity = params_.default_bram_capacity_bytes;
    }
    problem.min_card_bram_capacity_bytes = min_bram_capacity;
    problem.bram_budget_bytes = std::max<uint64_t>(
        1,
        static_cast<uint64_t>(
            std::floor(static_cast<double>(min_bram_capacity) * params_.tile_planner.bram_usable_ratio)));

    bool key_hit = true;
    for (size_t idx = 0; idx < card_limit; ++idx) {
        const CardId card_id = plan.assigned_cards[idx];
        if (card_id >= state.cards.size()) {
            key_hit = false;
            break;
        }
        const auto& card = state.cards.at(card_id);
        if (!card.resident_user.has_value() || card.resident_user.value() != req.user_id) {
            key_hit = false;
            break;
        }
    }
    problem.key_resident_hit = key_hit;

    problem.valid = true;
    return problem;
}

KeySwitchExecution KeySwitchExecutionModel::BuildWithMode(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state,
    bool allow_inter_card_steps) const {

    KeySwitchExecution execution;
    execution.problem = BuildProblem(req, plan, state);
    execution.method = execution.problem.method;
    execution.working_set_bytes = execution.problem.working_set_bytes;
    execution.key_resident_hit = execution.problem.key_resident_hit;

    if (!execution.problem.valid) {
        execution.fallback_used = true;
        execution.fallback_reason = KeySwitchFallbackReason::NoAssignedCard;
        return execution;
    }

    execution.tile_plan = planner_.Plan(execution.problem);
    if (!execution.tile_plan.valid) {
        execution.fallback_used = true;
        execution.fallback_reason = KeySwitchFallbackReason::TilePlanInvalid;
        execution.tile_count = 1;
        execution.ct_hbm_to_bram_bytes = execution.problem.input_bytes;
        execution.out_bram_to_hbm_bytes = execution.problem.output_bytes;
        if (!execution.problem.key_resident_hit) {
            execution.key_host_to_hbm_bytes = execution.problem.key_bytes;
        }
        return execution;
    }

    execution.valid = true;
    execution.tiled_execution = true;
    execution.key_persistent_bram = execution.tile_plan.key_persistent;
    execution.tile_count = execution.tile_plan.total_tile_count;
    execution.tile_cost = execution.tile_plan.cost;

    const uint64_t occupancy_budget =
        (execution.problem.bram_budget_bytes > execution.problem.bram_guard_bytes)
        ? (execution.problem.bram_budget_bytes - execution.problem.bram_guard_bytes)
        : 0;
    BufferTracker tracker(occupancy_budget);

    if (!execution.problem.key_resident_hit) {
        TileExecutionStep step;
        step.type = TileExecutionStepType::KeyLoadHostToHBM;
        step.stage_type = StageType::KeyLoad;
        step.bytes = execution.problem.key_bytes;
        step.key_hit = false;
        step.key_persistent = false;
        step.before = tracker.Live();
        step.after = tracker.Live();
        execution.key_host_to_hbm_bytes += step.bytes;
        execution.steps.push_back(std::move(step));
    }

    if (execution.tile_plan.key_persistent) {
        TileExecutionStep step;
        step.type = TileExecutionStepType::KeyLoadHBMToBRAM;
        step.stage_type = StageType::KeyLoad;
        step.bytes = execution.problem.key_bytes;
        step.key_hit = true;
        step.key_persistent = true;
        step.before = tracker.Live();
        tracker.AcquirePersistentKey(step.bytes);
        step.after = tracker.Live();
        execution.key_hbm_to_bram_bytes += step.bytes;
        execution.steps.push_back(std::move(step));
    }

    for (uint32_t ct_idx = 0; ct_idx < execution.tile_plan.ct_tiles; ++ct_idx) {
        const uint32_t ct_remain =
            execution.problem.ciphertexts - ct_idx * execution.tile_plan.ct_tile;
        const uint32_t ct_now = std::min<uint32_t>(execution.tile_plan.ct_tile, ct_remain);
        const uint64_t out_bytes =
            static_cast<uint64_t>(ct_now) * execution.problem.limbs * execution.problem.out_limb_bytes;
        const uint64_t static_temp_bytes = StaticTempBytes(execution.problem, ct_now, out_bytes);
        tracker.AcquireStaticAccum(out_bytes);
        tracker.AcquireStaticTemp(static_temp_bytes);

        for (uint32_t limb_idx = 0; limb_idx < execution.tile_plan.limb_tiles; ++limb_idx) {
            const uint32_t limb_remain =
                execution.problem.limbs - limb_idx * execution.tile_plan.limb_tile;
            const uint32_t limb_now = std::min<uint32_t>(execution.tile_plan.limb_tile, limb_remain);
            const uint64_t ct_chunk_bytes =
                static_cast<uint64_t>(ct_now) * limb_now * execution.problem.ct_limb_bytes;

            TileExecutionStep input_step;
            input_step.type = TileExecutionStepType::InputHBMToBRAM;
            input_step.stage_type = StageType::Dispatch;
            input_step.ct_tile_index = ct_idx;
            input_step.limb_tile_index = limb_idx;
            input_step.bytes = ct_chunk_bytes;
            input_step.key_hit = execution.problem.key_resident_hit;
            input_step.key_persistent = execution.tile_plan.key_persistent;
            input_step.before = tracker.Live();
            tracker.AcquireDynamicCt(ct_chunk_bytes);
            input_step.after = tracker.Live();
            execution.ct_hbm_to_bram_bytes += ct_chunk_bytes;
            execution.steps.push_back(std::move(input_step));

            for (uint32_t digit_idx = 0; digit_idx < execution.tile_plan.digit_tiles; ++digit_idx) {
                const uint32_t digit_remain =
                    execution.problem.digits - digit_idx * execution.tile_plan.digit_tile;
                const uint32_t digit_now =
                    std::min<uint32_t>(execution.tile_plan.digit_tile, digit_remain);

                uint64_t key_chunk_bytes = 0;
                if (!execution.tile_plan.key_persistent) {
                    key_chunk_bytes =
                        static_cast<uint64_t>(limb_now)
                        * digit_now
                        * execution.problem.key_digit_limb_bytes;
                    TileExecutionStep key_step;
                    key_step.type = TileExecutionStepType::KeyLoadHBMToBRAM;
                    key_step.stage_type = StageType::KeyLoad;
                    key_step.ct_tile_index = ct_idx;
                    key_step.limb_tile_index = limb_idx;
                    key_step.digit_tile_index = digit_idx;
                    key_step.bytes = key_chunk_bytes;
                    key_step.key_hit = true;
                    key_step.key_persistent = false;
                    key_step.before = tracker.Live();
                    tracker.AcquireDynamicKey(key_chunk_bytes);
                    key_step.after = tracker.Live();
                    execution.key_hbm_to_bram_bytes += key_chunk_bytes;
                    execution.steps.push_back(std::move(key_step));
                }

                const uint64_t dynamic_temp_bytes = DynamicWorkingTempBytes(
                    execution.problem,
                    ct_now,
                    limb_now,
                    digit_now);
                tracker.AcquireDynamicTemp(dynamic_temp_bytes);

                const uint64_t decompose_work =
                    static_cast<uint64_t>(ct_now)
                    * static_cast<uint64_t>(digit_now)
                    * static_cast<uint64_t>(limb_now)
                    * static_cast<uint64_t>(execution.problem.poly_modulus_degree);
                const uint64_t multiply_work =
                    decompose_work * static_cast<uint64_t>(execution.problem.polys);
                const uint64_t transform_work = decompose_work;
                const uint64_t accumulate_work = decompose_work;

                TileExecutionStep decompose_step;
                decompose_step.type = TileExecutionStepType::DecomposeTile;
                decompose_step.stage_type = StageType::Decompose;
                decompose_step.ct_tile_index = ct_idx;
                decompose_step.limb_tile_index = limb_idx;
                decompose_step.digit_tile_index = digit_idx;
                decompose_step.work_items = decompose_work;
                decompose_step.key_hit = execution.problem.key_resident_hit;
                decompose_step.key_persistent = execution.tile_plan.key_persistent;
                decompose_step.before = tracker.Live();
                decompose_step.after = tracker.Live();
                execution.steps.push_back(std::move(decompose_step));

                // Keep NTT/INTT as explicit placeholders for future refined modeling.
                TileExecutionStep ntt_step;
                ntt_step.type = TileExecutionStepType::NttTile;
                ntt_step.stage_type = StageType::BasisConvert;
                ntt_step.ct_tile_index = ct_idx;
                ntt_step.limb_tile_index = limb_idx;
                ntt_step.digit_tile_index = digit_idx;
                ntt_step.work_items = transform_work;
                ntt_step.key_hit = execution.problem.key_resident_hit;
                ntt_step.key_persistent = execution.tile_plan.key_persistent;
                ntt_step.before = tracker.Live();
                ntt_step.after = tracker.Live();
                execution.steps.push_back(std::move(ntt_step));

                TileExecutionStep inner_prod_step;
                inner_prod_step.type = TileExecutionStepType::KSInnerProdTile;
                inner_prod_step.stage_type = StageType::Multiply;
                inner_prod_step.ct_tile_index = ct_idx;
                inner_prod_step.limb_tile_index = limb_idx;
                inner_prod_step.digit_tile_index = digit_idx;
                inner_prod_step.work_items = multiply_work;
                inner_prod_step.key_hit = execution.problem.key_resident_hit;
                inner_prod_step.key_persistent = execution.tile_plan.key_persistent;
                inner_prod_step.before = tracker.Live();
                inner_prod_step.after = tracker.Live();
                execution.steps.push_back(std::move(inner_prod_step));

                TileExecutionStep intt_step;
                intt_step.type = TileExecutionStepType::InttTile;
                intt_step.stage_type = StageType::BasisConvert;
                intt_step.ct_tile_index = ct_idx;
                intt_step.limb_tile_index = limb_idx;
                intt_step.digit_tile_index = digit_idx;
                intt_step.work_items = transform_work;
                intt_step.key_hit = execution.problem.key_resident_hit;
                intt_step.key_persistent = execution.tile_plan.key_persistent;
                intt_step.before = tracker.Live();
                intt_step.after = tracker.Live();
                execution.steps.push_back(std::move(intt_step));

                TileExecutionStep accumulate_step;
                accumulate_step.type = TileExecutionStepType::AccumulateSubtractTile;
                accumulate_step.stage_type = StageType::Multiply;
                accumulate_step.ct_tile_index = ct_idx;
                accumulate_step.limb_tile_index = limb_idx;
                accumulate_step.digit_tile_index = digit_idx;
                accumulate_step.work_items = accumulate_work;
                accumulate_step.key_hit = execution.problem.key_resident_hit;
                accumulate_step.key_persistent = execution.tile_plan.key_persistent;
                accumulate_step.before = tracker.Live();
                accumulate_step.after = tracker.Live();
                execution.steps.push_back(std::move(accumulate_step));

                tracker.ReleaseDynamicTemp(dynamic_temp_bytes);
                if (!execution.tile_plan.key_persistent) {
                    tracker.ReleaseDynamicKey(key_chunk_bytes);
                }
            }

            tracker.ReleaseDynamicCt(ct_chunk_bytes);
        }

        const uint64_t basis_work =
            static_cast<uint64_t>(ct_now)
            * static_cast<uint64_t>(execution.problem.limbs)
            * static_cast<uint64_t>(execution.problem.polys)
            * static_cast<uint64_t>(execution.problem.poly_modulus_degree);
        TileExecutionStep basis_step;
        basis_step.type = TileExecutionStepType::BasisConvertTile;
        basis_step.stage_type = StageType::BasisConvert;
        basis_step.ct_tile_index = ct_idx;
        basis_step.work_items = basis_work;
        basis_step.key_hit = execution.problem.key_resident_hit;
        basis_step.key_persistent = execution.tile_plan.key_persistent;
        basis_step.before = tracker.Live();
        basis_step.after = tracker.Live();
        execution.steps.push_back(std::move(basis_step));

        TileExecutionStep output_step;
        output_step.type = TileExecutionStepType::OutputBRAMToHBM;
        output_step.stage_type = StageType::Dispatch;
        output_step.ct_tile_index = ct_idx;
        output_step.bytes = out_bytes;
        output_step.key_hit = execution.problem.key_resident_hit;
        output_step.key_persistent = execution.tile_plan.key_persistent;
        output_step.before = tracker.Live();
        tracker.ReleaseStaticAccum(out_bytes);
        tracker.ReleaseStaticTemp(static_temp_bytes);
        output_step.after = tracker.Live();
        execution.out_bram_to_hbm_bytes += out_bytes;
        execution.steps.push_back(std::move(output_step));
    }

    if (execution.tile_plan.key_persistent) {
        tracker.ReleasePersistentKey(execution.problem.key_bytes);
    }

    if (allow_inter_card_steps && execution.problem.cards > 1) {
        const uint32_t active_cards = static_cast<uint32_t>(
            std::min<size_t>(
                plan.assigned_cards.size(),
                std::max<uint32_t>(1, execution.problem.cards)));
        if (active_cards > 1) {
            const CardId root_card = plan.assigned_cards.front();
            const uint64_t per_peer_bytes = CeilDivU64(
                req.ks_profile.output_bytes,
                std::max<uint32_t>(1, active_cards));
            const uint32_t sync_group = 1;

            for (uint32_t card_idx = 1; card_idx < active_cards; ++card_idx) {
                const CardId peer_card = plan.assigned_cards[card_idx];

                TileExecutionStep send_step;
                send_step.type = TileExecutionStepType::InterCardSendStep;
                send_step.stage_type = StageType::Merge;
                send_step.bytes = per_peer_bytes;
                send_step.work_items = 1;
                send_step.src_card = static_cast<int32_t>(peer_card);
                send_step.dst_card = static_cast<int32_t>(root_card);
                send_step.fan_in = 2;
                send_step.sync_group = sync_group;
                send_step.key_hit = execution.problem.key_resident_hit;
                send_step.key_persistent = execution.tile_plan.key_persistent;
                send_step.before = tracker.Live();
                send_step.after = tracker.Live();
                execution.steps.push_back(std::move(send_step));

                TileExecutionStep recv_step;
                recv_step.type = TileExecutionStepType::InterCardRecvStep;
                recv_step.stage_type = StageType::Merge;
                recv_step.bytes = per_peer_bytes;
                recv_step.work_items = 1;
                recv_step.src_card = static_cast<int32_t>(peer_card);
                recv_step.dst_card = static_cast<int32_t>(root_card);
                recv_step.fan_in = 2;
                recv_step.sync_group = sync_group;
                recv_step.key_hit = execution.problem.key_resident_hit;
                recv_step.key_persistent = execution.tile_plan.key_persistent;
                recv_step.before = tracker.Live();
                recv_step.after = tracker.Live();
                execution.steps.push_back(std::move(recv_step));
            }

            TileExecutionStep reduce_step;
            reduce_step.type = TileExecutionStepType::InterCardReduceStep;
            reduce_step.stage_type = StageType::Merge;
            reduce_step.bytes = req.ks_profile.output_bytes;
            reduce_step.work_items = active_cards;
            reduce_step.dst_card = static_cast<int32_t>(root_card);
            reduce_step.fan_in = active_cards;
            reduce_step.sync_group = sync_group;
            reduce_step.key_hit = execution.problem.key_resident_hit;
            reduce_step.key_persistent = execution.tile_plan.key_persistent;
            reduce_step.before = tracker.Live();
            reduce_step.after = tracker.Live();
            execution.steps.push_back(std::move(reduce_step));

            TileExecutionStep barrier_step;
            barrier_step.type = TileExecutionStepType::BarrierStep;
            barrier_step.stage_type = StageType::Merge;
            barrier_step.work_items = active_cards;
            barrier_step.fan_in = active_cards;
            barrier_step.sync_group = sync_group;
            barrier_step.barrier_group = sync_group;
            barrier_step.key_hit = execution.problem.key_resident_hit;
            barrier_step.key_persistent = execution.tile_plan.key_persistent;
            barrier_step.before = tracker.Live();
            barrier_step.after = tracker.Live();
            execution.steps.push_back(std::move(barrier_step));
        }
    }

    execution.peak_buffers = tracker.Peaks();
    execution.peak_bram_bytes = execution.peak_buffers.total_peak_bytes;

    if (tracker.overflowed()) {
        execution.valid = false;
        execution.tiled_execution = false;
        execution.fallback_used = true;
        execution.fallback_reason = KeySwitchFallbackReason::BrambudgetOverflow;
        execution.steps.clear();
        execution.tile_count = 1;
        execution.peak_bram_bytes = 0;
        execution.peak_buffers = PeakBufferUsage{};
        execution.key_hbm_to_bram_bytes = 0;
        execution.ct_hbm_to_bram_bytes = execution.problem.input_bytes;
        execution.out_bram_to_hbm_bytes = execution.problem.output_bytes;
        execution.key_host_to_hbm_bytes = execution.problem.key_resident_hit
            ? 0
            : execution.problem.key_bytes;
    }

    return execution;
}

KeySwitchExecution KeySwitchExecutionModel::Build(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state
) const {

    const uint32_t assigned_cards = static_cast<uint32_t>(std::max<size_t>(1, plan.assigned_cards.size()));
    const KeySwitchMethod resolved_method = ResolveKeySwitchMethodForAssignedCards(req.ks_profile.method, assigned_cards);

    KeySwitchExecution execution = BuildByMethod(req, plan, state, resolved_method);
    if (!execution.valid && execution.fallback_used
        && execution.fallback_reason == KeySwitchFallbackReason::UnsupportedMethod) {
        return execution;
    }

    execution.requested_method = req.ks_profile.method;
    if (execution.effective_method == KeySwitchMethod::Poseidon) {
        execution.effective_method = execution.method;
    }
    if (execution.method == KeySwitchMethod::Poseidon) {
        execution.method = execution.effective_method;
    }
    return execution;
}

KeySwitchExecution KeySwitchExecutionModel::BuildByMethod(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state,
    KeySwitchMethod resolved_method) const {

    switch (resolved_method) {
    case KeySwitchMethod::SingleBoardClassic:
        return BuildSingleBoard(req, plan, state);
    case KeySwitchMethod::ScaleOutLimb:
        return BuildScaleOutLimb(req, plan, state);

    case KeySwitchMethod::Poseidon:
        std::cout << "Not Implement" << std::endl;
        exit(0);
    case KeySwitchMethod::OLA:
        std::cout << "Not Implement" << std::endl;
        exit(0);
    case KeySwitchMethod::FAB:
        std::cout << "Not Implement" << std::endl;
        exit(0);
    case KeySwitchMethod::FAST:
        std::cout << "Not Implement" << std::endl;
        exit(0);
    case KeySwitchMethod::HERA:
        std::cout << "Not Implement" << std::endl;
        exit(0);
    case KeySwitchMethod::Cinnamon:
        std::cout << "Not Implement" << std::endl;
        exit(0);

    default:
        return BuildUnsupportedMethod(req.ks_profile.method, resolved_method);
    }
}

KeySwitchExecution KeySwitchExecutionModel::BuildUnsupportedMethod(
    KeySwitchMethod requested_method,
    KeySwitchMethod effective_method) const {

    KeySwitchExecution unsupported;
    unsupported.method = effective_method;
    unsupported.requested_method = requested_method;
    unsupported.effective_method = effective_method;
    unsupported.fallback_used = true;
    unsupported.fallback_reason = KeySwitchFallbackReason::UnsupportedMethod;
    unsupported.valid = false;
    unsupported.tiled_execution = false;
    return unsupported;
}

KeySwitchExecution KeySwitchExecutionModel::BuildSingleBoard(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    if (!req.ks_profile.allow_local_moddown) {
        return UnsupportedConfig(KeySwitchMethod::SingleBoardClassic);
    }

    const PartitionStrategy partition = ResolvePartition(req.ks_profile.partition, KeySwitchMethod::SingleBoardClassic);
    if (partition != PartitionStrategy::None) {
        return UnsupportedConfig(KeySwitchMethod::SingleBoardClassic);
    }

    const CollectiveStrategy collective =
        ResolveCollective(req.ks_profile.collective, KeySwitchMethod::SingleBoardClassic);
    if (collective != CollectiveStrategy::None) {
        return UnsupportedConfig(KeySwitchMethod::SingleBoardClassic);
    }

    const KeyPlacement key_placement =
        ResolveKeyPlacement(req.ks_profile.key_placement, KeySwitchMethod::SingleBoardClassic);
    if (key_placement != KeyPlacement::StreamFromHBM) {
        return UnsupportedConfig(KeySwitchMethod::SingleBoardClassic);
    }

    Request single_req = req;
    single_req.ks_profile.method = KeySwitchMethod::SingleBoardClassic;
    single_req.ks_profile.partition = partition;
    single_req.ks_profile.key_placement = key_placement;
    single_req.ks_profile.collective = collective;
    single_req.ks_profile.scale_out_cards = 1;
    single_req.ks_profile.enable_inter_card_merge = false;
    single_req.ks_profile.allow_cross_card_reduce = false;

    ExecutionPlan single_plan;
    single_plan.request_id = plan.request_id;
    if (!plan.assigned_cards.empty()) {
        single_plan.assigned_cards.push_back(plan.assigned_cards.front());
    }

    KeySwitchExecution execution = BuildWithMode(
        single_req,
        single_plan,
        state,
        /*allow_inter_card_steps=*/false
    );

    if (ContainsInterCardSteps(execution.steps)) {
        execution.steps.clear();
        execution.valid = false;
        execution.tiled_execution = false;
        execution.fallback_used = true;
        execution.fallback_reason = KeySwitchFallbackReason::UnsupportedConfig;
        execution.tile_count = 1;
    }

    execution.method = KeySwitchMethod::SingleBoardClassic;
    execution.requested_method = req.ks_profile.method;
    execution.effective_method = KeySwitchMethod::SingleBoardClassic;
    execution.method_degraded = false;
    execution.degraded_reason = KeySwitchFallbackReason::None;
    execution.problem.method = KeySwitchMethod::SingleBoardClassic;
    execution.problem.cards = 1;
    return execution;
}

KeySwitchExecution KeySwitchExecutionModel::BuildScaleOutLimb(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    if (plan.assigned_cards.empty()) {
        KeySwitchExecution no_card;
        no_card.method = KeySwitchMethod::ScaleOutLimb;
        no_card.requested_method = req.ks_profile.method;
        no_card.effective_method = KeySwitchMethod::ScaleOutLimb;
        no_card.fallback_used = true;
        no_card.fallback_reason = KeySwitchFallbackReason::NoAssignedCard;
        no_card.valid = false;
        no_card.tiled_execution = false;
        return no_card;
    }

    if (!req.ks_profile.allow_local_moddown) {
        return UnsupportedConfig(KeySwitchMethod::ScaleOutLimb);
    }

    const uint32_t effective_cards = EffectiveScaleOutCards(req, plan);
    if (effective_cards <= 1) {
        // Explicit policy: auto-degrade to single-board when only one card is available.
        Request single_req = req;
        single_req.ks_profile.partition = PartitionStrategy::None;
        single_req.ks_profile.collective = CollectiveStrategy::None;
        single_req.ks_profile.key_placement = KeyPlacement::StreamFromHBM;
        KeySwitchExecution degraded = BuildSingleBoard(single_req, plan, state);
        if (!degraded.fallback_used) {
            degraded.method_degraded = true;
            degraded.degraded_reason = KeySwitchFallbackReason::DegradedToSingleBoard;
            degraded.fallback_reason = KeySwitchFallbackReason::None;
        }
        degraded.requested_method = req.ks_profile.method;
        degraded.effective_method = KeySwitchMethod::SingleBoardClassic;
        return degraded;
    }

    const PartitionStrategy partition =
        ResolvePartition(req.ks_profile.partition, KeySwitchMethod::ScaleOutLimb);
    if (partition != PartitionStrategy::ByLimb) {
        return UnsupportedConfig(KeySwitchMethod::ScaleOutLimb);
    }

    const CollectiveStrategy collective =
        ResolveCollective(req.ks_profile.collective, KeySwitchMethod::ScaleOutLimb);
    if (collective != CollectiveStrategy::GatherToRoot) {
        return UnsupportedConfig(KeySwitchMethod::ScaleOutLimb);
    }

    const KeyPlacement key_placement =
        ResolveKeyPlacement(req.ks_profile.key_placement, KeySwitchMethod::ScaleOutLimb);
    // ScaleOutLimb MVP: only replicated key placement is supported in this path.
    if (key_placement != KeyPlacement::ReplicatedPerCard) {
        return UnsupportedConfig(KeySwitchMethod::ScaleOutLimb);
    }

    if (!req.ks_profile.enable_inter_card_merge) {
        return UnsupportedConfig(KeySwitchMethod::ScaleOutLimb);
    }
    if (!req.ks_profile.allow_cross_card_reduce) {
        return UnsupportedConfig(KeySwitchMethod::ScaleOutLimb);
    }

    ExecutionPlan scale_plan = plan;
    if (scale_plan.assigned_cards.size() > effective_cards) {
        scale_plan.assigned_cards.resize(effective_cards);
    }

    Request scale_req = req;
    scale_req.ks_profile.method = KeySwitchMethod::ScaleOutLimb;
    scale_req.ks_profile.partition = partition;
    scale_req.ks_profile.key_placement = key_placement;
    scale_req.ks_profile.collective = collective;
    scale_req.ks_profile.scale_out_cards = effective_cards;

    KeySwitchExecution execution;
    execution.valid = true;
    execution.tiled_execution = true;
    execution.method = KeySwitchMethod::ScaleOutLimb;
    execution.requested_method = req.ks_profile.method;
    execution.effective_method = KeySwitchMethod::ScaleOutLimb;
    execution.method_degraded = false;
    execution.degraded_reason = KeySwitchFallbackReason::None;
    execution.problem = BuildProblem(scale_req, scale_plan, state);
    execution.problem.method = KeySwitchMethod::ScaleOutLimb;
    execution.problem.cards = effective_cards;
    execution.working_set_bytes = 0;
    execution.key_resident_hit = true;
    execution.key_persistent_bram = true;

    const uint32_t total_limbs = std::max<uint32_t>(1, req.ks_profile.num_rns_limbs);
    const std::vector<LimbShard> shards = BuildLimbShards(total_limbs, effective_cards);
    if (shards.empty()) {
        execution.fallback_used = true;
        execution.fallback_reason = KeySwitchFallbackReason::TilePlanInvalid;
        execution.valid = false;
        return execution;
    }

    std::vector<uint64_t> partial_bytes(shards.size(), 0);
    bool local_plan_initialized = false;

    for (uint32_t card_idx = 0; card_idx < static_cast<uint32_t>(shards.size()); ++card_idx) {
        const LimbShard& shard = shards[card_idx];
        if (shard.count == 0) {
            continue;
        }

        Request local_req = req;
        local_req.ks_profile.method = KeySwitchMethod::SingleBoardClassic;
        local_req.ks_profile.partition = PartitionStrategy::None;
        local_req.ks_profile.collective = CollectiveStrategy::None;
        local_req.ks_profile.key_placement = KeyPlacement::StreamFromHBM;
        local_req.ks_profile.scale_out_cards = 1;
        local_req.ks_profile.enable_inter_card_merge = false;
        local_req.ks_profile.allow_cross_card_reduce = false;

        local_req.ks_profile.num_rns_limbs = shard.count;
        local_req.ks_profile.input_bytes = ScaleBytesByRatio(
            req.ks_profile.input_bytes,
            shard.count,
            total_limbs);
        local_req.ks_profile.output_bytes = ScaleBytesByRatio(
            req.ks_profile.output_bytes,
            shard.count,
            total_limbs);
        // Replicated-per-card key placement for MVP.
        local_req.ks_profile.key_bytes = req.ks_profile.key_bytes;

        ExecutionPlan local_plan;
        local_plan.request_id = plan.request_id;
        local_plan.assigned_cards.push_back(scale_plan.assigned_cards[card_idx]);

        const KeySwitchExecution local = BuildSingleBoard(local_req, local_plan, state);
        if (!local.valid) {
            // Do not fallback to generic BuildWithMode() path.
            // Keep ScaleOutLimb semantics explicit and explainable.
            KeySwitchExecution failed = UnsupportedConfig(KeySwitchMethod::ScaleOutLimb);
            failed.requested_method = req.ks_profile.method;
            failed.effective_method = KeySwitchMethod::ScaleOutLimb;
            failed.problem = execution.problem;
            failed.problem.method = KeySwitchMethod::ScaleOutLimb;
            failed.problem.cards = effective_cards;
            failed.fallback_reason = (local.fallback_reason == KeySwitchFallbackReason::None)
                ? KeySwitchFallbackReason::UnsupportedConfig
                : local.fallback_reason;
            return failed;
        }

        execution.key_resident_hit = execution.key_resident_hit && local.key_resident_hit;
        execution.key_persistent_bram = execution.key_persistent_bram && local.key_persistent_bram;
        execution.working_set_bytes += local.working_set_bytes;
        execution.tile_count = SaturateU32(
            static_cast<uint64_t>(execution.tile_count) + local.tile_count);

        execution.key_host_to_hbm_bytes += local.key_host_to_hbm_bytes;
        execution.key_hbm_to_bram_bytes += local.key_hbm_to_bram_bytes;
        execution.ct_hbm_to_bram_bytes += local.ct_hbm_to_bram_bytes;

        execution.peak_bram_bytes = std::max<uint64_t>(execution.peak_bram_bytes, local.peak_bram_bytes);
        MergePeakBuffers(&execution.peak_buffers, local.peak_buffers);

        execution.tile_cost.tile_overhead_cost += local.tile_cost.tile_overhead_cost;
        execution.tile_cost.key_transfer_cost += local.tile_cost.key_transfer_cost;
        execution.tile_cost.ct_transfer_cost += local.tile_cost.ct_transfer_cost;
        execution.tile_cost.output_store_cost += local.tile_cost.output_store_cost;
        execution.tile_cost.decompose_compute_cost += local.tile_cost.decompose_compute_cost;
        execution.tile_cost.multiply_compute_cost += local.tile_cost.multiply_compute_cost;
        execution.tile_cost.basis_convert_cost += local.tile_cost.basis_convert_cost;
        execution.tile_cost.total_cost += local.tile_cost.total_cost;

        if (!local_plan_initialized) {
            execution.tile_plan = local.tile_plan;
            local_plan_initialized = true;
        }

        uint64_t local_partial_bytes = 0;
        const CardId local_card = local_plan.assigned_cards.front();
        for (const TileExecutionStep& step : local.steps) {
            if (step.type == TileExecutionStepType::OutputBRAMToHBM) {
                local_partial_bytes += step.bytes;
                continue;
            }
            TileExecutionStep local_step = step;
            // Tag local shard steps with owning card so CycleBackend can emit
            // per-card local primitives for ScaleOutLimb traces.
            local_step.src_card = static_cast<int32_t>(local_card);
            local_step.dst_card = static_cast<int32_t>(local_card);
            execution.steps.push_back(std::move(local_step));
        }
        if (local_partial_bytes == 0) {
            local_partial_bytes = local.out_bram_to_hbm_bytes;
        }
        if (local_partial_bytes == 0) {
            local_partial_bytes = local_req.ks_profile.output_bytes;
        }
        partial_bytes[card_idx] = local_partial_bytes;
    }

    const uint32_t active_cards = static_cast<uint32_t>(shards.size());
    execution.problem.cards = active_cards;
    if (active_cards > 1) {
        const CardId root_card = scale_plan.assigned_cards.front();
        const uint32_t sync_group = 1;
        uint64_t reduce_bytes = partial_bytes[0];
        for (uint32_t card_idx = 1; card_idx < active_cards; ++card_idx) {
            const uint64_t bytes = partial_bytes[card_idx];
            const CardId peer_card = scale_plan.assigned_cards[card_idx];

            TileExecutionStep send_step;
            send_step.type = TileExecutionStepType::InterCardSendStep;
            send_step.stage_type = StageType::Merge;
            send_step.bytes = bytes;
            send_step.work_items = 1;
            send_step.src_card = static_cast<int32_t>(peer_card);
            send_step.dst_card = static_cast<int32_t>(root_card);
            send_step.fan_in = 2;
            send_step.sync_group = sync_group;
            send_step.key_hit = execution.key_resident_hit;
            send_step.key_persistent = execution.key_persistent_bram;
            execution.steps.push_back(std::move(send_step));

            TileExecutionStep recv_step;
            recv_step.type = TileExecutionStepType::InterCardRecvStep;
            recv_step.stage_type = StageType::Merge;
            recv_step.bytes = bytes;
            recv_step.work_items = 1;
            recv_step.src_card = static_cast<int32_t>(peer_card);
            recv_step.dst_card = static_cast<int32_t>(root_card);
            recv_step.fan_in = 2;
            recv_step.sync_group = sync_group;
            recv_step.key_hit = execution.key_resident_hit;
            recv_step.key_persistent = execution.key_persistent_bram;
            execution.steps.push_back(std::move(recv_step));

            reduce_bytes += bytes;
        }

        TileExecutionStep reduce_step;
        reduce_step.type = TileExecutionStepType::InterCardReduceStep;
        reduce_step.stage_type = StageType::Merge;
        reduce_step.bytes = reduce_bytes;
        reduce_step.work_items = active_cards;
        reduce_step.dst_card = static_cast<int32_t>(root_card);
        reduce_step.fan_in = active_cards;
        reduce_step.sync_group = sync_group;
        reduce_step.key_hit = execution.key_resident_hit;
        reduce_step.key_persistent = execution.key_persistent_bram;
        execution.steps.push_back(std::move(reduce_step));

        TileExecutionStep barrier_step;
        barrier_step.type = TileExecutionStepType::BarrierStep;
        barrier_step.stage_type = StageType::Merge;
        barrier_step.work_items = active_cards;
        barrier_step.fan_in = active_cards;
        barrier_step.sync_group = sync_group;
        barrier_step.barrier_group = sync_group;
        barrier_step.key_hit = execution.key_resident_hit;
        barrier_step.key_persistent = execution.key_persistent_bram;
        execution.steps.push_back(std::move(barrier_step));
    }

    TileExecutionStep output_step;
    output_step.type = TileExecutionStepType::OutputBRAMToHBM;
    output_step.stage_type = StageType::Dispatch;
    output_step.bytes = req.ks_profile.output_bytes;
    output_step.work_items = 1;
    output_step.key_hit = execution.key_resident_hit;
    output_step.key_persistent = execution.key_persistent_bram;
    execution.steps.push_back(std::move(output_step));
    execution.out_bram_to_hbm_bytes = req.ks_profile.output_bytes;
    execution.problem.key_resident_hit = execution.key_resident_hit;

    return execution;
}
