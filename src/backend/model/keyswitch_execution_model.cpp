#include "backend/model/keyswitch_execution_model.h"
#include "backend/runtime_planner.h"
#include "model/keyswitch_method_resolver.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <limits>
#include <ostream>
#include <sstream>
#include <iostream>

namespace {

uint64_t CeilDivU64(uint64_t a, uint64_t b) {
    return (b == 0) ? 0 : (a + b - 1) / b;
}

uint32_t CeilDivU32(uint32_t a, uint32_t b) {
    return (b == 0) ? 0 : (a + b - 1) / b;
}

uint32_t KeyExtraLimbsForTile(
    uint32_t limb_idx,
    uint32_t limb_tile_size,
    uint32_t total_limbs,
    uint32_t num_k) {
    const uint32_t safe_limbs = std::max<uint32_t>(1, total_limbs);
    const uint32_t begin = std::min<uint32_t>(
        safe_limbs,
        static_cast<uint32_t>(static_cast<uint64_t>(limb_idx) * limb_tile_size));
    const uint32_t end = std::min<uint32_t>(safe_limbs, begin + limb_tile_size);
    const uint32_t k_before = static_cast<uint32_t>(
        (static_cast<uint64_t>(begin) * num_k) / safe_limbs);
    const uint32_t k_after = static_cast<uint32_t>(
        (static_cast<uint64_t>(end) * num_k) / safe_limbs);
    return (k_after >= k_before) ? (k_after - k_before) : 0;
}

uint32_t SaturateU32(uint64_t value) {
    return static_cast<uint32_t>(std::min<uint64_t>(value, std::numeric_limits<uint32_t>::max()));
}

constexpr KeySwitchMethod kSingleBoardBaseMethod = KeySwitchMethod::Poseidon;

PartitionStrategy ResolvePartition(
    PartitionStrategy requested,
    KeySwitchMethod method) {

    if (requested != PartitionStrategy::Auto) {
        return requested;
    }
    return (method == KeySwitchMethod::Cinnamon)
        ? PartitionStrategy::ByLimb
        : PartitionStrategy::None;
}

CollectiveStrategy ResolveCollective(
    CollectiveStrategy requested,
    KeySwitchMethod method) {

    if (requested != CollectiveStrategy::Auto) {
        return requested;
    }
    return (method == KeySwitchMethod::Cinnamon)
        ? CollectiveStrategy::GatherToRoot
        : CollectiveStrategy::None;
}

KeyPlacement ResolveKeyPlacement(
    KeyPlacement requested,
    KeySwitchMethod method) {

    if (requested != KeyPlacement::Auto) {
        return requested;
    }
    return (method == KeySwitchMethod::Cinnamon)
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

bool IsSharedSingleBoardMethod(KeySwitchMethod method) {
    switch (method) {
    case KeySwitchMethod::Poseidon:
    case KeySwitchMethod::OLA:
    case KeySwitchMethod::FAB:
    case KeySwitchMethod::FAST:
    case KeySwitchMethod::HERA:
    case KeySwitchMethod::DigitCentric:
    case KeySwitchMethod::OutputCentric:
    case KeySwitchMethod::MaxParallel:
        return true;
    default:
        return false;
    }
}

IntermediateStorageLevel StorageForConnection(
    StageConnectionMode mode,
    IntermediateStorageLevel fallback_level) {

    switch (mode) {
    case StageConnectionMode::DirectForward:
        return fallback_level;
    case StageConnectionMode::BufferInBRAM:
        return IntermediateStorageLevel::BRAM;
    case StageConnectionMode::SpillToHBM:
        return IntermediateStorageLevel::HBM;
    }
    return fallback_level;
}

std::vector<uint32_t> IterationOrder(uint32_t count) {
    std::vector<uint32_t> order;
    order.reserve(count);
    for (uint32_t idx = 0; idx < count; ++idx) {
        order.push_back(idx);
    }
    return order;
}

std::vector<StageConnectionMode> FallbackOrderForConnection(
    StageConnectionMode preferred) {

    std::vector<StageConnectionMode> order = {preferred};
    const std::array<StageConnectionMode, 3> defaults = {
        StageConnectionMode::DirectForward,
        StageConnectionMode::BufferInBRAM,
        StageConnectionMode::SpillToHBM,
    };
    for (StageConnectionMode mode : defaults) {
        if (std::find(order.begin(), order.end(), mode) == order.end()) {
            order.push_back(mode);
        }
    }
    return order;
}

void AddDependencyId(
    std::vector<uint64_t>* deps,
    uint64_t step_id) {

    if (step_id == 0) {
        return;
    }
    if (std::find(deps->begin(), deps->end(), step_id) == deps->end()) {
        deps->push_back(step_id);
    }
}

template <typename T>
void AppendCommaSeparated(
    const std::vector<T>& values,
    std::ostream& os,
    const char* (*formatter)(T)) {

    for (std::size_t idx = 0; idx < values.size(); ++idx) {
        if (idx != 0) {
            os << ", ";
        }
        os << formatter(values[idx]);
    }
}

void AppendCommaSeparatedU64(
    const std::vector<uint64_t>& values,
    std::ostream& os) {

    for (std::size_t idx = 0; idx < values.size(); ++idx) {
        if (idx != 0) {
            os << ", ";
        }
        os << values[idx];
    }
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
    uint32_t limb_now,
    uint64_t out_bytes) {

    const uint64_t ratio_bytes = TempBufferBytes(out_bytes, problem.temp_buffer_ratio);
    const uint64_t dep_bytes =
        static_cast<uint64_t>(ct_now)
        * static_cast<uint64_t>(limb_now)
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

struct CandidateStats {
    // 传输流量与次数（用于 CostTransfer）。
    uint64_t key_bytes = 0;
    uint64_t ct_bytes = 0;
    uint64_t out_bytes_total = 0;
    uint64_t key_transfer_count = 0;
    uint64_t ct_transfer_count = 0;
    uint64_t out_transfer_count = 0;

    // 计算工作量与 launch 次数（用于 CostCompute）。
    uint64_t decompose_work = 0;
    uint64_t ks_inner_work = 0;
    uint64_t accumulate_work = 0;
    uint64_t basis_work = 0;
    uint64_t decompose_launch = 0;
    uint64_t ks_inner_launch = 0;
    uint64_t accumulate_launch = 0;
    uint64_t basis_launch = 0;
};

// 计算某一维度在第 index 个 tile 上的实际覆盖长度。
// 作用：统一处理尾块（最后一个 tile 可能小于 tile_size）的边界逻辑。
uint32_t TileExtent(uint32_t index, uint32_t tile_size, uint32_t total_size) {
    const uint32_t remain = total_size - index * tile_size;
    return std::min<uint32_t>(tile_size, remain);
}

// 模拟单个 (ct_tile, limb_tile, digit_tile) 组合在 digit 维度的一次执行：
// 1) 处理 key 分块加载（仅非常驻模式）；
// 2) 申请/释放动态 temp；
// 3) 累加 decompose/inner/accumulate 工作量与 launch 次数。
void SimulateDigitTile(
    const KeySwitchProblem& problem,
    bool key_persistent,
    uint32_t limb_idx,
    uint32_t limb_tile,
    uint32_t ct_now,
    uint32_t limb_now,
    uint32_t digit_now,
    BufferTracker* tracker,
    CandidateStats* stats) {

    uint64_t key_chunk_bytes = 0;
    if (!key_persistent) {
        // 非常驻模式：每个 (limb, digit) 组合都需要一次 key 分块加载。
        const uint32_t key_limb_now = limb_now + KeyExtraLimbsForTile(
            limb_idx,
            limb_tile,
            problem.limbs,
            problem.num_k);
        key_chunk_bytes =
            static_cast<uint64_t>(key_limb_now) * digit_now * problem.key_digit_limb_bytes;
        tracker->AcquireDynamicKey(key_chunk_bytes);
        stats->key_bytes += key_chunk_bytes;
        stats->key_transfer_count += 1;
    }

    // 动态 temp 工作区：估算该组合执行中的临时峰值占用。
    const uint64_t dynamic_temp_bytes =
        DynamicWorkingTempBytes(problem, ct_now, limb_now, digit_now);
    tracker->AcquireDynamicTemp(dynamic_temp_bytes);

    // 工作量模型：local_decompose 作为基准，派生 inner/accumulate 的计算量与 launch 数。
    const uint64_t local_decompose =
        static_cast<uint64_t>(ct_now)
        * static_cast<uint64_t>(limb_now)
        * static_cast<uint64_t>(digit_now)
        * static_cast<uint64_t>(problem.poly_modulus_degree);
    stats->decompose_work += local_decompose;
    stats->ks_inner_work += local_decompose * static_cast<uint64_t>(problem.polys);
    stats->accumulate_work += local_decompose;
    stats->decompose_launch += 1;
    stats->ks_inner_launch += 1;
    stats->accumulate_launch += 1;

    // 该 (ct,limb,digit) 组合结束后释放动态占用。
    tracker->ReleaseDynamicTemp(dynamic_temp_bytes);
    if (!key_persistent) {
        tracker->ReleaseDynamicKey(key_chunk_bytes);
    }
}

// 基于累计统计量构建候选成本分解，并计算最终 total_cost。
// 该函数只做“统计量 -> 成本”的映射，不参与可行性判断。
TileCostBreakdown BuildCandidateCost(
    const TilePlanner::Params& params,
    uint32_t total_tile_count,
    const CandidateStats& stats) {

    TileCostBreakdown cost;
    const double tile_overhead_cost =
        static_cast<double>(total_tile_count) * static_cast<double>(params.per_tile_fixed_overhead_ns);
    const double key_transfer_cost = CostTransfer(
        stats.key_bytes,
        stats.key_transfer_count,
        params.hbm_to_bram_bw_bytes_per_ns,
        params.dma_setup_ns);
    const double ct_transfer_cost = CostTransfer(
        stats.ct_bytes,
        stats.ct_transfer_count,
        params.hbm_to_bram_bw_bytes_per_ns,
        params.dma_setup_ns);
    const double output_store_cost = CostTransfer(
        stats.out_bytes_total,
        stats.out_transfer_count,
        params.bram_to_hbm_bw_bytes_per_ns,
        params.dma_setup_ns);
    const double decompose_cost = CostCompute(
        stats.decompose_work,
        stats.decompose_launch,
        params.decompose_work_per_ns,
        params.kernel_launch_ns);
    const double ks_inner_cost = CostCompute(
        stats.ks_inner_work,
        stats.ks_inner_launch,
        params.multiply_work_per_ns,
        params.kernel_launch_ns);
    const double accumulate_cost = CostCompute(
        stats.accumulate_work,
        stats.accumulate_launch,
        params.multiply_work_per_ns,
        params.kernel_launch_ns);
    const double basis_cost = CostCompute(
        stats.basis_work,
        stats.basis_launch,
        params.basis_work_per_ns,
        params.kernel_launch_ns);

    // 回填分项，便于诊断 planner 选型原因。
    cost.tile_overhead_cost = tile_overhead_cost;
    cost.key_transfer_cost = key_transfer_cost;
    cost.ct_transfer_cost = ct_transfer_cost;
    cost.output_store_cost = output_store_cost;
    cost.decompose_compute_cost = decompose_cost;
    cost.multiply_compute_cost = ks_inner_cost + accumulate_cost;
    cost.basis_convert_cost = basis_cost;
    // 总成本 = 分项成本按权重线性组合。
    cost.total_cost =
        params.w_tile_overhead * tile_overhead_cost
        + params.w_key_transfer * key_transfer_cost
        + params.w_ct_transfer * ct_transfer_cost
        + params.w_output_store * output_store_cost
        + params.w_decompose_compute * decompose_cost
        + params.w_multiply_compute * (ks_inner_cost + accumulate_cost)
        + params.w_basis_convert * basis_cost;
    return cost;
}

// 评估一个 tile 候选是否可行，并估算峰值 BRAM 与成本分解。
// 返回 valid=false 表示该候选超预算或维度非法，不可执行。
CandidateEval EvaluateCandidate(
    const KeySwitchProblem& problem,
    const TilePlanner::Params& params,
    uint32_t ct_tile,
    uint32_t limb_tile,
    uint32_t digit_tile,
    bool key_persistent
) {

    // 作用：
    // 评估一个 tile 候选 (ct_tile, limb_tile, digit_tile, key_persistent) 是否可行，
    // 并在可行时给出该候选的峰值 BRAM 占用与分项成本。
    CandidateEval eval;

    // 基础合法性检查：tile 维度不能为 0，也不能超过问题规模上界。
    if (ct_tile == 0 || limb_tile == 0 || digit_tile == 0) {
        return eval;
    }
    if (ct_tile > problem.ciphertexts || limb_tile > problem.limbs || digit_tile > problem.digits) {
        return eval;
    }

    // 根据 tile 大小推导三维 tile 个数与总 tile 数。
    eval.ct_tiles = CeilDivU32(problem.ciphertexts, ct_tile);
    eval.limb_tiles = CeilDivU32(problem.limbs, limb_tile);
    eval.digit_tiles = CeilDivU32(problem.digits, digit_tile);
    eval.total_tile_count = SaturateU32(
            static_cast<uint64_t>(eval.ct_tiles)
            * static_cast<uint64_t>(eval.limb_tiles)
            * static_cast<uint64_t>(eval.digit_tiles)
        );

    // 有效容量预算 = bram_budget - guard（保留保护带，避免贴边溢出）。
    const uint64_t occupancy_budget =
        (problem.bram_budget_bytes > problem.bram_guard_bytes)
        ? (problem.bram_budget_bytes - problem.bram_guard_bytes)
        : 0;
    // BufferTracker 用于在“模拟执行过程中”追踪 persistent/static/dynamic 占用峰值。
    BufferTracker tracker(occupancy_budget);

    CandidateStats stats;

    // key 常驻模式：先一次性加载并占用 persistent key。
    // 后续内层循环不再重复加载 key 分块。
    if (key_persistent) {
        tracker.AcquirePersistentKey(problem.key_bytes);
        stats.key_bytes += problem.key_bytes;
        stats.key_transfer_count += 1;
    }

    // 三维遍历所有 tile 组合，模拟该候选的资源占用与工作量。
    for (uint32_t ct_idx = 0; ct_idx < eval.ct_tiles; ++ct_idx) {
        const uint32_t ct_now = TileExtent(ct_idx, ct_tile, problem.ciphertexts);

        for (uint32_t limb_idx = 0; limb_idx < eval.limb_tiles; ++limb_idx) {
            const uint32_t limb_now = TileExtent(limb_idx, limb_tile, problem.limbs);

            // 当前 (ct, limb) 下的输出静态缓冲、静态 temp 与输入分块字节。
            const uint64_t out_bytes =
                static_cast<uint64_t>(ct_now) * limb_now * problem.out_limb_bytes;
            const uint64_t static_temp_bytes =
                StaticTempBytes(problem, ct_now, limb_now, out_bytes);
            const uint64_t ct_chunk_bytes =
                static_cast<uint64_t>(ct_now) * limb_now * problem.ct_limb_bytes;

            // 进入该层时先占用静态/动态资源。
            tracker.AcquireStaticAccum(out_bytes);
            tracker.AcquireStaticTemp(static_temp_bytes);
            tracker.AcquireDynamicCt(ct_chunk_bytes);
            stats.ct_bytes += ct_chunk_bytes;
            stats.ct_transfer_count += 1;

            for (uint32_t digit_idx = 0; digit_idx < eval.digit_tiles; ++digit_idx) {
                const uint32_t digit_now = TileExtent(digit_idx, digit_tile, problem.digits);
                SimulateDigitTile(
                    problem,
                    key_persistent,
                    limb_idx,
                    limb_tile,
                    ct_now,
                    limb_now,
                    digit_now,
                    &tracker,
                    &stats);
            }

            // basis 阶段按 (ct, limb) 维度累计一次。
            stats.basis_work +=
                static_cast<uint64_t>(ct_now)
                * static_cast<uint64_t>(limb_now)
                * static_cast<uint64_t>(problem.polys)
                * static_cast<uint64_t>(problem.poly_modulus_degree);
            stats.basis_launch += 1;

            // 记录输出回写流量并释放本层 (ct,limb) 占用。
            stats.out_bytes_total += out_bytes;
            stats.out_transfer_count += 1;
            tracker.ReleaseDynamicCt(ct_chunk_bytes);
            tracker.ReleaseStaticAccum(out_bytes);
            tracker.ReleaseStaticTemp(static_temp_bytes);
        }
    }

    // 常驻 key 在评估结束时释放，恢复 tracker 状态闭环。
    if (key_persistent) {
        tracker.ReleasePersistentKey(problem.key_bytes);
    }

    // 超预算（任意时刻溢出）即判定候选不可行。
    if (tracker.overflowed()) {
        return eval;
    }

    // 可行候选：回填有效标记与估算峰值占用。
    eval.valid = true;
    eval.estimated_peak_bram_bytes = tracker.Peaks().total_peak_bytes;
    eval.cost = BuildCandidateCost(params, eval.total_tile_count, stats);

    return eval;
}

// 用一个新候选尝试更新当前最优解。
// 选择优先级：
// 1) total_cost 更小；
// 2) total_cost 近似相等时，peak_bram 更小；
// 3) 若仍相等，score 更大（偏好更粗粒度 tile）。
void TryUpdateBestCandidate(
    const KeySwitchProblem& problem,
    const TilePlanner::Params& params,
    bool key_persistent,
    uint32_t ct_tile,
    uint32_t limb_tile,
    uint32_t digit_tile,
    TileCandidate* best,
    CandidateEval* best_eval) {

    // EvaluateCandidate 会检查容量约束与成本模型；
    // invalid 候选（超预算/不可执行）直接丢弃。
    const CandidateEval eval = EvaluateCandidate(
        problem,
        params,
        ct_tile,
        limb_tile,
        digit_tile,
        key_persistent);
    if (!eval.valid) {
        return;
    }

    // 将评估结果转成可比较的候选摘要。
    TileCandidate candidate;
    candidate.valid = true;
    candidate.key_persistent = key_persistent;
    candidate.ct_tile = ct_tile;
    candidate.limb_tile = limb_tile;
    candidate.digit_tile = digit_tile;
    candidate.score = static_cast<uint64_t>(ct_tile) * limb_tile * digit_tile;
    candidate.estimated_peak_bram_bytes = eval.estimated_peak_bram_bytes;
    candidate.estimated_total_cost = eval.cost.total_cost;

    // 浮点比较容差，避免 total_cost 因数值误差导致抖动。
    constexpr double kCostEps = 1e-6;
    const auto cost_less = [](double lhs, double rhs) {
        return lhs + kCostEps < rhs;
    };
    const auto cost_equal = [](double lhs, double rhs) {
        return std::abs(lhs - rhs) <= kCostEps;
    };

    // 选择规则（按优先级）：
    // 1) total_cost 更小优先；
    // 2) 若成本近似相等，peak_bram 更小优先；
    // 3) 若仍相等，score（tile 覆盖体积）更大优先，倾向更粗粒度分块。
    bool choose = false;
    if (!best->valid) {
        choose = true;
    } else if (cost_less(candidate.estimated_total_cost, best->estimated_total_cost)) {
        choose = true;
    } else if (cost_equal(candidate.estimated_total_cost, best->estimated_total_cost)) {
        if (candidate.estimated_peak_bram_bytes < best->estimated_peak_bram_bytes) {
            choose = true;
        } else if (
            candidate.estimated_peak_bram_bytes == best->estimated_peak_bram_bytes
            && candidate.score > best->score) {
            choose = true;
        }
    }

    if (choose) {
        *best = candidate;
        *best_eval = eval;
    }
}

} // namespace

const char* ToString(LogicalNodeKind kind) {
    switch (kind) {
    case LogicalNodeKind::Input:
        return "Input";
    case LogicalNodeKind::KeySource:
        return "KeySource";
    case LogicalNodeKind::ModUp:
        return "ModUp";
    case LogicalNodeKind::InnerProd:
        return "InnerProd";
    case LogicalNodeKind::Reduction:
        return "Reduction";
    case LogicalNodeKind::ModDown:
        return "ModDown";
    case LogicalNodeKind::Output:
        return "Output";
    }
    return "Unknown";
}

const char* ToString(LogicalTensorRole role) {
    switch (role) {
    case LogicalTensorRole::None:
        return "None";
    case LogicalTensorRole::CiphertextTile:
        return "CiphertextTile";
    case LogicalTensorRole::KeyTile:
        return "KeyTile";
    case LogicalTensorRole::AccumTile:
        return "AccumTile";
    case LogicalTensorRole::TempTile:
        return "TempTile";
    }
    return "Unknown";
}

const char* ToString(StageConnectionMode mode) {
    switch (mode) {
    case StageConnectionMode::DirectForward:
        return "DirectForward";
    case StageConnectionMode::BufferInBRAM:
        return "BufferInBRAM";
    case StageConnectionMode::SpillToHBM:
        return "SpillToHBM";
    }
    return "Unknown";
}

const char* ToString(StageType stage_type) {
    switch (stage_type) {
    case StageType::KeyLoad:
        return "KeyLoad";
    case StageType::Dispatch:
        return "Dispatch";
    case StageType::Decompose:
        return "Decompose";
    case StageType::Multiply:
        return "Multiply";
    case StageType::BasisConvert:
        return "BasisConvert";
    case StageType::Merge:
        return "Merge";
    }
    return "Unknown";
}

void DumpLogicalGraph(const LogicalGraph& graph, std::ostream& os) {
    os << "LogicalGraph(valid=" << (graph.valid ? "true" : "false")
       << ", nodes=" << graph.nodes.size() << ")\n";
    for (const LogicalNode& node : graph.nodes) {
        std::vector<StageConnectionMode> fallback_display;
        for (const StageConnectionMode mode : node.edge_policy.fallback_order) {
            bool duplicate = false;
            for (const StageConnectionMode existing : fallback_display) {
                if (std::string(ToString(existing)) == ToString(mode)) {
                    duplicate = true;
                    break;
                }
            }
            if (!duplicate) {
                fallback_display.push_back(mode);
            }
        }
        os << "  [" << node.node_id << "] "
           << ToString(node.kind)
           << " stage=" << ToString(node.stage_type)
           << " output=" << ToString(node.produced_output)
           << " deps=[";
        AppendCommaSeparatedU64(node.depends_on, os);
        os << "] inputs=[";
        AppendCommaSeparated(node.required_inputs, os, ToString);
        os << "] preferred=" << ToString(node.edge_policy.preferred_connection)
           << " fallback=[";
        AppendCommaSeparated(fallback_display, os, ToString);
        os << "] shortcut=" << (node.edge_policy.allow_shortcut ? "true" : "false")
           << " key_persistent="
           << (node.edge_policy.allow_key_persistent ? "true" : "false")
           << "\n";
    }
}

std::string FormatLogicalGraph(const LogicalGraph& graph) {
    std::ostringstream os;
    DumpLogicalGraph(graph, os);
    return os.str();
}

TilePlanner::TilePlanner()
    : params_(Params{}) {}

TilePlanner::TilePlanner(const Params& params)
    : params_(params) {}

TilePlan TilePlanner::Plan(const KeySwitchProblem& problem) const {
    // 作用：
    // 在给定问题规模与 BRAM 预算约束下，遍历 tile 候选并选出“可执行且代价最低”的方案，
    // 同时回填该方案的分项成本与每个 ct_tile 的缓冲占用明细。
    TilePlan plan;
    // 输入问题无效时直接返回 invalid plan（调用方据此走 fallback）。
    if (!problem.valid) {
        return plan;
    }

    // 预算保护：可用预算必须大于 guard，否则说明几乎没有可分配空间，不进行搜索。
    const uint64_t budget = problem.bram_budget_bytes;
    const uint64_t guard = problem.bram_guard_bytes;
    if (budget <= guard) {
        return plan;
    }

    // best / best_eval 用于记录当前最优候选及其详细评估结果。
    TileCandidate best;
    CandidateEval best_eval;

    // 搜索空间：
    // - ct_tile:   [1, ciphertexts]
    // - limb_tile: [1, limbs]
    // - digit_tile:[1, digits]
    // 另外若允许 key 常驻，单独评估一类“digit_tile=digits 且 key_persistent=true”的候选。
    for (uint32_t ct_tile = 1; ct_tile <= problem.ciphertexts; ++ct_tile) {
        for (uint32_t limb_tile = 1; limb_tile <= problem.limbs; ++limb_tile) {
            if (params_.allow_key_persistent) {
                TryUpdateBestCandidate(
                    problem,
                    params_,
                    /*key_persistent=*/true,
                    ct_tile,
                    limb_tile,
                    problem.digits,
                    &best,
                    &best_eval);
            }

            for (uint32_t digit_tile = 1; digit_tile <= problem.digits; ++digit_tile) {
                TryUpdateBestCandidate(
                    problem,
                    params_,
                    /*key_persistent=*/false,
                    ct_tile,
                    limb_tile,
                    digit_tile,
                    &best,
                    &best_eval);
            }
        }
    }

    // 无任何可行候选，返回 invalid plan，交由上游 fallback。
    if (!best.valid) {
        return plan;
    }

    // 将最优候选落地为最终 plan，并计算各维 tile 数。
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
    // 预留每个 ct_tile 一条统计记录，减少 push_back 期间扩容。
    plan.per_tile_buffer_usage.reserve(plan.ct_tiles);
    for (uint32_t ct_idx = 0; ct_idx < plan.ct_tiles; ++ct_idx) {
        // 当前 ct_tile 实际覆盖 ct 数（尾块可能小于 ct_tile）。
        const uint32_t ct_remain = problem.ciphertexts - ct_idx * plan.ct_tile;
        const uint32_t ct_now = std::min<uint32_t>(plan.ct_tile, ct_remain);

        // 逐 ct_tile 回填缓冲明细：
        // persistent：长生命周期（如常驻 key）
        // static：     tile 级静态开销（如累计缓冲）
        // dynamic：    执行期峰值工作集（key/ct/temp 动态峰值）
        TileBufferUsage entry;
        entry.ct_tile_index = ct_idx;
        entry.ct_count = ct_now;
        entry.persistent_buffers.key_buffer_bytes = plan.key_persistent ? problem.key_bytes : 0;

        BufferBreakdown dynamic_peak{};
        uint64_t static_out_peak = 0;
        uint64_t static_temp_peak = 0;
        uint64_t total_out_bytes = 0;
        for (uint32_t limb_idx = 0; limb_idx < plan.limb_tiles; ++limb_idx) {
            // 先按 limb_tile 估算输入/输出与静态临时区峰值。
            const uint32_t limb_remain = problem.limbs - limb_idx * plan.limb_tile;
            const uint32_t limb_now = std::min<uint32_t>(plan.limb_tile, limb_remain);
            const uint64_t ct_chunk_bytes =
                static_cast<uint64_t>(ct_now) * limb_now * problem.ct_limb_bytes;
            const uint64_t out_chunk_bytes =
                static_cast<uint64_t>(ct_now) * limb_now * problem.out_limb_bytes;
            const uint64_t static_temp_bytes =
                StaticTempBytes(problem, ct_now, limb_now, out_chunk_bytes);
            static_out_peak = std::max<uint64_t>(static_out_peak, out_chunk_bytes);
            static_temp_peak = std::max<uint64_t>(static_temp_peak, static_temp_bytes);
            total_out_bytes += out_chunk_bytes;
            dynamic_peak.ciphertext_buffer_bytes = std::max<uint64_t>(
                dynamic_peak.ciphertext_buffer_bytes,
                ct_chunk_bytes);

            for (uint32_t digit_idx = 0; digit_idx < plan.digit_tiles; ++digit_idx) {
                // 再按 digit_tile 估算 key 分块和动态 temp 峰值；
                // 若 key 常驻，则当前组合不再额外占用动态 key 块。
                const uint32_t digit_remain = problem.digits - digit_idx * plan.digit_tile;
                const uint32_t digit_now = std::min<uint32_t>(plan.digit_tile, digit_remain);
                const uint32_t key_limb_now = limb_now + KeyExtraLimbsForTile(
                    limb_idx,
                    plan.limb_tile,
                    problem.limbs,
                    problem.num_k);
                const uint64_t key_chunk_bytes = plan.key_persistent
                    ? 0
                    : static_cast<uint64_t>(key_limb_now) * digit_now * problem.key_digit_limb_bytes;
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

        // 汇总该 ct_tile 的三类占用与语义分类统计。
        entry.static_buffers.accumulation_buffer_bytes = static_out_peak;
        entry.static_buffers.temp_working_buffer_bytes = static_temp_peak;
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
        entry.out_bytes = total_out_bytes;
        entry.temp_bytes =
            entry.static_buffers.temp_working_buffer_bytes
            + entry.dynamic_peak_buffers.temp_working_buffer_bytes;
        entry.peak_live_bytes =
            entry.persistent_bytes + entry.static_bytes + entry.dynamic_working_bytes;

        plan.per_tile_buffer_usage.push_back(entry);
        // 计划峰值取所有 ct_tile 峰值的最大值。
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

TilePlanner::Params KeySwitchExecutionModel::TilePlannerParams() const {
    return params_.tile_planner;
}

LogicalGraph KeySwitchExecutionModel::BuildSharedSingleBoardLogicalGraph(
    const KeySwitchMethodPolicy& policy
) const {

    LogicalGraph graph;
    graph.valid = true;
    graph.nodes.reserve(7);

    auto append_node = [&](LogicalNodeKind kind,
                           StageType stage_type,
                           std::vector<uint64_t> deps,
                           std::vector<LogicalTensorRole> inputs,
                           LogicalTensorRole output,
                           StageConnectionMode preferred_connection,
                           bool allow_shortcut,
                           bool allow_key_persistent) {
        LogicalNode node;
        node.node_id = static_cast<uint64_t>(graph.nodes.size() + 1);
        node.kind = kind;
        node.stage_type = stage_type;
        node.depends_on = std::move(deps);
        node.required_inputs = std::move(inputs);
        node.produced_output = output;
        node.edge_policy.preferred_connection = preferred_connection;
        node.edge_policy.fallback_order = FallbackOrderForConnection(preferred_connection);
        node.edge_policy.allow_shortcut = allow_shortcut;
        node.edge_policy.allow_key_persistent = allow_key_persistent;
        graph.nodes.push_back(std::move(node));
        return graph.nodes.back().node_id;
    };

    const uint64_t input_id = append_node(
        LogicalNodeKind::Input,
        StageType::Dispatch,
        {},
        {},
        LogicalTensorRole::CiphertextTile,
        StageConnectionMode::BufferInBRAM,
        false,
        false);
    const uint64_t key_id = append_node(
        LogicalNodeKind::KeySource,
        StageType::KeyLoad,
        {},
        {},
        LogicalTensorRole::KeyTile,
        StageConnectionMode::BufferInBRAM,
        false,
        true);
    const uint64_t modup_id = append_node(
        LogicalNodeKind::ModUp,
        StageType::BasisConvert,
        {input_id},
        {LogicalTensorRole::CiphertextTile},
        LogicalTensorRole::TempTile,
        policy.modup_to_innerprod,
        false,
        false);
    const uint64_t inner_id = append_node(
        LogicalNodeKind::InnerProd,
        StageType::Multiply,
        {modup_id, key_id},
        {LogicalTensorRole::TempTile, LogicalTensorRole::KeyTile},
        LogicalTensorRole::AccumTile,
        policy.innerprod_to_reduction,
        false,
        policy.key_pref_storage != IntermediateStorageLevel::HBM);
    const uint64_t reduction_id = append_node(
        LogicalNodeKind::Reduction,
        StageType::Multiply,
        {inner_id},
        {LogicalTensorRole::AccumTile},
        LogicalTensorRole::AccumTile,
        policy.reduction_to_moddown,
        policy.supports_moddown_shortcut,
        false);
    const uint64_t moddown_id = append_node(
        LogicalNodeKind::ModDown,
        StageType::BasisConvert,
        {reduction_id},
        {LogicalTensorRole::AccumTile},
        LogicalTensorRole::TempTile,
        policy.moddown_to_subtract,
        policy.supports_moddown_shortcut,
        false);
    append_node(
        LogicalNodeKind::Output,
        StageType::Dispatch,
        {moddown_id},
        {LogicalTensorRole::TempTile},
        LogicalTensorRole::None,
        StageConnectionMode::BufferInBRAM,
        false,
        false);

    return graph;
}

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

namespace {

void InitializeProblemBase(
    const Request& req,
    const TilePlanner::Params& planner_params,
    KeySwitchProblem* problem) {

    problem->working_set_bytes = req.ks_profile.input_bytes + req.ks_profile.key_bytes;
    problem->temp_buffer_ratio = planner_params.temp_buffer_ratio;
    problem->bram_guard_bytes = planner_params.bram_guard_bytes;
}

void ResolveProblemMethodAndCards(
    const Request& req,
    const ExecutionPlan& plan,
    KeySwitchProblem* problem) {

    const uint32_t assigned_cards = static_cast<uint32_t>(
        std::max<size_t>(1, plan.assigned_cards.size()));
    problem->method = ResolveKeySwitchMethodForAssignedCards(
        req.ks_profile.method,
        assigned_cards);
    problem->cards = (problem->method == KeySwitchMethod::Cinnamon) ? assigned_cards : 1;
}

void NormalizeProblemDimensions(
    const Request& req,
    KeySwitchProblem* problem) {

    if (problem->method == KeySwitchMethod::Cinnamon) {
        // Xiangchen: raise not implement and exit
        std::cerr << "Cinnamon method is not implemented yet." << std::endl;
        std::exit(1);
    } else {
        problem->ciphertexts = std::max<uint32_t>(1, req.ks_profile.num_ciphertexts);
        problem->limbs = std::max<uint32_t>(1, req.ks_profile.num_rns_limbs);
        problem->digits = std::max<uint32_t>(1, req.ks_profile.num_digits);
        problem->num_k = CeilDivU32(problem->limbs + 1, problem->digits);
        problem->digit_limbs = problem->num_k;
        problem->key_limbs = problem->limbs + problem->num_k;
        problem->polys = std::max<uint32_t>(1, req.ks_profile.num_polys);
        problem->poly_modulus_degree = std::max<uint32_t>(1, req.ks_profile.poly_modulus_degree);
    }

}

void NormalizeProblemBytes(
    const Request& req,
    KeySwitchProblem* problem) {

    if (problem->method == KeySwitchMethod::Cinnamon) {
        std::cerr << "Cinnamon method is not implemented yet." << std::endl;
        std::exit(1);
    } else {
        problem->input_bytes = std::max<uint64_t>(1, req.ks_profile.input_bytes);
        problem->output_bytes = std::max<uint64_t>(1, req.ks_profile.output_bytes);
        problem->key_bytes = std::max<uint64_t>(1, req.ks_profile.key_bytes);
    }
}

void DeriveProblemGranularityBytes(
    KeySwitchProblem* problem) {

    const uint64_t ct_limb_denom = static_cast<uint64_t>(problem->ciphertexts) * problem->limbs;
    const uint64_t out_limb_denom = static_cast<uint64_t>(problem->ciphertexts) * problem->limbs;
    // evalkey has 2 items
    const uint64_t key_denom = static_cast<uint64_t>(problem->digits) * (problem->key_limbs) * 2;
    // limb_bytes need to be divide by 2
    // Input bytes consider two polynomials
    problem->ct_limb_bytes = std::max<uint64_t>(1, CeilDivU64(CeilDivU64(problem->input_bytes, 2), ct_limb_denom));
    problem->out_limb_bytes = std::max<uint64_t>(1, CeilDivU64(CeilDivU64(problem->output_bytes, 2), out_limb_denom));
    problem->key_digit_limb_bytes = std::max<uint64_t>(1, CeilDivU64(problem->key_bytes, key_denom));
}

size_t EffectiveProblemCardLimit(
    const ExecutionPlan& plan,
    const KeySwitchProblem& problem) {

    return std::min<size_t>(plan.assigned_cards.size(), problem.cards);
}

uint64_t ResolveMinCardBramCapacity(
    const ExecutionPlan& plan,
    const SystemState& state,
    size_t card_limit,
    uint64_t default_bram_capacity_bytes) {

    uint64_t min_bram_capacity = 0;
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
        min_bram_capacity = default_bram_capacity_bytes;
    }
    return min_bram_capacity;
}

uint64_t ComputeBramBudgetBytes(
    uint64_t min_bram_capacity,
    double bram_usable_ratio) {

    return std::max<uint64_t>(
        1,
        static_cast<uint64_t>(
            std::floor(static_cast<double>(min_bram_capacity) * bram_usable_ratio)));
}

bool ResolveProblemResidentKeyHit(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state,
    size_t card_limit) {

    for (size_t idx = 0; idx < card_limit; ++idx) {
        const CardId card_id = plan.assigned_cards[idx];
        if (card_id >= state.cards.size()) {
            return false;
        }
        const auto& card = state.cards.at(card_id);
        if (!card.resident_user.has_value() || card.resident_user.value() != req.user_id) {
            return false;
        }
    }
    return true;
}

} // namespace

KeySwitchProblem KeySwitchExecutionModel::BuildProblem(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state
) const {

    KeySwitchProblem problem;
    InitializeProblemBase(req, params_.tile_planner, &problem);

    if (plan.assigned_cards.empty()) {
        problem.valid = false;
        return problem;
    }

    ResolveProblemMethodAndCards(req, plan, &problem);
    NormalizeProblemDimensions(req, &problem);
    NormalizeProblemBytes(req, &problem);
    DeriveProblemGranularityBytes(&problem);

    const size_t card_limit = EffectiveProblemCardLimit(plan, problem);
    const uint64_t min_bram_capacity = ResolveMinCardBramCapacity(
        plan,
        state,
        card_limit,
        params_.default_bram_capacity_bytes
    );
    problem.min_card_bram_capacity_bytes = min_bram_capacity;
    problem.bram_budget_bytes = ComputeBramBudgetBytes(min_bram_capacity, params_.tile_planner.bram_usable_ratio);
    problem.key_resident_hit = ResolveProblemResidentKeyHit(
        req,
        plan,
        state,
        card_limit);
    problem.valid = true;
    return problem;
}


void KeySwitchExecutionModel::AddDependency(
    BuildContext* ctx,
    uint64_t step_id,
    uint64_t dep_step_id
) const {

    for (auto& step : ctx->execution->steps) {
        if (step.step_id == step_id) {
            if (std::find(step.depends_on.begin(), step.depends_on.end(), dep_step_id) == step.depends_on.end()) {
                step.depends_on.push_back(dep_step_id);
            }
            return;
        }
    }
}

void KeySwitchExecutionModel::AddDependencies(
    BuildContext* ctx,
    uint64_t step_id,
    const std::vector<uint64_t>& dep_step_ids
) const {
    for (uint64_t dep_id : dep_step_ids) {
        AddDependency(ctx, step_id, dep_id);
    }
}

void KeySwitchExecutionModel::MarkSubgraphSteps(
    KeySwitchExecution* execution,
    const std::vector<uint64_t>& step_ids,
    StageType coarse_stage
) const {

    for (auto& step : execution->steps) {
        if (std::find(step_ids.begin(), step_ids.end(), step.step_id) != step_ids.end()) {
            step.stage_type = coarse_stage;
        }
    }
}


TileExecutionStep KeySwitchExecutionModel::MakeStep(
    TileExecutionStepType type,
    StageType stage_type,
    uint32_t ct_idx,
    uint32_t limb_idx,
    uint32_t digit_idx,
    uint64_t input_bytes,
    uint64_t output_bytes,
    uint64_t work_items,
    IntermediateStorageLevel input_storage,
    IntermediateStorageLevel output_storage
) const {

    TileExecutionStep step;
    step.type = type;
    step.stage_type = stage_type;

    step.ct_idx = ct_idx;
    step.limb_idx = limb_idx;
    step.digit_idx = digit_idx;
    step.tile_idx = ct_idx;

    step.ct_tile_index = ct_idx;
    step.limb_tile_index = limb_idx;
    step.digit_tile_index = digit_idx;

    step.input_bytes = input_bytes;
    step.output_bytes = output_bytes;
    step.bytes = (output_bytes > input_bytes) ? output_bytes : input_bytes;
    step.work_items = work_items;

    step.input_storage = input_storage;
    step.output_storage = output_storage;

    step.src_card = -1;
    step.dst_card = -1;
    step.fan_in = 1;
    step.sync_group = 0;
    step.barrier_group = 0;

    step.key_hit = false;
    step.key_persistent = false;
    step.fused_with_prev = false;
    step.fused_with_next = false;
    step.is_shortcut_path = false;

    return step;
}

uint64_t KeySwitchExecutionModel::AppendStep(
    BuildContext* ctx,
    TileExecutionStep step
) const {
    step.step_id = ctx->next_step_id++;
    if (step.fused_with_next) {
        step.materialize_output = false;
    }
    if (step.input_buffer_ids.empty()) {
        step.input_buffer_ids = step.depends_on;
    }
    step.output_buffer_id = step.step_id;

    // 若调用方未显式设置 step.bytes，则回退为 input/output 较大者，
    // 让通用统计字段始终有可用值（便于统一展示与后续估算）。
    step.bytes = (step.bytes == 0) ? std::max(step.input_bytes, step.output_bytes) : step.bytes;

    // 写入执行序列；使用 move 避免不必要拷贝。
    ctx->execution->steps.push_back(std::move(step));
    return ctx->execution->steps.back().step_id;
}

std::vector<uint64_t> KeySwitchExecutionModel::BuildModUpSubgraph(
    BuildContext* ctx,
    uint32_t ct_idx,
    uint32_t limb_idx,
    uint32_t digit_idx,
    uint64_t ct_chunk_bytes,
    uint64_t work_items,
    const KeySwitchMethodPolicy& policy,
    const std::vector<uint64_t>& deps) const {

    std::vector<uint64_t> ids;
    TileExecutionStep intt;
    intt.type = TileExecutionStepType::ModUpInttTile;
    intt.stage_type = StageType::BasisConvert;
    intt.ct_tile_index = ct_idx;
    intt.limb_tile_index = limb_idx;
    intt.digit_tile_index = digit_idx;
    intt.tile_idx = ct_idx;
    intt.limb_idx = limb_idx;
    intt.digit_idx = digit_idx;
    intt.work_items = work_items;
    intt.input_bytes = ct_chunk_bytes;
    intt.output_bytes = ct_chunk_bytes;
    intt.input_storage = IntermediateStorageLevel::BRAM;
    intt.output_storage = policy.fuse_modup_chain
        ? IntermediateStorageLevel::BRAM
        : IntermediateStorageLevel::BRAM;
    intt.fused_with_next = policy.fuse_modup_chain;
    intt.depends_on = deps;
    ids.push_back(AppendStep(ctx, intt));
    ctx->execution->modup_step_ids.push_back(ids.back());

    TileExecutionStep bconv;
    bconv.type = TileExecutionStepType::ModUpBConvTile;
    bconv.stage_type = StageType::BasisConvert;
    bconv.ct_tile_index = ct_idx;
    bconv.limb_tile_index = limb_idx;
    bconv.digit_tile_index = digit_idx;
    bconv.tile_idx = ct_idx;
    bconv.limb_idx = limb_idx;
    bconv.digit_idx = digit_idx;
    bconv.work_items = work_items;
    bconv.input_bytes = ct_chunk_bytes;
    bconv.output_bytes = ct_chunk_bytes;
    bconv.input_storage = intt.output_storage;
    bconv.output_storage = policy.fuse_modup_chain
        ? IntermediateStorageLevel::BRAM
        : IntermediateStorageLevel::BRAM;
    bconv.fused_with_prev = policy.fuse_modup_chain;
    bconv.fused_with_next = policy.fuse_modup_chain;
    bconv.depends_on = {ids.back()};
    ids.push_back(AppendStep(ctx, bconv));
    ctx->execution->modup_step_ids.push_back(ids.back());

    TileExecutionStep ntt;
    ntt.type = TileExecutionStepType::ModUpNttTile;
    ntt.stage_type = StageType::BasisConvert;
    ntt.ct_tile_index = ct_idx;
    ntt.limb_tile_index = limb_idx;
    ntt.digit_tile_index = digit_idx;
    ntt.tile_idx = ct_idx;
    ntt.limb_idx = limb_idx;
    ntt.digit_idx = digit_idx;
    ntt.work_items = work_items;
    ntt.input_bytes = ct_chunk_bytes;
    ntt.output_bytes = ct_chunk_bytes;
    ntt.input_storage = bconv.output_storage;
    ntt.output_storage = policy.modup_output_storage;
    ntt.fused_with_prev = policy.fuse_modup_chain;
    ntt.fused_with_next =
        (policy.modup_to_innerprod == StageConnectionMode::DirectForward);
    ntt.depends_on = {ids.back()};
    ids.push_back(AppendStep(ctx, ntt));
    ctx->execution->modup_step_ids.push_back(ids.back());
    return ids;
}

std::vector<uint64_t> KeySwitchExecutionModel::BuildInnerProdSubgraph(
    BuildContext* ctx,
    uint32_t ct_idx,
    uint32_t limb_idx,
    uint32_t digit_idx,
    uint64_t key_chunk_bytes,
    uint64_t work_items,
    const KeySwitchMethodPolicy& policy,
    const std::vector<uint64_t>& deps) const {

    std::vector<uint64_t> ids;
    TileExecutionStep inner;
    inner.type = TileExecutionStepType::KSInnerProdTile;
    inner.stage_type = StageType::Multiply;
    inner.ct_tile_index = ct_idx;
    inner.limb_tile_index = limb_idx;
    inner.digit_tile_index = digit_idx;
    inner.tile_idx = ct_idx;
    inner.limb_idx = limb_idx;
    inner.digit_idx = digit_idx;
    inner.work_items = work_items;
    inner.input_bytes = key_chunk_bytes;
    inner.output_bytes = key_chunk_bytes;
    inner.input_storage = StorageForConnection(
        policy.modup_to_innerprod,
        policy.modup_output_storage);
    inner.output_storage = policy.innerprod_output_storage;
    inner.fused_with_prev = policy.fuse_cross_stage;
    inner.depends_on = deps;
    ids.push_back(AppendStep(ctx, inner));
    ctx->execution->innerprod_step_ids.push_back(ids.back());
    return ids;
}

std::vector<uint64_t> KeySwitchExecutionModel::BuildReductionSubgraph(
    BuildContext* ctx,
    uint32_t ct_idx,
    uint32_t limb_idx,
    uint64_t work_items,
    const KeySwitchMethodPolicy& policy,
    const std::vector<uint64_t>& deps) const {

    std::vector<uint64_t> ids;
    TileExecutionStep reduce;
    reduce.type = TileExecutionStepType::CrossDigitReduceTile;
    reduce.stage_type = StageType::Multiply;
    reduce.ct_tile_index = ct_idx;
    reduce.limb_tile_index = limb_idx;
    reduce.tile_idx = ct_idx;
    reduce.limb_idx = limb_idx;
    reduce.work_items = work_items;
    reduce.input_bytes = std::max<uint64_t>(1, work_items);
    reduce.output_bytes = std::max<uint64_t>(1, work_items);
    reduce.input_storage = StorageForConnection(
        policy.innerprod_to_reduction,
        policy.innerprod_output_storage);
    reduce.output_storage = policy.reduction_output_storage;
    reduce.depends_on = deps;
    ids.push_back(AppendStep(ctx, reduce));
    ctx->execution->reduction_step_ids.push_back(ids.back());
    return ids;
}

std::vector<uint64_t> KeySwitchExecutionModel::BuildModDownSubgraph(
    BuildContext* ctx,
    uint32_t ct_idx,
    uint32_t limb_idx,
    uint64_t out_bytes,
    uint64_t work_items,
    const KeySwitchMethodPolicy& policy,
    const std::vector<uint64_t>& deps) const {

    const uint64_t shortcut_bytes = policy.supports_moddown_shortcut
        ? std::max<uint64_t>(1, out_bytes / 2)
        : out_bytes;
    const uint64_t shortcut_work = policy.supports_moddown_shortcut
        ? std::max<uint64_t>(1, work_items / 2)
        : work_items;

    std::vector<uint64_t> ids;
    TileExecutionStep intt;
    intt.type = TileExecutionStepType::ModDownInttTile;
    intt.stage_type = StageType::BasisConvert;
    intt.ct_tile_index = ct_idx;
    intt.limb_tile_index = limb_idx;
    intt.tile_idx = ct_idx;
    intt.limb_idx = limb_idx;
    intt.work_items = shortcut_work;
    intt.input_bytes = shortcut_bytes;
    intt.output_bytes = shortcut_bytes;
    intt.input_storage = StorageForConnection(
        policy.reduction_to_moddown,
        policy.reduction_output_storage);
    intt.output_storage = policy.fuse_moddown_chain
        ? policy.moddown_temp_storage
        : IntermediateStorageLevel::BRAM;
    intt.fused_with_next = policy.fuse_moddown_chain;
    intt.depends_on = deps;
    intt.is_shortcut_path = policy.supports_moddown_shortcut;
    ids.push_back(AppendStep(ctx, intt));
    ctx->execution->moddown_step_ids.push_back(ids.back());

    TileExecutionStep bconv;
    bconv.type = TileExecutionStepType::ModDownBConvTile;
    bconv.stage_type = StageType::BasisConvert;
    bconv.ct_tile_index = ct_idx;
    bconv.limb_tile_index = limb_idx;
    bconv.tile_idx = ct_idx;
    bconv.limb_idx = limb_idx;
    bconv.work_items = shortcut_work;
    bconv.input_bytes = shortcut_bytes;
    bconv.output_bytes = shortcut_bytes;
    bconv.input_storage = intt.output_storage;
    bconv.output_storage = policy.fuse_moddown_chain
        ? policy.moddown_temp_storage
        : IntermediateStorageLevel::BRAM;
    bconv.fused_with_prev = policy.fuse_moddown_chain;
    bconv.fused_with_next = policy.fuse_moddown_chain;
    bconv.depends_on = {ids.back()};
    bconv.is_shortcut_path = policy.supports_moddown_shortcut;
    ids.push_back(AppendStep(ctx, bconv));
    ctx->execution->moddown_step_ids.push_back(ids.back());

    TileExecutionStep ntt;
    ntt.type = TileExecutionStepType::ModDownNttTile;
    ntt.stage_type = StageType::BasisConvert;
    ntt.ct_tile_index = ct_idx;
    ntt.limb_tile_index = limb_idx;
    ntt.tile_idx = ct_idx;
    ntt.limb_idx = limb_idx;
    ntt.work_items = shortcut_work;
    ntt.input_bytes = shortcut_bytes;
    ntt.output_bytes = shortcut_bytes;
    ntt.input_storage = bconv.output_storage;
    ntt.output_storage = policy.moddown_temp_storage;
    ntt.fused_with_prev = policy.fuse_moddown_chain;
    ntt.fused_with_next =
        (policy.moddown_to_subtract == StageConnectionMode::DirectForward);
    ntt.depends_on = {ids.back()};
    ntt.is_shortcut_path = policy.supports_moddown_shortcut;
    ids.push_back(AppendStep(ctx, ntt));
    ctx->execution->moddown_step_ids.push_back(ids.back());

    TileExecutionStep sub;
    sub.type = TileExecutionStepType::FinalSubtractTile;
    sub.stage_type = StageType::Multiply;
    sub.ct_tile_index = ct_idx;
    sub.limb_tile_index = limb_idx;
    sub.tile_idx = ct_idx;
    sub.limb_idx = limb_idx;
    sub.work_items = work_items;
    sub.input_bytes = out_bytes;
    sub.output_bytes = out_bytes;
    sub.input_storage = StorageForConnection(
        policy.moddown_to_subtract,
        policy.moddown_temp_storage);
    sub.output_storage = IntermediateStorageLevel::BRAM;
    sub.fused_with_prev =
        (policy.moddown_to_subtract == StageConnectionMode::DirectForward);
    sub.depends_on = {ids.back()};
    sub.is_shortcut_path = policy.supports_moddown_shortcut;
    ids.push_back(AppendStep(ctx, sub));
    ctx->execution->moddown_step_ids.push_back(ids.back());
    return ids;
}

std::vector<uint64_t> KeySwitchExecutionModel::ConnectSubgraphsWithPolicy(
    BuildContext* ctx,
    uint32_t ct_idx,
    uint32_t limb_idx,
    uint32_t digit_idx,
    StageConnectionMode mode,
    uint64_t bytes,
    const std::vector<uint64_t>& deps
) const {

    switch(mode){
        case StageConnectionMode::DirectForward:
            return deps;
        case StageConnectionMode::BufferInBRAM:
            return deps;
        case StageConnectionMode::SpillToHBM:
            break;
    }

    TileExecutionStep spill;
    spill.type = TileExecutionStepType::IntermediateBRAMToHBM;
    spill.stage_type = StageType::Dispatch;
    spill.ct_tile_index = ct_idx;
    spill.limb_tile_index = limb_idx;
    spill.digit_tile_index = digit_idx;
    spill.tile_idx = ct_idx;
    spill.limb_idx = limb_idx;
    spill.digit_idx = digit_idx;
    spill.bytes = bytes;
    spill.input_bytes = bytes;
    spill.output_bytes = bytes;
    spill.input_storage = IntermediateStorageLevel::BRAM;
    spill.output_storage = IntermediateStorageLevel::HBM;
    spill.depends_on = deps;
    const uint64_t spill_id = AppendStep(ctx, spill);

    TileExecutionStep reload;
    reload.type = TileExecutionStepType::IntermediateHBMToBRAM;
    reload.stage_type = StageType::Dispatch;
    reload.ct_tile_index = ct_idx;
    reload.limb_tile_index = limb_idx;
    reload.digit_tile_index = digit_idx;
    reload.tile_idx = ct_idx;
    reload.limb_idx = limb_idx;
    reload.digit_idx = digit_idx;
    reload.bytes = bytes;
    reload.input_bytes = bytes;
    reload.output_bytes = bytes;
    reload.input_storage = IntermediateStorageLevel::HBM;
    reload.output_storage = IntermediateStorageLevel::BRAM;
    reload.depends_on = {spill_id};
    const uint64_t reload_id = AppendStep(ctx, reload);

    ctx->execution->out_bram_to_hbm_bytes += bytes;
    ctx->execution->ct_hbm_to_bram_bytes += bytes;
    ctx->hbm_write_bytes += bytes;
    ctx->hbm_read_bytes += bytes;
    ctx->hbm_round_trips += 1;

    return {reload_id};
}

void KeySwitchExecutionModel::FinalizeExecution(
    BuildContext* ctx) const {

    KeySwitchExecution& execution = *ctx->execution;
    execution.predicted_hbm_bytes =
        execution.key_host_to_hbm_bytes
        + execution.key_hbm_to_bram_bytes
        + execution.ct_hbm_to_bram_bytes
        + execution.out_bram_to_hbm_bytes;
}

KeySwitchExecution KeySwitchExecutionModel::BuildWithMode(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state,
    bool allow_inter_card_steps) const {

    // 该函数是 keyswitch 执行图的主构建入口，负责把请求映射为一组有依赖关系的 TileExecutionStep。
    // 关键流程：
    // 1) 解析问题规模（BuildProblem）并做早期失败返回；
    // 2) 调用 TilePlanner 决定 tile 切分与 key 常驻策略；
    // 3) 逐个 ct/limb/digit 生成 ModUp -> InnerProd -> Reduction -> ModDown 子图；
    // 4) 可选追加多卡 merge/reduce/barrier 步骤；
    // 5) 汇总峰值缓存与流量统计。
    KeySwitchExecution execution;
    execution.problem = BuildProblem(req, plan, state);
    execution.method = execution.problem.method;
    execution.working_set_bytes = execution.problem.working_set_bytes;
    execution.key_resident_hit = execution.problem.key_resident_hit;

    // 没有有效问题（例如没有可用卡）时直接返回 fallback，后续不生成任何步骤。
    if (!execution.problem.valid) {
        execution.fallback_used = true;
        execution.fallback_reason = KeySwitchFallbackReason::NoAssignedCard;
        return execution;
    }

    // 根据 method 解析执行策略（stage 连接方式、是否支持 overlap 等），并给出 tile 方案。
    execution.policy = ResolveMethodPolicy(execution.problem.method);
    execution.tile_plan = planner_.Plan(execution.problem);
    // tile 规划失败时退化为非 tiled 的流量估计路径，保证上层仍能拿到可解释的结果。
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

    // BuildContext 在构建期间维护 step_id 递增。
    BuildContext ctx;
    ctx.execution = &execution;

    uint64_t host_key_step_id = 0;
    // key 不在驻留缓存命中时，先补一条 Host->HBM 的 key 预载步骤。
    if (!execution.problem.key_resident_hit) {
        TileExecutionStep host_key_step;
        host_key_step.type = TileExecutionStepType::KeyLoadHostToHBM;
        host_key_step.stage_type = StageType::KeyLoad;
        host_key_step.bytes = execution.problem.key_bytes;
        host_key_step.input_bytes = execution.problem.key_bytes;
        host_key_step.output_bytes = execution.problem.key_bytes;
        host_key_step.input_storage = IntermediateStorageLevel::HBM;
        host_key_step.output_storage = IntermediateStorageLevel::HBM;
        host_key_step.key_hit = false;
        host_key_step.key_persistent = false;
        host_key_step_id = AppendStep(&ctx, host_key_step);
        execution.key_host_to_hbm_bytes += execution.problem.key_bytes;
    }

    uint64_t persistent_key_step_id = 0;
    // 若 tile 方案允许 key 常驻，则只搬运一次 HBM->BRAM，后续所有 inner-prod 共享该 key。
    if (execution.tile_plan.key_persistent) {
        TileExecutionStep persistent_key;
        persistent_key.type = TileExecutionStepType::KeyHBMToBRAM;
        persistent_key.stage_type = StageType::KeyLoad;
        persistent_key.bytes = execution.problem.key_bytes;
        persistent_key.input_bytes = execution.problem.key_bytes;
        persistent_key.output_bytes = execution.problem.key_bytes;
        persistent_key.input_storage = IntermediateStorageLevel::HBM;
        persistent_key.output_storage = IntermediateStorageLevel::BRAM;
        persistent_key.key_hit = true;
        persistent_key.key_persistent = true;
        if (host_key_step_id != 0) {
            persistent_key.depends_on = {host_key_step_id};
        }
        persistent_key_step_id = AppendStep(&ctx, persistent_key);
        execution.key_hbm_to_bram_bytes += execution.problem.key_bytes;
    }

    const uint32_t ct_tiles = std::max<uint32_t>(1, execution.tile_plan.ct_tiles);
    const uint32_t limb_tiles = std::max<uint32_t>(1, execution.tile_plan.limb_tiles);
    const uint32_t digit_tiles = std::max<uint32_t>(1, execution.tile_plan.digit_tiles);
    const std::vector<uint32_t> limb_order = IterationOrder(limb_tiles);
    const std::vector<uint32_t> digit_order = IterationOrder(digit_tiles);

    // 外层按 ct tile 遍历：每个 ct tile 内独立构建一套 limb/digit 子图。
    for (uint32_t ct_idx = 0; ct_idx < ct_tiles; ++ct_idx) {
        const uint32_t ct_remain =
            execution.problem.ciphertexts - ct_idx * execution.tile_plan.ct_tile;
        const uint32_t ct_now = std::min<uint32_t>(execution.tile_plan.ct_tile, ct_remain);

        // input_step_by_limb: 每个 limb tile 只需要一次输入搬运（HBM->BRAM），避免重复加载。
        std::vector<uint64_t> input_step_by_limb(limb_tiles, 0);
        // inner_terminal: 记录每个 (limb,digit) inner-prod 子图末端 step_id，
        // 供后续 reduction 汇聚时建立依赖。
        std::vector<std::vector<uint64_t>> inner_terminal(
            limb_tiles,
            std::vector<uint64_t>(digit_tiles, 0));

        auto ensure_input_step = [&](uint32_t limb_idx) {
            // 同一 ct tile 下，同一 limb tile 的输入数据只加载一次。
            if (input_step_by_limb[limb_idx] != 0) {
                return input_step_by_limb[limb_idx];
            }
            const uint32_t limb_remain =
                execution.problem.limbs - limb_idx * execution.tile_plan.limb_tile;
            const uint32_t limb_now = std::min<uint32_t>(execution.tile_plan.limb_tile, limb_remain);
            // 输入块大小 = 当前 ct 数 * 当前 limb 数 * 单 limb 密文字节数。
            const uint64_t ct_chunk_bytes =
                static_cast<uint64_t>(ct_now) * limb_now * execution.problem.ct_limb_bytes;
            TileExecutionStep input_step;
            input_step.type = TileExecutionStepType::InputHBMToBRAM;
            input_step.stage_type = StageType::Dispatch;
            input_step.ct_tile_index = ct_idx;
            input_step.limb_tile_index = limb_idx;
            input_step.tile_idx = ct_idx;
            input_step.limb_idx = limb_idx;
            input_step.bytes = ct_chunk_bytes;
            input_step.input_bytes = ct_chunk_bytes;
            input_step.output_bytes = ct_chunk_bytes;
            input_step.input_storage = IntermediateStorageLevel::HBM;
            input_step.output_storage = IntermediateStorageLevel::BRAM;
            input_step.key_hit = execution.problem.key_resident_hit;
            input_step.key_persistent = execution.tile_plan.key_persistent;
            input_step_by_limb[limb_idx] = AppendStep(&ctx, input_step);
            execution.ct_hbm_to_bram_bytes += ct_chunk_bytes;
            return input_step_by_limb[limb_idx];
        };

        // 为一个 (limb_idx, digit_idx) 对构建完整前半段：
        // Input(按需) -> ModUp 子图 -> (按策略连接) -> KeyLoad(按需) -> InnerProd 子图。
        auto build_for_pair = [&](uint32_t limb_idx, uint32_t digit_idx) {
            const uint32_t limb_remain =
                execution.problem.limbs - limb_idx * execution.tile_plan.limb_tile;
            const uint32_t limb_now = std::min<uint32_t>(execution.tile_plan.limb_tile, limb_remain);
            const uint32_t digit_remain =
                execution.problem.digits - digit_idx * execution.tile_plan.digit_tile;
            const uint32_t digit_now = std::min<uint32_t>(execution.tile_plan.digit_tile, digit_remain);
            const uint32_t key_limb_now = limb_now + KeyExtraLimbsForTile(
                limb_idx,
                execution.tile_plan.limb_tile,
                execution.problem.limbs,
                execution.problem.num_k);

            // 每个 pair 的数据规模与计算量估算，后续填入各子图 step 的 bytes/work_items。
            const uint64_t ct_chunk_bytes =
                static_cast<uint64_t>(ct_now) * limb_now * execution.problem.ct_limb_bytes;
            const uint64_t key_chunk_bytes =
                static_cast<uint64_t>(key_limb_now) * digit_now * execution.problem.key_digit_limb_bytes;
            const uint64_t decompose_work =
                static_cast<uint64_t>(ct_now)
                * static_cast<uint64_t>(digit_now)
                * static_cast<uint64_t>(limb_now)
                * static_cast<uint64_t>(execution.problem.poly_modulus_degree);
            const uint64_t inner_work =
                decompose_work * static_cast<uint64_t>(execution.problem.polys);

            std::vector<uint64_t> modup_deps = {ensure_input_step(limb_idx)};
            const std::vector<uint64_t> modup_ids = BuildModUpSubgraph(
                &ctx,
                ct_idx,
                limb_idx,
                digit_idx,
                ct_chunk_bytes,
                decompose_work,
                execution.policy,
                modup_deps);

            // 依据策略在 ModUp 与 InnerProd 之间插入直连/缓冲/落盘重载步骤。
            std::vector<uint64_t> inner_deps = ConnectSubgraphsWithPolicy(
                &ctx,
                ct_idx,
                limb_idx,
                digit_idx,
                execution.policy.modup_to_innerprod,
                ct_chunk_bytes,
                {modup_ids.back()});

            // key 常驻模式：依赖一次全局 key 预载。
            // 非常驻模式：每个 (limb,digit) 单独插入 HBM->BRAM key 加载步骤。
            if (execution.tile_plan.key_persistent && persistent_key_step_id != 0) {
                AddDependencyId(&inner_deps, persistent_key_step_id);
            } else {
                TileExecutionStep key_step;
                key_step.type = TileExecutionStepType::KeyHBMToBRAM;
                key_step.stage_type = StageType::KeyLoad;
                key_step.ct_tile_index = ct_idx;
                key_step.limb_tile_index = limb_idx;
                key_step.digit_tile_index = digit_idx;
                key_step.tile_idx = ct_idx;
                key_step.limb_idx = limb_idx;
                key_step.digit_idx = digit_idx;
                key_step.bytes = key_chunk_bytes;
                key_step.input_bytes = key_chunk_bytes;
                key_step.output_bytes = key_chunk_bytes;
                key_step.input_storage = IntermediateStorageLevel::HBM;
                key_step.output_storage = IntermediateStorageLevel::BRAM;
                key_step.key_hit = true;
                key_step.key_persistent = false;
                if (host_key_step_id != 0) {
                    key_step.depends_on = {host_key_step_id};
                }
                const uint64_t key_step_id = AppendStep(&ctx, key_step);
                execution.key_hbm_to_bram_bytes += key_chunk_bytes;
                AddDependencyId(&inner_deps, key_step_id);
            }

            const std::vector<uint64_t> inner_ids = BuildInnerProdSubgraph(
                &ctx,
                ct_idx,
                limb_idx,
                digit_idx,
                key_chunk_bytes,
                inner_work,
                execution.policy,
                inner_deps);

            // 记录 inner-prod 末端，用于后续 reduction 汇聚依赖。
            inner_terminal[limb_idx][digit_idx] = inner_ids.back();
        };

        // 执行顺序由策略决定：
        // - Digit 粒度或偏好 digit 局部性：digit 外层、limb 内层；
        // - 否则 limb 外层、digit 内层。
        // 该顺序会影响 key/input 复用机会与中间缓冲峰值。
        if (execution.policy.granularity == KeySwitchProcessingGranularity::Digit) {
            for (uint32_t digit_idx : digit_order) {
                for (uint32_t limb_idx : limb_order) {
                    build_for_pair(limb_idx, digit_idx);
                }
            }
        } else {
            for (uint32_t limb_idx : limb_order) {
                for (uint32_t digit_idx : digit_order) {
                    build_for_pair(limb_idx, digit_idx);
                }
            }
        }

        // 对每个 limb 汇聚所有 digit 的 inner-prod 结果，再执行 moddown 并写回输出。
        for (uint32_t limb_idx : limb_order) {
            const uint32_t limb_remain =
                execution.problem.limbs - limb_idx * execution.tile_plan.limb_tile;
            const uint32_t limb_now = std::min<uint32_t>(execution.tile_plan.limb_tile, limb_remain);
            const uint64_t out_bytes =
                static_cast<uint64_t>(ct_now) * limb_now * execution.problem.out_limb_bytes;
            const uint64_t moddown_input_bytes = execution.policy.supports_moddown_shortcut
                ? std::max<uint64_t>(1, out_bytes / 2)
                : out_bytes;
            const uint64_t reduction_work =
                static_cast<uint64_t>(ct_now)
                * static_cast<uint64_t>(limb_now)
                * static_cast<uint64_t>(execution.problem.poly_modulus_degree);

            uint64_t reduction_terminal = 0;
            if (execution.policy.supports_partial_reduction_overlap) {
                // 支持部分重叠时：按 digit 串行累计 reduction，
                // 每次都依赖上一轮 reduction 末端，降低瞬时并发峰值。
                for (uint32_t digit_idx : digit_order) {
                    std::vector<uint64_t> deps = ConnectSubgraphsWithPolicy(
                        &ctx,
                        ct_idx,
                        limb_idx,
                        digit_idx,
                        execution.policy.innerprod_to_reduction,
                        out_bytes,
                        {inner_terminal[limb_idx][digit_idx]});
                    AddDependencyId(&deps, reduction_terminal);
                    const std::vector<uint64_t> reduce_ids = BuildReductionSubgraph(
                        &ctx,
                        ct_idx,
                        limb_idx,
                        reduction_work,
                        execution.policy,
                        deps);
                    reduction_terminal = reduce_ids.back();
                }
            } else {
                // 不支持重叠时：先收集全部 digit 依赖，再一次性做 reduction。
                std::vector<uint64_t> deps;
                for (uint32_t digit_idx : digit_order) {
                    const std::vector<uint64_t> conn = ConnectSubgraphsWithPolicy(
                        &ctx,
                        ct_idx,
                        limb_idx,
                        digit_idx,
                        execution.policy.innerprod_to_reduction,
                        out_bytes,
                        {inner_terminal[limb_idx][digit_idx]});
                    for (uint64_t dep : conn) {
                        AddDependencyId(&deps, dep);
                    }
                }
                const std::vector<uint64_t> reduce_ids = BuildReductionSubgraph(
                    &ctx,
                    ct_idx,
                    limb_idx,
                    reduction_work,
                    execution.policy,
                    deps);
                reduction_terminal = reduce_ids.back();
            }

            // Reduction -> ModDown 的连接仍由策略控制（是否直连或插中间缓冲）。
            const std::vector<uint64_t> moddown_deps = ConnectSubgraphsWithPolicy(
                &ctx,
                ct_idx,
                limb_idx,
                /*digit_idx=*/0,
                execution.policy.reduction_to_moddown,
                moddown_input_bytes,
                {reduction_terminal});
            const uint64_t moddown_work =
                static_cast<uint64_t>(ct_now)
                * static_cast<uint64_t>(limb_now)
                * static_cast<uint64_t>(execution.problem.polys)
                * static_cast<uint64_t>(execution.problem.poly_modulus_degree);
            const std::vector<uint64_t> moddown_ids = BuildModDownSubgraph(
                &ctx,
                ct_idx,
                limb_idx,
                out_bytes,
                moddown_work,
                execution.policy,
                moddown_deps);

            // 输出 tile 写回 HBM，作为该 limb tile 的终点步骤。
            TileExecutionStep output;
            output.type = TileExecutionStepType::OutputBRAMToHBM;
            output.stage_type = StageType::Dispatch;
            output.ct_tile_index = ct_idx;
            output.limb_tile_index = limb_idx;
            output.tile_idx = ct_idx;
            output.limb_idx = limb_idx;
            output.bytes = out_bytes;
            output.input_bytes = out_bytes;
            output.output_bytes = out_bytes;
            output.input_storage = IntermediateStorageLevel::BRAM;
            output.output_storage = IntermediateStorageLevel::HBM;
            output.depends_on = {moddown_ids.back()};
            output.key_hit = execution.problem.key_resident_hit;
            output.key_persistent = execution.tile_plan.key_persistent;
            AppendStep(&ctx, output);
            execution.out_bram_to_hbm_bytes += out_bytes;
        }
    }

    // 可选的多卡收敛步骤：
    // 在单卡计算 DAG 后追加 peer->root 的 send/recv，随后 root 做 reduce 与 barrier。
    // 这些步骤主要用于建模卡间通信与同步开销，不改变单卡内部的计算子图。
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

                // peer 卡把分片结果发往 root 卡。
                TileExecutionStep send_step;
                send_step.type = TileExecutionStepType::InterCardSendStep;
                send_step.stage_type = StageType::Merge;
                send_step.bytes = per_peer_bytes;
                send_step.src_card = static_cast<int32_t>(peer_card);
                send_step.dst_card = static_cast<int32_t>(root_card);
                send_step.sync_group = sync_group;
                AppendStep(&ctx, send_step);

                // root 卡接收 peer 卡数据。
                TileExecutionStep recv_step;
                recv_step.type = TileExecutionStepType::InterCardRecvStep;
                recv_step.stage_type = StageType::Merge;
                recv_step.bytes = per_peer_bytes;
                recv_step.src_card = static_cast<int32_t>(peer_card);
                recv_step.dst_card = static_cast<int32_t>(root_card);
                recv_step.sync_group = sync_group;
                AppendStep(&ctx, recv_step);
            }

            // root 卡对全部输入执行归并规约。
            TileExecutionStep reduce_step;
            reduce_step.type = TileExecutionStepType::InterCardReduceStep;
            reduce_step.stage_type = StageType::Merge;
            reduce_step.bytes = req.ks_profile.output_bytes;
            reduce_step.src_card = static_cast<int32_t>(root_card);
            reduce_step.dst_card = static_cast<int32_t>(root_card);
            reduce_step.fan_in = active_cards;
            reduce_step.sync_group = sync_group;
            AppendStep(&ctx, reduce_step);

            // 最后 barrier，确保 merge 阶段所有参与卡完成同步。
            TileExecutionStep barrier_step;
            barrier_step.type = TileExecutionStepType::BarrierStep;
            barrier_step.stage_type = StageType::Merge;
            barrier_step.src_card = static_cast<int32_t>(root_card);
            barrier_step.dst_card = static_cast<int32_t>(root_card);
            barrier_step.work_items = active_cards;
            barrier_step.fan_in = active_cards;
            barrier_step.sync_group = sync_group;
            barrier_step.barrier_group = sync_group;
            AppendStep(&ctx, barrier_step);
        }
    }

    // 收尾：把构建过程中累计的峰值与流量字段写回 execution。
    FinalizeExecution(&ctx);
    return execution;
}
KeySwitchExecution KeySwitchExecutionModel::Build(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state
) const {

    // 统计本次请求实际被分配到的卡数（最少按 1 处理，避免空分配导致 method 解析异常）。
    const uint32_t assigned_cards = static_cast<uint32_t>(std::max<size_t>(1, plan.assigned_cards.size()));
    // 根据“请求 method + 实际卡数”解析出本次真正要走的执行方法。
    // 例如：Auto 在多卡时可能被解析为 Cinnamon，单卡时解析为 Poseidon（由 resolver 决定）。
    const KeySwitchMethod resolved_method = ResolveKeySwitchMethodForAssignedCards(req.ks_profile.method, assigned_cards);

    // 按解析后的方法分发到对应构建路径，得到完整 execution（含 fallback 信息）。
    KeySwitchExecution execution = BuildByMethod(req, plan, state, resolved_method);

    // 对“明确不支持的方法”直接透传返回，避免后续字段二次改写掩盖真实原因。
    if (!execution.valid && execution.fallback_used
        && execution.fallback_reason == KeySwitchFallbackReason::UnsupportedMethod) {
        return execution;
    }

    // requested_method：用户/请求最初指定的方法（用于结果对照与诊断）。
    execution.requested_method = req.ks_profile.method;
    // effective_method：如果下游尚未显式写入（仍为占位值 Poseidon），
    // 则用本次 execution.method 回填，确保输出字段语义完整。
    if (execution.effective_method == KeySwitchMethod::Poseidon) {
        execution.effective_method = execution.method;
    }
    // method 字段对外统一表示“最终生效的方法”。
    // 若它仍是占位 Poseidon，则同步成 effective_method，避免展示歧义。
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
    // 多卡
    case KeySwitchMethod::Cinnamon:
        return BuildCinnamon(req, plan, state);
    
    // 单卡
    case KeySwitchMethod::Poseidon:
    case KeySwitchMethod::OLA:
    case KeySwitchMethod::FAB:
    case KeySwitchMethod::FAST:
    case KeySwitchMethod::HERA:
        return BuildSharedSingleBoardMethod(req, plan, state, resolved_method);

    case KeySwitchMethod::Auto:
        return BuildUnsupportedMethod(req.ks_profile.method, resolved_method);

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

KeySwitchExecution KeySwitchExecutionModel::BuildSharedSingleBoardMethod(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state,
    KeySwitchMethod method
) const {

    if (!IsSharedSingleBoardMethod(method)) {
        return BuildUnsupportedMethod(req.ks_profile.method, method);
    }

    KeySwitchExecution execution;
    execution.valid = false;
    execution.tiled_execution = true;
    execution.fallback_used = false;
    execution.fallback_reason = KeySwitchFallbackReason::None;
    execution.method = method;
    execution.requested_method = req.ks_profile.method;
    execution.effective_method = method;
    execution.method_degraded = false;
    execution.degraded_reason = KeySwitchFallbackReason::None;

    Request method_req = req;
    method_req.ks_profile.method = method;
    method_req.ks_profile.partition = PartitionStrategy::None;
    method_req.ks_profile.collective = CollectiveStrategy::None;
    method_req.ks_profile.key_placement = KeyPlacement::StreamFromHBM;
    method_req.ks_profile.scale_out_cards = 1;
    method_req.ks_profile.enable_inter_card_merge = false;
    method_req.ks_profile.allow_cross_card_reduce = false;

    ExecutionPlan single_plan;
    single_plan.request_id = plan.request_id;
    if (!plan.assigned_cards.empty()) {
        single_plan.assigned_cards.push_back(plan.assigned_cards.front());
    }

    execution.problem = BuildProblem(method_req, single_plan, state);
    execution.problem.method = method;
    execution.problem.cards = 1;
    execution.working_set_bytes = execution.problem.working_set_bytes;
    execution.key_resident_hit = execution.problem.key_resident_hit;

    if (!execution.problem.valid) {
        execution.fallback_used = true;
        execution.fallback_reason = KeySwitchFallbackReason::NoAssignedCard;
        return execution;
    }

    execution.policy = ResolveMethodPolicy(method);
    execution.logical_graph = BuildSharedSingleBoardLogicalGraph(execution.policy);
    execution.valid = execution.logical_graph.valid && !execution.logical_graph.nodes.empty();
    return execution;
}

KeySwitchExecution KeySwitchExecutionModel::BuildSharedSingleBoardPhysical(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state,
    KeySwitchMethod method
) const {
    KeySwitchExecution execution = BuildSharedSingleBoardMethod(req, plan, state, method);
    if (!execution.valid) {
        return execution;
    }

    RuntimePlanner runtime_planner(params_.tile_planner);
    const RuntimePlan runtime_plan = runtime_planner.Plan(execution);
    if (!runtime_plan.valid) {
        execution.valid = false;
        execution.fallback_used = true;
        execution.fallback_reason = KeySwitchFallbackReason::TilePlanInvalid;
        return execution;
    }

    execution.tiled_execution = true;
    execution.fallback_used = false;
    execution.fallback_reason = KeySwitchFallbackReason::None;
    execution.tile_plan = runtime_plan.tile_plan;
    execution.steps = runtime_plan.steps;
    execution.tile_count = runtime_plan.tile_count;
    execution.key_persistent_bram = runtime_plan.key_persistent_bram;
    execution.key_resident_hit = runtime_plan.key_resident_hit;
    execution.working_set_bytes = runtime_plan.working_set_bytes;
    execution.key_host_to_hbm_bytes = runtime_plan.key_host_to_hbm_bytes;
    execution.key_hbm_to_bram_bytes = runtime_plan.key_hbm_to_bram_bytes;
    execution.ct_hbm_to_bram_bytes = runtime_plan.ct_hbm_to_bram_bytes;
    execution.out_bram_to_hbm_bytes = runtime_plan.out_bram_to_hbm_bytes;
    execution.tile_cost = execution.tile_plan.cost;
    execution.predicted_hbm_bytes =
        execution.key_host_to_hbm_bytes
        + execution.key_hbm_to_bram_bytes
        + execution.ct_hbm_to_bram_bytes
        + execution.out_bram_to_hbm_bytes;
    execution.valid = !execution.steps.empty();
    return execution;
}

KeySwitchExecution KeySwitchExecutionModel::BuildSingleBoard(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state
) const {

    KeySwitchMethod single_method = req.ks_profile.method;
    if (single_method == KeySwitchMethod::Auto
        || !IsSharedSingleBoardMethod(single_method)) {
        single_method = kSingleBoardBaseMethod;
    }

    if (!req.ks_profile.allow_local_moddown) {
        return UnsupportedConfig(single_method);
    }

    const PartitionStrategy partition = ResolvePartition(req.ks_profile.partition, single_method);
    if (partition != PartitionStrategy::None) {
        return UnsupportedConfig(single_method);
    }

    const CollectiveStrategy collective =
        ResolveCollective(req.ks_profile.collective, single_method);
    if (collective != CollectiveStrategy::None) {
        return UnsupportedConfig(single_method);
    }

    const KeyPlacement key_placement =
        ResolveKeyPlacement(req.ks_profile.key_placement, single_method);
    if (key_placement != KeyPlacement::StreamFromHBM) {
        return UnsupportedConfig(single_method);
    }

    Request single_req = req;
    single_req.ks_profile.method = single_method;
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

    KeySwitchExecution execution = BuildSharedSingleBoardMethod(
        single_req,
        single_plan,
        state,
        single_method);

    execution.method = single_method;
    execution.requested_method = req.ks_profile.method;
    execution.effective_method = single_method;
    execution.method_degraded = false;
    execution.degraded_reason = KeySwitchFallbackReason::None;
    execution.problem.method = single_method;
    execution.problem.cards = 1;
    return execution;
}

KeySwitchExecution KeySwitchExecutionModel::BuildCinnamon(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    if (plan.assigned_cards.empty()) {
        KeySwitchExecution no_card;
        no_card.method = KeySwitchMethod::Cinnamon;
        no_card.requested_method = req.ks_profile.method;
        no_card.effective_method = KeySwitchMethod::Cinnamon;
        no_card.fallback_used = true;
        no_card.fallback_reason = KeySwitchFallbackReason::NoAssignedCard;
        no_card.valid = false;
        no_card.tiled_execution = false;
        return no_card;
    }

    if (!req.ks_profile.allow_local_moddown) {
        return UnsupportedConfig(KeySwitchMethod::Cinnamon);
    }

    const uint32_t effective_cards = EffectiveScaleOutCards(req, plan);
    if (effective_cards <= 1) {
        // Explicit policy: auto-degrade to single-board when only one card is available.
        Request single_req = req;
        single_req.ks_profile.partition = PartitionStrategy::None;
        single_req.ks_profile.collective = CollectiveStrategy::None;
        single_req.ks_profile.key_placement = KeyPlacement::StreamFromHBM;
        KeySwitchExecution degraded = BuildSharedSingleBoardPhysical(
            single_req,
            plan,
            state,
            kSingleBoardBaseMethod);
        if (!degraded.fallback_used) {
            degraded.method_degraded = true;
            degraded.degraded_reason = KeySwitchFallbackReason::DegradedToSingleBoard;
            degraded.fallback_reason = KeySwitchFallbackReason::None;
        }
        degraded.requested_method = req.ks_profile.method;
        degraded.effective_method = kSingleBoardBaseMethod;
        return degraded;
    }

    const PartitionStrategy partition =
        ResolvePartition(req.ks_profile.partition, KeySwitchMethod::Cinnamon);
    if (partition != PartitionStrategy::ByLimb) {
        return UnsupportedConfig(KeySwitchMethod::Cinnamon);
    }

    const CollectiveStrategy collective =
        ResolveCollective(req.ks_profile.collective, KeySwitchMethod::Cinnamon);
    if (collective != CollectiveStrategy::GatherToRoot) {
        return UnsupportedConfig(KeySwitchMethod::Cinnamon);
    }

    const KeyPlacement key_placement =
        ResolveKeyPlacement(req.ks_profile.key_placement, KeySwitchMethod::Cinnamon);
    // Cinnamon MVP: only replicated key placement is supported in this path.
    if (key_placement != KeyPlacement::ReplicatedPerCard) {
        return UnsupportedConfig(KeySwitchMethod::Cinnamon);
    }

    if (!req.ks_profile.enable_inter_card_merge) {
        return UnsupportedConfig(KeySwitchMethod::Cinnamon);
    }
    if (!req.ks_profile.allow_cross_card_reduce) {
        return UnsupportedConfig(KeySwitchMethod::Cinnamon);
    }

    ExecutionPlan scale_plan = plan;
    if (scale_plan.assigned_cards.size() > effective_cards) {
        scale_plan.assigned_cards.resize(effective_cards);
    }

    Request scale_req = req;
    scale_req.ks_profile.method = KeySwitchMethod::Cinnamon;
    scale_req.ks_profile.partition = partition;
    scale_req.ks_profile.key_placement = key_placement;
    scale_req.ks_profile.collective = collective;
    scale_req.ks_profile.scale_out_cards = effective_cards;

    KeySwitchExecution execution;
    execution.valid = true;
    execution.tiled_execution = true;
    execution.method = KeySwitchMethod::Cinnamon;
    execution.requested_method = req.ks_profile.method;
    execution.effective_method = KeySwitchMethod::Cinnamon;
    execution.method_degraded = false;
    execution.degraded_reason = KeySwitchFallbackReason::None;
    execution.problem = BuildProblem(scale_req, scale_plan, state);
    execution.problem.method = KeySwitchMethod::Cinnamon;
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
        local_req.ks_profile.method = KeySwitchMethod::Auto;
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

        const KeySwitchExecution local = BuildSharedSingleBoardPhysical(
            local_req,
            local_plan,
            state,
            kSingleBoardBaseMethod);
        if (!local.valid) {
            // Do not fallback to generic BuildWithMode() path.
            // Keep Cinnamon semantics explicit and explainable.
            KeySwitchExecution failed = UnsupportedConfig(KeySwitchMethod::Cinnamon);
            failed.requested_method = req.ks_profile.method;
            failed.effective_method = KeySwitchMethod::Cinnamon;
            failed.problem = execution.problem;
            failed.problem.method = KeySwitchMethod::Cinnamon;
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
            // per-card local primitives for Cinnamon traces.
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
