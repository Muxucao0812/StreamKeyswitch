#include "backend/cycle_backend.h"
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

// BRAM 占用追踪器：按 buffer 名称管理 acquire/release，记录峰值并检测超预算。
class BramTracker {
public:
    explicit BramTracker(uint64_t budget_bytes)
        : budget_(budget_bytes) {}

    void Acquire(uint64_t bytes) {
        live_ += bytes;
        peak_ = std::max(peak_, live_);
        if (live_ > budget_) {
            overflowed_ = true;
        }
    }

    void Release(uint64_t bytes) {
        live_ = (bytes > live_) ? 0 : (live_ - bytes);
    }

    // 试探：如果 acquire 这么多字节，是否会超预算。
    bool CanAcquire(uint64_t bytes) const {
        return (live_ + bytes) <= budget_;
    }

    uint64_t Budget() const { return budget_; }
    uint64_t Remaining() const { return (budget_ > live_) ? (budget_ - live_) : 0; }
    bool Overflowed() const { return overflowed_; }
    uint64_t Peak() const { return peak_; }
    uint64_t Live() const { return live_; }

private:
    uint64_t budget_ = 0;
    uint64_t live_ = 0;
    uint64_t peak_ = 0;
    bool overflowed_ = false;
};

// Memory-aware 执行构建器。
// 在做 memory 决策的同时直接生成 CycleProgram（跳过 TileExecutionStep 中间层）。
// 所有数据流行为硬编码，不依赖 KeySwitchMethodPolicy。
struct ExecutionBuilder {
    const KeySwitchProblem& problem;
    const HardwareModel& hardware;
    BramTracker bram;
    CycleProgram program;
    uint64_t next_instruction_id = 0;

    ExecutionBuilder(
        const KeySwitchProblem& p,
        const HardwareModel& hw,
        KeySwitchMethod method)
        : problem(p)
        , hardware(hw)
        , bram(p.bram_budget_bytes > p.bram_guard_bytes
               ? p.bram_budget_bytes - p.bram_guard_bytes
               : 0) {
        program.method = method;
        program.name = "stream_keyswitch";
    }

    // 向 program 追加一个 instruction group，返回 group id。
    // num_limbs: 该操作涉及的 limb 数量，计算类指令 cycle = num_limbs × 单 poly cycle。
    uint32_t Emit(
        const std::string& name,
        CycleInstructionKind kind,
        CycleTransferPath transfer_path,
        StageType stage_type,
        uint64_t bytes,
        uint64_t work_items,
        uint32_t num_limbs,
        int64_t bram_delta_issue,
        int64_t bram_delta_complete,
        const std::vector<uint32_t>& deps) {

        const uint32_t limbs = std::max<uint32_t>(1, num_limbs);
        // 每条 instruction = 1 个 limb 的操作延迟。
        const uint64_t per_limb_cycles = std::max<uint64_t>(
            1, EstimateCycles(kind, transfer_path, bytes / limbs, 1));
        // 总延迟（用于打印，实际由 Driver 逐条调度）。
        const uint64_t total_cycles = EstimateCycles(kind, transfer_path, bytes, num_limbs);

        CycleInstructionGroup group;
        group.id = static_cast<uint32_t>(program.groups.size());
        group.name = name;
        group.kind = kind;
        group.transfer_path = transfer_path;
        group.stage_type = stage_type;
        group.bytes = bytes;
        group.work_items = work_items;
        group.live_bytes_delta_on_issue = bram_delta_issue;
        group.live_bytes_delta_on_complete = bram_delta_complete;
        group.dependencies = deps;

        const uint64_t bytes_per_limb = (limbs > 0) ? (bytes / limbs) : bytes;
        const uint64_t work_per_limb = (limbs > 0) ? (work_items / limbs) : work_items;

        for (uint32_t i = 0; i < limbs; ++i) {
            CycleInstruction instr;
            instr.id = next_instruction_id++;
            instr.group_id = group.id;
            instr.kind = kind;
            instr.transfer_path = transfer_path;
            instr.stage_type = stage_type;
            instr.bytes = bytes_per_limb;
            instr.work_items = work_per_limb;
            instr.latency_cycles = per_limb_cycles;
            group.instructions.push_back(std::move(instr));
        }

        const uint32_t gid = group.id;
        program.instruction_count += limbs;
        program.groups.push_back(std::move(group));

        std::cout << "  [group " << gid << "] " << name
                  << "  kind=" << ToString(kind)
                  << "  limbs=" << num_limbs
                  << "  instrs=" << limbs
                  << "  per_limb_cycles=" << per_limb_cycles
                  << "  total_cycles=" << total_cycles
                  << "  bytes=" << bytes
                  << "  bram_live=" << bram.Live()
                  << "\n";

        return gid;
    }

    uint64_t EstimateCycles(
        CycleInstructionKind kind,
        CycleTransferPath path,
        uint64_t bytes,
        uint32_t num_limbs) const {

        switch (kind) {
        case CycleInstructionKind::LoadHBM:
            return hardware.EstimateTransferCycles(
                (path == CycleTransferPath::HostToHBM) ? HardwareTransferPath::HostToHBM : HardwareTransferPath::HBMToSPM, bytes);
        case CycleInstructionKind::StoreHBM:
            return hardware.EstimateTransferCycles(HardwareTransferPath::SPMToHBM, bytes);
        case CycleInstructionKind::NTT:
            return hardware.EstimateNttCycles(problem, num_limbs);
        case CycleInstructionKind::INTT:
            return hardware.EstimateInttCycles(problem, num_limbs);
        case CycleInstructionKind::BConv:
            return hardware.EstimateBconvCycles(problem, num_limbs);
        case CycleInstructionKind::EweMul:
            return hardware.EstimateEweMulCycles(problem, num_limbs);
        case CycleInstructionKind::EweAdd:
            return hardware.EstimateEweAddCycles(problem, num_limbs);
        case CycleInstructionKind::EweSub:
            return hardware.EstimateEweSubCycles(problem, num_limbs);
        default:
            return 1;
        }
    }

    static uint32_t TileExtent(uint32_t idx, uint32_t tile_size, uint32_t total) {
        return std::min<uint32_t>(tile_size, total - idx * tile_size);
    }

    // 上一步的 terminal group id（用于建立依赖链）。
    uint32_t last_group = std::numeric_limits<uint32_t>::max();

    std::vector<uint32_t> Deps() const {
        if (last_group == std::numeric_limits<uint32_t>::max()) return {};
        return {last_group};
    }

    uint64_t LoadInput(uint32_t ct_now, uint32_t limb_now) {
        const uint64_t ct_chunk_bytes =
            static_cast<uint64_t>(ct_now) * limb_now * problem.ct_limb_bytes;
        std::cout << "=== LoadInput: ct=" << ct_now << " l=" << limb_now
                  << " bytes=" << ct_chunk_bytes << " ===\n";
        bram.Acquire(ct_chunk_bytes);
        last_group = Emit("load_input", CycleInstructionKind::LoadHBM,
            CycleTransferPath::HBMToSPM, StageType::Dispatch,
            ct_chunk_bytes, 0, limb_now,
            static_cast<int64_t>(ct_chunk_bytes), 0, Deps());
        return ct_chunk_bytes;
    }

    // ModUp 子图：对一个 digit 的输入做 INTT → BConv → NTT。
    //
    // 数据维度变化（per ciphertext, per digit）：
    //   INTT:  l limbs → l limbs（就地变换）
    //   BConv: l limbs → k limbs（矩阵乘法，生成额外 limbs）
    //   NTT:   l+k limbs → l+k limbs（就地变换）
    //
    // is_last_digit: 最后一个 digit 可以跳过 spill，输出直接留在 BRAM 给 InnerProd 用。
    // 返回 ModUp 输出占用的字节数。
    uint64_t ModUp(uint32_t ct_now, uint64_t input_bram_bytes, bool* spilled) {
        const uint32_t l = problem.limbs;
        const uint32_t k = problem.num_k;
        const uint64_t k_limbs_bytes =
            static_cast<uint64_t>(ct_now) * k * problem.ct_limb_bytes;
        const uint64_t modup_output_bytes = input_bram_bytes + k_limbs_bytes;

        std::cout << "=== ModUp: l=" << l << " k=" << k
                  << " input=" << input_bram_bytes
                  << " output=" << modup_output_bytes << " ===\n";

        // INTT: 对 l 个 limbs 做 INTT
        last_group = Emit("modup_intt", CycleInstructionKind::INTT,
            CycleTransferPath::None, StageType::BasisConvert,
            input_bram_bytes, input_bram_bytes, l,
            0, 0, Deps());

        // BConv: l limbs → k limbs
        bram.Acquire(k_limbs_bytes);
        last_group = Emit("modup_bconv", CycleInstructionKind::BConv,
            CycleTransferPath::None, StageType::BasisConvert,
            modup_output_bytes, modup_output_bytes, k,
            static_cast<int64_t>(k_limbs_bytes), 0, Deps());

        // NTT: 对 l+k 个 limbs 做 NTT
        last_group = Emit("modup_ntt", CycleInstructionKind::NTT,
            CycleTransferPath::None, StageType::BasisConvert,
            modup_output_bytes, modup_output_bytes, l + k,
            0, 0, Deps());

        // Spill 判断
        const uint64_t accum_bytes =
            static_cast<uint64_t>(ct_now) * problem.polys
            * problem.key_limbs * problem.ct_limb_bytes;
        const uint64_t one_limb_key = problem.key_digit_limb_bytes;
        const bool should_spill = !bram.CanAcquire(accum_bytes + one_limb_key);

        if (should_spill) {
            bram.Release(modup_output_bytes);
            last_group = Emit("modup_spill", CycleInstructionKind::StoreHBM,
                CycleTransferPath::SPMToHBM, StageType::Dispatch,
                modup_output_bytes, 0, l + k,
                0, -static_cast<int64_t>(modup_output_bytes), Deps());
            *spilled = true;
        } else {
            *spilled = false;
        }

        return modup_output_bytes;
    }

    // InnerProd + Reduction（融合，流式）：
    //
    // 流式处理：按 limb 逐个流过，每次只在 BRAM 中放 1 个 limb 的 modup_out + 1 个 limb 的 key，
    // 乘完累加到 accum 后释放，再搬下一个 limb。
    //
    // BRAM 峰值 = accum + 1_limb_modup + 1_limb_key。
    //
    // modup_spilled: ModUp 输出是否在 HBM（true=从HBM逐limb reload，false=从BRAM逐limb读取）
    // accum_in_bram: 调用方维护
    // 返回 accum 的字节数。
    uint64_t InnerProd(
        uint32_t ct_now,
        uint64_t modup_output_bytes,
        bool modup_spilled,
        bool is_last_digit,
        bool* accum_in_bram) {

        const uint32_t lk = problem.key_limbs;  // l+k
        const uint32_t p = problem.polys;
        const uint32_t p_lk = p * lk;  // accum 的 limb 数

        const uint64_t accum_bytes =
            static_cast<uint64_t>(ct_now) * p_lk * problem.ct_limb_bytes;

        const uint64_t one_limb_modup = static_cast<uint64_t>(ct_now) * problem.ct_limb_bytes;
        const uint64_t one_limb_key = problem.key_digit_limb_bytes;

        std::cout << "=== InnerProd: modup_limbs=" << lk
                  << " key_limbs=" << lk
                  << " accum_limbs=" << p_lk
                  << " spilled=" << (modup_spilled ? "yes" : "no")
                  << " last=" << (is_last_digit ? "yes" : "no") << " ===\n";

        // 确保 accum 在 BRAM。
        if (!(*accum_in_bram)) {
            bram.Acquire(accum_bytes);
            *accum_in_bram = true;
        }

        // 流式乘法累加：
        if (modup_spilled) {
            last_group = Emit("innerprod_reload_modup", CycleInstructionKind::LoadHBM,
                CycleTransferPath::HBMToSPM, StageType::Multiply,
                modup_output_bytes, 0, lk,
                static_cast<int64_t>(one_limb_modup), 0, Deps());
            bram.Acquire(one_limb_modup + one_limb_key);
            bram.Release(one_limb_modup + one_limb_key);
        } else {
            bram.Acquire(one_limb_key);
            bram.Release(one_limb_key);
            bram.Release(modup_output_bytes);
        }

        // 加载 key（流式）。
        const uint64_t total_key_bytes =
            static_cast<uint64_t>(lk) * problem.key_digit_limb_bytes;
        last_group = Emit("innerprod_load_key", CycleInstructionKind::LoadHBM,
            CycleTransferPath::HBMToSPM, StageType::Multiply,
            total_key_bytes, 0, lk,
            static_cast<int64_t>(one_limb_key), 0, Deps());

        // 乘法：对 p*(l+k) 个 limb 做 EweMul。
        last_group = Emit("innerprod_mul", CycleInstructionKind::EweMul,
            CycleTransferPath::None, StageType::Multiply,
            0, modup_output_bytes, p_lk,
            0, 0, Deps());

        // 累加：对 p*(l+k) 个 limb 做 EweAdd。
        if (p > 1) {
            last_group = Emit("innerprod_add", CycleInstructionKind::EweAdd,
                CycleTransferPath::None, StageType::Multiply,
                0, modup_output_bytes, p_lk,
                0, 0, Deps());
        }

        if (is_last_digit) {
            // 最后 digit：accum 留给 ModDown。
        } else {
            if (!bram.CanAcquire(modup_output_bytes)) {
                bram.Release(accum_bytes);
                last_group = Emit("innerprod_spill_accum", CycleInstructionKind::StoreHBM,
                    CycleTransferPath::SPMToHBM, StageType::Multiply,
                    accum_bytes, 0, p_lk,
                    0, -static_cast<int64_t>(accum_bytes), Deps());
                *accum_in_bram = false;
            }
        }

        return accum_bytes;
    }

    // ModDown：将 accum = 2×(l+k) limbs 变换回 2×l limbs 的最终输出。
    //
    // 数据流：
    //   accum (在 HBM) = [2×l limbs (NTT域)] + [2×k limbs (NTT域)]
    //
    //   1) 只 reload 2×k limbs 到 BRAM（2×l 部分留在 HBM，subtract 时再用）
    //   2) INTT：对 2×k limbs 就地变换（不能流式，整块在 BRAM）
    //   3) BConv：2×k limbs → 2×l limbs（输入全部在 BRAM）
    //      BRAM 峰值 = 2k + 2l（输入+输出同时存在）
    //   4) 释放 2×k limbs（BConv 输入不再需要）
    //   5) NTT：对 2×l limbs 就地变换（不能流式，整块在 BRAM）
    //   6) Subtract（流式）：逐 limb 从 HBM reload accum 的 2×l 部分，
    //      与 BConv 输出相减，结果逐 limb 写回 HBM
    //   7) 释放 BConv 输出
    //
    // accum_in_bram: accum 是否在 BRAM
    // accum_bytes: accum 大小 = polys×(l+k)×ct_limb_bytes×ct_now
    // 返回最终输出大小（2×l limbs）。
    uint64_t ModDown(uint32_t ct_now, bool accum_in_bram) {
        const uint32_t l = problem.limbs;
        const uint32_t k = problem.num_k;
        const uint32_t p = problem.polys;
        const uint32_t p_l = p * l;
        const uint32_t p_k = p * k;

        const uint64_t per_limb = static_cast<uint64_t>(ct_now) * problem.ct_limb_bytes;
        const uint64_t two_l_bytes = p_l * per_limb;
        const uint64_t two_k_bytes = p_k * per_limb;
        const uint64_t one_limb = per_limb;

        std::cout << "=== ModDown: p*l=" << p_l << " p*k=" << p_k
                  << " accum_in_bram=" << (accum_in_bram ? "yes" : "no") << " ===\n";

        if (accum_in_bram) {
            // accum (p*(l+k)) 在 BRAM。先 spill 2l 部分到 HBM，保留 2k。
            bram.Release(two_l_bytes);
            last_group = Emit("moddown_spill_2l", CycleInstructionKind::StoreHBM,
                CycleTransferPath::SPMToHBM, StageType::Dispatch,
                two_l_bytes, 0, p_l,
                0, -static_cast<int64_t>(two_l_bytes), Deps());
        } else {
            // accum 在 HBM，只 reload 2k 部分。
            bram.Acquire(two_k_bytes);
            last_group = Emit("moddown_reload_2k", CycleInstructionKind::LoadHBM,
                CycleTransferPath::HBMToSPM, StageType::BasisConvert,
                two_k_bytes, 0, p_k,
                static_cast<int64_t>(two_k_bytes), 0, Deps());
        }
        // BRAM = 2k limbs.

        // INTT
        last_group = Emit("moddown_intt", CycleInstructionKind::INTT,
            CycleTransferPath::None, StageType::BasisConvert,
            two_k_bytes, two_k_bytes, p_k,
            0, 0, Deps());

        // BConv: p*k -> p*l. Issue: +2l. Complete: -2k.
        bram.Acquire(two_l_bytes);
        bram.Release(two_k_bytes);
        last_group = Emit("moddown_bconv", CycleInstructionKind::BConv,
            CycleTransferPath::None, StageType::BasisConvert,
            two_k_bytes + two_l_bytes, two_k_bytes + two_l_bytes, p_l,
            static_cast<int64_t>(two_l_bytes),
            -static_cast<int64_t>(two_k_bytes), Deps());
        // BRAM = 2l limbs.

        // NTT
        last_group = Emit("moddown_ntt", CycleInstructionKind::NTT,
            CycleTransferPath::None, StageType::BasisConvert,
            two_l_bytes, two_l_bytes, p_l,
            0, 0, Deps());

        // Subtract (streaming): reload 2l + sub. Issue: +1 limb. Complete: -1 limb.
        bram.Acquire(one_limb);
        bram.Release(one_limb);
        last_group = Emit("moddown_reload_2l", CycleInstructionKind::LoadHBM,
            CycleTransferPath::HBMToSPM, StageType::Multiply,
            two_l_bytes, 0, p_l,
            static_cast<int64_t>(one_limb),
            -static_cast<int64_t>(one_limb), Deps());
        last_group = Emit("moddown_subtract", CycleInstructionKind::EweSub,
            CycleTransferPath::None, StageType::Multiply,
            two_l_bytes, two_l_bytes, p_l,
            0, 0, Deps());

        // Store output. Complete: -2l.
        bram.Release(two_l_bytes);
        last_group = Emit("moddown_store_output", CycleInstructionKind::StoreHBM,
            CycleTransferPath::SPMToHBM, StageType::Dispatch,
            two_l_bytes, 0, p_l,
            0, -static_cast<int64_t>(two_l_bytes), Deps());

        return two_l_bytes;
    }

    // 调试用：打印当前 BRAM 状态。
    void DumpState(const char* label) const {
        std::cout << "  [" << label << "] "
                  << "live=" << bram.Live()
                  << " peak=" << bram.Peak()
                  << " overflow=" << (bram.Overflowed() ? "YES" : "no")
                  << "\n";
    }
};

// Poseidon：按 digit 循环，每个 digit 做 ModUp + InnerProd，digit 间累加。
CycleProgram BuildPoseidonProgram(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware) {

    ExecutionBuilder builder(problem, hardware, KeySwitchMethod::Poseidon);

    const uint32_t ct_now = problem.ciphertexts;
    const uint32_t limb_now = problem.limbs;
    bool accum_in_bram = false;

    for (uint32_t digit_idx = 0; digit_idx < problem.digits; ++digit_idx) {
        builder.LoadInput(ct_now, limb_now);

        bool modup_spilled = false;
        const uint64_t modup_out = builder.ModUp(ct_now,
            static_cast<uint64_t>(ct_now) * limb_now * problem.ct_limb_bytes,
            &modup_spilled);

        const bool is_last = (digit_idx == problem.digits - 1);
        builder.InnerProd(ct_now, modup_out, modup_spilled, is_last, &accum_in_bram);
    }

    builder.ModDown(ct_now, accum_in_bram);

    builder.program.estimated_peak_live_bytes = builder.bram.Peak();
    return std::move(builder.program);
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
    case KeySwitchMethod::DigitCentric:
    case KeySwitchMethod::OutputCentric:
    case KeySwitchMethod::MaxParallel:
        // TODO: 各方法的实现待补充。
        return CycleProgram{};
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
