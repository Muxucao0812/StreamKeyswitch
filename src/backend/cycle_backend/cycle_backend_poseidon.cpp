#include "backend/cycle_backend/cycle_backend_poseidon.h"
#include "backend/cycle_backend/bram_tracker.h"

#include <algorithm>
#include <iostream>
#include <limits>
#include <string>

namespace {

// Memory-aware 执行构建器。
// 在做 memory 决策的同时直接生成 CycleProgram（跳过 TileExecutionStep 中间层）。
// 所有数据流行为硬编码，不依赖 KeySwitchMethodPolicy。

// 1. 定义操作属性描述符 (Descriptor)
struct EmitDesc {
    std::string name;
    CycleInstructionKind kind;
    CycleTransferPath transfer_path = CycleTransferPath::None;
    StageType stage_type;

    // 数据维度
    uint64_t bytes = 0;
    uint32_t input_limbs = 0;
    uint32_t output_limbs = 0;

    // 计算维度
    uint64_t work_items = 0;

    // BRAM 状态变化
    int64_t bram_delta_issue = 0;
    int64_t bram_delta_complete = 0;

    // 依赖关系
    std::vector<uint32_t> deps;

    EmitDesc() = default;

    EmitDesc(
        std::string name_value,
        CycleInstructionKind kind_value,
        CycleTransferPath transfer_path_value,
        StageType stage_type_value,
        uint64_t bytes_value = 0,
        uint32_t input_limbs_value = 0,
        uint32_t output_limbs_value = 0,
        uint64_t work_items_value = 0,
        int64_t bram_delta_issue_value = 0,
        int64_t bram_delta_complete_value = 0,
        std::vector<uint32_t> deps_value = {})
        : name(std::move(name_value))
        , kind(kind_value)
        , transfer_path(transfer_path_value)
        , stage_type(stage_type_value)
        , bytes(bytes_value)
        , input_limbs(input_limbs_value)
        , output_limbs(output_limbs_value)
        , work_items(work_items_value)
        , bram_delta_issue(bram_delta_issue_value)
        , bram_delta_complete(bram_delta_complete_value)
        , deps(std::move(deps_value)) {}
};

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
        , bram(
              p.bram_budget_bytes > p.bram_guard_bytes
                  ? p.bram_budget_bytes - p.bram_guard_bytes
                  : 0) {
        program.method = method;
        program.name = "stream_keyswitch";
    }

    uint32_t Emit(const EmitDesc& desc) {
        uint32_t micro_ops = 1;

        switch (desc.kind) {
        case CycleInstructionKind::NTT:
        case CycleInstructionKind::INTT:
        case CycleInstructionKind::EweMul:
        case CycleInstructionKind::EweAdd:
        case CycleInstructionKind::EweSub:
            micro_ops = std::max<uint32_t>(1, desc.input_limbs);
            break;
        case CycleInstructionKind::BConv:
            micro_ops = std::max<uint32_t>(1, desc.input_limbs * desc.output_limbs);
            break;
        case CycleInstructionKind::LoadHBM:
            micro_ops = std::max<uint32_t>(1, desc.input_limbs);
            break;
        case CycleInstructionKind::StoreHBM:
            micro_ops = std::max<uint32_t>(1, desc.output_limbs);
            break;
        default:
            micro_ops = 1;
        }

        const uint64_t bytes_per_op =
            (micro_ops > 0) ? (desc.bytes / micro_ops) : desc.bytes;
        const uint64_t work_per_op =
            (micro_ops > 0) ? (desc.work_items / micro_ops) : desc.work_items;

        const uint64_t per_op_cycles = std::max<uint64_t>(
            1,
            EstimateCycles(
                /*kind*/ desc.kind,
                /*path*/ desc.transfer_path,
                /*bytes*/ bytes_per_op,
                /*input_limbs*/ 1,
                /*output_limbs*/ 1));
        const uint64_t total_cycles = EstimateCycles(
            /*kind*/ desc.kind,
            /*path*/ desc.transfer_path,
            /*bytes*/ desc.bytes,
            /*input_limbs*/ desc.input_limbs,
            /*output_limbs*/ desc.output_limbs);

        CycleInstructionGroup group;
        group.id = static_cast<uint32_t>(program.groups.size());
        group.name = desc.name;
        group.kind = desc.kind;
        group.transfer_path = desc.transfer_path;
        group.stage_type = desc.stage_type;
        group.bytes = desc.bytes;
        group.work_items = desc.work_items;
        group.live_bytes_delta_on_issue = desc.bram_delta_issue;
        group.live_bytes_delta_on_complete = desc.bram_delta_complete;
        group.dependencies = desc.deps;

        for (uint32_t i = 0; i < micro_ops; ++i) {
            CycleInstruction instr;
            instr.id = next_instruction_id++;
            instr.group_id = group.id;
            instr.kind = desc.kind;
            instr.transfer_path = desc.transfer_path;
            instr.stage_type = desc.stage_type;
            instr.bytes = bytes_per_op;
            instr.work_items = work_per_op;
            instr.latency_cycles = per_op_cycles;
            group.instructions.push_back(std::move(instr));
        }

        const uint32_t gid = group.id;
        program.instruction_count += micro_ops;
        program.groups.push_back(std::move(group));

        std::cout << "  [group " << gid << "] " << desc.name
                  << "  kind=" << ToString(desc.kind)
                  << "  micro_ops=" << micro_ops
                  << "  per_op_cycles=" << per_op_cycles
                  << "  total_cycles=" << total_cycles
                  << "  bytes=" << desc.bytes
                  << "  bram_live=" << bram.Live()
                  << "\n";

        return gid;
    }

    uint64_t EstimateCycles(
        CycleInstructionKind kind,
        CycleTransferPath path,
        uint64_t bytes,
        uint32_t input_limbs,
        uint32_t output_limbs) const {
        switch (kind) {
        case CycleInstructionKind::LoadHBM:
            return hardware.EstimateTransferCycles(
                (path == CycleTransferPath::HostToHBM) ? HardwareTransferPath::HostToHBM
                                                       : HardwareTransferPath::HBMToSPM,
                bytes);
        case CycleInstructionKind::StoreHBM:
            return hardware.EstimateTransferCycles(HardwareTransferPath::SPMToHBM, bytes);
        case CycleInstructionKind::NTT:
            return hardware.EstimateNttCycles(problem, input_limbs);
        case CycleInstructionKind::INTT:
            return hardware.EstimateInttCycles(problem, input_limbs);
        case CycleInstructionKind::BConv:
            return hardware.EstimateBconvCycles(problem, input_limbs, output_limbs);
        case CycleInstructionKind::EweMul:
            return hardware.EstimateEweMulCycles(problem, input_limbs);
        case CycleInstructionKind::EweAdd:
            return hardware.EstimateEweAddCycles(problem, input_limbs);
        case CycleInstructionKind::EweSub:
            return hardware.EstimateEweSubCycles(problem, input_limbs);
        default:
            return 1;
        }
    }

    uint32_t last_group = std::numeric_limits<uint32_t>::max();
    bool accum_initialized = false;
    std::vector<uint32_t> Deps() const {
        if (last_group == std::numeric_limits<uint32_t>::max()) return {};
        return {last_group};
    }

    uint64_t PoseidonLoadInput(
        uint32_t ct_now,
        uint32_t poly_now,
        uint32_t limb_now) {
        const uint64_t ct_chunk_bytes =
            static_cast<uint64_t>(ct_now) * poly_now * limb_now * problem.ct_limb_bytes;
        std::cout << "=== PoseidonLoadInput: ct=" << ct_now << " l=" << limb_now
                  << " bytes=" << ct_chunk_bytes << " ===\n";
        bram.Acquire(ct_chunk_bytes);

        last_group = Emit(EmitDesc(
            /*name=*/"load_input",
            /*kind=*/CycleInstructionKind::LoadHBM,
            /*transfer_path=*/CycleTransferPath::HBMToSPM,
            /*stage_type=*/StageType::Dispatch,
            /*bytes=*/ct_chunk_bytes,
            /*input_limbs=*/limb_now,
            /*output_limbs=*/0,
            /*work_items=*/0,
            /*bram_delta_issue=*/static_cast<int64_t>(ct_chunk_bytes),
            /*bram_delta_complete=*/static_cast<int64_t>(ct_chunk_bytes),
            /*deps=*/Deps()));
        return ct_chunk_bytes;
    }

    // PoseidonModUp 子图：对一个 digit 的输入做 INTT → BConv → NTT。
    //
    // 数据维度变化（per ciphertext, per digit）：
    //   INTT:  l limbs → l limbs（就地变换）
    //   BConv: l limbs → k limbs（矩阵乘法，生成额外 limbs）
    //   NTT:   l+k limbs → l+k limbs（就地变换）
    //
    // is_last_digit: 最后一个 digit 可以跳过 spill，输出直接留在 BRAM 给 PoseidonInnerProd 用。
    // 返回 PoseidonModUp 输出占用的字节数。
    uint64_t PoseidonModUp(
        uint32_t ct_now,
        uint64_t input_bram_bytes,
        bool* spilled) {
        const uint32_t l = problem.digit_limbs;  // l
        const uint32_t k = problem.num_k;
        const uint64_t k_limbs_bytes =
            static_cast<uint64_t>(ct_now) * k * problem.ct_limb_bytes;
        const uint64_t modup_output_bytes = input_bram_bytes + k_limbs_bytes;

        std::cout << "=== PoseidonModUp: l=" << l << " k=" << k << " input=" << input_bram_bytes
                  << " output=" << modup_output_bytes << " ===\n";

        // INTT: 对 l 个 limbs 做 INTT
        last_group = Emit(EmitDesc(
            /*name=*/"modup_intt",
            /*kind=*/CycleInstructionKind::INTT,
            /*transfer_path=*/CycleTransferPath::None,
            /*stage_type=*/StageType::BasisConvert,
            /*bytes=*/input_bram_bytes,
            /*input_limbs=*/l,
            /*output_limbs=*/0,
            /*work_items=*/0,
            /*bram_delta_issue=*/0,
            /*bram_delta_complete=*/0,
            /*deps=*/Deps()));

        // BConv: l limbs → k limbs
        bram.Acquire(k_limbs_bytes);
        last_group = Emit(EmitDesc(
            /*name=*/"modup_bconv",
            /*kind=*/CycleInstructionKind::BConv,
            /*transfer_path=*/CycleTransferPath::None,
            /*stage_type=*/StageType::BasisConvert,
            /*bytes=*/modup_output_bytes,
            /*input_limbs=*/l,
            /*output_limbs=*/k,
            /*work_items=*/0,
            /*bram_delta_issue=*/0,
            /*bram_delta_complete=*/static_cast<int64_t>(k_limbs_bytes),
            /*deps=*/Deps()));

        // NTT: 对 l+k 个 limbs 做 NTT
        last_group = Emit(EmitDesc(
            /*name=*/"modup_ntt",
            /*kind=*/CycleInstructionKind::NTT,
            /*transfer_path=*/CycleTransferPath::None,
            /*stage_type=*/StageType::BasisConvert,
            /*bytes=*/modup_output_bytes,
            /*input_limbs=*/l + k,
            /*output_limbs=*/0,
            /*work_items=*/0,
            /*bram_delta_issue=*/0,
            /*bram_delta_complete=*/0,
            /*deps=*/Deps()));

        // Spill 判断
        const uint64_t one_limb_key = problem.key_digit_limb_bytes;
        const bool should_spill = !bram.CanAcquire(modup_output_bytes + one_limb_key);

        // 判断是否需要spill，如果需要，先释放 modup 输出占用的 BRAM，再 emit spill 指令将其写回 HBM。
        if (should_spill) {
            bram.Release(modup_output_bytes);
            last_group = Emit(EmitDesc(
                /*name=*/"modup_spill",
                /*kind=*/CycleInstructionKind::StoreHBM,
                /*transfer_path=*/CycleTransferPath::SPMToHBM,
                /*stage_type=*/StageType::Dispatch,
                /*bytes=*/modup_output_bytes,
                /*input_limbs=*/0,
                /*output_limbs=*/l + k,
                /*work_items=*/0,
                /*bram_delta_issue=*/0,
                /*bram_delta_complete=*/-static_cast<int64_t>(modup_output_bytes),
                /*deps=*/Deps()));
            *spilled = true;
        } else {
            *spilled = false;
        }
        return modup_output_bytes;
    }

    // PoseidonInnerProd（简化整块版）：
    //
    // 当前策略：
    // 1) modup 若 spilled，则整块 reload 到 BRAM；
    // 2) 一次性加载 p*(l+k) 个 key；
    // 3) 一次性完成 inner-product 乘法；
    // 4) 非最后 digit：partial spill 到 HBM；
    //    最后 digit：partial 保留在 BRAM，作为后续 cross-digit reduce 的初始 LHS。
    //
    // cross-digit add 不在本函数内做，后续单独阶段统一处理。
    //
    // modup_spilled: PoseidonModUp 输出是否在 HBM（true=整块 reload，false=已在 BRAM）
    // accum_in_bram: 调用方维护
    // 返回 accum 的字节数。
    uint64_t PoseidonInnerProd(
        uint32_t ct_now,
        uint64_t modup_output_bytes,
        bool modup_spilled,
        bool is_last_digit,
        bool* accum_in_bram) {
        // 简化版本：
        // 1) modup 若在 HBM，则整块 reload。
        // 2) 一次性加载 p*(l+k) 个 key。
        // 3) 直接做整块 inner-product 乘法。
        // 4) 非最后 digit spill；最后 digit 留 BRAM（供第一次 reduce 直接使用）。
        const uint32_t lk = problem.key_limbs;  // l+k
        const uint32_t p = problem.polys;
        const uint32_t p_lk = p * lk;  // accum 的 limb 数

        const uint64_t accum_bytes = static_cast<uint64_t>(ct_now) * p_lk * problem.ct_limb_bytes;
        const uint64_t total_key_bytes = static_cast<uint64_t>(p_lk) * problem.key_digit_limb_bytes;

        std::cout << "=== PoseidonInnerProd: modup_limbs=" << lk
                  << " key_limbs=" << lk
                  << " accum_limbs=" << p_lk
                  << " spilled=" << (modup_spilled ? "yes" : "no")
                  << " last=" << (is_last_digit ? "yes" : "no")
                  << " ===\n";

        if (modup_spilled) {
            bram.Acquire(modup_output_bytes);
            last_group = Emit(EmitDesc(
                /*name=*/"innerprod_reload_modup_all",
                /*kind=*/CycleInstructionKind::LoadHBM,
                /*transfer_path=*/CycleTransferPath::HBMToSPM,
                /*stage_type=*/StageType::Multiply,
                /*bytes=*/modup_output_bytes,
                /*input_limbs=*/lk,
                /*output_limbs=*/0,
                /*work_items=*/0,
                /*bram_delta_issue=*/static_cast<int64_t>(modup_output_bytes),
                /*bram_delta_complete=*/0,
                /*deps=*/Deps()));
        }

        // 一次性加载全部 key。
        bram.Acquire(total_key_bytes);
        last_group = Emit(EmitDesc(
            /*name=*/"innerprod_load_key_all",
            /*kind=*/CycleInstructionKind::LoadHBM,
            /*transfer_path=*/CycleTransferPath::HBMToSPM,
            /*stage_type=*/StageType::Multiply,
            /*bytes=*/total_key_bytes,
            /*input_limbs=*/p_lk,
            /*output_limbs=*/0,
            /*work_items=*/0,
            /*bram_delta_issue=*/static_cast<int64_t>(total_key_bytes),
            /*bram_delta_complete=*/0,
            /*deps=*/Deps()));

        // 乘法输出先落在 BRAM（后续统一 spill）。
        bram.Acquire(accum_bytes);
        last_group = Emit(EmitDesc(
            /*name=*/"innerprod_mul_all",
            /*kind=*/CycleInstructionKind::EweMul,
            /*transfer_path=*/CycleTransferPath::None,
            /*stage_type=*/StageType::Multiply,
            /*bytes=*/modup_output_bytes + total_key_bytes,
            /*input_limbs=*/p_lk,
            /*output_limbs=*/0,
            /*work_items=*/0,
            /*bram_delta_issue=*/static_cast<int64_t>(accum_bytes),
            /*bram_delta_complete=*/0,
            /*deps=*/Deps()));

        // 释放本轮输入窗口（modup + key）。
        bram.Release(total_key_bytes);
        bram.Release(modup_output_bytes);

        if (!is_last_digit) {
            // 非最后 digit：partial 写回 HBM，等待 reduce 阶段再读。
            bram.Release(accum_bytes);
            last_group = Emit(EmitDesc(
                /*name=*/"innerprod_spill_partial",
                /*kind=*/CycleInstructionKind::StoreHBM,
                /*transfer_path=*/CycleTransferPath::SPMToHBM,
                /*stage_type=*/StageType::Dispatch,
                /*bytes=*/accum_bytes,
                /*input_limbs=*/0,
                /*output_limbs=*/p_lk,
                /*work_items=*/0,
                /*bram_delta_issue=*/0,
                /*bram_delta_complete=*/-static_cast<int64_t>(accum_bytes),
                /*deps=*/Deps()));
            *accum_in_bram = false;
        } else {
            // 最后 digit：累加值留在 BRAM，第一次 reduce 不需要 reload LHS。
            *accum_in_bram = true;
        }

        accum_initialized = *accum_in_bram;

        return accum_bytes;
    }

    // PoseidonReduceCrossDigitAdd：
    // 每次把一个新的 digit partial（HBM）加到当前累加值（HBM/BRAM）上。
    // - 第一次调用时，若上一步最后一个 innerprod 把 partial 留在 BRAM，则可直接做 LHS；
    // - 只有最后一次归并才写回 HBM；中间轮次把累计值留在 BRAM。
    uint64_t PoseidonReduceCrossDigitAdd(
        uint32_t ct_now, 
        bool is_last_reduce,
        bool* accum_in_bram
    ) {
        const uint32_t p_lk = problem.polys * problem.key_limbs;
        const uint64_t accum_bytes =
            static_cast<uint64_t>(ct_now) * p_lk * problem.ct_limb_bytes;

        std::cout << "=== PoseidonReduceCrossDigitAdd: accum_limbs=" << p_lk
                  << " last_reduce=" << (is_last_reduce ? "yes" : "no")
                  << " accum_in_bram=" << ((*accum_in_bram) ? "yes" : "no")
                  << " ===\n";

        // LHS：当前累计和。若不在 BRAM，则先 reload。
        if (!(*accum_in_bram)) {
            bram.Acquire(accum_bytes);
            last_group = Emit(EmitDesc(
                /*name=*/"reduce_reload_accum_lhs",
                /*kind=*/CycleInstructionKind::LoadHBM,
                /*transfer_path=*/CycleTransferPath::HBMToSPM,
                /*stage_type=*/StageType::Multiply,
                /*bytes=*/accum_bytes,
                /*input_limbs=*/p_lk,
                /*output_limbs=*/0,
                /*work_items=*/0,
                /*bram_delta_issue=*/static_cast<int64_t>(accum_bytes),
                /*bram_delta_complete=*/0,
                /*deps=*/Deps()));
            *accum_in_bram = true;
        }

        // RHS：本次新增的 digit partial（总是在 HBM）。
        bram.Acquire(accum_bytes);
        last_group = Emit(EmitDesc(
            /*name=*/"reduce_reload_partial_rhs",
            /*kind=*/CycleInstructionKind::LoadHBM,
            /*transfer_path=*/CycleTransferPath::HBMToSPM,
            /*stage_type=*/StageType::Multiply,
            /*bytes=*/accum_bytes,
            /*input_limbs=*/p_lk,
            /*output_limbs=*/0,
            /*work_items=*/0,
            /*bram_delta_issue=*/static_cast<int64_t>(accum_bytes),
            /*bram_delta_complete=*/0,
            /*deps=*/Deps()));

        // LHS += RHS；加完 RHS 可释放，LHS 保留为新的累计和。
        bram.Release(accum_bytes);
        last_group = Emit(EmitDesc(
            /*name=*/"reduce_cross_digit_add",
            /*kind=*/CycleInstructionKind::EweAdd,
            /*transfer_path=*/CycleTransferPath::None,
            /*stage_type=*/StageType::Multiply,
            /*bytes=*/accum_bytes,
            /*input_limbs=*/p_lk,
            /*output_limbs=*/0,
            /*work_items=*/0,
            /*bram_delta_issue=*/0,
            /*bram_delta_complete=*/-static_cast<int64_t>(accum_bytes),
            /*deps=*/Deps()));

        *accum_in_bram = true;
  
        return accum_bytes;
    }

    // PoseidonModDown：将 accum = 2×(l+k) limbs 变换回 2×l limbs 的最终输出。
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
    uint64_t PoseidonModDown(uint32_t ct_now, bool accum_in_bram) {
        const uint32_t l = problem.limbs;
        const uint32_t k = problem.num_k;
        const uint32_t p = problem.polys;
        const uint32_t p_l = p * l;
        const uint32_t p_k = p * k;

        const uint64_t per_limb = static_cast<uint64_t>(ct_now) * problem.ct_limb_bytes;
        const uint64_t two_l_bytes = p_l * per_limb;
        const uint64_t two_k_bytes = p_k * per_limb;
        const uint64_t one_limb = per_limb;

        std::cout << "=== PoseidonModDown: p*l=" << p_l << " p*k=" << p_k
                  << " accum_in_bram=" << (accum_in_bram ? "yes" : "no") << " ===\n";

        if (accum_in_bram) {
            // accum (p*(l+k)) 在 BRAM。先 spill 2l 部分到 HBM，保留 2k。
            bram.Release(two_l_bytes);
            last_group = Emit(EmitDesc(
                /*name=*/"moddown_spill_2l",
                /*kind=*/CycleInstructionKind::StoreHBM,
                /*transfer_path=*/CycleTransferPath::SPMToHBM,
                /*stage_type=*/StageType::Dispatch,
                /*bytes=*/two_l_bytes,
                /*input_limbs=*/p_l,
                /*output_limbs=*/0,
                /*work_items=*/0,
                /*bram_delta_issue=*/-static_cast<int64_t>(two_l_bytes),
                /*bram_delta_complete=*/0,
                /*deps=*/Deps()));
        } else {
            // accum 在 HBM，只 reload 2k 部分。
            bram.Acquire(two_k_bytes);
            last_group = Emit(EmitDesc(
                /*name=*/"moddown_reload_2k",
                /*kind=*/CycleInstructionKind::LoadHBM,
                /*transfer_path=*/CycleTransferPath::HBMToSPM,
                /*stage_type=*/StageType::BasisConvert,
                /*bytes=*/two_k_bytes,
                /*input_limbs=*/p_k,
                /*output_limbs=*/0,
                /*work_items=*/0,
                /*bram_delta_issue=*/static_cast<int64_t>(two_k_bytes),
                /*bram_delta_complete=*/0,
                /*deps=*/Deps()));
        }

        // INTT
        last_group = Emit(EmitDesc(
            /*name=*/"moddown_intt",
            /*kind=*/CycleInstructionKind::INTT,
            /*transfer_path=*/CycleTransferPath::None,
            /*stage_type=*/StageType::BasisConvert,
            /*bytes=*/two_k_bytes,
            /*input_limbs=*/p_k,
            /*output_limbs=*/0,
            /*work_items=*/0,
            /*bram_delta_issue=*/0,
            /*bram_delta_complete=*/0,
            /*deps=*/Deps()));

        // BConv: p*k -> p*l. Issue: +2l. Complete: -2k.
        bram.Acquire(two_l_bytes);
        bram.Release(two_k_bytes);
        last_group = Emit(EmitDesc(
            /*name=*/"moddown_bconv",
            /*kind=*/CycleInstructionKind::BConv,
            /*transfer_path=*/CycleTransferPath::None,
            /*stage_type=*/StageType::BasisConvert,
            /*bytes=*/two_k_bytes + two_l_bytes,
            /*input_limbs=*/p_k,
            /*output_limbs=*/p_l,
            /*work_items=*/0,
            /*bram_delta_issue=*/static_cast<int64_t>(two_l_bytes),
            /*bram_delta_complete=*/-static_cast<int64_t>(two_k_bytes),
            /*deps=*/Deps()));

        // NTT
        last_group = Emit(EmitDesc(
            /*name=*/"moddown_ntt",
            /*kind=*/CycleInstructionKind::NTT,
            /*transfer_path=*/CycleTransferPath::None,
            /*stage_type=*/StageType::BasisConvert,
            /*bytes=*/two_l_bytes,
            /*input_limbs=*/p_l,
            /*output_limbs=*/0,
            /*work_items=*/0,
            /*bram_delta_issue=*/0,
            /*bram_delta_complete=*/0,
            /*deps=*/Deps()));

        // Subtract (streaming): reload 2l + sub. Issue: +1 limb. Complete: -1 limb.
        bram.Acquire(one_limb);
        bram.Release(one_limb);
        last_group = Emit(EmitDesc(
            /*name=*/"moddown_reload_2l",
            /*kind=*/CycleInstructionKind::LoadHBM,
            /*transfer_path=*/CycleTransferPath::HBMToSPM,
            /*stage_type=*/StageType::Multiply,
            /*bytes=*/two_l_bytes,
            /*input_limbs=*/p_l,
            /*output_limbs=*/0,
            /*work_items=*/0,
            /*bram_delta_issue=*/static_cast<int64_t>(one_limb),
            /*bram_delta_complete=*/-static_cast<int64_t>(one_limb),
            /*deps=*/Deps()));

        last_group = Emit(EmitDesc(
            /*name=*/"moddown_subtract",
            /*kind=*/CycleInstructionKind::EweSub,
            /*transfer_path=*/CycleTransferPath::None,
            /*stage_type=*/StageType::Multiply,
            /*bytes=*/two_l_bytes,
            /*input_limbs=*/p_l,
            /*output_limbs=*/0,
            /*work_items=*/0,
            /*bram_delta_issue=*/0,
            /*bram_delta_complete=*/0,
            /*deps=*/Deps()));

        // Store output. Complete: -2l.
        bram.Release(two_l_bytes);
        last_group = Emit(EmitDesc(
            /*name=*/"moddown_store_output",
            /*kind=*/CycleInstructionKind::StoreHBM,
            /*transfer_path=*/CycleTransferPath::SPMToHBM,
            /*stage_type=*/StageType::Dispatch,
            /*bytes=*/two_l_bytes,
            /*input_limbs=*/0,
            /*output_limbs=*/p_l,
            /*work_items=*/0,
            /*bram_delta_issue=*/0,
            /*bram_delta_complete=*/-static_cast<int64_t>(two_l_bytes),
            /*deps=*/Deps()));

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

} // namespace

// Poseidon：按 digit 循环，每个 digit 做 PoseidonModUp + PoseidonInnerProd，digit 间累加。
CycleProgram BuildPoseidonProgram(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware) {
    ExecutionBuilder builder(problem, hardware, KeySwitchMethod::Poseidon);

    const uint32_t ct_now = problem.ciphertexts;
    const uint32_t digit_limb_now = problem.digit_limbs;

    bool accum_in_bram = false;

    // ct <b,a>
    for (uint32_t digit_idx = 0; digit_idx < problem.digits; ++digit_idx) {
        // only process a
        builder.PoseidonLoadInput(ct_now, 1, digit_limb_now);

        bool modup_spilled = false;
        const uint64_t modup_out = builder.PoseidonModUp(
            ct_now,
            static_cast<uint64_t>(ct_now) * 1 * digit_limb_now * problem.ct_limb_bytes,
            &modup_spilled);

        const bool is_last = (digit_idx == problem.digits - 1);
        builder.PoseidonInnerProd(ct_now, modup_out, modup_spilled, is_last, &accum_in_bram);
    }
    // cross-digit reduce：
    // 最后一个 innerprod 结果留在 BRAM 作为初始 LHS；
    // 其余 (digits-1) 个 partial 从 HBM 逐个加进来。
    for (uint32_t digit_idx = 1; digit_idx < problem.digits; ++digit_idx) {
        const bool is_last_reduce = (digit_idx == problem.digits - 1);
        builder.PoseidonReduceCrossDigitAdd(ct_now, is_last_reduce, &accum_in_bram);
    }

    builder.PoseidonModDown(ct_now, accum_in_bram);

    builder.program.estimated_peak_live_bytes = builder.bram.Peak();
    return std::move(builder.program);
}
