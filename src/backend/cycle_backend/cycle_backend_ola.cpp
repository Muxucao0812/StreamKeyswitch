#include "backend/cycle_backend/cycle_backend_ola.h"
#include "backend/cycle_backend/bram_tracker.h"

#include <algorithm>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace {

struct OLAEmitDesc {
    std::string name;
    CycleInstructionKind kind;
    CycleTransferPath transfer_path = CycleTransferPath::None;
    TileExecutionStepType source_step_type = TileExecutionStepType::InputHBMToBRAM;
    StageType stage_type = StageType::Dispatch;
    uint64_t bytes = 0;
    uint32_t input_limbs = 0;
    uint32_t output_limbs = 0;
    uint64_t work_items = 0;
    int64_t bram_delta_issue = 0;
    int64_t bram_delta_complete = 0;
    std::vector<uint32_t> deps;
};

struct OLAExecutionBuilder {
    const KeySwitchProblem& problem;
    const HardwareModel& hardware;
    BramTracker bram;
    CycleProgram program;
    uint64_t next_instruction_id = 0;
    uint32_t last_group = std::numeric_limits<uint32_t>::max();
    std::vector<uint64_t> digit_bytes;
    std::vector<bool> digit_in_bram;
    std::vector<bool> digit_intt_done;
    std::vector<bool> digit_bconv_done;
    std::vector<bool> digit_ntt_done;
    bool build_ok = true;

    OLAExecutionBuilder(
        const KeySwitchProblem& p,
        const HardwareModel& hw)
        : problem(p)
        , hardware(hw)
        , bram(
            p.bram_budget_bytes > p.bram_guard_bytes
                ? (p.bram_budget_bytes - p.bram_guard_bytes)
                : 0) {
        program.method = KeySwitchMethod::OLA;
        program.name = "ola_keyswitch";

        const uint32_t safe_digits = std::max<uint32_t>(1, problem.digits);
        digit_bytes.assign(safe_digits, 0);
        digit_in_bram.assign(safe_digits, false);
        digit_intt_done.assign(safe_digits, false);
        digit_bconv_done.assign(safe_digits, false);
        digit_ntt_done.assign(safe_digits, false);
    }

    bool Ok() const { return build_ok && !bram.Overflowed(); }

    std::vector<uint32_t> Deps() const {
        if (last_group == std::numeric_limits<uint32_t>::max()) {
            return {};
        }
        return {last_group};
    }

    uint32_t EstimateCycles(
        CycleInstructionKind kind,
        CycleTransferPath path,
        uint64_t bytes,
        uint32_t input_limbs,
        uint32_t output_limbs) const {
        switch (kind) {
        case CycleInstructionKind::LoadHBM:
            return hardware.EstimateTransferCycles(
                (path == CycleTransferPath::HostToHBM)
                    ? HardwareTransferPath::HostToHBM
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

    uint32_t LimbsForCiphertextBytes(uint64_t bytes) const {
        const uint64_t per_limb_bytes =
            static_cast<uint64_t>(std::max<uint32_t>(1, problem.ciphertexts))
            * std::max<uint64_t>(1, problem.ct_limb_bytes);
        const uint64_t limbs = (bytes + per_limb_bytes - 1) / per_limb_bytes;
        if (limbs == 0) {
            return 1;
        }
        if (limbs > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
            return std::numeric_limits<uint32_t>::max();
        }
        return static_cast<uint32_t>(limbs);
    }

    uint32_t Emit(const OLAEmitDesc& desc) {
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
            micro_ops = std::max<uint32_t>(
                1,
                desc.input_limbs * std::max<uint32_t>(1, desc.output_limbs));
            break;
        case CycleInstructionKind::LoadHBM:
            micro_ops = std::max<uint32_t>(1, desc.input_limbs);
            break;
        case CycleInstructionKind::StoreHBM:
            micro_ops = std::max<uint32_t>(1, desc.output_limbs);
            break;
        default:
            micro_ops = 1;
            break;
        }

        const uint64_t bytes_per_op =
            (micro_ops > 0) ? (desc.bytes / micro_ops) : desc.bytes;
        const uint64_t work_per_op =
            (micro_ops > 0) ? (desc.work_items / micro_ops) : desc.work_items;
        const uint64_t per_op_cycles = std::max<uint64_t>(
            1,
            EstimateCycles(
                desc.kind,
                desc.transfer_path,
                bytes_per_op,
                /*input_limbs=*/1,
                /*output_limbs=*/1));

        CycleInstructionGroup group;
        group.id = static_cast<uint32_t>(program.groups.size());
        group.name = desc.name;
        group.kind = desc.kind;
        group.transfer_path = desc.transfer_path;
        group.source_step_type = desc.source_step_type;
        group.stage_type = desc.stage_type;
        group.bytes = desc.bytes;
        group.work_items = desc.work_items;
        group.live_bytes_delta_on_issue = desc.bram_delta_issue;
        group.live_bytes_delta_on_complete = desc.bram_delta_complete;
        group.dependencies = desc.deps;

        for (uint32_t op_idx = 0; op_idx < micro_ops; ++op_idx) {
            CycleInstruction instruction;
            instruction.id = next_instruction_id++;
            instruction.group_id = group.id;
            instruction.kind = desc.kind;
            instruction.transfer_path = desc.transfer_path;
            instruction.source_step_type = desc.source_step_type;
            instruction.stage_type = desc.stage_type;
            instruction.bytes = bytes_per_op;
            instruction.work_items = work_per_op;
            instruction.latency_cycles = per_op_cycles;
            group.instructions.push_back(std::move(instruction));
        }

        const uint32_t group_id = group.id;
        program.instruction_count += micro_ops;
        program.groups.push_back(std::move(group));
        last_group = group_id;
        return group_id;
    }

    bool SpillOneDigit(uint32_t avoid_digit_idx) {
        for (uint32_t idx = 0; idx < digit_in_bram.size(); ++idx) {
            if (idx == avoid_digit_idx) {
                continue;
            }
            if (!digit_in_bram[idx]) {
                continue;
            }
            if (digit_bytes[idx] == 0) {
                continue;
            }
            const uint64_t bytes = digit_bytes[idx];
            bram.Release(bytes);
            digit_in_bram[idx] = false;

            Emit(OLAEmitDesc{
                "ola_spill_digit_stub",
                CycleInstructionKind::StoreHBM,
                CycleTransferPath::SPMToHBM,
                TileExecutionStepType::IntermediateBRAMToHBM,
                StageType::Dispatch,
                bytes,
                0,
                LimbsForCiphertextBytes(bytes),
                0,
                0,
                -static_cast<int64_t>(bytes),
                Deps()});
            return true;
        }

        return false;
    }

    bool OLASpillDigitIfResident(uint32_t digit_idx) {
        if (digit_idx >= digit_in_bram.size()) {
            build_ok = false;
            return false;
        }
        if (!digit_in_bram[digit_idx]) {
            return true;
        }

        const uint64_t bytes = digit_bytes[digit_idx];
        if (bytes == 0) {
            build_ok = false;
            return false;
        }

        bram.Release(bytes);
        digit_in_bram[digit_idx] = false;

        Emit(OLAEmitDesc{
            "ola_spill_digit_stub",
            CycleInstructionKind::StoreHBM,
            CycleTransferPath::SPMToHBM,
            TileExecutionStepType::IntermediateBRAMToHBM,
            StageType::Dispatch,
            bytes,
            0,
            LimbsForCiphertextBytes(bytes),
            0,
            0,
            -static_cast<int64_t>(bytes),
            Deps()});

        return true;
    }

    bool OLASpillDigitsKeepOne(uint32_t keep_digit_idx) {
        for (uint32_t idx = 0; idx < digit_in_bram.size(); ++idx) {
            if (idx == keep_digit_idx) {
                continue;
            }
            if (!OLASpillDigitIfResident(idx)) {
                return false;
            }
        }
        return true;
    }

    bool EnsureCapacityForBytes(uint64_t bytes, uint32_t avoid_digit_idx) {
        if (bytes == 0) {
            return true;
        }
        if (bytes > bram.Budget()) {
            build_ok = false;
            return false;
        }

        while (!bram.CanAcquire(bytes)) {
            if (!SpillOneDigit(avoid_digit_idx)) {
                build_ok = false;
                return false;
            }
        }
        return true;
    }

    uint64_t OLALoadInput(
        uint32_t digit_idx,
        uint32_t ct_now,
        uint32_t poly_now,
        uint32_t limb_now) {
        if (digit_idx >= digit_bytes.size()) {
            build_ok = false;
            return 0;
        }

        const uint64_t input_bytes =
            static_cast<uint64_t>(ct_now) * poly_now * limb_now * problem.ct_limb_bytes;
        if (!EnsureCapacityForBytes(input_bytes, digit_idx)) {
            return 0;
        }

        bram.Acquire(input_bytes);
        digit_bytes[digit_idx] = input_bytes;
        digit_in_bram[digit_idx] = true;

        Emit(OLAEmitDesc{
            "ola_load_input",
            CycleInstructionKind::LoadHBM,
            CycleTransferPath::HBMToSPM,
            TileExecutionStepType::InputHBMToBRAM,
            StageType::Dispatch,
            input_bytes,
            limb_now,
            0,
            0,
            static_cast<int64_t>(input_bytes),
            0,
            Deps()});

        return input_bytes;
    }

    bool OLAEnsureDigitResident(uint32_t digit_idx) {
        if (digit_idx >= digit_bytes.size()) {
            build_ok = false;
            return false;
        }
        if (digit_in_bram[digit_idx]) {
            return true;
        }

        const uint64_t bytes = digit_bytes[digit_idx];
        if (bytes == 0) {
            build_ok = false;
            return false;
        }
        if (!EnsureCapacityForBytes(bytes, digit_idx)) {
            return false;
        }

        bram.Acquire(bytes);
        digit_in_bram[digit_idx] = true;

        Emit(OLAEmitDesc{
            "ola_reload_digit_stub",
            CycleInstructionKind::LoadHBM,
            CycleTransferPath::HBMToSPM,
            TileExecutionStepType::IntermediateHBMToBRAM,
            StageType::Dispatch,
            bytes,
            LimbsForCiphertextBytes(bytes),
            0,
            0,
            static_cast<int64_t>(bytes),
            0,
            Deps()});
        return true;
    }

    uint64_t OLAInttDigits(
        uint32_t digit_idx,
        uint32_t ct_now,
        uint32_t limb_now) {
        (void)ct_now;
        (void)limb_now;
        if (!OLAEnsureDigitResident(digit_idx)) {
            return 0;
        }

        const uint64_t intt_bytes = digit_bytes[digit_idx];
        digit_intt_done[digit_idx] = true;

        Emit(OLAEmitDesc{
            "ola_intt_digits_stub",
            CycleInstructionKind::INTT,
            CycleTransferPath::None,
            TileExecutionStepType::ModUpInttTile,
            StageType::Decompose,
            intt_bytes,
            LimbsForCiphertextBytes(intt_bytes),
            0,
            0,
            0,
            0,
            Deps()});

        return intt_bytes;
    }

    uint64_t OLABconvDigits(
        uint32_t digit_idx,
        uint32_t ct_now,
        uint32_t limb_now) {
        (void)ct_now;
        (void)limb_now;
        if (!OLAEnsureDigitResident(digit_idx)) {
            return 0;
        }

        const uint64_t bconv_input_bytes = digit_bytes[digit_idx];
        const uint64_t bconv_output_bytes = static_cast<uint64_t>(ct_now)
            * problem.key_limbs * problem.ct_limb_bytes;
        if (bconv_output_bytes < bconv_input_bytes) {
            build_ok = false;
            return 0;
        }
        const uint64_t bconv_extra_bytes = bconv_output_bytes - bconv_input_bytes;
        if (!EnsureCapacityForBytes(bconv_extra_bytes, digit_idx)) {
            return 0;
        }
        if (bconv_extra_bytes > 0) {
            bram.Acquire(bconv_extra_bytes);
        }
        digit_bytes[digit_idx] = bconv_output_bytes;
        digit_bconv_done[digit_idx] = true;

        Emit(OLAEmitDesc{
            "ola_bconv_digits_stub",
            CycleInstructionKind::BConv,
            CycleTransferPath::None,
            TileExecutionStepType::ModUpBConvTile,
            StageType::BasisConvert,
            bconv_output_bytes,
            problem.digit_limbs,
            problem.num_k,
            0,
            0,
            static_cast<int64_t>(bconv_extra_bytes),
            Deps()});

        return bconv_output_bytes;
    }

    uint64_t OLANttDigits(
        uint32_t digit_idx,
        uint32_t ct_now,
        uint32_t limb_now) {
        (void)ct_now;
        (void)limb_now;
        if (!OLAEnsureDigitResident(digit_idx)) {
            return 0;
        }

        const uint64_t ntt_bytes = digit_bytes[digit_idx];
        digit_ntt_done[digit_idx] = true;

        Emit(OLAEmitDesc{
            "ola_ntt_digits_stub",
            CycleInstructionKind::NTT,
            CycleTransferPath::None,
            TileExecutionStepType::ModUpNttTile,
            StageType::BasisConvert,
            ntt_bytes,
            LimbsForCiphertextBytes(ntt_bytes),
            0,
            0,
            0,
            0,
            Deps()});

        return ntt_bytes;
    }

    uint64_t OLAModUp(
        uint32_t ct_now,
        uint64_t input_bram_bytes,
        bool* spilled) {
        (void)input_bram_bytes;
        if (spilled != nullptr) {
            *spilled = false;
        }

        const uint64_t modup_bytes =
            static_cast<uint64_t>(ct_now) * problem.polys * problem.key_limbs * problem.ct_limb_bytes;

        Emit(OLAEmitDesc{
            "ola_modup_stub",
            CycleInstructionKind::BConv,
            CycleTransferPath::None,
            TileExecutionStepType::ModUpBConvTile,
            StageType::BasisConvert,
            modup_bytes,
            problem.digit_limbs,
            problem.num_k,
            0,
            0,
            0,
            Deps()});

        return modup_bytes;
    }

    uint64_t OLAInnerProd(
        uint32_t digit_idx,
        uint32_t ct_now,
        bool* accum_in_bram) {
        if (digit_idx >= digit_in_bram.size()) {
            build_ok = false;
            return 0;
        }
        if (!digit_ntt_done[digit_idx]) {
            build_ok = false;
            return 0;
        }
        if (!OLAEnsureDigitResident(digit_idx)) {
            return 0;
        }

        const uint32_t p_lk = problem.polys * problem.key_limbs;  // 2*(l+k)
        const uint64_t partial_bytes =
            static_cast<uint64_t>(ct_now) * p_lk * problem.ct_limb_bytes;
        const uint64_t total_key_bytes =
            static_cast<uint64_t>(p_lk) * problem.key_digit_limb_bytes;
        if (bram.Budget() == 0) {
            build_ok = false;
            return 0;
        }

        // 内积阶段采用流式窗口：
        // group 字节仍按全量统计，BRAM 只保留一个窗口，避免 2*(l+k) 整块常驻导致溢出。
        // 第一段 key 窗口使用固定上限，第二段 partial 窗口按当前剩余 BRAM 动态收缩，
        // 避免因为 digit 扩容后在中后轮触发不可回收的容量溢出。
        const uint64_t free_before_key =
            (bram.Live() >= bram.Budget()) ? 0 : (bram.Budget() - bram.Live());
        if (free_before_key == 0) {
            build_ok = false;
            return 0;
        }
        const uint64_t key_window_cap = std::max<uint64_t>(1, bram.Budget() / 4);
        const uint64_t key_window_target = std::max<uint64_t>(1, free_before_key / 2);
        const uint64_t key_window_bytes = std::min<uint64_t>(
            total_key_bytes,
            std::min<uint64_t>(key_window_cap, key_window_target));

        if (!EnsureCapacityForBytes(key_window_bytes, digit_idx)) {
            return 0;
        }
        bram.Acquire(key_window_bytes);
        Emit(OLAEmitDesc{
            "ola_innerprod_load_key_stub",
            CycleInstructionKind::LoadHBM,
            CycleTransferPath::HBMToSPM,
            TileExecutionStepType::KeyHBMToBRAM,
            StageType::KeyLoad,
            total_key_bytes,
            p_lk,
            0,
            0,
            static_cast<int64_t>(key_window_bytes),
            0,
            Deps()});

        const uint64_t available_after_key =
            (bram.Live() >= bram.Budget()) ? 1 : (bram.Budget() - bram.Live());
        const uint64_t partial_window_bytes = std::min<uint64_t>(
            partial_bytes,
            std::max<uint64_t>(1, available_after_key));

        if (!EnsureCapacityForBytes(partial_window_bytes, digit_idx)) {
            return 0;
        }
        bram.Acquire(partial_window_bytes);
        bram.Release(key_window_bytes);
        Emit(OLAEmitDesc{
            "ola_innerprod_mul_stub",
            CycleInstructionKind::EweMul,
            CycleTransferPath::None,
            TileExecutionStepType::KSInnerProdTile,
            StageType::Multiply,
            digit_bytes[digit_idx] + total_key_bytes,
            p_lk,
            0,
            0,
            static_cast<int64_t>(partial_window_bytes),
            -static_cast<int64_t>(key_window_bytes),
            Deps()});

        if (accum_in_bram == nullptr) {
            build_ok = false;
            return 0;
        }
        if (!*accum_in_bram) {
            *accum_in_bram = true;
            return partial_bytes;
        }

        bram.Release(partial_window_bytes);
        Emit(OLAEmitDesc{
            "ola_innerprod_accumulate_stub",
            CycleInstructionKind::EweAdd,
            CycleTransferPath::None,
            TileExecutionStepType::CrossDigitReduceTile,
            StageType::Merge,
            partial_bytes,
            p_lk,
            0,
            0,
            0,
            -static_cast<int64_t>(partial_window_bytes),
            Deps()});

        return partial_bytes;
    }

    uint64_t OLAReduceCrossDigitAdd(
        uint32_t ct_now,
        bool is_last_reduce,
        bool* accum_in_bram) {
        if (accum_in_bram != nullptr) {
            *accum_in_bram = true;
        }

        const uint64_t accum_bytes =
            static_cast<uint64_t>(ct_now) * problem.polys * problem.key_limbs * problem.ct_limb_bytes;

        Emit(OLAEmitDesc{
            is_last_reduce ? "ola_reduce_cross_digit_last_stub" : "ola_reduce_cross_digit_stub",
            CycleInstructionKind::EweAdd,
            CycleTransferPath::None,
            TileExecutionStepType::CrossDigitReduceTile,
            StageType::Merge,
            accum_bytes,
            problem.polys * problem.key_limbs,
            0,
            0,
            0,
            0,
            Deps()});

        return accum_bytes;
    }

    uint64_t OLAModDown(uint32_t ct_now, bool accum_in_bram) {
        (void)accum_in_bram;

        const uint64_t output_bytes =
            static_cast<uint64_t>(ct_now) * problem.polys * problem.limbs * problem.ct_limb_bytes;

        Emit(OLAEmitDesc{
            "ola_moddown_stub",
            CycleInstructionKind::INTT,
            CycleTransferPath::None,
            TileExecutionStepType::ModDownInttTile,
            StageType::BasisConvert,
            output_bytes,
            problem.polys * problem.limbs,
            0,
            0,
            0,
            0,
            Deps()});

        Emit(OLAEmitDesc{
            "ola_store_output_stub",
            CycleInstructionKind::StoreHBM,
            CycleTransferPath::SPMToHBM,
            TileExecutionStepType::OutputBRAMToHBM,
            StageType::Dispatch,
            output_bytes,
            0,
            problem.polys * problem.limbs,
            0,
            0,
            0,
            Deps()});

        return output_bytes;
    }
};

} // namespace

CycleProgram BuildOLAProgram(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware
) {
    OLAExecutionBuilder builder(problem, hardware);

    const uint32_t ct_now = problem.ciphertexts;
    const uint32_t digit_limb_now = problem.digit_limbs;

    // Step 1: load all digit inputs.
    for (uint32_t digit_idx = 0; digit_idx < problem.digits; ++digit_idx) {
        builder.OLALoadInput(digit_idx, ct_now, /*poly_now=*/1, digit_limb_now);
        if (!builder.Ok()) {
            return CycleProgram{};
        }
    }

    // Step 2: run INTT for all loaded digits.
    for (uint32_t digit_idx = 0; digit_idx < problem.digits; ++digit_idx) {
        builder.OLAInttDigits(digit_idx, ct_now, digit_limb_now);
        if (!builder.Ok()) {
            return CycleProgram{};
        }
    }

    // Step 3: after INTT, move out (digits - 1) and keep one digit in BRAM.
    if (problem.digits > 0) {
        if (!builder.OLASpillDigitsKeepOne(problem.digits - 1)) {
            return CycleProgram{};
        }
    }

    // Step 4: run BCONV for all digits.
    for (uint32_t digit_idx = 0; digit_idx < problem.digits; ++digit_idx) {
        builder.OLABconvDigits(digit_idx, ct_now, digit_limb_now);
        if (!builder.Ok()) {
            return CycleProgram{};
        }
    }

    // Step 5: after BCONV, move out (digits - 1) and keep one digit in BRAM.
    if (problem.digits > 0) {
        if (!builder.OLASpillDigitsKeepOne(problem.digits - 1)) {
            return CycleProgram{};
        }
    }

    // Step 6: run NTT for all digits.
    for (uint32_t digit_idx = 0; digit_idx < problem.digits; ++digit_idx) {
        builder.OLANttDigits(digit_idx, ct_now, digit_limb_now);
        if (!builder.Ok()) {
            return CycleProgram{};
        }
    }

    // Step 7: keep one digit in BRAM after NTT, move out the other (digits - 1).
    if (problem.digits > 0) {
        if (!builder.OLASpillDigitsKeepOne(problem.digits - 1)) {
            return CycleProgram{};
        }
    }

    // Step 8: inner-product over all digits.
    // Each digit uses 2 eval-key branches and contributes 2*(l+k) limbs partial result.
    bool accum_in_bram = false;
    for (uint32_t digit_idx = 0; digit_idx < problem.digits; ++digit_idx) {
        builder.OLAInnerProd(digit_idx, ct_now, &accum_in_bram);
        if (!builder.Ok()) {
            return CycleProgram{};
        }
        if (!builder.OLASpillDigitIfResident(digit_idx)) {
            return CycleProgram{};
        }
    }

    builder.program.estimated_peak_live_bytes = builder.bram.Peak();
    return std::move(builder.program);
}
