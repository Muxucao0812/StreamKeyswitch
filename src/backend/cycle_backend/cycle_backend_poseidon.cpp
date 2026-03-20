#include "backend/cycle_backend/cycle_backend_poseidon.h"
#include "backend/cycle_backend/cycle_backend_primitives.h"

#include <cstdint>
#include <string>
#include <utility>

CycleProgram BuildPoseidonProgram(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware
) {
    CycleProgramBuilder builder(
        problem,
        hardware,
        KeySwitchMethod::Poseidon,
        "stream_keyswitch");

    const uint32_t ct_now = problem.ciphertexts;
    const uint32_t digit_limb_now = problem.digit_limbs;
    const uint32_t lk = problem.key_limbs;
    const uint32_t p_lk = problem.polys * problem.key_limbs;

    auto fail = [&builder]() {
        builder.build_ok = false;
    };

    auto emit_op = [&builder](
                       const std::string& name,
                       CycleInstructionKind kind,
                       CycleTransferPath transfer_path,
                       CycleOpType type,
                       uint64_t bytes,
                       uint32_t input_limbs,
                       uint32_t output_limbs,
                       uint64_t work_items = 0) {
        CyclePrimitiveDesc desc;
        desc.name = name;
        desc.transfer_path = transfer_path;
        desc.type = type;
        desc.bytes = bytes;
        desc.input_limbs = input_limbs;
        desc.output_limbs = output_limbs;
        desc.work_items = work_items;
        desc.deps = builder.Deps();
        builder.EmitPrimitive(kind, desc);
    };

    bool accum_in_bram = false;

    for (uint32_t digit_idx = 0; digit_idx < problem.digits; ++digit_idx) {
        const uint64_t input_bram_bytes =
            static_cast<uint64_t>(ct_now) * digit_limb_now * problem.ct_limb_bytes;
        builder.bram.AcquireOnIssue(input_bram_bytes);
        emit_op(
            "load_input",
            CycleInstructionKind::LoadHBM,
            CycleTransferPath::HBMToSPM,
            CycleOpType::DataLoad,
            input_bram_bytes,
            digit_limb_now,
            0);
        if (!builder.Ok()) {
            return CycleProgram{};
        }

        emit_op(
            "modup_intt",
            CycleInstructionKind::INTT,
            CycleTransferPath::None,
            CycleOpType::INTT,
            input_bram_bytes,
            digit_limb_now,
            0);
        if (!builder.Ok()) {
            return CycleProgram{};
        }

        const uint64_t k_limbs_bytes =
            static_cast<uint64_t>(ct_now) * problem.num_k * problem.ct_limb_bytes;
        const uint64_t modup_output_bytes = input_bram_bytes + k_limbs_bytes;

        builder.bram.AcquireOnComplete(k_limbs_bytes);
        emit_op(
            "modup_bconv",
            CycleInstructionKind::BConv,
            CycleTransferPath::None,
            CycleOpType::BConv,
            modup_output_bytes,
            digit_limb_now,
            problem.num_k);
        if (!builder.Ok()) {
            return CycleProgram{};
        }

        emit_op(
            "modup_ntt",
            CycleInstructionKind::NTT,
            CycleTransferPath::None,
            CycleOpType::NTT,
            modup_output_bytes,
            lk,
            0);
        if (!builder.Ok()) {
            return CycleProgram{};
        }

        bool modup_spilled = false;
        const uint64_t one_limb_key = problem.key_digit_limb_bytes;
        const bool should_spill = !builder.bram.CanAcquire(modup_output_bytes + one_limb_key);
        if (should_spill) {
            builder.bram.ReleaseOnComplete(modup_output_bytes);
            emit_op(
                "modup_spill",
                CycleInstructionKind::StoreHBM,
                CycleTransferPath::SPMToHBM,
                CycleOpType::Spill,
                modup_output_bytes,
                0,
                lk);
            if (!builder.Ok()) {
                return CycleProgram{};
            }
            modup_spilled = true;
        }

        if (modup_spilled) {
            builder.bram.AcquireOnIssue(modup_output_bytes);
            emit_op(
                "innerprod_reload_modup_all",
                CycleInstructionKind::LoadHBM,
                CycleTransferPath::HBMToSPM,
                CycleOpType::DataLoad,
                modup_output_bytes,
                lk,
                0);
            if (!builder.Ok()) {
                return CycleProgram{};
            }
        }

        const uint64_t total_key_bytes =
            static_cast<uint64_t>(p_lk) * problem.key_digit_limb_bytes;
        builder.bram.AcquireOnIssue(total_key_bytes);
        emit_op(
            "innerprod_load_key_all",
            CycleInstructionKind::LoadHBM,
            CycleTransferPath::HBMToSPM,
            CycleOpType::KeyLoad,
            total_key_bytes,
            p_lk,
            0);
        if (!builder.Ok()) {
            return CycleProgram{};
        }

        const uint64_t accum_bytes =
            static_cast<uint64_t>(ct_now) * p_lk * problem.ct_limb_bytes;
        builder.bram.AcquireOnIssue(accum_bytes);
        builder.bram.ReleaseOnComplete(total_key_bytes + modup_output_bytes);
        emit_op(
            "innerprod_mul_all",
            CycleInstructionKind::EweMul,
            CycleTransferPath::None,
            CycleOpType::Multiply,
            modup_output_bytes + total_key_bytes,
            p_lk,
            0);
        if (!builder.Ok()) {
            return CycleProgram{};
        }

        const bool is_last_digit = (digit_idx + 1 == problem.digits);
        if (!is_last_digit) {
            builder.bram.ReleaseOnComplete(accum_bytes);
            emit_op(
                "innerprod_spill_partial",
                CycleInstructionKind::StoreHBM,
                CycleTransferPath::SPMToHBM,
                CycleOpType::Spill,
                accum_bytes,
                0,
                p_lk);
            if (!builder.Ok()) {
                return CycleProgram{};
            }
            accum_in_bram = false;
        } else {
            accum_in_bram = true;
        }
    }

    for (uint32_t digit_idx = 1; digit_idx < problem.digits; ++digit_idx) {
        const bool is_last_reduce = (digit_idx + 1 == problem.digits);
        (void)is_last_reduce;

        const uint64_t accum_bytes =
            static_cast<uint64_t>(ct_now) * p_lk * problem.ct_limb_bytes;

        if (!accum_in_bram) {
            builder.bram.AcquireOnIssue(accum_bytes);
            emit_op(
                "reduce_reload_accum_lhs",
                CycleInstructionKind::LoadHBM,
                CycleTransferPath::HBMToSPM,
                CycleOpType::DataLoad,
                accum_bytes,
                p_lk,
                0);
            if (!builder.Ok()) {
                return CycleProgram{};
            }
            accum_in_bram = true;
        }

        builder.bram.AcquireOnIssue(accum_bytes);
        emit_op(
            "reduce_reload_partial_rhs",
            CycleInstructionKind::LoadHBM,
            CycleTransferPath::HBMToSPM,
            CycleOpType::DataLoad,
            accum_bytes,
            p_lk,
            0);
        if (!builder.Ok()) {
            return CycleProgram{};
        }

        builder.bram.ReleaseOnComplete(accum_bytes);
        emit_op(
            "reduce_cross_digit_add",
            CycleInstructionKind::EweAdd,
            CycleTransferPath::None,
            CycleOpType::Add,
            accum_bytes,
            p_lk,
            0);
        if (!builder.Ok()) {
            return CycleProgram{};
        }

        accum_in_bram = true;
    }

    const uint32_t p = problem.polys;
    const uint32_t l = problem.limbs;
    const uint32_t k = problem.num_k;
    const uint32_t p_l = p * l;
    const uint32_t p_k = p * k;

    const uint64_t per_limb =
        static_cast<uint64_t>(ct_now) * problem.ct_limb_bytes;
    const uint64_t two_l_bytes = static_cast<uint64_t>(p_l) * per_limb;
    const uint64_t two_k_bytes = static_cast<uint64_t>(p_k) * per_limb;
    const uint64_t one_limb = per_limb;

    if (accum_in_bram) {
        builder.bram.ReleaseOnIssue(two_l_bytes);
        emit_op(
            "moddown_spill_2l",
            CycleInstructionKind::StoreHBM,
            CycleTransferPath::SPMToHBM,
            CycleOpType::Spill,
            two_l_bytes,
            0,
            p_l);
        if (!builder.Ok()) {
            return CycleProgram{};
        }
    } else {
        builder.bram.AcquireOnIssue(two_k_bytes);
        emit_op(
            "moddown_reload_2k",
            CycleInstructionKind::LoadHBM,
            CycleTransferPath::HBMToSPM,
            CycleOpType::DataLoad,
            two_k_bytes,
            p_k,
            0);
        if (!builder.Ok()) {
            return CycleProgram{};
        }
    }

    emit_op(
        "moddown_intt",
        CycleInstructionKind::INTT,
        CycleTransferPath::None,
        CycleOpType::INTT,
        two_k_bytes,
        p_k,
        0);
    if (!builder.Ok()) {
        return CycleProgram{};
    }

    builder.bram.AcquireOnIssue(two_l_bytes);
    builder.bram.ReleaseOnComplete(two_k_bytes);
    emit_op(
        "moddown_bconv",
        CycleInstructionKind::BConv,
        CycleTransferPath::None,
        CycleOpType::BConv,
        two_k_bytes + two_l_bytes,
        p_k,
        p_l);
    if (!builder.Ok()) {
        return CycleProgram{};
    }

    emit_op(
        "moddown_ntt",
        CycleInstructionKind::NTT,
        CycleTransferPath::None,
        CycleOpType::NTT,
        two_l_bytes,
        p_l,
        0);
    if (!builder.Ok()) {
        return CycleProgram{};
    }

    builder.bram.AcquireOnIssue(one_limb);
    builder.bram.ReleaseOnComplete(one_limb);
    emit_op(
        "moddown_reload_2l",
        CycleInstructionKind::LoadHBM,
        CycleTransferPath::HBMToSPM,
        CycleOpType::DataLoad,
        two_l_bytes,
        p_l,
        0);
    if (!builder.Ok()) {
        return CycleProgram{};
    }

    emit_op(
        "moddown_subtract",
        CycleInstructionKind::EweSub,
        CycleTransferPath::None,
        CycleOpType::Sub,
        two_l_bytes,
        p_l,
        0);
    if (!builder.Ok()) {
        return CycleProgram{};
    }

    builder.bram.ReleaseOnComplete(two_l_bytes);
    emit_op(
        "moddown_store_output",
        CycleInstructionKind::StoreHBM,
        CycleTransferPath::SPMToHBM,
        CycleOpType::Spill,
        two_l_bytes,
        0,
        p_l);

    if (!builder.Ok()) {
        return CycleProgram{};
    }

    if (!builder.ValidateMemoryAccounting("Poseidon") || !builder.Ok()) {
        fail();
        return CycleProgram{};
    }

    builder.program.estimated_peak_live_bytes = builder.bram.Peak();
    return std::move(builder.program);
}
