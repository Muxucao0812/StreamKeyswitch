#include "backend/cycle_backend/cycle_backend_ola.h"
#include "backend/cycle_backend/cycle_backend_primitives.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

CycleProgram BuildOLAProgram(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware
) {
    CycleProgramBuilder builder(
        problem,
        hardware,
        KeySwitchMethod::OLA,
        "ola_keyswitch");

    const uint32_t ct_now = problem.ciphertexts;
    const uint32_t digit_limb_now = problem.digit_limbs;
    const uint32_t safe_digits = std::max<uint32_t>(1, problem.digits);

    std::vector<uint64_t> digit_bytes(safe_digits, 0);
    std::vector<bool> digit_in_bram(safe_digits, false);
    std::vector<bool> digit_ntt_done(safe_digits, false);

    auto fail = [&builder]() {
        builder.build_ok = false;
    };

    auto ciphertext_limbs_from_bytes = [&problem](uint64_t bytes) -> uint32_t {
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

    auto spill_digit_if_resident = [&](uint32_t digit_idx) -> bool {
        if (digit_idx >= digit_in_bram.size()) {
            fail();
            return false;
        }
        if (!digit_in_bram[digit_idx]) {
            return true;
        }

        const uint64_t bytes = digit_bytes[digit_idx];
        if (bytes == 0) {
            fail();
            return false;
        }

        builder.bram.ReleaseOnIssue(bytes);
        digit_in_bram[digit_idx] = false;

        emit_op(
            "ola_spill_digit_stub",
            CycleInstructionKind::StoreHBM,
            CycleTransferPath::SPMToHBM,
            CycleOpType::Spill,
            bytes,
            0,
            ciphertext_limbs_from_bytes(bytes));

        return builder.Ok();
    };

    auto spill_one_digit = [&](uint32_t avoid_digit_idx) -> bool {
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
            builder.bram.ReleaseOnIssue(bytes);
            digit_in_bram[idx] = false;

            emit_op(
                "ola_spill_digit_stub",
                CycleInstructionKind::StoreHBM,
                CycleTransferPath::SPMToHBM,
                CycleOpType::Spill,
                bytes,
                0,
                ciphertext_limbs_from_bytes(bytes));

            return builder.Ok();
        }
        return false;
    };

    auto ensure_capacity_for_bytes = [&](uint64_t bytes, uint32_t avoid_digit_idx) -> bool {
        if (bytes == 0) {
            return true;
        }
        if (bytes > builder.bram.Budget()) {
            fail();
            return false;
        }

        while (!builder.bram.CanAcquire(bytes)) {
            if (!spill_one_digit(avoid_digit_idx)) {
                fail();
                return false;
            }
        }
        return true;
    };

    auto ensure_digit_resident = [&](uint32_t digit_idx) -> bool {
        if (digit_idx >= digit_bytes.size()) {
            fail();
            return false;
        }
        if (digit_in_bram[digit_idx]) {
            return true;
        }

        const uint64_t bytes = digit_bytes[digit_idx];
        if (bytes == 0) {
            fail();
            return false;
        }
        if (!ensure_capacity_for_bytes(bytes, digit_idx)) {
            return false;
        }

        builder.bram.AcquireOnIssue(bytes);
        digit_in_bram[digit_idx] = true;

        emit_op(
            "ola_reload_digit_stub",
            CycleInstructionKind::LoadHBM,
            CycleTransferPath::HBMToSPM,
            CycleOpType::DataLoad,
            bytes,
            ciphertext_limbs_from_bytes(bytes),
            0);

        return builder.Ok();
    };

    auto load_input_tensor = [&](uint32_t digit_idx, uint32_t poly_now, uint32_t limb_now) -> bool {
        if (digit_idx >= digit_bytes.size()) {
            fail();
            return false;
        }

        const uint64_t input_bytes =
            static_cast<uint64_t>(ct_now) * poly_now * limb_now * problem.ct_limb_bytes;
        if (!ensure_capacity_for_bytes(input_bytes, digit_idx)) {
            return false;
        }

        builder.bram.AcquireOnIssue(input_bytes);
        digit_bytes[digit_idx] = input_bytes;
        digit_in_bram[digit_idx] = true;
        emit_op(
            "load_input",
            CycleInstructionKind::LoadHBM,
            CycleTransferPath::HBMToSPM,
            CycleOpType::DataLoad,
            input_bytes,
            std::max<uint32_t>(1, limb_now),
            0);
        return builder.Ok();
    };

    bool accum_in_bram = false;

    // Step 1: load all digit inputs.
    for (uint32_t digit_idx = 0; digit_idx < problem.digits; ++digit_idx) {
        if (!load_input_tensor(digit_idx, /*poly_now=*/1, digit_limb_now)) {
            return CycleProgram{};
        }
    }

    // Step 2: run INTT for all loaded digits.
    for (uint32_t digit_idx = 0; digit_idx < problem.digits; ++digit_idx) {
        if (!ensure_digit_resident(digit_idx)) {
            return CycleProgram{};
        }

        const uint64_t intt_bytes = digit_bytes[digit_idx];
        emit_op(
            "ola_intt_digits_stub",
            CycleInstructionKind::INTT,
            CycleTransferPath::None,
            CycleOpType::INTT,
            intt_bytes,
            ciphertext_limbs_from_bytes(intt_bytes),
            0);

        if (!builder.Ok()) {
            return CycleProgram{};
        }
    }

    // Step 3: spill (digits - 1), keep one digit resident.
    if (problem.digits > 0) {
        for (uint32_t digit_idx = 1; digit_idx < problem.digits; ++digit_idx) {
            if (!spill_digit_if_resident(digit_idx)) {
                return CycleProgram{};
            }
        }
    }

    // Step 4: BConv for all digits.
    for (uint32_t digit_idx = 0; digit_idx < problem.digits; ++digit_idx) {
        if (!ensure_digit_resident(digit_idx)) {
            return CycleProgram{};
        }

        const uint64_t bconv_input_bytes = digit_bytes[digit_idx];
        const uint64_t bconv_output_bytes =
            static_cast<uint64_t>(ct_now) * problem.key_limbs * problem.ct_limb_bytes;
        if (bconv_output_bytes < bconv_input_bytes) {
            fail();
            return CycleProgram{};
        }

        const uint64_t bconv_extra_bytes = bconv_output_bytes - bconv_input_bytes;
        if (!ensure_capacity_for_bytes(bconv_extra_bytes, digit_idx)) {
            return CycleProgram{};
        }
        if (bconv_extra_bytes > 0) {
            builder.bram.AcquireOnIssue(bconv_extra_bytes);
        }
        digit_bytes[digit_idx] = bconv_output_bytes;

        emit_op(
            "ola_bconv_digits_stub",
            CycleInstructionKind::BConv,
            CycleTransferPath::None,
            CycleOpType::BConv,
            bconv_output_bytes,
            problem.digit_limbs,
            problem.num_k);

        if (!builder.Ok()) {
            return CycleProgram{};
        }

        const bool is_last_digit = (digit_idx + 1 == problem.digits);
        if (!is_last_digit && !spill_digit_if_resident(digit_idx)) {
            return CycleProgram{};
        }
    }

    // Step 5: NTT for all digits.
    for (uint32_t digit_idx = problem.digits; digit_idx-- > 0;) {
        if (!ensure_digit_resident(digit_idx)) {
            return CycleProgram{};
        }

        const uint64_t ntt_bytes = digit_bytes[digit_idx];
        digit_ntt_done[digit_idx] = true;

        emit_op(
            "ola_ntt_digits_stub",
            CycleInstructionKind::NTT,
            CycleTransferPath::None,
            CycleOpType::NTT,
            ntt_bytes,
            ciphertext_limbs_from_bytes(ntt_bytes),
            0);

        if (!builder.Ok()) {
            return CycleProgram{};
        }
    }

    // Step 6: inner-product across digits.
    for (uint32_t digit_idx = problem.digits; digit_idx-- > 0;) {
        if (digit_idx >= digit_in_bram.size()) {
            fail();
            return CycleProgram{};
        }
        if (!digit_ntt_done[digit_idx]) {
            fail();
            return CycleProgram{};
        }
        if (!ensure_digit_resident(digit_idx)) {
            return CycleProgram{};
        }

        const uint32_t p_lk = problem.polys * problem.key_limbs;
        const uint64_t partial_bytes =
            static_cast<uint64_t>(ct_now) * p_lk * problem.ct_limb_bytes;
        const uint64_t total_key_bytes =
            static_cast<uint64_t>(p_lk) * problem.key_digit_limb_bytes;
        if (builder.bram.Budget() == 0) {
            fail();
            return CycleProgram{};
        }

        const uint64_t free_before_key =
            (builder.bram.Live() >= builder.bram.Budget())
            ? 0
            : (builder.bram.Budget() - builder.bram.Live());
        if (free_before_key == 0) {
            fail();
            return CycleProgram{};
        }

        const uint64_t key_window_cap = std::max<uint64_t>(1, builder.bram.Budget() / 4);
        const uint64_t key_window_target = std::max<uint64_t>(1, free_before_key / 2);
        const uint64_t key_window_bytes = std::min<uint64_t>(
            total_key_bytes,
            std::min<uint64_t>(key_window_cap, key_window_target));

        if (!ensure_capacity_for_bytes(key_window_bytes, digit_idx)) {
            return CycleProgram{};
        }

        builder.bram.AcquireOnIssue(key_window_bytes);
        emit_op(
            "ola_innerprod_load_key_stub",
            CycleInstructionKind::LoadHBM,
            CycleTransferPath::HBMToSPM,
            CycleOpType::KeyLoad,
            total_key_bytes,
            p_lk,
            0);

        if (!builder.Ok()) {
            return CycleProgram{};
        }

        const uint64_t available_after_key =
            (builder.bram.Live() >= builder.bram.Budget())
            ? 1
            : (builder.bram.Budget() - builder.bram.Live());
        const uint64_t partial_window_bytes = std::min<uint64_t>(
            partial_bytes,
            std::max<uint64_t>(1, available_after_key));

        if (!ensure_capacity_for_bytes(partial_window_bytes, digit_idx)) {
            return CycleProgram{};
        }

        builder.bram.AcquireOnIssue(partial_window_bytes);
        builder.bram.ReleaseOnIssue(key_window_bytes);

        emit_op(
            "ola_innerprod_mul_stub",
            CycleInstructionKind::EweMul,
            CycleTransferPath::None,
            CycleOpType::Multiply,
            digit_bytes[digit_idx] + total_key_bytes,
            p_lk,
            0);

        if (!builder.Ok()) {
            return CycleProgram{};
        }

        if (!accum_in_bram) {
            accum_in_bram = true;
            continue;
        }

        builder.bram.ReleaseOnIssue(partial_window_bytes);
        emit_op(
            "ola_innerprod_accumulate_stub",
            CycleInstructionKind::EweAdd,
            CycleTransferPath::None,
            CycleOpType::Add,
            partial_bytes,
            p_lk,
            0);

        if (!builder.Ok()) {
            return CycleProgram{};
        }
    }

    // Step 7: run INTT again for all digits.
    for (uint32_t digit_idx = 0; digit_idx < problem.digits; ++digit_idx) {
        if (!ensure_digit_resident(digit_idx)) {
            return CycleProgram{};
        }

        const uint64_t intt_bytes = digit_bytes[digit_idx];
        emit_op(
            "ola_intt_digits_stub",
            CycleInstructionKind::INTT,
            CycleTransferPath::None,
            CycleOpType::INTT,
            intt_bytes,
            ciphertext_limbs_from_bytes(intt_bytes),
            0);

        if (!builder.Ok()) {
            return CycleProgram{};
        }
    }

    // Step 8: spill 2*l limbs to HBM.
    for (uint32_t poly_idx = 0; poly_idx < problem.polys; ++poly_idx) {
        (void)poly_idx;

        const uint64_t bytes =
            static_cast<uint64_t>(ct_now) * problem.limbs * problem.ct_limb_bytes;
        builder.bram.ReleaseOnIssue(bytes);
        emit_op(
            "ola_spill_poly_stub",
            CycleInstructionKind::StoreHBM,
            CycleTransferPath::SPMToHBM,
            CycleOpType::Spill,
            bytes,
            problem.limbs,
            0);

        if (!builder.Ok()) {
            return CycleProgram{};
        }
    }

    // Step 9: BConv 2*k limbs to 2*l limbs.
    for (uint32_t poly_idx = 0; poly_idx < problem.polys; ++poly_idx) {
        (void)poly_idx;

        const uint64_t modup_bytes = static_cast<uint64_t>(ct_now)
            * problem.polys * problem.key_limbs * problem.ct_limb_bytes;
        emit_op(
            "ola_modup_stub",
            CycleInstructionKind::BConv,
            CycleTransferPath::None,
            CycleOpType::BConv,
            modup_bytes,
            problem.digit_limbs,
            problem.num_k);

        if (!builder.Ok()) {
            return CycleProgram{};
        }
    }

    // Step 10: NTT on the retained digit buffer.
    for (uint32_t poly_idx = 0; poly_idx < problem.polys; ++poly_idx) {
        (void)poly_idx;

        if (!ensure_digit_resident(/*digit_idx=*/0)) {
            return CycleProgram{};
        }

        const uint64_t ntt_bytes = digit_bytes[0];
        digit_ntt_done[0] = true;

        emit_op(
            "ola_ntt_digits_stub",
            CycleInstructionKind::NTT,
            CycleTransferPath::None,
            CycleOpType::NTT,
            ntt_bytes,
            ciphertext_limbs_from_bytes(ntt_bytes),
            0);

        if (!builder.Ok()) {
            return CycleProgram{};
        }
    }

    // Step 11: reload 2*l limbs and cross-digit add.
    for (uint32_t poly_idx = 0; poly_idx < problem.polys; ++poly_idx) {
        (void)poly_idx;

        if (!load_input_tensor(/*digit_idx=*/0, /*poly_now=*/1, problem.limbs)) {
            return CycleProgram{};
        }

        const uint64_t accum_bytes = static_cast<uint64_t>(ct_now)
            * problem.polys * problem.key_limbs * problem.ct_limb_bytes;
        emit_op(
            "ola_reduce_cross_digit_stub",
            CycleInstructionKind::EweAdd,
            CycleTransferPath::None,
            CycleOpType::Add,
            accum_bytes,
            problem.polys * problem.key_limbs,
            0);

        if (!builder.Ok()) {
            return CycleProgram{};
        }

        accum_in_bram = true;
    }

    if (!builder.ValidateMemoryAccounting("OLA") || !builder.Ok()) {
        return CycleProgram{};
    }

    builder.program.estimated_peak_live_bytes = builder.bram.Peak();
    return std::move(builder.program);
}
