#include "backend/cycle_backend/cycle_backend_max_parallel.h"
#include "backend/cycle_backend/cycle_backend_primitives.h"
#include <cstdint>
#include <string>
#include <utility>
#include <iostream>

CycleProgram BuildMaxParallelProgram(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware
) {
    CycleProgramBuilder builder(
        problem,
        hardware,
        KeySwitchMethod::MaxParallel,
        "max_parallel_keyswitch");

    const uint32_t ct_now = problem.ciphertexts;
    const uint32_t p = problem.polys;
    const uint32_t digit_num = problem.digits;
    const uint32_t digit_limbs = problem.digit_limbs; // number of limbs in each digit
    const uint32_t l = problem.limbs; // number of limbs in one polynomial
    const uint32_t lk = problem.key_limbs; // number of limbs in the key, which is l + digit_limbs

    std::cout << "Building Max Parallel program with ct " << ct_now
              << ", p " << p
              << ", digit_num " << digit_num
              << ", digit_limbs " << digit_limbs
              << ", l " << l
              << ", lk " << lk
              << std::endl;

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

    // Step 1: Load all digit inputs to BRAM
    std::cout << "Step 1: load ct to BRAM, total bytes: " << static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes << std::endl;
    for (uint32_t i = 0; i < digit_num; i++){
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
        );
        emit_op(
            /*name*/"load_input_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::LoadHBM,
            /*transfer_path*/CycleTransferPath::HBMToSPM,
            /*type*/CycleOpType::DataLoad,
            /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/p * digit_limbs,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(ct_now) * digit_limbs
        );
        if (!builder.Ok()) {
            return CycleProgram{};
        }
    }

    // Step 2: INTT for all limbs
    std::cout << "Step 2: INTT for all limbs" << std::endl;
    for (uint32_t i = 0; i < digit_num; i++){
        emit_op(
            /*name*/"intt_all_limbs_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::INTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::INTT,
            /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(digit_limbs)
        );
        if (!builder.Ok()) {
            return CycleProgram{};
        }
    }

    // Step 3: BConv for each limbs
    std::cout << "Step 3: BConv for each limb" << std::endl;
    for (uint32_t i = 0; i < digit_num; i++) {
        emit_op(
            /*name*/"bconv_2k_limbs_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::BConv,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::BConv,
             /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/(lk - digit_limbs),
            /*work_items*/lk
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(lk - digit_limbs) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            return CycleProgram{};
        }
    }

    // For digit_num digits with (lk - digit_limbs) limbs, do ntt
    for (uint32_t i = 0; i < digit_num; i++) {
        emit_op(
            /*name*/"ntt_2k_limbs_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::NTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::NTT,
            /*bytes*/static_cast<uint64_t>(lk - digit_limbs) * problem.ct_limb_bytes,
            /*input_limbs*/(lk - digit_limbs),
            /*output_limbs*/0,
            /*work_items*/(lk - digit_limbs)
        );
        if (!builder.Ok()) {
             return CycleProgram{};
        }
    }

    // Spill digit_num - 1 digits to HBM, keep one digit in BRAM for reuse in inner product
    std::cout << "Step 4: spill digit_num - 1 digits to HBM, keep one digit in BRAM for reuse in inner product" << std::endl;
    for (uint32_t i = 0; i < digit_num - 1; i++) {
        emit_op(
            /*name*/"spill_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::StoreHBM,
            /*transfer_path*/CycleTransferPath::SPMToHBM,
            /*type*/CycleOpType::Spill,
            /*bytes*/static_cast<uint64_t>(lk) * problem.ct_limb_bytes,
            /*input_limbs*/0,
            /*output_limbs*/lk,
            /*work_items*/static_cast<uint64_t>(lk)
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(lk) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
             return CycleProgram{};
        }
    }

    // Apply Innerprod
    for (uint32_t i = 0; i < digit_num; i++){
        if (i > 0) {
            // Load the spilled digit back to BRAM
            emit_op(
                /*name*/"load_spilled_digit_" + std::to_string(i),
                /*kind*/CycleInstructionKind::LoadHBM,
                /*transfer_path*/CycleTransferPath::HBMToSPM,
                /*type*/CycleOpType::DataLoad,
                /*bytes*/static_cast<uint64_t>(lk) * problem.ct_limb_bytes,
                /*input_limbs*/lk,
                /*output_limbs*/0,
                /*work_items*/static_cast<uint64_t>(lk)
            );
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(lk) * problem.ct_limb_bytes
            );
            if (!builder.Ok()) {
                return CycleProgram{};
            }
        }
        // Load Evalkey
        for (uint32_t m = 0; m < p; m++){
            emit_op(
                /*name*/"load_eval_key_digit_" + std::to_string(i) + "_part_" + std::to_string(m),
                /*kind*/CycleInstructionKind::LoadHBM,
                /*transfer_path*/CycleTransferPath::HBMToSPM,
                /*type*/CycleOpType::KeyLoad,
                 /*bytes*/static_cast<uint64_t>(lk) * problem.ct_limb_bytes,
                /*input_limbs*/lk,
                /*output_limbs*/0,
                /*work_items*/lk
             );
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(lk) * problem.ct_limb_bytes
            );
            if (!builder.Ok()) {
                return CycleProgram{};
            }
        }
        // Multiply
        for (uint32_t m = 0; m < p; m++){
            emit_op(
                /*name*/"bconv_inner_product_digit_" + std::to_string(i) + "_part_" + std::to_string(m),
                /*kind*/CycleInstructionKind::BConv,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::Multiply,
                /*bytes*/static_cast<uint64_t>(lk) * problem.ct_limb_bytes,
                /*input_limbs*/lk,
                /*output_limbs*/lk,
                /*work_items*/lk
             );
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(lk) * problem.ct_limb_bytes
            );
            builder.bram.ReleaseOnIssue(
                static_cast<uint64_t>(lk) * problem.ct_limb_bytes
            );
        }
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(lk) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
             return CycleProgram{};
        }
        // Send the result to HBM for accumulation in the end
        emit_op(
            /*name*/"store_result_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::StoreHBM,
            /*transfer_path*/CycleTransferPath::SPMToHBM,
            /*type*/CycleOpType::Spill,
            /*bytes*/static_cast<uint64_t>(p * lk) * problem.ct_limb_bytes,
            /*input_limbs*/0,
            /*output_limbs*/p * lk,
            /*work_items*/static_cast<uint64_t>(p * lk)
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(p * lk) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
             return CycleProgram{};
        }
    }

    // Accumulate the partial results for each digit in HBM, and store back to BRAM for final INTT
    std::cout << "Step 5: accumulate the partial results for each digit in HBM, and store back to BRAM for final INTT" << std::endl;
    for (uint32_t i = 0; i < p; i++){
        for (uint32_t m = 0; m < digit_num; m++){
            emit_op(
                /*name*/"load_partial_result_digit_" + std::to_string(m) + "_part_" + std::to_string(i),
                /*kind*/CycleInstructionKind::LoadHBM,
                /*transfer_path*/CycleTransferPath::HBMToSPM,
                /*type*/CycleOpType::DataLoad,
                /*bytes*/static_cast<uint64_t>(lk) * problem.ct_limb_bytes,
                /*input_limbs*/lk,
                /*output_limbs*/0,
                /*work_items*/lk
             );
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(lk) * problem.ct_limb_bytes
            );
            if (!builder.Ok()) {
                return CycleProgram{};
            }
            if (m > 0) {
                emit_op(
                    /*name*/"accumulate_partial_result_digit_" + std::to_string(m) + "_part_" + std::to_string(i),
                    /*kind*/CycleInstructionKind::BConv,
                    /*transfer_path*/CycleTransferPath::None,
                    /*type*/CycleOpType::Add,
                    /*bytes*/static_cast<uint64_t>(lk) * problem.ct_limb_bytes,
                    /*input_limbs*/lk,
                    /*output_limbs*/lk,
                    /*work_items*/lk
                );
                builder.bram.AcquireOnIssue(
                    static_cast<uint64_t>(lk) * problem.ct_limb_bytes
                );
                builder.bram.ReleaseOnIssue(
                    static_cast<uint64_t>(lk*2) * problem.ct_limb_bytes
                );
                if (!builder.Ok()) {
                    return CycleProgram{};
                }
            }
        }        
    }

    // Store the 2*l limbs of the final result back to HBM
    std::cout << "Step 6: store the 2*l limbs of the final result back to HBM" << std::endl;
    for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"store_final_result_part_" + std::to_string(i),
            /*kind*/CycleInstructionKind::StoreHBM,
            /*transfer_path*/CycleTransferPath::SPMToHBM,
            /*type*/CycleOpType::Spill,
            /*bytes*/static_cast<uint64_t>(l) * problem.ct_limb_bytes,
            /*input_limbs*/0,
            /*output_limbs*/l,
            /*work_items*/static_cast<uint64_t>(l)
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(l) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
             return CycleProgram{};
        }
    }

    // Step 7: INTT for 2*(lk-l) limbs
    std::cout << "Step 7: INTT for 2*(lk-l) limbs" << std::endl;
    for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"intt_final_result_part_" + std::to_string(i),
            /*kind*/CycleInstructionKind::INTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::INTT,
            /*bytes*/static_cast<uint64_t>(lk - l) * problem.ct_limb_bytes,
            /*input_limbs*/(lk - l),
            /*output_limbs*/0,
            /*work_items*/(lk - l)
        );
        if (!builder.Ok()) {
             return CycleProgram{};
        }
    }

    // Bconv for 2*(lk-l) limbs
    std::cout << "Step 8: BConv for 2*(lk-l) limbs" << std::endl;
    for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"bconv_final_result_part_" + std::to_string(i),
            /*kind*/CycleInstructionKind::BConv,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::BConv,
            /*bytes*/static_cast<uint64_t>(lk - l) * problem.ct_limb_bytes,
            /*input_limbs*/(lk - l),
            /*output_limbs*/l,
            /*work_items*/(lk - l)
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(l) * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(lk - l) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
             return CycleProgram{};
        }
    }

    // NTT for 2*l limbs
    std::cout << "Step 9: NTT for 2*l limbs" << std::endl;
    for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"ntt_final_result_part_" + std::to_string(i),
            /*kind*/CycleInstructionKind::NTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::NTT,
            /*bytes*/static_cast<uint64_t>(l) * problem.ct_limb_bytes,
            /*input_limbs*/l,
            /*output_limbs*/0,
            /*work_items*/(l)
        );
        if (!builder.Ok()) {
             return CycleProgram{};
        }
    }

    // Load 2*l limbs to BRAM, sub, and store back to HBM
    std::cout << "Step 10: Load 2*l limbs to BRAM, sub, and store back to HBM" << std::endl;
    for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"load_final_result_part_" + std::to_string(i),
            /*kind*/CycleInstructionKind::LoadHBM,
            /*transfer_path*/CycleTransferPath::HBMToSPM,
            /*type*/CycleOpType::DataLoad,
            /*bytes*/static_cast<uint64_t>(l) * problem.ct_limb_bytes,
            /*input_limbs*/l,
            /*output_limbs*/0,
            /*work_items*/(l)
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(l) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
             return CycleProgram{};
        }
        emit_op(
            /*name*/"sub_final_result_part_" + std::to_string(i),
            /*kind*/CycleInstructionKind::BConv,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::Sub,
            /*bytes*/static_cast<uint64_t>(l) * problem.ct_limb_bytes,
            /*input_limbs*/l,
            /*output_limbs*/l,
            /*work_items*/(l)
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(l) * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(2*l) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
             return CycleProgram{};
        }
        emit_op(
            /*name*/"store_final_result_part_" + std::to_string(i),
            /*kind*/CycleInstructionKind::StoreHBM,
            /*transfer_path*/CycleTransferPath::SPMToHBM,  
            /*type*/CycleOpType::Spill,
            /*bytes*/static_cast<uint64_t>(l) * problem.ct_limb_bytes,
            /*input_limbs*/0,
            /*output_limbs*/l,
            /*work_items*/(l)
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(l) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
             return CycleProgram{};
        }
    }
    

    builder.program.estimated_peak_live_bytes = builder.bram.Peak();
    return std::move(builder.program);
}
