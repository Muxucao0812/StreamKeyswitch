#include "backend/cycle_backend/cycle_backend_digit_centric.h"
#include "backend/cycle_backend/cycle_backend_primitives.h"
#include <cstdint>
#include <string>
#include <utility>
#include <iostream>

CycleProgram BuildDigitalCentricProgram(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware
) {
    CycleProgramBuilder builder(
        problem,
        hardware,
        KeySwitchMethod::DigitCentric,
        "digital_centric_keyswitch");

    const uint32_t ct_now = problem.ciphertexts;
    const uint32_t p = problem.polys;
    const uint32_t digit_num = problem.digits;
    const uint32_t digit_limbs = problem.digit_limbs; // number of limbs
    const uint32_t l = problem.limbs; // number of limbs in one polynomial
    const uint32_t lk = problem.key_limbs; // number of limbs in the key

    std::cout << "Building Digit-Centric program with ct " << ct_now
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

    // Load digit, intt, bconv, ntt, multiply
    for (uint32_t i = 0; i < digit_num; i++){
        // Load digit from HBM to BRAM
        emit_op(
            /*name*/"load_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::LoadHBM,
            /*transfer_path*/CycleTransferPath::HBMToSPM,
            /*type*/CycleOpType::DataLoad,
             /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(ct_now) * digit_limbs
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            std::cerr << "Failed to acquire BRAM for digit " << i << std::endl;
            return CycleProgram();
        }
        // INTT
        emit_op(
            /*name*/"intt_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::INTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::INTT,
             /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/digit_limbs,
            /*work_items*/static_cast<uint64_t>(ct_now) * digit_limbs
        );
        // Basis conversion
        emit_op(
            /*name*/"bconv_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::BConv,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::BConv,
            /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/(lk-digit_limbs),
            /*work_items*/static_cast<uint64_t>(ct_now) * digit_limbs
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * (lk-digit_limbs) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            std::cerr << "Failed to acquire BRAM for bconv of digit " << i << std::endl;
            return CycleProgram();
        }

        // NTT
        emit_op(
            /*name*/"ntt_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::NTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::NTT,
            /*bytes*/static_cast<uint64_t>(ct_now) * (lk-digit_limbs) * problem.ct_limb_bytes,
            /*input_limbs*/(lk-digit_limbs),
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(ct_now) * (lk-digit_limbs)
        );

        // Load key from HBM to BRAM 
        for (uint32_t j = 0; j < p; j++) {
            emit_op(
                /*name*/"load_key_" + std::to_string(i) + "_" + std::to_string(j),
                /*kind*/CycleInstructionKind::LoadHBM,
                /*transfer_path*/CycleTransferPath::HBMToSPM,
                /*type*/CycleOpType::KeyLoad,
                /*bytes*/static_cast<uint64_t>(lk) * problem.ct_limb_bytes,
                /*input_limbs*/lk,
                /*output_limbs*/0,
                /*work_items*/static_cast<uint64_t>(lk)
            );
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(lk) * problem.ct_limb_bytes
            );
            if (!builder.Ok()) {
                std::cerr << "Failed to acquire BRAM for key " << j << " of digit " << i << std::endl;
                return CycleProgram();
            }

            emit_op(
                /*name*/"mul_digit_" + std::to_string(i) + "_" + std::to_string(j),
                /*kind*/CycleInstructionKind::EweMul,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::Multiply,
                /*bytes*/static_cast<uint64_t>(ct_now) * (lk-digit_limbs) * problem.ct_limb_bytes,
                /*input_limbs*/lk,
                /*output_limbs*/lk,
                /*work_items*/static_cast<uint64_t>(ct_now) * lk
            );
            builder.bram.ReleaseOnIssue(
                static_cast<uint64_t>(lk) * problem.ct_limb_bytes
            );
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(lk) * problem.ct_limb_bytes
            );
            if (!builder.Ok()) {
                std::cerr << "Failed to release BRAM for key " << j << " of digit " << i << std::endl;
                return CycleProgram();
            }
        }
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            std::cerr << "Failed to release BRAM for digit " << i << std::endl;
            return CycleProgram();
        }
        // Store result back to HBM
        if (i != digit_num - 1){
            emit_op(
                /*name*/"store_result_digit_" + std::to_string(i),
                /*kind*/CycleInstructionKind::StoreHBM,
                /*transfer_path*/CycleTransferPath::SPMToHBM,
                /*type*/CycleOpType::DataLoad,
                /*bytes*/static_cast<uint64_t>(ct_now) * p * lk * problem.ct_limb_bytes,
                /*input_limbs*/0,
                /*output_limbs*/p * lk,
                /*work_items*/static_cast<uint64_t>(ct_now) * p * lk
            );
            builder.bram.ReleaseOnIssue(
                static_cast<uint64_t>(ct_now) * p * lk * problem.ct_limb_bytes
            );
            if (!builder.Ok()) {
                std::cerr << "Failed to release BRAM for storing result of digit " << i << std::endl;
                return CycleProgram();
            }
        }
    }

    // Load digit to BRAM to reduce
    for (uint32_t i = 0; i < p; i++){
        for (uint32_t j = 0; j < digit_num - 1; j++){
            emit_op(
                /*name*/"load_result_digit_" + std::to_string(j) + "_" + std::to_string(i),
                /*kind*/CycleInstructionKind::LoadHBM,
                /*transfer_path*/CycleTransferPath::HBMToSPM,
                /*type*/CycleOpType::DataLoad,
                /*bytes*/static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes,
                /*input_limbs*/lk,
                /*output_limbs*/0,
                /*work_items*/static_cast<uint64_t>(ct_now) * lk
            );
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
            );
            if (!builder.Ok()) {
                std::cerr << "Failed to acquire BRAM for loading result of digit " << j << " for reduction, p " << i << std::endl;
                return CycleProgram();
            }

            emit_op(
                /*name*/"reduce_digit_" + std::to_string(j) + "_" + std::to_string(i),
                /*kind*/CycleInstructionKind::EweAdd,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::Add,
                /*bytes*/static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes,
                /*input_limbs*/lk,
                /*output_limbs*/lk,
                /*work_items*/static_cast<uint64_t>(ct_now) * lk
            );
            builder.bram.ReleaseOnIssue(
                static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
            );
            builder.bram.ReleaseOnIssue(
                static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
            );
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
            );
            if (!builder.Ok()) {
                std::cerr << "Failed to acquire BRAM for reduction of digit " << j << " for reduction, p " << i << std::endl;
                return CycleProgram();
            }
        }
    }

    // Store 2*l limbs to HBM
    for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"store_final_result_" + std::to_string(i),
            /*kind*/CycleInstructionKind::StoreHBM,
            /*transfer_path*/CycleTransferPath::SPMToHBM,
            /*type*/CycleOpType::DataLoad,
            /*bytes*/static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes,
            /*input_limbs*/0,
            /*output_limbs*/l,
            /*work_items*/static_cast<uint64_t>(ct_now) * l
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            std::cerr << "Failed to release BRAM for storing final result for p " << i << std::endl;
            return CycleProgram();
        }
    }

    // INTT for 2*(lk-l) limbs
    for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"intt_final_result_" + std::to_string(i),
            /*kind*/CycleInstructionKind::INTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::INTT,
            /*bytes*/static_cast<uint64_t>(ct_now) * (lk-l) * problem.ct_limb_bytes,
            /*input_limbs*/(lk-l),
            /*output_limbs*/(lk-l),
            /*work_items*/static_cast<uint64_t>(ct_now) * (lk-l)
        );
    }

    // Basis conversion for 2*(lk-l) limbs
    for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"bconv_final_result_" + std::to_string(i),
            /*kind*/CycleInstructionKind::BConv,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::BConv,
            /*bytes*/static_cast<uint64_t>(ct_now) * (lk-l) * problem.ct_limb_bytes,
            /*input_limbs*/(lk-l),
            /*output_limbs*/l,
            /*work_items*/static_cast<uint64_t>(ct_now) * (lk-l)
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * (lk-l) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            std::cerr << "Failed to acquire/release BRAM for basis conversion of final result for p " << i << std::endl;
            return CycleProgram();
        }
    }

    // Load 2*l limbs to BRAM to reduce
    for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"load_final_result_" + std::to_string(i),
            /*kind*/CycleInstructionKind::LoadHBM,
            /*transfer_path*/CycleTransferPath::HBMToSPM,
            /*type*/CycleOpType::DataLoad,
            /*bytes*/static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes,
            /*input_limbs*/l,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(ct_now) * l
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            std::cerr << "Failed to acquire BRAM for loading final result for reduction for p " << i << std::endl;
            return CycleProgram();
        }
        emit_op(
            /*name*/"reduce_final_result_" + std::to_string(i),
            /*kind*/CycleInstructionKind::EweAdd,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::Add,
            /*bytes*/static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes,
            /*input_limbs*/l,
            /*output_limbs*/l,
            /*work_items*/static_cast<uint64_t>(ct_now) * l
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            std::cerr << "Failed to acquire/release BRAM for reduction of final result for p " << i << std::endl;
            return CycleProgram();
        }

        // Store final result back to HBM
        emit_op(
            /*name*/"store_final_result_reduced_" + std::to_string(i),
            /*kind*/CycleInstructionKind::StoreHBM,
            /*transfer_path*/CycleTransferPath::SPMToHBM,
            /*type*/CycleOpType::DataLoad,
            /*bytes*/static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes,
            /*input_limbs*/0,
            /*output_limbs*/l,
            /*work_items*/static_cast<uint64_t>(ct_now) * l
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            std::cerr << "Failed to release BRAM for storing reduced final result for p " << i << std::endl;
            return CycleProgram();
        }
    }

  


    builder.program.estimated_peak_live_bytes = builder.bram.Peak();
    return std::move(builder.program);
    
}
