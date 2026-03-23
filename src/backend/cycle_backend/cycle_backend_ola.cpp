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
    const uint32_t p = problem.polys;
    const uint32_t digit_num = problem.digits;
    const uint32_t digit_limbs = problem.digit_limbs; // number of limbs
    const uint32_t l = problem.limbs; // number of limbs in one polynomial
    const uint32_t lk = problem.key_limbs; // number of limbs in the key
    const uint32_t k = problem.num_k;

    std::cout << "Building OLA program with ct " << ct_now
              << ", p " << p
              << ", digit_num " << digit_num
              << ", digit_limbs " << digit_limbs
              << ", l " << l
              << ", lk " << lk
              << ", k " << k
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




    // load all digit inputs.
    for (uint32_t i = 0; i < digit_num; i++){
        emit_op(
            /*name*/"Load_digit" + std::to_string(i),
            /*kind*/CycleInstructionKind::LoadHBM,
            /*transfer_path*/CycleTransferPath::HBMToSPM,
            /*type*/CycleOpType::DataLoad,
            /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/0,
            /*work_items*/digit_limbs * ct_now
         );
        builder.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
         );
    }

    // INTT for all digits.
    for (uint32_t i = 0; i < digit_num; i++) {
        emit_op(
            /*name*/"INTT_digit" + std::to_string(i),
            /*kind*/CycleInstructionKind::INTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::INTT,
            /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/0,
            /*work_items*/digit_limbs * ct_now
        );
    }

    // Spill all digits to HBM
    for (uint32_t i = 0; i < digit_num; i++){
        emit_op(
            /*name*/"Spill_digit" + std::to_string(i),
            /*kind*/CycleInstructionKind::StoreHBM,
            /*transfer_path*/CycleTransferPath::SPMToHBM,
            /*type*/CycleOpType::Spill,
            /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/0,
            /*output_limbs*/digit_limbs,
            /*work_items*/digit_limbs * ct_now
         );
        builder.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
         );
    }

    // Load one digit to do bconv, send to HBM
    for (uint32_t i = 0; i < digit_num; i++){
        emit_op(
            /*name*/"Load_digit_for_bconv_" + std::to_string(i),
            /*kind*/CycleInstructionKind::LoadHBM,
            /*transfer_path*/CycleTransferPath::HBMToSPM,
            /*type*/CycleOpType::DataLoad,
             /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/0,
            /*work_items*/digit_limbs * ct_now
        );
        builder.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
         );
        // Basis conversion
        emit_op(
            /*name*/"bconv_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::BConv,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::BConv,
             /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/lk,
            /*work_items*/digit_limbs * ct_now
        );
        builder.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
        );
        builder.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
        );

        emit_op(
            /*name*/"ntt_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::NTT,
             /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::NTT,
             /*bytes*/static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes,
            /*input_limbs*/lk,
            /*output_limbs*/0,
            /*work_items*/digit_limbs * ct_now
        );
         builder.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
        );
        builder.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
        );

        emit_op(
            /*name*/"Spill_bconv_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::StoreHBM,
            /*transfer_path*/CycleTransferPath::SPMToHBM,
            /*type*/CycleOpType::Spill,
             /*bytes*/static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes,
            /*input_limbs*/0,
            /*output_limbs*/lk,
            /*work_items*/lk * ct_now
        );
        builder.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
        );
    }

    // Run NTT for all digits and inner product
   for (uint32_t i = 0; i < p; i++){
        for (uint32_t j = 0; j < digit_num; j++){
            // Load bconv result in HBM to BRAM
            emit_op(
                /*name*/"Load_bconv_result_digit_" + std::to_string(j) + "_" + std::to_string(i),
                /*kind*/CycleInstructionKind::LoadHBM,
                /*transfer_path*/CycleTransferPath::HBMToSPM,
                /*type*/CycleOpType::DataLoad,
                /*bytes*/static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes,
                /*input_limbs*/lk,
                /*output_limbs*/0,
                /*work_items*/static_cast<uint64_t>(ct_now) * lk
            );
            builder.AcquireOnIssue(
                static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
            );

            // Load eval key for this digit from HBM to BRAM
            emit_op(
                /*name*/"Load_eval_key_digit_" + std::to_string(j) + "_" + std::to_string(i),
                /*kind*/CycleInstructionKind::LoadHBM,
                /*transfer_path*/CycleTransferPath::HBMToSPM,
                /*type*/CycleOpType::KeyLoad,
                /*bytes*/static_cast<uint64_t>(lk) * problem.ct_limb_bytes,
                /*input_limbs*/lk,
                /*output_limbs*/0,
                /*work_items*/static_cast<uint64_t>(lk)
            );
            builder.AcquireOnIssue(
                static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
            );

            // Inner product for this digit
            emit_op(
                /*name*/"Inner_product_digit_" + std::to_string(j) + "_" + std::to_string(i),
                /*kind*/CycleInstructionKind::EweMul,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::Multiply,
                /*bytes*/static_cast<uint64_t>(lk) * ct_now * problem.ct_limb_bytes,
                /*input_limbs*/lk,
                /*output_limbs*/lk,
                /*work_items*/static_cast<uint64_t>(lk) * ct_now
            );
            builder.ReleaseOnIssue(
                static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
            );
            builder.ReleaseOnIssue(
                static_cast<uint64_t>(lk) * problem.ct_limb_bytes
            );
            builder.AcquireOnIssue(
                static_cast<uint64_t>(lk) * problem.ct_limb_bytes
            );
            if (j > 0){
                // Accumulate with previous digits' result
                emit_op(
                    /*name*/"Accumulate_digit_" + std::to_string(j) + "_" + std::to_string(i),
                    /*kind*/CycleInstructionKind::EweAdd,
                    /*transfer_path*/CycleTransferPath::None,
                    /*type*/CycleOpType::Add,
                    /*bytes*/static_cast<uint64_t>(lk) * ct_now * problem.ct_limb_bytes,
                    /*input_limbs*/lk,
                    /*output_limbs*/lk,
                    /*work_items*/static_cast<uint64_t>(lk) * ct_now
                );
                builder.ReleaseOnIssue(
                    static_cast<uint64_t>(lk) * problem.ct_limb_bytes
                );
                builder.ReleaseOnIssue(
                    static_cast<uint64_t>(lk) * problem.ct_limb_bytes
                );
                builder.AcquireOnIssue(
                    static_cast<uint64_t>(lk) * problem.ct_limb_bytes
                );
            }
        }
        // INTT for this digit
        emit_op(
            /*name*/"INTT_poly_" + std::to_string(i),
            /*kind*/CycleInstructionKind::INTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::INTT,
            /*bytes*/static_cast<uint64_t>(k) * ct_now * problem.ct_limb_bytes,
            /*input_limbs*/k,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(k) * ct_now
        );
        builder.ReleaseOnIssue(
            static_cast<uint64_t>(k) * problem.ct_limb_bytes
        );
        builder.AcquireOnIssue(
            static_cast<uint64_t>(k) * problem.ct_limb_bytes
        );
        // Send to HBM
        emit_op(
            /*name*/"Store_result_poly_" + std::to_string(i),
            /*kind*/CycleInstructionKind::StoreHBM,
            /*transfer_path*/CycleTransferPath::SPMToHBM,
            /*type*/CycleOpType::DataLoad,
             /*bytes*/static_cast<uint64_t>(ct_now) * (lk-k) * problem.ct_limb_bytes,
            /*input_limbs*/0,
            /*output_limbs*/(lk-k),
            /*work_items*/static_cast<uint64_t>(ct_now) * (lk-k)
        );
        builder.ReleaseOnIssue(
            static_cast<uint64_t>(lk-k) * problem.ct_limb_bytes
        );
   }

//    Bconv 
   for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"Bconv_poly_" + std::to_string(i),
            /*kind*/CycleInstructionKind::BConv,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::BConv,
            /*bytes*/static_cast<uint64_t>(ct_now) * k * problem.ct_limb_bytes,
            /*input_limbs*/k,
            /*output_limbs*/l,
            /*work_items*/static_cast<uint64_t>(ct_now) * k
        );
        builder.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        builder.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * k * problem.ct_limb_bytes
        );

        // NTT
        emit_op(
            /*name*/"NTT_poly_" + std::to_string(i),
            /*kind*/CycleInstructionKind::NTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::NTT,
            /*bytes*/static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes,
            /*input_limbs*/l,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(ct_now) * l
        );
        builder.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        builder.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
   }

//    Load 2*l limbs to BRAM to reduce the latency of final result store and subsequent INTT and basis conversion, and spill back to HBM after NTT and basis conversion
   for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"Load_final_result_poly_" + std::to_string(i),
            /*kind*/CycleInstructionKind::LoadHBM,
            /*transfer_path*/CycleTransferPath::HBMToSPM,
            /*type*/CycleOpType::DataLoad,
             /*bytes*/static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes,
            /*input_limbs*/l,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(ct_now) * l
        );
        builder.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        emit_op(
            /*name*/"Subtract_eval_key_poly_" + std::to_string(i),
            /*kind*/CycleInstructionKind::EweSub,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::Sub,
             /*bytes*/static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes,
            /*input_limbs*/l,
            /*output_limbs*/l,
            /*work_items*/static_cast<uint64_t>(ct_now) * l
        );
        builder.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        builder.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        builder.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );

        emit_op(
            /*name*/"Store_final_result_poly_" + std::to_string(i),
            /*kind*/CycleInstructionKind::StoreHBM,
            /*transfer_path*/CycleTransferPath::SPMToHBM,
            /*type*/CycleOpType::DataLoad,
             /*bytes*/static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes,
            /*input_limbs*/0,
            /*output_limbs*/l,
            /*work_items*/static_cast<uint64_t>(ct_now) * l
        );
        builder.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
   }



  

    if(!builder.Ok()) {
        return CycleProgram{};
    }
    if (builder.bram.Live()) {
        std::cerr << "Error: BRAM live bytes is zero at the end of program building." << std::endl;
        return CycleProgram{};
    }

    builder.program.estimated_peak_live_bytes = builder.bram.Peak();
    return std::move(builder.program);
}
