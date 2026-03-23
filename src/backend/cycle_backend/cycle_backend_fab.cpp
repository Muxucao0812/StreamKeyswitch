#include "backend/cycle_backend/cycle_backend_fab.h"
#include "backend/cycle_backend/cycle_backend_primitives.h"

#include <cstdint>
#include <string>
#include <utility>
#include <iostream>


CycleProgram BuildFABProgram(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware
) {
    CycleProgramBuilder builder(
        problem,
        hardware,
        KeySwitchMethod::FAB,
        "stream_keyswitch");

    const uint32_t ct_now = problem.ciphertexts;
    const uint32_t p = problem.polys;
    const uint32_t digit_num = problem.digits;
    const uint32_t digit_limbs = problem.digit_limbs; // number of limbs
    const uint32_t l = problem.limbs; // number of limbs in one polynomial
    const uint32_t lk = problem.key_limbs;
    const uint32_t k = problem.num_k;

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

    // Step 1: load l limbs ct in to BRAM 
    for (uint32_t i = 0; i < digit_num; i++) {
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
        builder.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
        );
    }
  

    // Step 2: for each digit, Load corresponding evalkeys and do inner product to get partial results, then reduce and spill the partial results to HBM.
    for (uint32_t i = 0; i < p; i++){
        for (uint32_t j = 0; j < digit_num; j++){
            // Load eval key for this digit from HBM to BRAM
            emit_op(
                /*name*/"Load_eval_key_digit_" + std::to_string(j) + "_" + std::to_string(i),
                /*kind*/CycleInstructionKind::LoadHBM,
                /*transfer_path*/CycleTransferPath::HBMToSPM,
                /*type*/CycleOpType::KeyLoad,
                /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
                /*input_limbs*/digit_limbs,
                /*output_limbs*/0,
                /*work_items*/static_cast<uint64_t>(digit_limbs)
            );
            builder.AcquireOnIssue(
                static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
            );
            // Multiply in BRAM for this digit
            emit_op(
                /*name*/"Inner_product_digit_" + std::to_string(j) + "_" + std::to_string(i),
                /*kind*/CycleInstructionKind::EweMul,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::Multiply,
                /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
                /*input_limbs*/digit_limbs,
                /*output_limbs*/digit_limbs,
                /*work_items*/static_cast<uint64_t>(ct_now) * digit_limbs
            );
            builder.ReleaseOnIssue(
                static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
            );
            builder.AcquireOnIssue(
                static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
            );

            // Send to HBM
            emit_op(
                /*name*/"store_bconv_result_digit_" + std::to_string(j) + "_" + std::to_string(i),
                /*kind*/CycleInstructionKind::StoreHBM,
                /*transfer_path*/CycleTransferPath::SPMToHBM,
                /*type*/CycleOpType::DataLoad,
                /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
                /*input_limbs*/0,
                /*output_limbs*/digit_limbs,
                /*work_items*/static_cast<uint64_t>(ct_now) * digit_limbs
             );
             builder.ReleaseOnIssue(
                static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
             );
        }
    }

    // INTT for each digit
    for (uint32_t i = 0; i < digit_num; i++) {
        emit_op(
            /*name*/"INTT_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::INTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::INTT,
            /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(ct_now) * digit_limbs
        );
        builder.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
        );
        builder.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
        );
    }

    // Bconv
    for (uint32_t i = 0; i < digit_num; i++){
        emit_op(
            /*name*/"Bconv_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::BConv,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::BConv,
             /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/(lk-digit_limbs),
            /*work_items*/static_cast<uint64_t>(ct_now) * digit_limbs
        );
        builder.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * (lk-digit_limbs) * problem.ct_limb_bytes
        );
        builder.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
        );
    }

    for (uint32_t i = 0; i < p; i++){
        for (uint32_t j = 0; j < digit_num; j++){
            // Load eval key
            emit_op(
                /*name*/"Load_eval_key_for_reduce_digit_" + std::to_string(j) + "_" + std::to_string(i),
                /*kind*/CycleInstructionKind::LoadHBM,
                /*transfer_path*/CycleTransferPath::HBMToSPM,
                /*type*/CycleOpType::KeyLoad,
                /*bytes*/static_cast<uint64_t>(lk-digit_limbs) * problem.ct_limb_bytes,
                /*input_limbs*/(lk-digit_limbs),
                /*output_limbs*/0,
                /*work_items*/static_cast<uint64_t>(lk-digit_limbs)
            );
            builder.AcquireOnIssue(
                static_cast<uint64_t>(lk-digit_limbs) * problem.ct_limb_bytes
            );
            // Multiply in BRAM for this digit
            emit_op(
                /*name*/"Reduce_inner_product_digit_" + std::to_string(j) + "_" + std::to_string(i),
                /*kind*/CycleInstructionKind::EweMul,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::Multiply,
                /*bytes*/static_cast<uint64_t>(ct_now) * (lk-digit_limbs) * problem.ct_limb_bytes,
                /*input_limbs*/(lk-digit_limbs),
                /*output_limbs*/(lk-digit_limbs),
                /*work_items*/static_cast<uint64_t>(ct_now) * (lk-digit_limbs)
             );
             builder.ReleaseOnIssue(
                static_cast<uint64_t>(lk-digit_limbs) * problem.ct_limb_bytes
             );
             builder.AcquireOnIssue(
                static_cast<uint64_t>(lk-digit_limbs) * problem.ct_limb_bytes
             );
            //  Load previous result
            emit_op(
                /*name*/"Load_previous_reduce_result_digit_" + std::to_string(j) + "_" + std::to_string(i),
                /*kind*/CycleInstructionKind::LoadHBM,
                /*transfer_path*/CycleTransferPath::HBMToSPM,
                /*type*/CycleOpType::DataLoad,
                /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
                /*input_limbs*/digit_limbs,
                /*output_limbs*/0,
                /*work_items*/static_cast<uint64_t>(digit_limbs)
            );
            builder.AcquireOnIssue(
                static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
            );
            if(j > 0){
                // Accumulate with previous digits' result
                emit_op(
                    /*name*/"Accumulate_reduce_result_digit_" + std::to_string(j) + "_" + std::to_string(i),
                    /*kind*/CycleInstructionKind::EweAdd,
                    /*transfer_path*/CycleTransferPath::None,
                    /*type*/CycleOpType::Add,
                    /*bytes*/static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes,
                    /*input_limbs*/lk,
                    /*output_limbs*/lk,
                    /*work_items*/static_cast<uint64_t>(ct_now) * lk
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
        // Store reduced result to HBM
        emit_op(
            /*name*/"store_reduce_result_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::StoreHBM,
            /*transfer_path*/CycleTransferPath::SPMToHBM,
            /*type*/CycleOpType::DataLoad,
            /*bytes*/static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes,
            /*input_limbs*/0,
            /*output_limbs*/lk,
            /*work_items*/static_cast<uint64_t>(ct_now) * lk
         );
         builder.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
         );
    }

     // Load 2*k limbs for each p to do the final INTT and reduction
    for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"load_result_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::LoadHBM,
            /*transfer_path*/CycleTransferPath::HBMToSPM,
            /*type*/CycleOpType::DataLoad,
             /*bytes*/static_cast<uint64_t>(ct_now) * k * problem.ct_limb_bytes,
            /*input_limbs*/k,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(ct_now) * k
        );
        builder.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * k * problem.ct_limb_bytes
        );

        // INTT
        emit_op(
            /*name*/"intt_final_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::INTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::INTT,
             /*bytes*/static_cast<uint64_t>(ct_now) * k * problem.ct_limb_bytes,
            /*input_limbs*/k,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(ct_now) * k
        );
        builder.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * k * problem.ct_limb_bytes
        );
        builder.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * k * problem.ct_limb_bytes
        );

        // BConv
        emit_op(
            /*name*/"bconv_final_digit_" + std::to_string(i),
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

        // NTT for the final result in digit form
        emit_op(
            /*name*/"ntt_final_digit_" + std::to_string(i),
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


    // Load 2*l limbs and subtract the original ct in NTT form to get the final result in evaluation form
    for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"load_ntt_digit_" + std::to_string(i),
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
            /*name*/"sub_ntt_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::EweSub,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::Sub,
             /*bytes*/static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes,
            /*input_limbs*/l,
            /*output_limbs*/l,
            /*work_items*/static_cast<uint64_t>(ct_now) * l
        );

        builder.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        builder.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        builder.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );

        // Send to HBM for output
        emit_op(
            /*name*/"store_output_digit_" + std::to_string(i),
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



    if (!builder.Ok()) {
        return CycleProgram{};
    }
    if (builder.bram.Live()){
        std::cerr << "Warning: BRAM live bytes is not zero after program construction, live bytes: " << builder.bram.Live() << std::endl;
        return CycleProgram{};
    }

    builder.program.estimated_peak_live_bytes = builder.bram.Peak();
    return std::move(builder.program);

}
