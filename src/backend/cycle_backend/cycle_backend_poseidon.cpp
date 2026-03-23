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
    const uint32_t p = problem.polys;
    const uint32_t digit_limbs = problem.digit_limbs;
    const uint32_t digit_num = problem.digits;
    const uint32_t l = problem.limbs;
    const uint32_t k = problem.num_k;
    const uint32_t lk = problem.key_limbs;

    std::cout << "Building Poseidon program with ct " << ct_now
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

    for (uint32_t i = 0; i < digit_num; i++){
        emit_op(
            /*name*/"Load_digit" + std::to_string(i),
            /*kind*/CycleInstructionKind::LoadHBM,
            /*transfer_path*/CycleTransferPath::HBMToSPM,
            /*type*/CycleOpType::DataLoad,
            /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/0,
            /*work_items*/digit_limbs
        );
        builder.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
        );
    }

    for (uint32_t i = 0; i < digit_num; i++) {
        emit_op(
            /*name*/"Modup_INTT_digit" + std::to_string(i),
            /*kind*/CycleInstructionKind::INTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::INTT,
            /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(ct_now) * digit_limbs
        );
        builder.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
        );
        builder.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
        );
    }

    for (uint32_t i = 0; i < digit_num; i++){
        emit_op(
            /*name*/"bconv_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::BConv,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::BConv,
             /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/(lk - digit_limbs),
            /*work_items*/static_cast<uint64_t>(ct_now) * digit_limbs
        );
        builder.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * (lk - digit_limbs) * problem.ct_limb_bytes
        );
    }

    // NTT for all digits
    for (uint32_t i = 0; i < digit_num; i++){
        emit_op(
            /*name*/"ntt_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::NTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::NTT,
             /*bytes*/static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes,
            /*input_limbs*/lk,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(ct_now) * lk
        );
        builder.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
        );
        builder.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
        );
     
    }

    // Spill the ct to HBM
    for (uint32_t i = 0; i < digit_num; i++){
        emit_op(
            /*name*/"spill_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::StoreHBM,
            /*transfer_path*/CycleTransferPath::SPMToHBM,
            /*type*/CycleOpType::Spill,
             /*bytes*/static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes,
            /*input_limbs*/0,
            /*output_limbs*/lk,
            /*work_items*/static_cast<uint64_t>(ct_now) * lk
        );
        builder.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
        );
    }

    for (uint32_t i = 0; i < p; i++){
        for (uint32_t j = 0; j < digit_num; j++){
            emit_op(
                /*name*/"load_data_digit_" + std::to_string(j) + "_" + std::to_string(i),
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
            // Load evalkey
            emit_op(
                /*name*/"load_evalkey_digit_" + std::to_string(j) + "_" + std::to_string(i),
                /*kind*/CycleInstructionKind::LoadHBM,
                /*transfer_path*/CycleTransferPath::HBMToSPM,
                /*type*/CycleOpType::KeyLoad,
                /*bytes*/static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes,   
                /*input_limbs*/lk,
                /*output_limbs*/0,
                /*work_items*/static_cast<uint64_t>(ct_now) * lk
            );
            builder.AcquireOnIssue(
                static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
            );

            // Mul-accumulate in eval form
            emit_op(
                /*name*/"mul_digit_" + std::to_string(j) + "_" + std::to_string(i),
                /*kind*/CycleInstructionKind::EweMul,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::Multiply,
                /*bytes*/static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes,
                /*input_limbs*/lk,
                /*output_limbs*/lk,
                /*work_items*/static_cast<uint64_t>(ct_now) * lk
            );
            builder.ReleaseOnIssue(
                static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
            );
            builder.ReleaseOnIssue(
                static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
            );
            builder.AcquireOnIssue(
                static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
            );

            if(j > 0){
                emit_op(
                    /*name*/"add_digit_" + std::to_string(j) + "_" + std::to_string(i),
                    /*kind*/CycleInstructionKind::EweAdd,
                    /*transfer_path*/CycleTransferPath::None,
                    /*type*/CycleOpType::Add,
                    /*bytes*/static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes,
                    /*input_limbs*/lk,
                    /*output_limbs*/lk,
                    /*work_items*/static_cast<uint64_t>(ct_now) * lk
                );
                builder.AcquireOnIssue(
                    static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
                );
                builder.ReleaseOnIssue(
                    static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
                );
                builder.ReleaseOnIssue(
                    static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
                );
                if (!builder.Ok()) {
                    std::cerr << "Failed to build add for digit " << j << ", p " << i << std::endl;
                    return CycleProgram();
                }
            }
        }
        // Store the result of this digit to HBM
        emit_op(
            /*name*/"store_result_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::StoreHBM,
            /*transfer_path*/CycleTransferPath::SPMToHBM,
            /*type*/CycleOpType::DataLoad,
             /*bytes*/static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes,
            /*input_limbs*/lk,
            /*output_limbs*/0,
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

    // check bram has data
    if (builder.bram.Live()) {
        std::cerr << "Error: BRAM live bytes is zero at the end of program building." << std::endl;
        return CycleProgram();
    }

    

    builder.program.estimated_peak_live_bytes = builder.bram.Peak();
    return std::move(builder.program);
}
