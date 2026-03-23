#include "backend/cycle_backend/cycle_backend_hera.h"
#include "backend/cycle_backend/cycle_backend_primitives.h"
#include <cstdint>
#include <string>
#include <utility>
#include <iostream>


CycleProgram BuildHERAProgram(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware
) {
    CycleProgramBuilder builder(
        problem,
        hardware,
        KeySwitchMethod::HERA,
        "hera_keyswitch"
    );

    const uint32_t ct_now = problem.ciphertexts;
    const uint32_t p = problem.polys;
    const uint32_t digit_num = problem.digits;
    const uint32_t digit_limbs = problem.digit_limbs; // number of limbs in each digit
    const uint32_t l = problem.limbs; // number of limbs in one polynomial
    const uint32_t lk = problem.key_limbs; // number of limbs in the key, which is l + digit_limbs 
    const uint32_t k = problem.num_k; // number of digits in the key, which is also the number of limbs in each digit

    std::cout << "Building HERA program with ct " << ct_now
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
        builder.AcquireOnIssue(static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes);
    }
   

    // Step 2: INTT for all limbs
    std::cout << "Step 2: INTT for all limbs" << std::endl;
    for (uint32_t i = 0; i < digit_num; i++) {
        emit_op(
            /*name*/"intt_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::INTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::INTT,
            /*bytes*/0,
            /*input_limbs*/digit_limbs * ct_now,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(digit_limbs) * ct_now
            );
        builder.bram.AcquireOnIssue(static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes);
        builder.bram.ReleaseOnIssue(static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes);
    }
  

    // Step 3: BConv for 2*k limbs
    std::cout << "Step 3: BConv for 2*k limbs" << std::endl;
    for (uint32_t i = 0; i < k; i++) {
        for (uint32_t j = 0; j < digit_num; j++){
            std::cout << "Step 3." << (i+1) << ": bconv for 2*k limbs" << std::endl;
            emit_op(
                /*name*/"bconv_2k_limbs",
                /*kind*/CycleInstructionKind::BConv,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::BConv,
                /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
                /*input_limbs*/digit_limbs,
                /*output_limbs*/1,
                /*work_items*/digit_limbs
            );

            // add bram acquire
            builder.bram.AcquireOnIssue(static_cast<uint64_t>(1) * problem.ct_limb_bytes);

            // NTT for limb
            std::cout << "Step 3." << (i+1) << ": NTT for limb" << std::endl;
            emit_op(
                /*name*/"ntt_limb",
                /*kind*/CycleInstructionKind::NTT,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::NTT,
                /*bytes*/static_cast<uint64_t>(1) * problem.ct_limb_bytes,
                /*input_limbs*/1,
                /*output_limbs*/0,
                /*work_items*/1
            );
            builder.ReleaseOnIssue(static_cast<uint64_t>(1) * problem.ct_limb_bytes);
            builder.AcquireOnIssue(static_cast<uint64_t>(1) * problem.ct_limb_bytes);

            for (uint32_t k = 0; k < p; k++){
                emit_op(
                    /*name*/"Load_eval_key_limb",
                    /*kind*/CycleInstructionKind::LoadHBM,
                    /*transfer_path*/CycleTransferPath::HBMToSPM,
                    /*type*/CycleOpType::KeyLoad,
                    /*bytes*/static_cast<uint64_t>(1) * problem.ct_limb_bytes,
                    /*input_limbs*/1,
                    /*output_limbs*/0,
                    /*work_items*/1
                );
                builder.bram.AcquireOnIssue(static_cast<uint64_t>(1) * problem.ct_limb_bytes);

                emit_op(
                    /*name*/"bconv_inner_product",
                    /*kind*/CycleInstructionKind::EweMul,
                    /*transfer_path*/CycleTransferPath::None,
                    /*type*/CycleOpType::Multiply,
                    /*bytes*/static_cast<uint64_t>(1) * problem.ct_limb_bytes,
                    /*input_limbs*/1,
                    /*output_limbs*/0,
                    /*work_items*/1
                );
                // Get result
                builder.bram.AcquireOnIssue(
                    static_cast<uint64_t>(1) * problem.ct_limb_bytes
                );
                // Release eval key limb
                builder.bram.ReleaseOnIssue(
                    static_cast<uint64_t>(1) * problem.ct_limb_bytes
                );
            }
            // Release ct limb
            builder.bram.ReleaseOnIssue(
                static_cast<uint64_t>(1) * problem.ct_limb_bytes
            );


            if(j > 0){
                for (uint32_t k = 0; k < p; k++){
                    emit_op(
                        /*name*/"accumulate_limb",
                        /*kind*/CycleInstructionKind::EweAdd,
                        /*transfer_path*/CycleTransferPath::None,
                        /*type*/CycleOpType::Add,
                        /*bytes*/static_cast<uint64_t>(1) * problem.ct_limb_bytes,
                        /*input_limbs*/1,
                        /*output_limbs*/0,
                        /*work_items*/1
                    );
                    builder.bram.AcquireOnIssue(
                        static_cast<uint64_t>(1) * problem.ct_limb_bytes
                    );
                    builder.bram.ReleaseOnIssue(
                        static_cast<uint64_t>(1) * problem.ct_limb_bytes
                    );
                    builder.bram.ReleaseOnIssue(
                        static_cast<uint64_t>(1) * problem.ct_limb_bytes
                    );
                }
            }
        }
    }

    builder.ReleaseOnIssue(static_cast<uint64_t>(k) * p * problem.ct_limb_bytes);

    // INTT for 2*k limbs 
    std::cout << "Step 4 : INTT for 2*k limbs" << std::endl;
    for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"intt_2k_limbs",
            /*kind*/CycleInstructionKind::INTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::INTT,
            /*bytes*/static_cast<uint64_t>(k) * problem.ct_limb_bytes,
            /*input_limbs*/k,
            /*output_limbs*/0,
            /*work_items*/k
        );
    }

    // extra l limbs need to be innerproduct
    for (uint32_t i = 0; i < l; i++){
        std::cout << "Step 4." << (i+1) << ": BConv for one limb" << std::endl;
        for (uint32_t j = 0; j < digit_num; j++){
            emit_op(
                /*name*/"bconv_one_limb",
                /*kind*/CycleInstructionKind::BConv,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::BConv,
                /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
                /*input_limbs*/digit_limbs,
                /*output_limbs*/1,
                /*work_items*/digit_limbs
            );
            // add more one fly limb
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(1) * problem.ct_limb_bytes
            );
      
            // NTT for limb
            std::cout << "Step 4." << (i+1) << ": NTT for limb" << std::endl;
            emit_op(
                /*name*/"ntt_one_limb",
                /*kind*/CycleInstructionKind::NTT,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::NTT,
                /*bytes*/static_cast<uint64_t>(1) * problem.ct_limb_bytes,
                /*input_limbs*/1,
                /*output_limbs*/0,
                /*work_items*/1
            );
            builder.ReleaseOnIssue(
                static_cast<uint64_t>(1) * problem.ct_limb_bytes
            );
            builder.AcquireOnIssue(
                static_cast<uint64_t>(1) * problem.ct_limb_bytes
            );

            // Send eval key limb
            std::cout << "Step 4." << (i+1) << ": send eval key limb" << std::endl;
            for (uint32_t m = 0; m < p; m++){
                emit_op(
                    /*name*/"Load_eval_key_limb",
                    /*kind*/CycleInstructionKind::LoadHBM,
                    /*transfer_path*/CycleTransferPath::HBMToSPM,
                    /*type*/CycleOpType::KeyLoad,
                    /*bytes*/static_cast<uint64_t>(1) * problem.ct_limb_bytes,
                    /*input_limbs*/1,
                    /*output_limbs*/0,
                    /*work_items*/1
                );
                builder.bram.AcquireOnIssue(static_cast<uint64_t>(1) * problem.ct_limb_bytes);

                // Inner product for one limb across p ciphertexts
                std::cout << "Step 4." << (i+1) << ": bconv for one limb" << std::endl;
                emit_op(
                    /*name*/"bconv_inner_product",
                    /*kind*/CycleInstructionKind::BConv,
                    /*transfer_path*/CycleTransferPath::None,
                    /*type*/CycleOpType::Multiply,
                    /*bytes*/static_cast<uint64_t>(1) * problem.ct_limb_bytes,
                    /*input_limbs*/1,
                    /*output_limbs*/0,
                    /*work_items*/1
                );
                builder.bram.AcquireOnIssue(
                    static_cast<uint64_t>(1) * problem.ct_limb_bytes
                );
                builder.bram.ReleaseOnIssue(
                    static_cast<uint64_t>(1) * problem.ct_limb_bytes
                );
            }
            // Release ct limb
            builder.bram.ReleaseOnIssue(
                static_cast<uint64_t>(1) * problem.ct_limb_bytes
            );

            if (j > 0) {
                for (uint32_t m = 0; m < p; m++){
                    emit_op(
                        /*name*/"accumulate_limb",
                        /*kind*/CycleInstructionKind::EweAdd,
                        /*transfer_path*/CycleTransferPath::None,
                        /*type*/CycleOpType::Add,
                        /*bytes*/static_cast<uint64_t>(1) * problem.ct_limb_bytes,
                        /*input_limbs*/1,
                        /*output_limbs*/0,
                        /*work_items*/1
                    );
                    builder.bram.AcquireOnIssue(
                        static_cast<uint64_t>(1) * problem.ct_limb_bytes
                    );
                    builder.bram.ReleaseOnIssue(
                        static_cast<uint64_t>(1) * problem.ct_limb_bytes
                    );
                    builder.bram.ReleaseOnIssue(
                        static_cast<uint64_t>(1) * problem.ct_limb_bytes
                    );
                }
            }  
        }
        // 2*k limbs Bconv
        std::cout << "Step 5." << (i+1) << ": BConv for 2*k limbs" << std::endl;
        emit_op(
            /*name*/"bconv_2k_limbs",
            /*kind*/CycleInstructionKind::BConv,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::BConv,
            /*bytes*/static_cast<uint64_t>(k) * problem.ct_limb_bytes,
             /*input_limbs*/k,
            /*output_limbs*/1,
            /*work_items*/k
         );
        builder.bram.AcquireOnIssue(static_cast<uint64_t>(1) * problem.ct_limb_bytes);
        // NTT for limb
        std::cout << "Step 5." << (i+1) << ": NTT for limb" << std::endl;
        emit_op(
            /*name*/"ntt_limb",
            /*kind*/CycleInstructionKind::NTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::NTT,
             /*bytes*/static_cast<uint64_t>(1) * problem.ct_limb_bytes,
            /*input_limbs*/1,
            /*output_limbs*/0,
            /*work_items*/1
        );
        builder.ReleaseOnIssue(static_cast<uint64_t>(1) * problem.ct_limb_bytes);
        builder.AcquireOnIssue(static_cast<uint64_t>(1) * problem.ct_limb_bytes);

        // Sub
        emit_op(
            /*name*/"sub_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::EweSub,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::Sub,
             /*bytes*/static_cast<uint64_t>(1) * problem.ct_limb_bytes,
            /*input_limbs*/1,
            /*output_limbs*/0,
            /*work_items*/1
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(1) * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(1) * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(1) * problem.ct_limb_bytes
         );

        //  Store output digit limb to HBM
        std::cout << "Step 6." << (i+1) << ": store output limb to HBM" << std::endl;
        emit_op(
            /*name*/"store_output_limb_" + std::to_string(i),
            /*kind*/CycleInstructionKind::StoreHBM,
            /*transfer_path*/CycleTransferPath::SPMToHBM,
            /*type*/CycleOpType::DataLoad,
             /*bytes*/static_cast<uint64_t>(1) * problem.ct_limb_bytes,
             /*input_limbs*/0,
             /*output_limbs*/1,
             /*work_items*/1
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(1) * problem.ct_limb_bytes
        );
    }

    builder.bram.ReleaseOnIssue(static_cast<uint64_t>(ct_now) * digit_num * digit_limbs * problem.ct_limb_bytes);
    builder.bram.ReleaseOnIssue(static_cast<uint64_t>(ct_now) * k * 2 * problem.ct_limb_bytes);

    emit_op(
        /*name*/"NTT",
        /*kind*/CycleInstructionKind::NTT,
        /*transfer_path*/CycleTransferPath::None,
        /*type*/CycleOpType::NTT,
        /*bytes*/static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes,
        /*input_limbs*/l * ct_now ,
        /*output_limbs*/0,
        /*work_items*/static_cast<uint64_t>(ct_now) * l
    );

     if (!builder.Ok()) {
        return CycleProgram{};
    }

    if(builder.bram.Live()){
        std::cerr << "Warning: BRAM live bytes is not zero after processing, live bytes: " << builder.bram.Live() << std::endl;
        return CycleProgram{};
    }

    builder.program.estimated_peak_live_bytes = builder.bram.Peak();
    return std::move(builder.program);

}
