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
    std::cout << "Step 1: load ct to BRAM, total bytes: " << static_cast<uint64_t>(ct_now) * p * l * problem.ct_limb_bytes << std::endl;
    builder.bram.AcquireOnIssue(
        static_cast<uint64_t>(ct_now) * p * l * problem.ct_limb_bytes
    );
    emit_op(
        /*name*/"load_input",
        /*kind*/CycleInstructionKind::LoadHBM,
        /*transfer_path*/CycleTransferPath::HBMToSPM,
        /*type*/CycleOpType::DataLoad,
        /*bytes*/static_cast<uint64_t>(ct_now) * p * l * problem.ct_limb_bytes,
        /*input_limbs*/p * l,
        /*output_limbs*/0);
    if (!builder.Ok()) {
        return CycleProgram{};  
    }

    // Step 2: INTT for all limbs
    std::cout << "Step 2: INTT for all limbs" << std::endl;
    emit_op(
        /*name*/"intt_all_limbs",
        /*kind*/CycleInstructionKind::INTT,
        /*transfer_path*/CycleTransferPath::None,
        /*type*/CycleOpType::INTT,
        /*bytes*/0,
        /*input_limbs*/l * ct_now * p,
        /*output_limbs*/0
    );
    if (!builder.Ok()) {
        return CycleProgram{};
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
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(1) * problem.ct_limb_bytes
            );
            if (!builder.Ok()) {
                return CycleProgram{};
            }

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


            // Send two eval key limbs
            std::cout << "Step 3." << (i+1) << ": send eval key limb" << std::endl;
            emit_op(
                /*name*/"send_eval_key_limb",
                /*kind*/CycleInstructionKind::LoadHBM,
                /*transfer_path*/CycleTransferPath::HBMToSPM,
                /*type*/CycleOpType::KeyLoad,
                /*bytes*/static_cast<uint64_t>(p) * problem.ct_limb_bytes,
                /*input_limbs*/p,
                /*output_limbs*/0,
                /*work_items*/p
            );
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(p) * problem.ct_limb_bytes
            );
            if (!builder.Ok()) {
                return CycleProgram{};
            }

            // Inner product for one limb across p ciphertexts
            std::cout << "Step 3." << (i+1) << ": inner product for one limb" << std::endl;
            emit_op(
                /*name*/"inner_product_limb",
                /*kind*/CycleInstructionKind::EweMul,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::Multiply,
                /*bytes*/static_cast<uint64_t>(p) * problem.ct_limb_bytes,
                /*input_limbs*/p,
                /*output_limbs*/0,
                /*work_items*/p
            );
            // Get result
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(p) * problem.ct_limb_bytes
            );
            // Release eval key limb
            builder.bram.ReleaseOnIssue(
                static_cast<uint64_t>(p) * problem.ct_limb_bytes
            );
            // Release ct limb
            builder.bram.ReleaseOnIssue(
                static_cast<uint64_t>(1) * problem.ct_limb_bytes
            );
            if (!builder.Ok()) {
                return CycleProgram{};
            }
            if(j > 0){
                std::cout << "Step 3." << (i+1) << ": Accumulate result to previous limb" << std::endl;
                emit_op(
                    /*name*/"accumulate_limb",
                    /*kind*/CycleInstructionKind::EweAdd,
                    /*transfer_path*/CycleTransferPath::None,
                    /*type*/CycleOpType::Add,
                    /*bytes*/static_cast<uint64_t>(p) * problem.ct_limb_bytes,
                    /*input_limbs*/p,
                    /*output_limbs*/0,
                    /*work_items*/p
                );
                builder.bram.ReleaseOnIssue(
                    static_cast<uint64_t>(p) * problem.ct_limb_bytes
                );
                if (!builder.Ok()) {
                    return CycleProgram{};
                }
            }
        }
    }

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
            if (!builder.Ok()) {
                return CycleProgram{};
            }
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

            // Send eval key limb
            std::cout << "Step 4." << (i+1) << ": send eval key limb" << std::endl;
            emit_op(
                /*name*/"send_eval_key_limb",
                /*kind*/CycleInstructionKind::LoadHBM,
                /*transfer_path*/CycleTransferPath::HBMToSPM,
                /*type*/CycleOpType::KeyLoad,
                /*bytes*/static_cast<uint64_t>(p) * problem.ct_limb_bytes,
                /*input_limbs*/p,
                /*output_limbs*/0,
                /*work_items*/p
            );
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(p) * problem.ct_limb_bytes
            );
            if (!builder.Ok()) {
                return CycleProgram{};
            }

            // Inner product for one limb across p ciphertexts
            std::cout << "Step 4." << (i+1) << ": inner product for one limb" << std::endl;
            emit_op(
                /*name*/"inner_product_limb",
                /*kind*/CycleInstructionKind::EweMul,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::Multiply,
                /*bytes*/static_cast<uint64_t>(p) * problem.ct_limb_bytes,
                /*input_limbs*/p,
                /*output_limbs*/0,
                /*work_items*/p
            );  

            // release eval key limb
            builder.bram.ReleaseOnIssue(
                static_cast<uint64_t>(p) * problem.ct_limb_bytes
            );
             // release the one limb
            builder.bram.ReleaseOnIssue(
                static_cast<uint64_t>(1) * problem.ct_limb_bytes
            );
            // get result
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(2) * problem.ct_limb_bytes
            );
            if (!builder.Ok()) {
                return CycleProgram{};
            }
            if (j > 0){
                std::cout << "Step 4." << (i+1) << ": Accumulate result to previous limb" << std::endl;
                emit_op(
                    /*name*/"accumulate_limb",
                    /*kind*/CycleInstructionKind::EweAdd,
                    /*transfer_path*/CycleTransferPath::None,
                    /*type*/CycleOpType::Add,
                    /*bytes*/static_cast<uint64_t>(p) * problem.ct_limb_bytes,
                    /*input_limbs*/p,
                    /*output_limbs*/0,
                    /*work_items*/p
                );
                builder.bram.ReleaseOnIssue(static_cast<uint64_t>(p) * problem.ct_limb_bytes);
                builder.bram.ReleaseOnIssue(static_cast<uint64_t>(p) * problem.ct_limb_bytes);
                builder.bram.AcquireOnIssue(static_cast<uint64_t>(p) * problem.ct_limb_bytes);
                if (!builder.Ok()) {
                    return CycleProgram{};
                }
            }
            
            // For other 2*k limbs, we can directly bconv
            for (uint32_t m = 0; m < p; m++){
                emit_op(
                    /*name*/"bconv_one_limb",
                    /*kind*/CycleInstructionKind::BConv,
                    /*transfer_path*/CycleTransferPath::None,
                    /*type*/CycleOpType::BConv,
                    /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
                    /*input_limbs*/k,
                    /*output_limbs*/1,
                    /*work_items*/k
                );
                builder.bram.AcquireOnIssue(
                    static_cast<uint64_t>(1) * problem.ct_limb_bytes
                );
                if (!builder.Ok()) {
                    return CycleProgram{};
                }

                // NTT for limb
                std::cout << "Step 4." << (i+1) << ": NTT for limb" << std::endl;
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
            }

            // subtract the limbs and move to hbm
            for (uint32_t m = 0; m < p; m++){
                emit_op(
                    /*name*/"final_subtract_limb",
                    /*kind*/CycleInstructionKind::EweSub,
                    /*transfer_path*/CycleTransferPath::SPMToHBM,
                    /*type*/CycleOpType::Sub,
                    /*bytes*/static_cast<uint64_t>(1) * problem.ct_limb_bytes,
                    /*input_limbs*/1,
                    /*output_limbs*/0,
                    /*work_items*/1
                );
                builder.bram.ReleaseOnIssue(
                    static_cast<uint64_t>(1) * problem.ct_limb_bytes
                );
                builder.bram.ReleaseOnIssue(
                    static_cast<uint64_t>(1) * problem.ct_limb_bytes
                );
                builder.bram.AcquireOnIssue(
                    static_cast<uint64_t>(1) * problem.ct_limb_bytes
                );
                if (!builder.Ok()) {
                    return CycleProgram{};
                }

                // Send result back to HBM
                std::cout << "Step 4." << (i+1) << ": send result back to HBM" << std::endl;
                emit_op(
                    /*name*/"store_result",
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
                if (!builder.Ok()) {
                    return CycleProgram{};
                }
            }
        }
    }

    builder.program.estimated_peak_live_bytes = builder.bram.Peak();
    return std::move(builder.program);

}
