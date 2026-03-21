#include "backend/cycle_backend/cycle_backend_output_centric.h"
#include "backend/cycle_backend/cycle_backend_primitives.h"
#include <cstdint>
#include <string>
#include <utility>
#include <iostream>

CycleProgram BuildOutputCentricProgram(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware
) {
    CycleProgramBuilder builder(
        problem,
        hardware,
        KeySwitchMethod::OutputCentric,
        "output_centric_keyswitch");

    const uint32_t ct_now = problem.ciphertexts;
    const uint32_t p = problem.polys;
    const uint32_t digit_num = problem.digits;
    const uint32_t digit_limbs = problem.digit_limbs;
    const uint32_t l = problem.limbs; // number of limbs in one polynomial
    const uint32_t lk = problem.key_limbs; // number of limbs in the key
    std::cout << "Building Output-Centric program with ct " << ct_now
              << ", p " << p
              << ", digit_num " << digit_num
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

    // Check digit_num == 3
    if (digit_num != 3) {
        std::cerr << "Output-Centric method currently only supports digit_num == 3, but got " << digit_num << std::endl;
        return CycleProgram{};
    }

    // Step 1: Load the two input digits to BRAM
    std::cout << "Step 1: load the two input digits to BRAM, total bytes: " << static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes << std::endl;
    for (uint32_t i = 0; i < 2; i++){
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
        );
        emit_op(
            /*name*/"load_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::LoadHBM,
            /*transfer_path*/CycleTransferPath::HBMToSPM,
            /*type*/CycleOpType::DataLoad,
            /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/ct_now * digit_limbs,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(ct_now) * digit_limbs
        );
        if (!builder.Ok()) {
             return CycleProgram{};
        }
    }

    // Step 2: INTT for the two digits
    std::cout << "Step 2: INTT for the two digits" << std::endl;
    for (uint32_t i = 0; i < 2; i++){
        emit_op(
            /*name*/"intt_digit_" + std::to_string(i),
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

    // Step 3: BConv for two digit to generate the last digit limbs
    std::cout << "Step 3: BConv for two digit to generate the last digit limbs" << std::endl;
    for (uint32_t i = 0; i < 2; i++){
        emit_op(
            /*name*/"bconv_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::BConv,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::BConv,
            /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/digit_limbs,
            /*work_items*/static_cast<uint64_t>(digit_limbs)
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
             return CycleProgram{};
        }
    }

    // Step 4: For two digits, NTT for new generated limbs
    std::cout << "Step 4: For two digits, NTT for new generated limbs" << std::endl;
    for (uint32_t i = 0; i < 2; i++){
        emit_op(
            /*name*/"ntt_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::NTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::NTT,
            /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
             return CycleProgram{};
        }
    }

    // Step 5: For each digit, load the 2 * digit_limbs eval key to BRAM
    std::cout << "Step 5: For each digit, load the 2 * digit_limbs eval key to BRAM" << std::endl;
    for (uint32_t i = 0; i < digit_num; i++){
        for (uint32_t m = 0; m < p; m++){
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
            );
            emit_op(
                /*name*/"load_eval_key_digit_" + std::to_string(i) + "_part_" + std::to_string(m),
                /*kind*/CycleInstructionKind::LoadHBM,
                /*transfer_path*/CycleTransferPath::HBMToSPM,
                /*type*/CycleOpType::KeyLoad,
                /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
                /*input_limbs*/digit_limbs,
                /*output_limbs*/0,
                /*work_items*/static_cast<uint64_t>(digit_limbs)
             );
            if (!builder.Ok()) {
                return CycleProgram{};
            }
        }
    }

    // Step 6: For each digit, Multiply and accumulate the partial results for each digit with digit_limbs limbs
    std::cout << "Step 6: For each digit, Multiply and accumulate the partial results for each digit with digit_limbs limbs" << std::endl;
    for (uint32_t m = 0; m < p; m++){
        for (uint32_t i = 0; i < digit_num; i++){
            emit_op(
                /*name*/"mul_acc_digit_" + std::to_string(i) + "_part_" + std::to_string(m),
                /*kind*/CycleInstructionKind::EweMul,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::Multiply,
                /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
                /*input_limbs*/digit_limbs,
                /*output_limbs*/digit_limbs,
                /*work_items*/static_cast<uint64_t>(digit_limbs)
             );
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
            );
            builder.bram.ReleaseOnIssue(
                static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes            
            );
            if (!builder.Ok()) {
                return CycleProgram{};
            }
            if (i > 0){
                emit_op(
                    /*name*/"acc_digit_" + std::to_string(i) + "_part_" + std::to_string(m),
                    /*kind*/CycleInstructionKind::EweAdd,
                    /*transfer_path*/CycleTransferPath::None,
                    /*type*/CycleOpType::Add,
                    /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
                    /*input_limbs*/digit_limbs,
                    /*output_limbs*/digit_limbs,
                    /*work_items*/static_cast<uint64_t>(digit_limbs)
                );
                builder.bram.AcquireOnIssue(
                    static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
                );
                builder.bram.ReleaseOnIssue(
                    static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
                );
                builder.bram.ReleaseOnIssue(
                    static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
                );
                if (!builder.Ok()) {
                    return CycleProgram{};
                }
            }
        }
    }

    // Send the accumulated results with digit_limbs limbs for each digit back to HBM
    std::cout << "Step 7: Send the accumulated results with digit_limbs limbs for each digit back to HBM" << std::endl;
    for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"store_acc_result_digit_part_" + std::to_string(i),
            /*kind*/CycleInstructionKind::StoreHBM,
            /*transfer_path*/CycleTransferPath::SPMToHBM,
            /*type*/CycleOpType::Spill,
            /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
            /*input_limbs*/0,
            /*output_limbs*/digit_limbs,
            /*work_items*/static_cast<uint64_t>(digit_limbs)
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
             return CycleProgram{};
        }
    }

    // Step 8: Load the last digit with digit_limbs limbs to BRAM, and INTT
    std::cout << "Step 8: Load the last digit with digit_limbs limbs to BRAM, and INTT" << std::endl;
    emit_op(
        /*name*/"load_last_digit",
        /*kind*/CycleInstructionKind::LoadHBM,
        /*transfer_path*/CycleTransferPath::HBMToSPM,
        /*type*/CycleOpType::DataLoad,
        /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
        /*input_limbs*/digit_limbs,
        /*output_limbs*/0,
        /*work_items*/static_cast<uint64_t>(digit_limbs)
    );
    builder.bram.AcquireOnIssue(
        static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
    );
    if (!builder.Ok()) {
         return CycleProgram{};
    }
    emit_op(
        /*name*/"intt_last_digit",
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

    // Step 9: BConv for the two digits and NTT
    std::cout << "Step 9: BConv for the two digits and NTT" << std::endl;
    for (uint32_t i = 0; i < 2; i++) {
        emit_op(
            /*name*/"bconv_last_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::BConv,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::BConv,
             /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/digit_limbs,
            /*work_items*/static_cast<uint64_t>(digit_limbs)
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            return CycleProgram{};
        }
        emit_op(
            /*name*/"ntt_last_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::NTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::NTT,
            /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(digit_limbs)
        );
        if (!builder.Ok()) {
            return CycleProgram{};
        }
    }

    // Step 10: For each digit, load the corresponding eval key
    std::cout << "Step 10: For each digit, load the corresponding eval key" << std::endl;
    for (uint32_t i = 0; i < digit_num; i++){
        for (uint32_t m = 0; m < p; m++){
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
            );
            emit_op(
                /*name*/"load_eval_key_for_last_digit_" + std::to_string(i),
                /*kind*/CycleInstructionKind::LoadHBM,
                /*transfer_path*/CycleTransferPath::HBMToSPM,
                /*type*/CycleOpType::KeyLoad,
                /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
                /*input_limbs*/digit_limbs,
                /*output_limbs*/0,
                /*work_items*/static_cast<uint64_t>(digit_limbs)
             );
            if (!builder.Ok()) {
                return CycleProgram{};
            }
        }
    }

    // Step 11: For each digit, Multiply and accumulate the partial results for each digit with digit_limbs limbs, and store the accumulated result back to HBM
    std::cout << "Step 11: For each digit, Multiply and accumulate the partial results for each digit with digit_limbs limbs, and store the accumulated result back to HBM" << std::endl;
    for (uint32_t m = 0; m < p; m++){
        for (uint32_t i = 0; i < digit_num; i++){
            emit_op(
                /*name*/"mul_acc_last_digit_" + std::to_string(i) + "_part_" + std::to_string(m),
                /*kind*/CycleInstructionKind::EweMul,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::Multiply,
                /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
                /*input_limbs*/digit_limbs,
                /*output_limbs*/digit_limbs,
                /*work_items*/static_cast<uint64_t>(digit_limbs)
             );
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
            );
            builder.bram.ReleaseOnIssue(
                static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
            );
            if (!builder.Ok()) {
                return CycleProgram{};
            }
            if (i > 0){
                emit_op(
                    /*name*/"acc_last_digit_" + std::to_string(i) + "_part_" + std::to_string(m),
                    /*kind*/CycleInstructionKind::EweAdd,
                    /*transfer_path*/CycleTransferPath::None,
                    /*type*/CycleOpType::Add,
                    /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
                    /*input_limbs*/digit_limbs,
                    /*output_limbs*/digit_limbs,
                    /*work_items*/static_cast<uint64_t>(digit_limbs)
                );
                builder.bram.AcquireOnIssue(
                    static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
                );
                builder.bram.ReleaseOnIssue(
                    static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
                );
                builder.bram.ReleaseOnIssue(
                    static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
                );
                if (!builder.Ok()) {
                    return CycleProgram{};
                }
            }
        }
    }

    // Send the accumulated results with digit_limbs limbs for each digit back to HBM
    std::cout << "Step 12: Send the accumulated results with digit_limbs limbs for each digit back to HBM" << std::endl;
    for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"store_acc_result_last_digit_part_" + std::to_string(i),
            /*kind*/CycleInstructionKind::StoreHBM,
            /*transfer_path*/CycleTransferPath::SPMToHBM,
            /*type*/CycleOpType::Spill,
             /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
            /*input_limbs*/0,
            /*output_limbs*/digit_limbs,
            /*work_items*/static_cast<uint64_t>(digit_limbs)
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
             return CycleProgram{};
        }
    }

    // Step 13: Repeat BConv, NTT, Mul-Acc for the last digit with digit_limbs limbs for l/digit_limbs times, and store the final result with l limbs back to HBM
    std::cout << "Step 13: Repeat BConv, NTT, Mul-Acc for the last digit with digit_limbs limbs for l/digit_limbs times, and store the final result with l limbs back to HBM" << std::endl;
    for (uint32_t i = 0; i < 2; i++) {
        emit_op(
            /*name*/"bconv_last_digit_" + std::to_string(i) + "_full",
            /*kind*/CycleInstructionKind::BConv,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::BConv,
             /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/digit_limbs,
            /*work_items*/digit_limbs
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            return CycleProgram{};
        }
        emit_op(
            /*name*/"ntt_last_digit_" + std::to_string(i) + "_full",
            /*kind*/CycleInstructionKind::NTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::NTT,
            /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(digit_limbs)
        );
        if (!builder.Ok()) {
            return CycleProgram{};
        }
    }

    // For each digit, load the corresponding eval key for the full digit limbs, and do mul-acc for l/digit_limbs times
    std::cout << "For each digit, load the corresponding eval key for the full digit limbs, and do mul-acc for l/digit_limbs times" << std::endl;
    for (uint32_t i = 0; i < digit_num; i++){
        for (uint32_t m = 0; m < p; m++){
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
            );
            emit_op(
                /*name*/"load_eval_key_for_full_last_digit_" + std::to_string(i),
                /*kind*/CycleInstructionKind::LoadHBM,
                /*transfer_path*/CycleTransferPath::HBMToSPM,
                /*type*/CycleOpType::KeyLoad,
                /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
                /*input_limbs*/digit_limbs,
                /*output_limbs*/0,
                /*work_items*/static_cast<uint64_t>(digit_limbs)
             );
            if (!builder.Ok()) {
                return CycleProgram{};
            }
        }
    }

    for (uint32_t m = 0; m < p; m++){
        for (uint32_t i = 0; i < digit_num; i++){
            emit_op(
                /*name*/"mul_acc_full_last_digit_" + std::to_string(i) + "_part_" + std::to_string(m),
                /*kind*/CycleInstructionKind::EweMul,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::Multiply,
                /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
                /*input_limbs*/digit_limbs,
                /*output_limbs*/digit_limbs,
                /*work_items*/static_cast<uint64_t>(digit_limbs)
             );
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
            );
            builder.bram.ReleaseOnIssue(
                static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
            );
            if (!builder.Ok()) {
                return CycleProgram{};
            }
            if (i > 0){
                emit_op(
                    /*name*/"acc_full_last_digit_" + std::to_string(i) + "_part_" + std::to_string(m),
                    /*kind*/CycleInstructionKind::EweAdd,
                    /*transfer_path*/CycleTransferPath::None,
                    /*type*/CycleOpType::Add,
                    /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
                    /*input_limbs*/digit_limbs,
                    /*output_limbs*/digit_limbs,
                    /*work_items*/static_cast<uint64_t>(digit_limbs)
                );
                builder.bram.AcquireOnIssue(
                    static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
                );
                builder.bram.ReleaseOnIssue(
                    static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
                );
                builder.bram.ReleaseOnIssue(
                    static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
                );
                if (!builder.Ok()) {
                    return CycleProgram{};
                }
            }
        }
    }

    // Send the accumulated result with digit_limbs limbs for each digit back to HBM
    std::cout << "Send the accumulated result with digit_limbs limbs for each digit back to HBM" << std::endl;
    for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"store_acc_result_full_last_digit_part_" + std::to_string(i),
            /*kind*/CycleInstructionKind::StoreHBM,
            /*transfer_path*/CycleTransferPath::SPMToHBM,
            /*type*/CycleOpType::Spill,
             /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
            /*input_limbs*/0,
            /*output_limbs*/digit_limbs,
            /*work_items*/static_cast<uint64_t>(digit_limbs)
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
             return CycleProgram{};
        }
    }

    // Bconv for the last digit with digit_limbs limbs for l/digit_limbs times
    std::cout << "Bconv for the last digit with digit_limbs limbs for l/digit_limbs times" << std::endl;
    for (uint32_t i = 0; i < digit_num; i++) {
        emit_op(
            /*name*/"bconv_full_last_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::BConv,
             /*transfer_path*/CycleTransferPath::None,
             /*type*/CycleOpType::BConv,
             /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/digit_limbs,
            /*work_items*/static_cast<uint64_t>(digit_limbs)
         );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
             return CycleProgram{};
        }

        emit_op(
            /*name*/"ntt_full_last_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::NTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::NTT,
            /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
             /*output_limbs*/0,
             /*work_items*/static_cast<uint64_t>(digit_limbs)
         );
         if (!builder.Ok()) {
             return CycleProgram{};
         }
    }

    // For each digit, load the corresponding eval key for the full digit limbs, and do mul-acc for l/digit_limbs times, and store the final result back to HBM
    std::cout << "For each digit, load the corresponding eval key for the full digit limbs, and do mul-acc for l/digit_limbs times, and store the final result back to HBM" << std::endl;
    for (uint32_t i = 0; i < digit_num; i++){
        for (uint32_t m = 0; m < p; m++){
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
            );
            emit_op(
                /*name*/"load_eval_key_for_full_last_digit_" + std::to_string(i) + "_part_" + std::to_string(m),
                /*kind*/CycleInstructionKind::LoadHBM,
                /*transfer_path*/CycleTransferPath::HBMToSPM,
                /*type*/CycleOpType::KeyLoad,
                /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
                /*input_limbs*/digit_limbs,
                /*output_limbs*/0,
                /*work_items*/static_cast<uint64_t>(digit_limbs)
             );
            if (!builder.Ok()) {
                return CycleProgram{};
            }
        }
    }

    // For each digit, do mul-acc for l/digit_limbs times, and store the final result back to HBM
    std::cout << "For each digit, do mul-acc for l/digit_limbs times, and store the final result back to HBM" << std::endl;
    for (uint32_t m = 0; m < p; m++){
        for (uint32_t i = 0; i < digit_num; i++){
            emit_op(
                /*name*/"mul_acc_full_last_digit_" + std::to_string(i) + "_part_" + std::to_string(m) + "_final",
                /*kind*/CycleInstructionKind::EweMul,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::Multiply,
                /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
                /*input_limbs*/digit_limbs,
                /*output_limbs*/digit_limbs,
                /*work_items*/static_cast<uint64_t>(digit_limbs)
             );
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
            );
            builder.bram.ReleaseOnIssue(
                static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
            );
            if (!builder.Ok()) {
                return CycleProgram{};
            }
            if (i > 0){
                emit_op(
                    /*name*/"acc_full_last_digit_" + std::to_string(i) + "_part_" + std::to_string(m) + "_final",
                    /*kind*/CycleInstructionKind::EweAdd,
                    /*transfer_path*/CycleTransferPath::None,
                    /*type*/CycleOpType::Add,
                    /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
                    /*input_limbs*/digit_limbs,
                    /*output_limbs*/digit_limbs,
                    /*work_items*/static_cast<uint64_t>(digit_limbs)
                );
                builder.bram.AcquireOnIssue(
                    static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
                );
                builder.bram.ReleaseOnIssue(
                    static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
                );
                builder.bram.ReleaseOnIssue(
                    static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
                );
                if (!builder.Ok()) {
                    return CycleProgram{};
                }
            }
        }
    }

    if (digit_num != (lk-l)){
        std::cerr << "Output-Centric method currently only supports digit_num == lk-l, but got digit_num " << digit_num << " and lk-l " << (lk-l) << std::endl;
        return CycleProgram{};
    }
    // INTT, BConv NTT
    for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"moddigit_" + std::to_string(i),
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
        emit_op(
            /*name*/"bconv_moddigit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::BConv,
             /*transfer_path*/CycleTransferPath::None,
             /*type*/CycleOpType::BConv,
             /*bytes*/static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/l,
            /*work_items*/static_cast<uint64_t>(digit_limbs)
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(l) * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(digit_limbs) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            return CycleProgram{};
        }
        emit_op(
            /*name*/"ntt_moddigit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::NTT,
             /*transfer_path*/CycleTransferPath::None,
             /*type*/CycleOpType::NTT,
             /*bytes*/static_cast<uint64_t>(l) * problem.ct_limb_bytes,
            /*input_limbs*/l,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(l)
        );

        if (!builder.Ok()) {
            return CycleProgram{};
        }
    }

    // Load 2*l limbs from hbm, sub, and send back to hbm
    for (uint32_t i = 0; i < p; i++){
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(l) * problem.ct_limb_bytes
        );
        emit_op(
            /*name*/"load_moddigit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::LoadHBM,
            /*transfer_path*/CycleTransferPath::HBMToSPM,
            /*type*/CycleOpType::DataLoad,
            /*bytes*/static_cast<uint64_t>(l) * problem.ct_limb_bytes,
            /*input_limbs*/l,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(l)
        );
        if (!builder.Ok()) {
            return CycleProgram{};
        }
        emit_op(
            /*name*/"sub_moddigit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::EweSub,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::Sub,
            /*bytes*/static_cast<uint64_t>(l) * problem.ct_limb_bytes,
            /*input_limbs*/l,
            /*output_limbs*/l,
            /*work_items*/static_cast<uint64_t>(l)
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(l) * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(l) * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(l) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            return CycleProgram{};
        }

        emit_op(
            /*name*/"store_moddigit_" + std::to_string(i),
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



    




    builder.program.estimated_peak_live_bytes = builder.bram.Peak();
    return std::move(builder.program);
}
