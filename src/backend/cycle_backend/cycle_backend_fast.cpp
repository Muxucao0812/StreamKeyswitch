#include "backend/cycle_backend/cycle_backend_fast.h"
#include "backend/cycle_backend/cycle_backend_primitives.h"
#include <cstdint>
#include <string>
#include <utility>
#include <iostream>

CycleProgram BuildFastProgram(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware
) {
    CycleProgramBuilder builder(
        problem,
        hardware,
        KeySwitchMethod::FAST,
        "fast_keyswitch");

    const uint32_t ct_now = problem.ciphertexts;
    const uint32_t p = problem.polys;
    const uint32_t digit_num = problem.digits;
    const uint32_t digit_limbs = problem.digit_limbs; // number of limbs in each digit
    const uint32_t l = problem.limbs; // number of limbs in one polynomial
    const uint32_t lk = problem.key_limbs; // number of limbs in the key, which is l + digit_limbs 


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
        static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
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

    // Step 3: Interation BConv NTT Innerprod
    std::cout << "Step 3: BConv NTT Innerprod" << std::endl;
    for (uint32_t i = 0; i < lk; i++) {
        for (uint32_t j = 0; j < digit_num; j++){
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(1) * problem.ct_limb_bytes
            );
            emit_op(
                /*name*/"bconv",
                /*kind*/CycleInstructionKind::BConv,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::BConv,
                /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
                /*input_limbs*/digit_limbs,
                /*output_limbs*/1,
                /*work_items*/static_cast<uint64_t>(ct_now) * digit_limbs
            );
   
            if (!builder.Ok()) {
                return CycleProgram{}; 
            }
            emit_op(
                /*name*/"NTT",
                /*kind*/CycleInstructionKind::NTT,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::NTT,
                /*bytes*/0,
                /*input_limbs*/1,
                /*output_limbs*/0,
                /*work_items*/static_cast<uint64_t>(ct_now)
            );

            for (uint32_t k = 0; k < p; k++){
                // Thie is multiply with evalkey
                builder.bram.AcquireOnIssue(
                    static_cast<uint64_t>(1) * problem.ct_limb_bytes
                );
                if (!builder.Ok()) {
                    return CycleProgram{};
                }
                emit_op(
                    /*name*/"mul_evalkey",
                    /*kind*/CycleInstructionKind::EweMul,
                    /*transfer_path*/CycleTransferPath::None,
                    /*type*/CycleOpType::Multiply,
                    /*bytes*/0,
                    /*input_limbs*/p,
                    /*output_limbs*/0,
                    /*work_items*/static_cast<uint64_t>(ct_now) * p
                );
            }
            // release the ct limb after processing
            builder.bram.ReleaseOnIssue(
                static_cast<uint64_t>(1) * problem.ct_limb_bytes
            );
            if (!builder.Ok()) {
                return CycleProgram{};
            }
            if (j > 0) {
                // accumulate the results of different digits
                emit_op(
                    /*name*/"accumulate",
                    /*kind*/CycleInstructionKind::EweAdd,
                    /*transfer_path*/CycleTransferPath::None,
                    /*type*/CycleOpType::Add,
                    /*bytes*/static_cast<uint64_t>(ct_now) * p * 2 * problem.ct_limb_bytes,
                    /*input_limbs*/p * 2,
                    /*output_limbs*/0,
                    /*work_items*/static_cast<uint64_t>(ct_now) * p
                );
                builder.bram.ReleaseOnIssue(
                    static_cast<uint64_t>(p) * problem.ct_limb_bytes
                );
                if (!builder.Ok()) {
                    return CycleProgram{};
                }
            }
        }
        // spill the accumulated result to HBM after processing each key limb, to free up BRAM for next key limb processing
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(p) * problem.ct_limb_bytes
        );
        emit_op(
            /*name*/"store_intermediate",
            /*kind*/CycleInstructionKind::StoreHBM,
            /*transfer_path*/CycleTransferPath::SPMToHBM,
            /*type*/CycleOpType::Spill,
            /*bytes*/static_cast<uint64_t>(p) * problem.ct_limb_bytes,
            /*input_limbs*/0,
            /*output_limbs*/p
        );
        if (!builder.Ok()) {
            return CycleProgram{};
        }      
    }

    // 做完之后片上的数据都没有用了
    builder.bram.ReleaseOnIssue(
        static_cast<uint64_t>(l) * ct_now * p * problem.ct_limb_bytes
    );
    if (!builder.Ok()) {
        return CycleProgram{};
    }

    

    // Step 4: Load 2*k limbs ct to BRAM and do INTT
    for (uint32_t i = 0; i < p; i++){
        std::cout << "Step 4." << (i+1) << ": load intermediate result to BRAM" << std::endl;
        builder.bram.AcquireOnIssue(static_cast<uint64_t>(lk-l) * problem.ct_limb_bytes);
        emit_op(
            /*name*/"load_intermediate",
            /*kind*/CycleInstructionKind::LoadHBM,
            /*transfer_path*/CycleTransferPath::HBMToSPM,
            /*type*/CycleOpType::DataLoad,
            /*bytes*/static_cast<uint64_t>(lk - l) * problem.ct_limb_bytes,
            /*input_limbs*/lk - l,
            /*output_limbs*/0,
            /*work_items*/lk - l
        );
        if (!builder.Ok()) {
            return CycleProgram{};
        }
        std::cout << "Step 4." << (i+1) << ": INTT for intermediate result" << std::endl;
        emit_op(
            /*name*/"intt_intermediate",
            /*kind*/CycleInstructionKind::INTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::INTT,
            /*bytes*/0,
            /*input_limbs*/lk - l,
            /*output_limbs*/0,
            /*work_items*/lk - l
        );
        std::cout << "Step 4." << (i+1) << ": bconv for intermediate result" << std::endl;
        emit_op(
            /*name*/"bconv_intermediate",
            /*kind*/CycleInstructionKind::BConv,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::BConv,
            /*bytes*/0,
            /*input_limbs*/lk - l,
            /*output_limbs*/l,
            /*work_items*/lk - l
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

        std::cout << "Step 4." << (i+1) << ": Load ct from HBM to BRAM" << std::endl;
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(l) * problem.ct_limb_bytes
        );
        emit_op(
            /*name*/"load_ct_for_sub",
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

        std::cout << "Step 4." << (i+1) << ": subtract" << std::endl;
        emit_op(
            /*name*/"subtract_intermediate",
            /*kind*/CycleInstructionKind::EweSub,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::Sub,
            /*bytes*/0,
            /*input_limbs*/l,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(l)
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(l) * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            return CycleProgram{};
        }
        std::cout << "Step 4." << (i+1) << ": store subtracted result back to HBM" << std::endl;
        emit_op(
            /*name*/"store_subtracted_intermediate",
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
