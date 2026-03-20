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
    const uint32_t l = problem.digit_limbs;
    const uint32_t lk = problem.key_limbs;
    const uint32_t p_lk = problem.polys * problem.key_limbs;

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
    std::cout << "Step 1: load ct to BRAM, total bytes: " << builder.bram.Peak() << std::endl;
    builder.bram.AcquireOnIssue(
        static_cast<uint64_t>(ct_now) * p * digit_num * l * problem.ct_limb_bytes
    );
    emit_op(
        /*name*/"load_input",
        /*kind*/CycleInstructionKind::LoadHBM,
        /*transfer_path*/CycleTransferPath::HBMToSPM,
        /*type*/CycleOpType::DataLoad,
        /*bytes*/static_cast<uint64_t>(ct_now) * p * digit_num * l * problem.ct_limb_bytes,
        /*input_limbs*/p * digit_num * l,
        0
    );

    // Step 2: load evalkey in to BRAM iteratively and do the InnerProduct
    std::cout << "Step 2: load evalkey to BRAM and do computation" << std::endl;
    for (uint32_t poly_idx = 0; poly_idx < p; ++poly_idx) {
        for (uint32_t evalkey_idx = 0; evalkey_idx < l; ++evalkey_idx) {
            for (uint32_t digit_idx = 0; digit_idx < digit_num; ++digit_idx) {
                // Implementation for loading evalkey and doing InnerProduct
                builder.bram.AcquireOnIssue(static_cast<uint64_t> (problem.ct_limb_bytes ));
                emit_op(
                    /*name*/"load_evalkey",
                    /*kind*/CycleInstructionKind::LoadHBM,
                    /*transfer_path*/CycleTransferPath::HBMToSPM,
                    /*type*/CycleOpType::DataLoad,
                    /*bytes*/static_cast<uint64_t>(problem.ct_limb_bytes),
                    /*input_limbs*/1,
                    /*output_limbs*/0
                );
                emit_op(
                    /*name*/"inner_product",
                    /*kind*/CycleInstructionKind::EweMul,
                    /*transfer_path*/CycleTransferPath::None,
                    /*type*/CycleOpType::Multiply,
                    /*bytes*/0, // No data movement for compute op
                    /*input_limbs*/1,
                    /*output_limbs*/0 // one limb output for the inner product
                );
            }
            for (uint32_t digit_idx = 0; digit_idx < digit_num - 1; ++digit_idx) {
                builder.bram.ReleaseOnComplete(static_cast<uint64_t> (problem.ct_limb_bytes ));
                emit_op(
                    /*name*/"reduce_and_spill",
                    /*kind*/CycleInstructionKind::EweAdd,
                    /*transfer_path*/CycleTransferPath::None,
                    /*type*/CycleOpType::Add,
                    /*bytes*/0, // No data movement for compute op
                    /*input_limbs*/1,
                    /*output_limbs*/0 // one limb output for the reduced result
                );
            }
            builder.bram.ReleaseOnComplete(static_cast<uint64_t> (problem.ct_limb_bytes ));
            emit_op(
                /*name*/"Spill_InnerProduct_Result",
                /*kind*/CycleInstructionKind::StoreHBM,
                /*transfer_path*/CycleTransferPath::SPMToHBM,
                /*type*/CycleOpType::Spill,
                /*bytes*/static_cast<uint64_t>(problem.ct_limb_bytes),
                /*input_limbs*/0,
                /*output_limbs*/1
            );
        }
    }
    if (!builder.Ok()) {
        return CycleProgram{};
    }

    // Step 3: Do INTT for each digital
    for (uint32_t digit_idx = 0; digit_idx < digit_num; ++digit_idx){
        // Implementation for INTT on the reduced results in HBM and the subsequent ModDown steps
        std::cout << "Step 3: INTT and ModDown for digit " << digit_idx << std::endl;
        emit_op(
            /*name*/"intt",
            /*kind*/CycleInstructionKind::INTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::INTT,
            /*bytes*/0, // No data movement for compute op
            /*input_limbs*/0,
            /*output_limbs*/0
        );

        builder.bram.AcquireOnIssue(static_cast<uint64_t>(problem.ct_limb_bytes) * (lk - l));
        emit_op(
            /*name*/"modup_bconv",
            /*kind*/CycleInstructionKind::BConv,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::BConv,
            /*bytes*/static_cast<uint64_t>(problem.ct_limb_bytes) * (lk - l),
            /*input_limbs*/l,
            /*output_limbs*/(lk - l)
        );
        if (!builder.Ok()) {
            return CycleProgram{};
        }

        for (uint32_t poly_idx = 0; poly_idx < 2; ++poly_idx){
            for (uint32_t key_idx = 0; key_idx < (lk - l); ++key_idx){
                builder.bram.AcquireOnIssue(static_cast<uint64_t>(problem.key_digit_limb_bytes));
                emit_op(
                    /*name*/"load_evalkey",
                    /*kind*/CycleInstructionKind::LoadHBM,
                    /*transfer_path*/CycleTransferPath::HBMToSPM,
                    /*type*/CycleOpType::KeyLoad,
                    /*bytes*/static_cast<uint64_t>(problem.key_digit_limb_bytes),
                    /*input_limbs*/1,
                    /*output_limbs*/0
                );
                emit_op(
                    /*name*/"inner_product",
                    /*kind*/CycleInstructionKind::EweMul,
                    /*transfer_path*/CycleTransferPath::None,
                    /*type*/CycleOpType::Multiply,
                    /*bytes*/0, // No data movement for compute op
                    /*input_limbs*/1,
                    /*output_limbs*/0 // one limb output for the inner product
                );
            }
            // release one digital lk-l limbs in BRAM after processing each poly to save BRAM space, and spill the intermediate result to HBM
            builder.bram.ReleaseOnComplete(static_cast<uint64_t>(problem.ct_limb_bytes) * (lk - l));
        }
        if (digit_idx > 0){
            builder.bram.ReleaseOnComplete(static_cast<uint64_t>(problem.ct_limb_bytes) * (lk - l) * 2);
            emit_op(
                /*name*/"reduce_and_spill",
                /*kind*/CycleInstructionKind::EweAdd,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::Add,
                /*bytes*/0, // No data movement for compute op
                /*input_limbs*/(lk - l) * 2,
                /*output_limbs*/0 // one limb output for the reduced result
             );
        }
    }

    if (!builder.Ok()) {
        return CycleProgram{};
    }

    builder.program.estimated_peak_live_bytes = builder.bram.Peak();
    return std::move(builder.program);

}
