#include "backend/cycle_backend/cycle_backend_cinnamon_ib.h"

#include "backend/cycle_backend/cycle_backend_primitives.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

CycleProgram BuildCinnamonInputBroadcastProgram(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware
) {

    CycleProgramBuilder builder(
        problem,
        hardware,
        problem.method,
        "cinnamon_input_broadcast_keyswitch"
    );

    const uint32_t active_cards = std::max<uint32_t>(1, problem.active_cards);
    const uint32_t ct_now = problem.ciphertexts;
    const uint32_t p = problem.polys;
    const uint32_t digit_num = problem.digits;
    const uint32_t digit_limbs = problem.digit_limbs; // number of limbs
    const uint32_t l = problem.limbs;
    const uint32_t k = problem.num_k;
    const uint32_t lk = problem.key_limbs; // number of limbs in the key

    std::cout << "Building Cinnamon Input Broadcast program with" 
            << ",active_cards " << active_cards 
            << ",ct " << ct_now
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
        return builder.EmitPrimitive(kind, desc);
    };

    // Send the all ciphertext from card 0 to all other cards if active_cards > 1
    // so that each card can perform the key switch for its assigned digit(s).
    for (uint32_t card_idx = 1; card_idx < active_cards; card_idx++) {
        emit_op(
            /*name*/"recv_input_card_" + std::to_string(card_idx+1),
            /*kind*/CycleInstructionKind::InterCardSend,
            /*transfer_path*/CycleTransferPath::HBMToHBM,
            /*type*/CycleOpType::InterCardComm,
             /*bytes*/static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes,
            /*input_limbs*/0,
            /*output_limbs*/l,
            /*work_items*/static_cast<uint64_t>(ct_now) * l
        );
    }

    builder.bram.AcquireOnIssue(
        static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
    );


    for (uint32_t i = 0; i < digit_num; i++){
        // INTT for all limbs
        emit_op(
            /*name*/"intt_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::INTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::INTT,
            /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(ct_now) * digit_limbs
        );
        if (i == 0){
            // Only BConv to generate P and NTT
            // Else generate P and the one digit_limb
            emit_op(
                /*name*/"bconv_digit_" + std::to_string(i),
                /*kind*/CycleInstructionKind::BConv,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::BConv,
                /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
                /*input_limbs*/digit_limbs,
                /*output_limbs*/k,
                /*work_items*/static_cast<uint64_t>(ct_now) * digit_limbs
            );
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(ct_now) * k * problem.ct_limb_bytes
            );
            emit_op(
                /*name*/"ntt_digit_" + std::to_string(i),
                /*kind*/CycleInstructionKind::NTT,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::NTT,
                /*bytes*/static_cast<uint64_t>(ct_now) * k * problem.ct_limb_bytes,
                /*input_limbs*/k,
                /*output_limbs*/0,
                /*work_items*/static_cast<uint64_t>(ct_now) * k
            );
        }else{
            emit_op(
                /*name*/"bconv_digit_" + std::to_string(i),
                /*kind*/CycleInstructionKind::BConv,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::BConv,
                /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
                /*input_limbs*/digit_limbs,
                /*output_limbs*/(k+digit_limbs),
                /*work_items*/static_cast<uint64_t>(ct_now) * digit_limbs
            );
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(ct_now) * (k+digit_limbs) * problem.ct_limb_bytes
            );
            builder.bram.ReleaseOnIssue(
                static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
            );
            emit_op(
                /*name*/"ntt_digit_" + std::to_string(i),
                /*kind*/CycleInstructionKind::NTT,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::NTT,
                /*bytes*/static_cast<uint64_t>(ct_now) * (k+digit_limbs) * problem.ct_limb_bytes,
                /*input_limbs*/(k+digit_limbs),
                /*output_limbs*/0,
                /*work_items*/static_cast<uint64_t>(ct_now) * (k+digit_limbs)
            );
        }
        if (!builder.Ok()) {
            std::cerr << "Failed to build INTT/BConv/NTT for digit " << i << std::endl;
            return CycleProgram();
        }
    }

    // Now need to load the 2*(k+digit_limbs) limbs eval keys
    for (uint32_t i = 0; i < p; i++) {
        for (uint32_t j = 0; j < digit_num; j++){
            // emit load
            emit_op(
                /*name*/"load_key_" + std::to_string(i) + "_" + std::to_string(j),
                /*kind*/CycleInstructionKind::LoadHBM,
                /*transfer_path*/CycleTransferPath::HBMToSPM,
                /*type*/CycleOpType::KeyLoad,
                /*bytes*/static_cast<uint64_t>(ct_now) * (k+digit_limbs) * problem.ct_limb_bytes,
                /*input_limbs*/(k+digit_limbs),
                /*output_limbs*/0,
                /*work_items*/static_cast<uint64_t>(ct_now) * (k+digit_limbs)
            );
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(ct_now) * (k+digit_limbs) * problem.ct_limb_bytes
            );
            if (!builder.Ok()) {
                std::cerr << "Failed to acquire BRAM for loading key for digit " << j << ", p " << i << std::endl;
                return CycleProgram();
            }
            // emit mult
            emit_op(
                /*name*/"mul_digit_" + std::to_string(j) + "_" + std::to_string(i),
                /*kind*/CycleInstructionKind::EweMul,
                /*transfer_path*/CycleTransferPath::None,
                /*type*/CycleOpType::Multiply,
                /*bytes*/static_cast<uint64_t>(ct_now) * (k+digit_limbs) * problem.ct_limb_bytes,
                /*input_limbs*/(k+digit_limbs),
                /*output_limbs*/(k+digit_limbs),
                /*work_items*/static_cast<uint64_t>(ct_now) * (k+digit_limbs)
            );
            builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(ct_now) * (k+digit_limbs) * problem.ct_limb_bytes
            );
            builder.bram.ReleaseOnIssue(
                static_cast<uint64_t>(ct_now) * (k+digit_limbs) * problem.ct_limb_bytes
            );
            if(j > 0){
                // emit add to accumulate the result for each digit
                emit_op(
                    /*name*/"add_digit_" + std::to_string(j) + "_" + std::to_string(i),
                    /*kind*/CycleInstructionKind::EweAdd,
                    /*transfer_path*/CycleTransferPath::None,
                    /*type*/CycleOpType::Add,
                    /*bytes*/static_cast<uint64_t>(ct_now) * (k+digit_limbs) * problem.ct_limb_bytes,
                    /*input_limbs*/(k+digit_limbs),
                    /*output_limbs*/(k+digit_limbs),
                    /*work_items*/static_cast<uint64_t>(ct_now) * (k+digit_limbs)
                );
                builder.bram.AcquireOnIssue(
                    static_cast<uint64_t>(ct_now) * (k+digit_limbs) * problem.ct_limb_bytes
                );
                builder.bram.ReleaseOnIssue(
                    static_cast<uint64_t>(ct_now) * (k+digit_limbs) * problem.ct_limb_bytes
                );
                builder.bram.ReleaseOnIssue(
                    static_cast<uint64_t>(ct_now) * (k+digit_limbs) * problem.ct_limb_bytes
                );
                if (!builder.Ok()) {
                    std::cerr << "Failed to build add for digit " << j << ", p " << i << std::endl;
                    return CycleProgram();
                }
            }
        }
    }

    // Release the digit_num * (k+digit_limbs) * problem.ct_limb_bytes
    for (uint32_t i = 0; i < digit_num; i++){
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * (k+digit_limbs) * problem.ct_limb_bytes
        );
    }

    // INTT for 2*k limbs to get the final result in evaluation form
    for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"final_ntt_" + std::to_string(i),
            /*kind*/CycleInstructionKind::NTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::NTT,
             /*bytes*/static_cast<uint64_t>(ct_now) * k * problem.ct_limb_bytes,
            /*input_limbs*/k,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(ct_now) * k
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * k * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * k * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            std::cerr << "Failed to build final NTT for p " << i << std::endl;
            return CycleProgram();
        }
    }

    
    // BConv
    for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"final_bconv_" + std::to_string(i),
            /*kind*/CycleInstructionKind::BConv,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::BConv,
            /*bytes*/static_cast<uint64_t>(ct_now) * k * problem.ct_limb_bytes,
            /*input_limbs*/k,
            /*output_limbs*/digit_limbs,
            /*work_items*/static_cast<uint64_t>(ct_now) * k
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * k * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            std::cerr << "Failed to build final BConv for p " << i << std::endl;
            return CycleProgram();
        }
        // NTT for the final result in digit form
        emit_op(
            /*name*/"final_ntt_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::NTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::NTT,
            /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(ct_now) * digit_limbs
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            std::cerr << "Failed to build final NTT for digit result for p " << i << std::endl;
            return CycleProgram();
        }
    }

    // Subtract the result
    for (uint32_t i = 0; i < p; i++){
        emit_op(
            /*name*/"subtract_input_" + std::to_string(i),
            /*kind*/CycleInstructionKind::EweSub,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::Sub,
            /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
             /*input_limbs*/digit_limbs,
             /*output_limbs*/digit_limbs,
             /*work_items*/static_cast<uint64_t>(ct_now) * digit_limbs
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            std::cerr << "Failed to build subtraction of input for p " << i << std::endl;
            return CycleProgram();
        }
    }

    // Collect the result from all cards if active_cards > 1
    for (uint32_t card_idx = 1; card_idx < active_cards; card_idx++) {
        emit_op(
            /*name*/"send_result_card_" + std::to_string(card_idx+1),
            /*kind*/CycleInstructionKind::InterCardSend,
            /*transfer_path*/CycleTransferPath::HBMToHBM,
            /*type*/CycleOpType::InterCardComm,
             /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/0,
            /*output_limbs*/digit_limbs,
            /*work_items*/static_cast<uint64_t>(ct_now) * digit_limbs
        );
        builder.bram.AcquireOnIssue(
                static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            std::cerr << "Failed to release BRAM for sending result from card " << card_idx << std::endl;
            return CycleProgram();
        }
    }


    builder.program.estimated_peak_live_bytes = builder.bram.Peak();
    return std::move(builder.program);
}
