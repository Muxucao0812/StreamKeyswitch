#include "backend/cycle_backend/cycle_backend_cinnamon_oa.h"

#include "backend/cycle_backend/cycle_backend_primitives.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

CycleProgram BuildCinnamonOutputAggregationProgram(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware
) {

    CycleProgramBuilder builder(
        problem,
        hardware,
        problem.method,
        "cinnamon_output_aggregation_keyswitch"
    );
    
    const uint32_t active_cards = std::max<uint32_t>(1, problem.active_cards);
    const uint32_t ct_now = problem.ciphertexts;
    const uint32_t p = problem.polys;
    const uint32_t digit_num = problem.digits;
    const uint32_t digit_limbs = problem.digit_limbs; // number of limbs
    const uint32_t l = problem.limbs;
    const uint32_t k = problem.num_k;
    const uint32_t lk = problem.key_limbs; // number of limbs in the key

    std::cout << "Building Cinnamon Output Aggregation program with" 
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

    // Send all digit_limbs to other cards
    for (uint32_t card_idx = 1; card_idx < active_cards; card_idx++) {
        emit_op(
            /*name*/"send_digit_limbs_card_" + std::to_string(card_idx+1),
            /*kind*/CycleInstructionKind::InterCardSend,
            /*transfer_path*/CycleTransferPath::HBMToHBM,
            /*type*/CycleOpType::InterCardComm,
             /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/digit_limbs,
            /*output_limbs*/0,
            /*work_items*/static_cast<uint64_t>(ct_now) * digit_limbs
        );
    }
    builder.bram.AcquireOnIssue(
        static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes
    );

    // INTT
    emit_op(
        /*name*/"ntt_input",
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

    // BConv
    emit_op(
        /*name*/"bconv_input",
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
        std::cerr << "Failed to acquire BRAM for bconv of input" << std::endl;
        return CycleProgram();
    }


    // NTT for the result in digit form
    emit_op(
        /*name*/"ntt_digit_result",
        /*kind*/CycleInstructionKind::NTT,
        /*transfer_path*/CycleTransferPath::None,
        /*type*/CycleOpType::NTT,
        /*bytes*/static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes,
        /*input_limbs*/lk, 
        /*output_limbs*/0,
        /*work_items*/static_cast<uint64_t>(ct_now) * lk
    );
    builder.bram.AcquireOnIssue(
        static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
    );
    builder.bram.ReleaseOnIssue(
        static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
    );

    // Load 2*l limbs eval keys
    for (uint32_t i = 0; i < p; i++) {
        emit_op(
            /*name*/"load_eval_keys_" + std::to_string(i),
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
            std::cerr << "Failed to acquire BRAM for loading eval keys for p " << i << std::endl;
            return CycleProgram();
        }
    }

    // Multiply in eval form for each digit, can be done in parallel across digits and polys
    for (uint32_t i = 0; i < p; i++) {
        emit_op(
            /*name*/"mul_eval_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::EweMul,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::Multiply,
            /*bytes*/static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes,
            /*input_limbs*/lk,
            /*output_limbs*/lk,
             /*work_items*/static_cast<uint64_t>(ct_now) * lk
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
        );

        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * lk * problem.ct_limb_bytes
        );
     
        if (!builder.Ok()) {
            std::cerr << "Failed to acquire/release BRAM for multiplication of digit " << i << std::endl;
            return CycleProgram();
        }
    }

    // INTT for 2*k limbs after multiplication
    for (uint32_t i = 0; i < p; i++) {
        emit_op(
            /*name*/"ntt_after_mul_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::INTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::INTT,
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
            std::cerr << "Failed to acquire/release BRAM for INTT after multiplication of digit " << i << std::endl;
            return CycleProgram();
        }
    }

    // BConv for 2*k limbs with 2*l limbs eval keys to get the result in digit form
    for (uint32_t i = 0; i < p; i++) {
        emit_op(
            /*name*/"bconv_after_mul_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::BConv,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::BConv,
            /*bytes*/static_cast<uint64_t>(ct_now) * k * problem.ct_limb_bytes,
            /*input_limbs*/k,
            /*output_limbs*/l,
             /*work_items*/static_cast<uint64_t>(ct_now) * k
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * k * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            std::cerr << "Failed to acquire/release BRAM for BConv after multiplication of digit " << i << std::endl;
            return CycleProgram();
        }
    }

    // NTT 
    for (uint32_t i = 0; i < p; i++) {
        emit_op(
            /*name*/"ntt_after_bconv_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::NTT,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::NTT,
             /*bytes*/static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes,
            /*input_limbs*/l,
            /*output_limbs*/0,
             /*work_items*/static_cast<uint64_t>(ct_now) * l
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            std::cerr << "Failed to acquire/release BRAM for NTT after BConv of digit " << i << std::endl;
            return CycleProgram();
        }
    }

    // Subtract
    for (uint32_t i = 0; i < p; i++) {
        emit_op(
            /*name*/"sub_after_bconv_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::EweSub,
            /*transfer_path*/CycleTransferPath::None,
            /*type*/CycleOpType::Sub,
             /*bytes*/static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes,
            /*input_limbs*/l,
            /*output_limbs*/l,
             /*work_items*/static_cast<uint64_t>(ct_now) * l
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            std::cerr << "Failed to acquire/release BRAM for subtraction after BConv of digit " << i << std::endl;
            return CycleProgram();
        }
    }

    // Sync across cards to ensure all previous ops are done before next steps
    for (uint32_t card_idx = 1; card_idx < active_cards; card_idx++) {
        emit_op(
            /*name*/"recv_digit_limbs_card_" + std::to_string(card_idx+1),
            /*kind*/CycleInstructionKind::InterCardRecv,
            /*transfer_path*/CycleTransferPath::HBMToHBM,
            /*type*/CycleOpType::InterCardComm,
             /*bytes*/static_cast<uint64_t>(ct_now) * digit_limbs * problem.ct_limb_bytes,
            /*input_limbs*/l,
            /*output_limbs*/0
        );
    }

    // Load from HBM and Add
    for (uint32_t i = 1; i < digit_num; i++) {
        emit_op(
            /*name*/"load_digit_" + std::to_string(i),
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
            std::cerr << "Failed to acquire BRAM for loading digit " << i << std::endl;
            return CycleProgram();
        }

        emit_op(
            /*name*/"add_digit_" + std::to_string(i),
            /*kind*/CycleInstructionKind::EweAdd,
            /*transfer_path*/CycleTransferPath::None,
             /*type*/CycleOpType::Add,
             /*bytes*/static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes,
            /*input_limbs*/l,
            /*output_limbs*/l,
             /*work_items*/static_cast<uint64_t>(ct_now) * l
        );
        builder.bram.AcquireOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        builder.bram.ReleaseOnIssue(
            static_cast<uint64_t>(ct_now) * l * problem.ct_limb_bytes
        );
        if (!builder.Ok()) {
            std::cerr << "Failed to acquire/release BRAM for addition of digit " << i << std::endl;
            return CycleProgram();
        }
    }

    builder.program.estimated_peak_live_bytes = builder.bram.Peak();
    return std::move(builder.program);

}
