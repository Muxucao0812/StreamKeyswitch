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
        "stream_keyswitch");

    const uint32_t ct_now = problem.ciphertexts;
    const uint32_t p = problem.polys;
    const uint32_t digit_num = problem.digits;
    const uint32_t l = problem.digit_limbs;

    auto emit_op = [&builder](
                       const std::string& name,
                       CycleInstructionKind kind,
                       CycleTransferPath transfer_path,
                       TileExecutionStepType source_step_type,
                       StageType stage_type,
                       uint64_t bytes,
                       uint32_t input_limbs,
                       uint32_t output_limbs,
                       uint64_t work_items = 0) {
        CyclePrimitiveDesc desc;
        desc.name = name;
        desc.transfer_path = transfer_path;
        desc.source_step_type = source_step_type;
        desc.stage_type = stage_type;
        desc.bytes = bytes;
        desc.input_limbs = input_limbs;
        desc.output_limbs = output_limbs;
        desc.work_items = work_items;
        desc.deps = builder.Deps();
        builder.EmitPrimitive(kind, desc);
    };


    // Step 1: Load all digit inputs to BRAM
    std::cout << "Step 1: load ct to BRAM, total bytes: " << static_cast<uint64_t>(ct_now) * p * digit_num * l * problem.ct_limb_bytes << std::endl;
    builder.bram.AcquireOnIssue(
        static_cast<uint64_t>(ct_now) * p * digit_num * l * problem.ct_limb_bytes
    );
    emit_op(
        "load_input",
        CycleInstructionKind::LoadHBM,
        CycleTransferPath::HBMToSPM,
        TileExecutionStepType::InputHBMToBRAM,
        StageType::Dispatch,
        static_cast<uint64_t>(ct_now) * p * digit_num * l * problem.ct_limb_bytes,
        p * digit_num * l,
        0);
    if (!builder.Ok()) {
        return CycleProgram{};
    }

    builder.program.estimated_peak_live_bytes = builder.bram.Peak();
    return std::move(builder.program);

}

