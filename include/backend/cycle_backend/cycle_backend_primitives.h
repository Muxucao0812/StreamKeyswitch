#pragma once

#include "backend/cycle_backend/bram_accounting.h"
#include "backend/cycle_sim/instruction.h"
#include "backend/hw/hardware_model.h"

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

struct CyclePrimitiveDesc {
    std::string name;
    CycleTransferPath transfer_path = CycleTransferPath::None;
    TileExecutionStepType source_step_type = TileExecutionStepType::InputHBMToBRAM;
    StageType stage_type = StageType::Dispatch;
    uint64_t bytes = 0;
    uint32_t input_limbs = 0;
    uint32_t output_limbs = 0;
    uint64_t work_items = 0;
    std::vector<uint32_t> deps;
};

class CyclePrimitiveEmitter {
public:
    CyclePrimitiveEmitter(
        const KeySwitchProblem& problem,
        const HardwareModel& hardware,
        KeySwitchMethod method,
        std::string program_name);

    uint32_t EmitLoadHBM(const CyclePrimitiveDesc& desc);
    uint32_t EmitStoreHBM(const CyclePrimitiveDesc& desc);
    uint32_t EmitNTT(const CyclePrimitiveDesc& desc);
    uint32_t EmitINTT(const CyclePrimitiveDesc& desc);
    uint32_t EmitBConv(const CyclePrimitiveDesc& desc);
    uint32_t EmitEweMul(const CyclePrimitiveDesc& desc);
    uint32_t EmitEweAdd(const CyclePrimitiveDesc& desc);
    uint32_t EmitEweSub(const CyclePrimitiveDesc& desc);

    bool ValidateMemoryAccounting(const char* label) const;

    BramAccounting& Bram() { return bram_; }
    const BramAccounting& Bram() const { return bram_; }

    CycleProgram& Program() { return program_; }
    const CycleProgram& Program() const { return program_; }

private:
    uint32_t Emit(CycleInstructionKind kind, const CyclePrimitiveDesc& desc);
    uint64_t EstimateCycles(
        CycleInstructionKind kind,
        CycleTransferPath path,
        uint64_t bytes,
        uint32_t input_limbs,
        uint32_t output_limbs) const;
    static uint32_t MicroOps(
        CycleInstructionKind kind,
        uint32_t input_limbs,
        uint32_t output_limbs);
    static uint64_t ApplyLiveDelta(uint64_t current_live_bytes, int64_t delta);

private:
    const KeySwitchProblem& problem_;
    const HardwareModel& hardware_;
    BramAccounting bram_;
    CycleProgram program_;
    uint64_t next_instruction_id_ = 0;
};

class CycleProgramBuilder {
public:
    CycleProgramBuilder(
        const KeySwitchProblem& problem,
        const HardwareModel& hardware,
        KeySwitchMethod method,
        std::string program_name);

    bool Ok() const;
    bool ValidateMemoryAccounting(const char* label);
    std::vector<uint32_t> Deps() const;

    uint32_t EmitPrimitive(
        CycleInstructionKind kind,
        const CyclePrimitiveDesc& desc);

public:
    const KeySwitchProblem& problem;
    const HardwareModel& hardware;
    CyclePrimitiveEmitter primitive;
    BramAccounting& bram;
    CycleProgram& program;
    bool build_ok = true;
    uint32_t last_group = std::numeric_limits<uint32_t>::max();
};
