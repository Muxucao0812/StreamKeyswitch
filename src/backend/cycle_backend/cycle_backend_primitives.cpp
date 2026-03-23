#include "backend/cycle_backend/cycle_backend_primitives.h"

#include <algorithm>
#include <iostream>
#include <limits>
#include <utility>

CyclePrimitiveEmitter::CyclePrimitiveEmitter(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware,
    KeySwitchMethod method,
    std::string program_name)
    : problem_(problem)
    , hardware_(hardware)
    , bram_(
        problem.bram_budget_bytes > problem.bram_guard_bytes
            ? (problem.bram_budget_bytes - problem.bram_guard_bytes)
            : 0) {
    program_.method = method;
    program_.name = std::move(program_name);
}

uint32_t CyclePrimitiveEmitter::EmitLoadHBM(const CyclePrimitiveDesc& desc) {
    return Emit(CycleInstructionKind::LoadHBM, desc);
}

uint32_t CyclePrimitiveEmitter::EmitStoreHBM(const CyclePrimitiveDesc& desc) {
    return Emit(CycleInstructionKind::StoreHBM, desc);
}

uint32_t CyclePrimitiveEmitter::EmitNTT(const CyclePrimitiveDesc& desc) {
    return Emit(CycleInstructionKind::NTT, desc);
}

uint32_t CyclePrimitiveEmitter::EmitINTT(const CyclePrimitiveDesc& desc) {
    return Emit(CycleInstructionKind::INTT, desc);
}

uint32_t CyclePrimitiveEmitter::EmitBConv(const CyclePrimitiveDesc& desc) {
    return Emit(CycleInstructionKind::BConv, desc);
}

uint32_t CyclePrimitiveEmitter::EmitEweMul(const CyclePrimitiveDesc& desc) {
    return Emit(CycleInstructionKind::EweMul, desc);
}

uint32_t CyclePrimitiveEmitter::EmitEweAdd(const CyclePrimitiveDesc& desc) {
    return Emit(CycleInstructionKind::EweAdd, desc);
}

uint32_t CyclePrimitiveEmitter::EmitEweSub(const CyclePrimitiveDesc& desc) {
    return Emit(CycleInstructionKind::EweSub, desc);
}

uint32_t CyclePrimitiveEmitter::EmitInterCardSend(const CyclePrimitiveDesc& desc) {
    return Emit(CycleInstructionKind::InterCardSend, desc);
}

uint32_t CyclePrimitiveEmitter::EmitInterCardRecv(const CyclePrimitiveDesc& desc) {
    return Emit(CycleInstructionKind::InterCardRecv, desc);
}


bool CyclePrimitiveEmitter::ValidateMemoryAccounting(const char* label) const {
    uint64_t replay_live = 0;
    uint64_t replay_peak = 0;
    bool underflow = false;
    for (const CycleInstructionGroup& group : program_.groups) {
        if (group.live_bytes_delta_on_issue < 0) {
            const uint64_t released =
                static_cast<uint64_t>(-group.live_bytes_delta_on_issue);
            if (released > replay_live) {
                underflow = true;
            }
        }
        replay_live = ApplyLiveDelta(replay_live, group.live_bytes_delta_on_issue);
        replay_peak = std::max<uint64_t>(replay_peak, replay_live);

        if (group.live_bytes_delta_on_complete < 0) {
            const uint64_t released =
                static_cast<uint64_t>(-group.live_bytes_delta_on_complete);
            if (released > replay_live) {
                underflow = true;
            }
        }
        replay_live = ApplyLiveDelta(replay_live, group.live_bytes_delta_on_complete);
        replay_peak = std::max<uint64_t>(replay_peak, replay_live);
    }

    const bool peak_mismatch = (replay_peak != bram_.Peak());
    const bool live_mismatch = (replay_live != bram_.Live());
    if (!peak_mismatch && !live_mismatch) {
        if (underflow) {
            std::cout << "[" << label << " memory accounting warning]"
                      << " replay detected release-underflow; "
                      << "peak/live remain consistent with tracker.\n";
        }
        return true;
    }

    std::cout << "[" << label << " memory accounting mismatch]"
              << " underflow=" << (underflow ? "yes" : "no")
              << " replay_peak=" << replay_peak
              << " tracker_peak=" << bram_.Peak()
              << " replay_live_end=" << replay_live
              << " tracker_live_end=" << bram_.Live()
              << "\n";
    return false;
}

uint32_t CyclePrimitiveEmitter::Emit(
    CycleInstructionKind kind,
    const CyclePrimitiveDesc& desc) {

    const uint32_t micro_ops = MicroOps(kind, desc.input_limbs, desc.output_limbs);
    
    const uint64_t bytes_per_op = (micro_ops > 0) ? (desc.bytes / micro_ops) : desc.bytes;
    const uint64_t work_per_op = (micro_ops > 0) ? (desc.work_items / micro_ops) : desc.work_items;
    const uint64_t per_op_cycles = EstimateCycles(kind, desc.transfer_path, bytes_per_op, 1, 1);
    const uint64_t total_cycles = EstimateCycles(kind, desc.transfer_path, desc.bytes, desc.input_limbs, desc.output_limbs);

    CycleInstructionGroup group;
    group.id = static_cast<uint32_t>(program_.groups.size());
    group.name = desc.name;
    group.kind = kind;
    group.transfer_path = desc.transfer_path;
    group.type = desc.type;
    group.bytes = desc.bytes;
    group.work_items = desc.work_items;
    const auto deltas = bram_.FlushGroupDeltas();
    group.live_bytes_delta_on_issue = deltas.first;
    group.live_bytes_delta_on_complete = deltas.second;
    group.dependencies = desc.deps;

    for (uint32_t op_idx = 0; op_idx < micro_ops; ++op_idx) {
        CycleInstruction instr;
        instr.id = next_instruction_id_++;
        instr.group_id = group.id;
        instr.kind = kind;
        instr.transfer_path = desc.transfer_path;
        instr.type = desc.type;
        instr.bytes = bytes_per_op;
        instr.work_items = work_per_op;
        instr.latency_cycles = per_op_cycles;
        group.instructions.push_back(std::move(instr));
    }

    const uint32_t group_id = group.id;
    program_.instruction_count += micro_ops;
    program_.groups.push_back(std::move(group));

    std::cout << "Emitted group " << group_id << ": " << desc.name
              << ", kind=" << static_cast<uint32_t>(kind)
              << ", bytes=" << desc.bytes
              << ", work_items=" << desc.work_items
              << ", micro_ops=" << micro_ops
              << ", est_cycles/op=" << per_op_cycles
              << ", total_est_cycles=" << total_cycles
              << ", bram_live=" << bram_.Live()
              << std::endl;
    return group_id;
}

uint64_t CyclePrimitiveEmitter::EstimateCycles(
    CycleInstructionKind kind,
    CycleTransferPath path,
    uint64_t bytes,
    uint32_t input_limbs,
    uint32_t output_limbs) const {

    switch (kind) {
    case CycleInstructionKind::LoadHBM:
        return hardware_.EstimateTransferCycles(
            (path == CycleTransferPath::HostToHBM)
                ? HardwareTransferPath::HostToHBM
                : HardwareTransferPath::HBMToSPM,
            bytes);
    case CycleInstructionKind::StoreHBM:
        return hardware_.EstimateTransferCycles(HardwareTransferPath::SPMToHBM, bytes);
    case CycleInstructionKind::NTT:
        return hardware_.EstimateNttCycles(problem_, input_limbs);
    case CycleInstructionKind::INTT:
        return hardware_.EstimateInttCycles(problem_, input_limbs);
    case CycleInstructionKind::BConv:
        return hardware_.EstimateBconvCycles(problem_, input_limbs, output_limbs);
    case CycleInstructionKind::EweMul:
        return hardware_.EstimateEweMulCycles(problem_, input_limbs);
    case CycleInstructionKind::EweAdd:
        return hardware_.EstimateEweAddCycles(problem_, input_limbs);
    case CycleInstructionKind::EweSub:
        return hardware_.EstimateEweSubCycles(problem_, input_limbs);
    case CycleInstructionKind::InterCardSend:
    case CycleInstructionKind::InterCardRecv:
        return hardware_.EstimateInterconnectTransferCycles(bytes);
    default:
        return 1;
    }
}

uint32_t CyclePrimitiveEmitter::MicroOps(
    CycleInstructionKind kind,
    uint32_t input_limbs,
    uint32_t output_limbs
) {

    switch (kind) {
        case CycleInstructionKind::NTT:
        case CycleInstructionKind::INTT:
        case CycleInstructionKind::EweMul:
        case CycleInstructionKind::EweAdd:
        case CycleInstructionKind::EweSub:
        case CycleInstructionKind::LoadHBM:
            return std::max<uint32_t>(1, input_limbs);
        case CycleInstructionKind::StoreHBM:
            return std::max<uint32_t>(1, output_limbs);
        case CycleInstructionKind::BConv:
            return std::max<uint32_t>(1, input_limbs * std::max<uint32_t>(1, output_limbs));
        case CycleInstructionKind::InterCardSend:
            return std::max<uint32_t>(1, output_limbs);
        case CycleInstructionKind::InterCardRecv:
            return std::max<uint32_t>(1, input_limbs);
    
    default:
        return 1;
    }
}

uint64_t CyclePrimitiveEmitter::ApplyLiveDelta(
    uint64_t current_live_bytes,
    int64_t delta) {

    if (delta >= 0) {
        return current_live_bytes + static_cast<uint64_t>(delta);
    }
    const uint64_t released = static_cast<uint64_t>(-delta);
    return (released >= current_live_bytes) ? 0 : (current_live_bytes - released);
}

CycleProgramBuilder::CycleProgramBuilder(
    const KeySwitchProblem& p,
    const HardwareModel& hw,
    KeySwitchMethod method,
    std::string program_name)
    : problem(p)
    , hardware(hw)
    , primitive(p, hw, method, std::move(program_name))
    , bram(primitive.Bram())
    , program(primitive.Program()) {}

bool CycleProgramBuilder::Ok() const {
    return build_ok && bram.Ok();
}

bool CycleProgramBuilder::ValidateMemoryAccounting(const char* label) {
    const bool ok = primitive.ValidateMemoryAccounting(label);
    if (!ok) {
        build_ok = false;
    }
    return ok;
}

std::vector<uint32_t> CycleProgramBuilder::Deps() const {
    if (last_group == std::numeric_limits<uint32_t>::max()) {
        return {};
    }
    return {last_group};
}

bool CycleProgramBuilder::AcquireOnIssue(uint64_t bytes) {
    if (!build_ok) {
        return false;
    }
    if (!bram.AcquireOnIssue(bytes)) {
        build_ok = false;
        return false;
    }
    return true;
}

bool CycleProgramBuilder::ReleaseOnIssue(uint64_t bytes) {
    if (!build_ok) {
        return false;
    }
    if (!bram.ReleaseOnIssue(bytes)) {
        build_ok = false;
        return false;
    }
    return true;
}

uint32_t CycleProgramBuilder::EmitPrimitive(
    CycleInstructionKind kind,
    const CyclePrimitiveDesc& desc
) {

    if (!Ok()) {
        build_ok = false;
        return std::numeric_limits<uint32_t>::max();
    }

    uint32_t group_id = std::numeric_limits<uint32_t>::max();
    switch (kind) {
    case CycleInstructionKind::LoadHBM:
        group_id = primitive.EmitLoadHBM(desc);
        break;
    case CycleInstructionKind::StoreHBM:
        group_id = primitive.EmitStoreHBM(desc);
        break;
    case CycleInstructionKind::NTT:
        group_id = primitive.EmitNTT(desc);
        break;
    case CycleInstructionKind::INTT:
        group_id = primitive.EmitINTT(desc);
        break;
    case CycleInstructionKind::BConv:
        group_id = primitive.EmitBConv(desc);
        break;
    case CycleInstructionKind::EweMul:
        group_id = primitive.EmitEweMul(desc);
        break;
    case CycleInstructionKind::EweAdd:
        group_id = primitive.EmitEweAdd(desc);
        break;
    case CycleInstructionKind::EweSub:
        group_id = primitive.EmitEweSub(desc);
        break;
    case CycleInstructionKind::InterCardSend:
        group_id = primitive.EmitInterCardSend(desc);
        break;
    case CycleInstructionKind::InterCardRecv:
        group_id = primitive.EmitInterCardRecv(desc);
        break;
    default:
        build_ok = false;
        return std::numeric_limits<uint32_t>::max();
    }

    last_group = group_id;
    return group_id;
}
