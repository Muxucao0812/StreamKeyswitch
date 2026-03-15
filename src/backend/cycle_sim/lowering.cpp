#include "backend/cycle_sim/lowering.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <limits>
#include <unordered_map>
#include <utility>

namespace {

uint64_t CeilDivU64(uint64_t a, uint64_t b) {
    return (b == 0) ? 0 : ((a + b - 1) / b);
}

bool IsInterCardStepType(TileExecutionStepType step_type) {
    switch (step_type) {
    case TileExecutionStepType::InterCardSendStep:
    case TileExecutionStepType::InterCardRecvStep:
    case TileExecutionStepType::InterCardReduceStep:
    case TileExecutionStepType::BarrierStep:
    case TileExecutionStepType::InterCardCommTile:
    case TileExecutionStepType::InterCardBarrier:
    case TileExecutionStepType::Merge:
        return true;
    default:
        return false;
    }
}

bool IsSharedSingleBoardMethod(KeySwitchMethod method) {
    switch (method) {
    case KeySwitchMethod::Poseidon:
    case KeySwitchMethod::FAB:
    case KeySwitchMethod::FAST:
    case KeySwitchMethod::OLA:
    case KeySwitchMethod::HERA:
        return true;
    default:
        return false;
    }
}

const char* MethodTag(KeySwitchMethod method) {
    switch (method) {
    case KeySwitchMethod::Poseidon:
        return "poseidon";
    case KeySwitchMethod::FAB:
        return "fab";
    case KeySwitchMethod::FAST:
        return "fast";
    case KeySwitchMethod::OLA:
        return "ola";
    case KeySwitchMethod::HERA:
        return "hera";
    case KeySwitchMethod::Cinnamon:
        return "cinnamon";
    case KeySwitchMethod::Auto:
        return "auto";
    }

    return "unknown";
}

KeySwitchMethod ResolveLoweringMethod(const KeySwitchExecution& execution) {
    if (execution.effective_method != KeySwitchMethod::Auto
        && execution.effective_method != KeySwitchMethod::Poseidon) {
        return execution.effective_method;
    }
    if (execution.method != KeySwitchMethod::Auto) {
        return execution.method;
    }
    return execution.problem.method;
}

uint64_t PositiveDelta(uint64_t before, uint64_t after) {
    return (after > before) ? (after - before) : 0;
}

int64_t NegativeTotalLiveDelta(
    const BufferUsage& before,
    const BufferUsage& after) {

    return (before.total_live_bytes > after.total_live_bytes)
        ? -static_cast<int64_t>(before.total_live_bytes - after.total_live_bytes)
        : 0;
}

uint64_t InstructionCountForWorkItems(
    const KeySwitchProblem& problem,
    uint64_t work_items) {

    if (work_items == 0) {
        return 0;
    }

    (void)problem;
    (void)work_items;
    // Keep one instruction per lowered group; method differences are carried by
    // dependency shape, transfer groups, and resource mapping.
    return 1;
}

uint64_t InstructionCountForMultiplyAdds(
    const KeySwitchProblem& problem,
    uint64_t multiply_instruction_count) {

    if (problem.polys <= 1 || multiply_instruction_count == 0) {
        return 0;
    }
    return 1;
}

uint64_t EstimateLatencyCycles(
    const HardwareModel& hardware,
    const KeySwitchProblem& problem,
    CycleInstructionKind kind,
    CycleTransferPath transfer_path,
    uint64_t bytes) {

    switch (kind) {
    case CycleInstructionKind::LoadHBM:
        return hardware.EstimateTransferCycles(
            (transfer_path == CycleTransferPath::HostToHBM)
                ? HardwareTransferPath::HostToHBM
                : HardwareTransferPath::HBMToSPM,
            bytes);
    case CycleInstructionKind::StoreHBM:
        return hardware.EstimateTransferCycles(
            HardwareTransferPath::SPMToHBM,
            bytes);
    case CycleInstructionKind::Decompose:
        return hardware.EstimateDecomposeCycles(problem);
    case CycleInstructionKind::NTT:
        return hardware.EstimateNttCycles(problem);
    case CycleInstructionKind::INTT:
        return hardware.EstimateInttCycles(problem);
    case CycleInstructionKind::EweMul:
        return hardware.EstimateEweMulCycles(problem);
    case CycleInstructionKind::EweAdd:
        return hardware.EstimateEweAddCycles(problem);
    case CycleInstructionKind::EweSub:
        return hardware.EstimateEweSubCycles(problem);
    case CycleInstructionKind::BConv:
        return hardware.EstimateBconvCycles(problem);
    }

    return 0;
}

void AddDependency(
    std::vector<uint32_t>* dependencies,
    uint32_t group_id) {

    if (group_id == std::numeric_limits<uint32_t>::max()) {
        return;
    }
    if (std::find(dependencies->begin(), dependencies->end(), group_id)
        == dependencies->end()) {
        dependencies->push_back(group_id);
    }
}

uint32_t AppendGroup(
    const HardwareModel& hardware,
    const KeySwitchProblem& problem,
    const TileExecutionStep& step,
    const std::string& group_name,
    CycleInstructionKind kind,
    CycleTransferPath transfer_path,
    uint64_t instruction_count,
    uint64_t bytes,
    uint64_t work_items,
    int64_t live_bytes_delta_on_issue,
    int64_t live_bytes_delta_on_complete,
    int64_t rf_live_bytes_delta_on_issue,
    int64_t rf_live_bytes_delta_on_complete,
    int64_t sram_live_bytes_delta_on_issue,
    int64_t sram_live_bytes_delta_on_complete,
    const std::vector<uint32_t>& dependencies,
    uint64_t* next_instruction_id,
    CycleProgram* program) {

    if (instruction_count == 0) {
        return std::numeric_limits<uint32_t>::max();
    }

    CycleInstructionGroup group;
    group.id = static_cast<uint32_t>(program->groups.size());
    group.name = group_name;
    group.kind = kind;
    group.transfer_path = transfer_path;
    group.source_step_type = step.type;
    group.stage_type = step.stage_type;
    group.ct_tile_index = step.ct_tile_index;
    group.limb_tile_index = step.limb_tile_index;
    group.digit_tile_index = step.digit_tile_index;
    group.input_storage = step.input_storage;
    group.output_storage = step.output_storage;
    group.fused_with_prev = step.fused_with_prev;
    group.fused_with_next = step.fused_with_next;
    group.is_shortcut_path = step.is_shortcut_path;
    group.bytes = bytes;
    group.work_items = work_items;
    group.live_bytes_before = step.before.total_live_bytes;
    group.live_bytes_after = step.after.total_live_bytes;
    group.live_bytes_delta_on_issue = live_bytes_delta_on_issue;
    group.live_bytes_delta_on_complete = live_bytes_delta_on_complete;
    group.rf_live_bytes_delta_on_issue = rf_live_bytes_delta_on_issue;
    group.rf_live_bytes_delta_on_complete = rf_live_bytes_delta_on_complete;
    group.sram_live_bytes_delta_on_issue = sram_live_bytes_delta_on_issue;
    group.sram_live_bytes_delta_on_complete = sram_live_bytes_delta_on_complete;
    group.dependencies = dependencies;
    group.instructions.reserve(static_cast<std::size_t>(instruction_count));

    const uint64_t latency_cycles = EstimateLatencyCycles(
        hardware,
        problem,
        kind,
        transfer_path,
        bytes);
    const uint64_t bounded_latency_cycles = std::max<uint64_t>(
        1,
        std::min<uint64_t>(64, latency_cycles));
    const uint64_t bytes_per_instruction =
        (instruction_count == 0) ? 0 : (bytes / instruction_count);
    const uint64_t bytes_remainder =
        (instruction_count == 0) ? 0 : (bytes % instruction_count);
    const uint64_t work_per_instruction =
        (instruction_count == 0) ? 0 : (work_items / instruction_count);
    const uint64_t work_remainder =
        (instruction_count == 0) ? 0 : (work_items % instruction_count);

    for (uint64_t idx = 0; idx < instruction_count; ++idx) {
        CycleInstruction instruction;
        instruction.id = (*next_instruction_id)++;
        instruction.group_id = group.id;
        instruction.kind = kind;
        instruction.transfer_path = transfer_path;
        instruction.source_step_type = step.type;
        instruction.stage_type = step.stage_type;
        instruction.ct_tile_index = step.ct_tile_index;
        instruction.limb_tile_index = step.limb_tile_index;
        instruction.digit_tile_index = step.digit_tile_index;
        instruction.input_storage = step.input_storage;
        instruction.output_storage = step.output_storage;
        instruction.fused_with_prev = step.fused_with_prev;
        instruction.fused_with_next = step.fused_with_next;
        instruction.is_shortcut_path = step.is_shortcut_path;
        instruction.bytes = bytes_per_instruction + (idx < bytes_remainder ? 1 : 0);
        instruction.work_items = work_per_instruction + (idx < work_remainder ? 1 : 0);
        instruction.latency_cycles = bounded_latency_cycles;
        group.instructions.push_back(std::move(instruction));
    }

    const uint32_t group_id = group.id;
    program->instruction_count += instruction_count;
    program->groups.push_back(std::move(group));
    return group_id;
}

std::size_t FlatIndex(
    uint32_t ct_tile_index,
    uint32_t limb_tile_index,
    uint32_t digit_tile_index,
    uint32_t limb_tiles,
    uint32_t digit_tiles) {

    return (static_cast<std::size_t>(ct_tile_index) * limb_tiles + limb_tile_index)
        * digit_tiles
        + digit_tile_index;
}

std::size_t FlatIndex2D(
    uint32_t ct_tile_index,
    uint32_t limb_tile_index,
    uint32_t limb_tiles) {

    return static_cast<std::size_t>(ct_tile_index) * limb_tiles + limb_tile_index;
}

} // namespace

SingleBoardCycleLowerer::SingleBoardCycleLowerer(
    const HardwareModel& hardware,
    KeySwitchMethod method)
    : hardware_(hardware),
      method_(method) {}
CycleLoweringResult SingleBoardCycleLowerer::Lower(
    const KeySwitchExecution& execution) const {

    CycleLoweringResult result;
    result.program.method = method_;
    result.program.name = std::string(MethodTag(method_)) + "_single_board";

    if (!execution.valid) {
        result.valid = false;
        result.fallback_reason = execution.fallback_reason;
        return result;
    }

    if (!IsSharedSingleBoardMethod(method_)) {
        result.valid = false;
        result.fallback_reason = KeySwitchFallbackReason::UnsupportedMethod;
        result.fallback_reason_message =
            std::string(MethodTag(method_)) + "_single_board_cycle_sim_not_supported";
        return result;
    }

    if (execution.problem.cards > 1) {
        result.valid = false;
        result.fallback_reason = KeySwitchFallbackReason::UnsupportedConfig;
        result.fallback_reason_message =
            std::string(MethodTag(method_)) + "_single_board_cycle_sim_multi_card_not_supported";
        return result;
    }

    const uint32_t invalid_group = std::numeric_limits<uint32_t>::max();
    uint64_t next_instruction_id = 0;
    std::unordered_map<uint64_t, uint32_t> step_terminal_group;

    auto map_dependencies = [&](const TileExecutionStep& step) {
        std::vector<uint32_t> deps;
        for (uint64_t dep_step_id : step.depends_on) {
            const auto it = step_terminal_group.find(dep_step_id);
            if (it == step_terminal_group.end()) {
                continue;
            }
            AddDependency(&deps, it->second);
        }
        return deps;
    };

    auto storage_deltas = [](const TileExecutionStep& step) {
        int64_t rf_issue = 0;
        int64_t rf_complete = 0;
        int64_t sram_issue = 0;
        int64_t sram_complete = 0;
        if (step.output_storage == IntermediateStorageLevel::RF) {
            rf_issue += static_cast<int64_t>(step.output_bytes);
        } else if (step.output_storage == IntermediateStorageLevel::SRAM) {
            sram_issue += static_cast<int64_t>(step.output_bytes);
        }

        if (!step.fused_with_prev) {
            if (step.input_storage == IntermediateStorageLevel::RF) {
                rf_complete -= static_cast<int64_t>(step.input_bytes);
            } else if (step.input_storage == IntermediateStorageLevel::SRAM) {
                sram_complete -= static_cast<int64_t>(step.input_bytes);
            }
        }

        return std::array<int64_t, 4>{rf_issue, rf_complete, sram_issue, sram_complete};
    };

    auto append_single_group = [&](const TileExecutionStep& step,
                                   const std::string& name,
                                   CycleInstructionKind kind,
                                   CycleTransferPath transfer_path,
                                   uint64_t instruction_count,
                                   uint64_t bytes,
                                   uint64_t work_items,
                                   const std::vector<uint32_t>& deps) {
        const auto deltas = storage_deltas(step);
        const int64_t total_issue = deltas[0] + deltas[2];
        const int64_t total_complete = deltas[1] + deltas[3];
        return AppendGroup(
            hardware_,
            execution.problem,
            step,
            name,
            kind,
            transfer_path,
            instruction_count,
            bytes,
            work_items,
            total_issue,
            total_complete,
            deltas[0],
            deltas[1],
            deltas[2],
            deltas[3],
            deps,
            &next_instruction_id,
            &result.program);
    };

    for (const TileExecutionStep& step : execution.steps) {
        if (IsInterCardStepType(step.type)) {
            result.valid = false;
            result.fallback_reason = KeySwitchFallbackReason::UnsupportedConfig;
            result.fallback_reason_message =
                std::string(MethodTag(method_)) + "_single_board_cycle_sim_inter_card_not_supported";
            return result;
        }

        std::vector<uint32_t> deps = map_dependencies(step);
        const uint64_t step_bytes = (step.bytes == 0)
            ? std::max(step.input_bytes, step.output_bytes)
            : step.bytes;
        const uint64_t compute_instructions = InstructionCountForWorkItems(
            execution.problem,
            step.work_items);

        uint32_t terminal = invalid_group;
        switch (step.type) {
        case TileExecutionStepType::KeyLoadHostToHBM:
            terminal = append_single_group(
                step,
                "key_host_to_hbm",
                CycleInstructionKind::LoadHBM,
                CycleTransferPath::HostToHBM,
                /*instruction_count=*/1,
                step_bytes,
                /*work_items=*/0,
                deps);
            break;

        case TileExecutionStepType::KeyLoadHBMToBRAM:
        case TileExecutionStepType::KeyHBMToBRAM:
            terminal = append_single_group(
                step,
                "key_hbm_to_spm",
                CycleInstructionKind::LoadHBM,
                CycleTransferPath::HBMToSPM,
                /*instruction_count=*/1,
                step_bytes,
                /*work_items=*/0,
                deps);
            break;

        case TileExecutionStepType::InputHBMToBRAM:
        case TileExecutionStepType::IntermediateHBMToBRAM:
            terminal = append_single_group(
                step,
                "input_hbm_to_spm",
                CycleInstructionKind::LoadHBM,
                CycleTransferPath::HBMToSPM,
                /*instruction_count=*/1,
                step_bytes,
                /*work_items=*/0,
                deps);
            break;

        case TileExecutionStepType::IntermediateBRAMToHBM:
        case TileExecutionStepType::OutputBRAMToHBM:
            terminal = append_single_group(
                step,
                "output_spm_to_hbm",
                CycleInstructionKind::StoreHBM,
                CycleTransferPath::SPMToHBM,
                /*instruction_count=*/1,
                step_bytes,
                /*work_items=*/0,
                deps);
            break;

        case TileExecutionStepType::ModUpInttTile:
        case TileExecutionStepType::ModDownInttTile:
        case TileExecutionStepType::InttTile:
            terminal = append_single_group(
                step,
                "intt",
                CycleInstructionKind::INTT,
                CycleTransferPath::None,
                std::max<uint64_t>(1, compute_instructions),
                /*bytes=*/0,
                step.work_items,
                deps);
            break;

        case TileExecutionStepType::ModUpBConvTile:
        case TileExecutionStepType::ModDownBConvTile:
        case TileExecutionStepType::BasisConvertTile:
            terminal = append_single_group(
                step,
                "bconv",
                CycleInstructionKind::BConv,
                CycleTransferPath::None,
                std::max<uint64_t>(1, compute_instructions),
                /*bytes=*/0,
                step.work_items,
                deps);
            break;

        case TileExecutionStepType::ModUpNttTile:
        case TileExecutionStepType::ModDownNttTile:
        case TileExecutionStepType::NttTile:
            terminal = append_single_group(
                step,
                "ntt",
                CycleInstructionKind::NTT,
                CycleTransferPath::None,
                std::max<uint64_t>(1, compute_instructions),
                /*bytes=*/0,
                step.work_items,
                deps);
            break;

        case TileExecutionStepType::DecomposeTile:
            terminal = append_single_group(
                step,
                "decompose",
                CycleInstructionKind::Decompose,
                CycleTransferPath::None,
                std::max<uint64_t>(1, compute_instructions),
                /*bytes=*/0,
                step.work_items,
                deps);
            break;

        case TileExecutionStepType::KSInnerProdTile: {
            const auto deltas = storage_deltas(step);
            const int64_t total_issue = deltas[0] + deltas[2];
            const int64_t total_complete = deltas[1] + deltas[3];
            const uint64_t mul_instruction_count = std::max<uint64_t>(1, compute_instructions);
            const uint32_t mul_group = AppendGroup(
                hardware_,
                execution.problem,
                step,
                "ks_inner_mul",
                CycleInstructionKind::EweMul,
                CycleTransferPath::None,
                mul_instruction_count,
                /*bytes=*/0,
                step.work_items,
                total_issue,
                0,
                deltas[0],
                0,
                deltas[2],
                0,
                deps,
                &next_instruction_id,
                &result.program);

            const uint64_t add_instruction_count =
                InstructionCountForMultiplyAdds(execution.problem, mul_instruction_count);
            terminal = mul_group;
            if (add_instruction_count > 0) {
                std::vector<uint32_t> add_deps;
                AddDependency(&add_deps, mul_group);
                terminal = AppendGroup(
                    hardware_,
                    execution.problem,
                    step,
                    "ks_inner_add",
                    CycleInstructionKind::EweAdd,
                    CycleTransferPath::None,
                    add_instruction_count,
                    /*bytes=*/0,
                    /*work_items=*/0,
                    0,
                    total_complete,
                    0,
                    deltas[1],
                    0,
                    deltas[3],
                    add_deps,
                    &next_instruction_id,
                    &result.program);
            } else {
                // Release on the mul group when no follow-up add group exists.
                result.program.groups[mul_group].live_bytes_delta_on_complete = total_complete;
                result.program.groups[mul_group].rf_live_bytes_delta_on_complete = deltas[1];
                result.program.groups[mul_group].sram_live_bytes_delta_on_complete = deltas[3];
            }
            break;
        }

        case TileExecutionStepType::CrossDigitReduceTile:
            terminal = append_single_group(
                step,
                "cross_digit_reduce",
                CycleInstructionKind::EweAdd,
                CycleTransferPath::None,
                std::max<uint64_t>(1, compute_instructions),
                /*bytes=*/0,
                step.work_items,
                deps);
            break;

        case TileExecutionStepType::FinalSubtractTile:
            terminal = append_single_group(
                step,
                "final_subtract",
                CycleInstructionKind::EweSub,
                CycleTransferPath::None,
                std::max<uint64_t>(1, compute_instructions),
                /*bytes=*/0,
                step.work_items,
                deps);
            break;

        case TileExecutionStepType::AccumulateSubtractTile: {
            const auto deltas = storage_deltas(step);
            const int64_t total_issue = deltas[0] + deltas[2];
            const int64_t total_complete = deltas[1] + deltas[3];
            const uint64_t instruction_count = std::max<uint64_t>(1, compute_instructions);
            const uint32_t add_group = AppendGroup(
                hardware_,
                execution.problem,
                step,
                "accumulate",
                CycleInstructionKind::EweAdd,
                CycleTransferPath::None,
                instruction_count,
                /*bytes=*/0,
                step.work_items,
                total_issue,
                0,
                deltas[0],
                0,
                deltas[2],
                0,
                deps,
                &next_instruction_id,
                &result.program);
            std::vector<uint32_t> sub_deps;
            AddDependency(&sub_deps, add_group);
            terminal = AppendGroup(
                hardware_,
                execution.problem,
                step,
                "subtract",
                CycleInstructionKind::EweSub,
                CycleTransferPath::None,
                instruction_count,
                /*bytes=*/0,
                step.work_items,
                0,
                total_complete,
                0,
                deltas[1],
                0,
                deltas[3],
                sub_deps,
                &next_instruction_id,
                &result.program);
            break;
        }

        case TileExecutionStepType::InterCardSendStep:
        case TileExecutionStepType::InterCardRecvStep:
        case TileExecutionStepType::InterCardReduceStep:
        case TileExecutionStepType::BarrierStep:
        case TileExecutionStepType::InterCardCommTile:
        case TileExecutionStepType::InterCardBarrier:
        case TileExecutionStepType::Merge:
            result.valid = false;
            result.fallback_reason = KeySwitchFallbackReason::UnsupportedConfig;
            result.fallback_reason_message =
                std::string(MethodTag(method_)) + "_single_board_cycle_sim_inter_card_not_supported";
            return result;
        }

        if (terminal != invalid_group) {
            step_terminal_group[step.step_id] = terminal;
        }
    }

    result.program.estimated_peak_live_bytes =
        std::max<uint64_t>(execution.predicted_rf_peak, execution.predicted_sram_peak);
    result.valid = true;
    return result;
}

PoseidonCycleLowerer::PoseidonCycleLowerer(const HardwareModel& hardware)
    : impl_(hardware, KeySwitchMethod::Poseidon) {}

CycleLoweringResult PoseidonCycleLowerer::Lower(
    const KeySwitchExecution& execution) const {

    return impl_.Lower(execution);
}

CycleLowererSelector::CycleLowererSelector(const HardwareModel& hardware)
    : hardware_(hardware) {}

CycleLoweringResult CycleLowererSelector::Lower(
    const KeySwitchExecution& execution) const {

    const KeySwitchMethod method = ResolveLoweringMethod(execution);
    switch (method) {
    case KeySwitchMethod::Poseidon:
    case KeySwitchMethod::FAB:
    case KeySwitchMethod::FAST:
    case KeySwitchMethod::OLA:
    case KeySwitchMethod::HERA:
        return SingleBoardCycleLowerer(hardware_, method).Lower(execution);

    case KeySwitchMethod::Cinnamon: {
        CycleLoweringResult result;
        result.valid = false;
        result.fallback_reason = KeySwitchFallbackReason::UnsupportedMethod;
        result.fallback_reason_message = "cinnamon_cycle_sim_not_implemented";
        result.program.method = KeySwitchMethod::Cinnamon;
        result.program.name = "cinnamon";
        return result;
    }

    default: {
        CycleLoweringResult result;
        result.valid = false;
        result.fallback_reason = KeySwitchFallbackReason::UnsupportedMethod;
        result.fallback_reason_message =
            std::string(MethodTag(method)) + "_cycle_sim_not_supported";
        result.program.method = method;
        result.program.name = MethodTag(method);
        return result;
    }
    }
}
