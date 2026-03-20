#include "backend/cycle_sim/lowering.h"

#include <algorithm>
#include <array>
#include <cmath>
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

bool IsKeyLoadStepType(TileExecutionStepType step_type) {
    switch (step_type) {
    case TileExecutionStepType::KeyLoadHostToHBM:
    case TileExecutionStepType::KeyLoadHBMToBRAM:
    case TileExecutionStepType::KeyHBMToBRAM:
        return true;
    default:
        return false;
    }
}

CycleOpType InferCycleOpType(
    const TileExecutionStep& step,
    CycleInstructionKind kind,
    CycleTransferPath transfer_path) {

    switch (kind) {
    case CycleInstructionKind::LoadHBM:
        return IsKeyLoadStepType(step.type)
            ? CycleOpType::KeyLoad
            : CycleOpType::DataLoad;

    case CycleInstructionKind::StoreHBM:
        return CycleOpType::Spill;

    case CycleInstructionKind::NTT:
    case CycleInstructionKind::NTTLoad:
    case CycleInstructionKind::NTTButterflyLocal:
    case CycleInstructionKind::NTTTranspose1:
    case CycleInstructionKind::NTTButterflyGlobal:
    case CycleInstructionKind::NTTTranspose2:
    case CycleInstructionKind::NTTStore:
        return CycleOpType::NTT;

    case CycleInstructionKind::INTT:
    case CycleInstructionKind::INTTLoad:
    case CycleInstructionKind::INTTButterflyLocal:
    case CycleInstructionKind::INTTTranspose1:
    case CycleInstructionKind::INTTButterflyGlobal:
    case CycleInstructionKind::INTTTranspose2:
    case CycleInstructionKind::INTTStore:
        return CycleOpType::INTT;

    case CycleInstructionKind::BConv:
    case CycleInstructionKind::BConvLoad:
    case CycleInstructionKind::BConvMAC:
    case CycleInstructionKind::BConvReduce:
    case CycleInstructionKind::BConvStore:
    case CycleInstructionKind::Decompose:
        // Legacy decompose is folded into the transform/basis-convert bucket.
        return CycleOpType::BConv;

    case CycleInstructionKind::EweMul:
        return CycleOpType::Multiply;

    case CycleInstructionKind::EweAdd:
        return IsInterCardStepType(step.type)
            ? CycleOpType::InterCardComm
            : CycleOpType::Add;

    case CycleInstructionKind::EweSub:
        return CycleOpType::Sub;

    case CycleInstructionKind::InterCardSend:
    case CycleInstructionKind::InterCardRecv:
    case CycleInstructionKind::InterCardReduce:
        return CycleOpType::InterCardComm;
    }

    if (transfer_path == CycleTransferPath::SPMToHBM) {
        return CycleOpType::Spill;
    }
    if (transfer_path == CycleTransferPath::HBMToSPM
        || transfer_path == CycleTransferPath::HostToHBM) {
        return IsKeyLoadStepType(step.type)
            ? CycleOpType::KeyLoad
            : CycleOpType::DataLoad;
    }
    return CycleOpType::Multiply;
}

bool IsSharedSingleBoardMethod(KeySwitchMethod method) {
    switch (method) {
    case KeySwitchMethod::Poseidon:
    case KeySwitchMethod::FAB:
    case KeySwitchMethod::FAST:
    case KeySwitchMethod::OLA:
    case KeySwitchMethod::HERA:
    case KeySwitchMethod::DigitCentric:
    case KeySwitchMethod::OutputCentric:
    case KeySwitchMethod::MaxParallel:
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

uint64_t EstimateNttSubphaseCycles(
    const HardwareModel& hardware,
    const KeySwitchProblem& problem,
    CycleInstructionKind kind) {

    const uint32_t waves = hardware.WavesPerPoly(problem);
    const HardwareConfig& cfg = hardware.config();
    const uint32_t spu_delay = std::max<uint32_t>(1, cfg.spu_stream_delay_cycles);
    const uint32_t bfly_delay = std::max<uint32_t>(1, cfg.butterfly_delay_cycles);

    auto phase = [&](uint32_t startup, uint32_t per_wave, uint32_t drain, double passes) -> uint64_t {
        const uint64_t phase_waves = static_cast<uint64_t>(
            std::ceil(static_cast<double>(std::max<uint32_t>(1, waves)) * passes - 1e-12));
        return static_cast<uint64_t>(startup)
            + phase_waves * static_cast<uint64_t>(std::max<uint32_t>(1, per_wave))
            + static_cast<uint64_t>(drain);
    };

    switch (kind) {
    case CycleInstructionKind::NTTLoad:
    case CycleInstructionKind::INTTStore:
        return phase(0, 1, 0, 1.0);
    case CycleInstructionKind::NTTButterflyLocal:
    case CycleInstructionKind::INTTButterflyLocal:
        return phase(1, spu_delay, 1, 1.0) + phase(0, bfly_delay, 0, 8.0);
    case CycleInstructionKind::NTTTranspose1:
    case CycleInstructionKind::INTTTranspose1:
        return static_cast<uint64_t>(cfg.intra_transpose_delay_cycles);
    case CycleInstructionKind::NTTButterflyGlobal:
    case CycleInstructionKind::INTTButterflyGlobal:
        return phase(0, 1, 0, 1.0) + phase(1, spu_delay, 1, 1.0) + phase(0, bfly_delay, 0, 8.0);
    case CycleInstructionKind::NTTTranspose2:
    case CycleInstructionKind::INTTTranspose2:
        return static_cast<uint64_t>(cfg.inter_transpose_delay_cycles);
    case CycleInstructionKind::NTTStore:
    case CycleInstructionKind::INTTLoad:
        return phase(1, spu_delay, 1, 1.0);
    default:
        return 0;
    }
}

uint64_t EstimateBconvSubphaseCycles(
    const HardwareModel& hardware,
    const KeySwitchProblem& problem,
    CycleInstructionKind kind) {

    const uint32_t waves = hardware.WavesPerPoly(problem);
    const HardwareConfig& cfg = hardware.config();

    auto phase = [&](uint32_t startup, uint32_t per_wave, uint32_t drain, double p) -> uint64_t {
        const uint64_t phase_waves = static_cast<uint64_t>(
            std::ceil(static_cast<double>(std::max<uint32_t>(1, waves)) * p - 1e-12));
        return static_cast<uint64_t>(startup)
            + phase_waves * static_cast<uint64_t>(std::max<uint32_t>(1, per_wave))
            + static_cast<uint64_t>(drain);
    };

    switch (kind) {
    case CycleInstructionKind::BConvLoad:
        return phase(0, 1, 0, 1.0);
    case CycleInstructionKind::BConvMAC: {
        // Single MAC pass: multiply one input limb group with its BConv matrix
        // column and accumulate into the output polynomial.  On the 256-lane
        // compute array every wave pushes 256 coefficients through the
        // multiply-add pipeline.  Lowering emits one BConvMAC group per pass;
        // the full BConv cost emerges from alpha chained groups.
        const uint32_t mul_delay = std::max<uint32_t>(1, cfg.ewe_mul_delay_cycles);
        return phase(mul_delay, /*per_wave=*/1, /*drain=*/0, /*passes=*/1.0);
    }
    case CycleInstructionKind::BConvReduce:
        return phase(std::max<uint32_t>(1, cfg.ewe_add_delay_cycles), 1, 0, 1.0);
    case CycleInstructionKind::BConvStore:
        return phase(0, 1, 0, 1.0);
    default:
        return 0;
    }
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
    case CycleInstructionKind::NTTLoad:
    case CycleInstructionKind::NTTButterflyLocal:
    case CycleInstructionKind::NTTTranspose1:
    case CycleInstructionKind::NTTButterflyGlobal:
    case CycleInstructionKind::NTTTranspose2:
    case CycleInstructionKind::NTTStore:
    case CycleInstructionKind::INTTLoad:
    case CycleInstructionKind::INTTButterflyLocal:
    case CycleInstructionKind::INTTTranspose1:
    case CycleInstructionKind::INTTButterflyGlobal:
    case CycleInstructionKind::INTTTranspose2:
    case CycleInstructionKind::INTTStore:
        return EstimateNttSubphaseCycles(hardware, problem, kind);
    case CycleInstructionKind::BConvLoad:
    case CycleInstructionKind::BConvMAC:
    case CycleInstructionKind::BConvReduce:
    case CycleInstructionKind::BConvStore:
        return EstimateBconvSubphaseCycles(hardware, problem, kind);
    case CycleInstructionKind::InterCardSend:
    case CycleInstructionKind::InterCardRecv:
        return hardware.EstimateInterconnectTransferCycles(bytes);
    case CycleInstructionKind::InterCardReduce:
        return hardware.EstimateInterconnectTransferCycles(bytes)
            + hardware.EstimateEweAddCycles(problem);
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
    const std::vector<uint32_t>& dependencies,
    uint64_t* next_instruction_id,
    CycleProgram* program) {

    if (instruction_count == 0) {
        return std::numeric_limits<uint32_t>::max();
    }

    const CycleOpType cycle_type = InferCycleOpType(step, kind, transfer_path);

    CycleInstructionGroup group;
    group.id = static_cast<uint32_t>(program->groups.size());
    group.name = group_name;
    group.kind = kind;
    group.transfer_path = transfer_path;
    group.type = cycle_type;
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
    group.live_bytes_before = 0;
    group.live_bytes_after = 0;
    group.live_bytes_delta_on_issue = live_bytes_delta_on_issue;
    group.live_bytes_delta_on_complete = live_bytes_delta_on_complete;
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
        instruction.type = cycle_type;
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
        int64_t bram_issue = 0;
        int64_t bram_complete = 0;
        if (step.output_storage == IntermediateStorageLevel::BRAM) {
            bram_issue += static_cast<int64_t>(step.output_bytes);
        }

        if (!step.fused_with_prev) {
            if (step.input_storage == IntermediateStorageLevel::BRAM) {
                bram_complete -= static_cast<int64_t>(step.input_bytes);
            }
        }

        return std::array<int64_t, 2>{bram_issue, bram_complete};
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
            deltas[0],
            deltas[1],
            deps,
            &next_instruction_id,
            &result.program);
    };

    for (const TileExecutionStep& step : execution.steps) {
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
        case TileExecutionStepType::InttTile: {
            const auto deltas = storage_deltas(step);
            const uint32_t g_load = AppendGroup(
                hardware_, execution.problem, step, "intt_load",
                CycleInstructionKind::INTTLoad, CycleTransferPath::None,
                1, /*bytes=*/0, step.work_items,
                deltas[0], 0, deps,
                &next_instruction_id, &result.program);
            std::vector<uint32_t> d1{g_load};
            const uint32_t g_bfly_local = AppendGroup(
                hardware_, execution.problem, step, "intt_bfly_local",
                CycleInstructionKind::INTTButterflyLocal, CycleTransferPath::None,
                1, /*bytes=*/0, step.work_items,
                0, 0, d1,
                &next_instruction_id, &result.program);
            std::vector<uint32_t> d2{g_bfly_local};
            const uint32_t g_tr1 = AppendGroup(
                hardware_, execution.problem, step, "intt_transpose1",
                CycleInstructionKind::INTTTranspose1, CycleTransferPath::None,
                1, /*bytes=*/0, step.work_items,
                0, 0, d2,
                &next_instruction_id, &result.program);
            std::vector<uint32_t> d3{g_tr1};
            const uint32_t g_bfly_global = AppendGroup(
                hardware_, execution.problem, step, "intt_bfly_global",
                CycleInstructionKind::INTTButterflyGlobal, CycleTransferPath::None,
                1, /*bytes=*/0, step.work_items,
                0, 0, d3,
                &next_instruction_id, &result.program);
            std::vector<uint32_t> d4{g_bfly_global};
            const uint32_t g_tr2 = AppendGroup(
                hardware_, execution.problem, step, "intt_transpose2",
                CycleInstructionKind::INTTTranspose2, CycleTransferPath::None,
                1, /*bytes=*/0, step.work_items,
                0, 0, d4,
                &next_instruction_id, &result.program);
            std::vector<uint32_t> d5{g_tr2};
            terminal = AppendGroup(
                hardware_, execution.problem, step, "intt_store",
                CycleInstructionKind::INTTStore, CycleTransferPath::None,
                1, /*bytes=*/0, step.work_items,
                0, deltas[1], d5,
                &next_instruction_id, &result.program);
            break;
        }

        case TileExecutionStepType::ModUpBConvTile:
        case TileExecutionStepType::ModDownBConvTile:
        case TileExecutionStepType::BasisConvertTile: {
            const auto deltas = storage_deltas(step);
            const uint32_t alpha = std::max<uint32_t>(
                1, hardware_.Alpha(execution.problem));

            const uint32_t g_load = AppendGroup(
                hardware_, execution.problem, step, "bconv_load",
                CycleInstructionKind::BConvLoad, CycleTransferPath::None,
                1, /*bytes=*/0, step.work_items,
                deltas[0], 0, deps,
                &next_instruction_id, &result.program);

            uint32_t prev_mac = g_load;
            for (uint32_t pass = 0; pass < alpha; ++pass) {
                std::vector<uint32_t> mac_deps{prev_mac};
                prev_mac = AppendGroup(
                    hardware_, execution.problem, step,
                    "bconv_mac_p" + std::to_string(pass),
                    CycleInstructionKind::BConvMAC, CycleTransferPath::None,
                    1, /*bytes=*/0, step.work_items,
                    0, 0, mac_deps,
                    &next_instruction_id, &result.program);
            }

            std::vector<uint32_t> reduce_deps{prev_mac};
            const uint32_t g_reduce = AppendGroup(
                hardware_, execution.problem, step, "bconv_reduce",
                CycleInstructionKind::BConvReduce, CycleTransferPath::None,
                1, /*bytes=*/0, step.work_items,
                0, 0, reduce_deps,
                &next_instruction_id, &result.program);
            std::vector<uint32_t> store_deps{g_reduce};
            terminal = AppendGroup(
                hardware_, execution.problem, step, "bconv_store",
                CycleInstructionKind::BConvStore, CycleTransferPath::None,
                1, /*bytes=*/0, step.work_items,
                0, deltas[1], store_deps,
                &next_instruction_id, &result.program);
            break;
        }

        case TileExecutionStepType::ModUpNttTile:
        case TileExecutionStepType::ModDownNttTile:
        case TileExecutionStepType::NttTile: {
            const auto deltas = storage_deltas(step);
            const uint32_t g_load = AppendGroup(
                hardware_, execution.problem, step, "ntt_load",
                CycleInstructionKind::NTTLoad, CycleTransferPath::None,
                1, /*bytes=*/0, step.work_items,
                deltas[0], 0, deps,
                &next_instruction_id, &result.program);
            std::vector<uint32_t> d1{g_load};
            const uint32_t g_bfly_local = AppendGroup(
                hardware_, execution.problem, step, "ntt_bfly_local",
                CycleInstructionKind::NTTButterflyLocal, CycleTransferPath::None,
                1, /*bytes=*/0, step.work_items,
                0, 0, d1,
                &next_instruction_id, &result.program);
            std::vector<uint32_t> d2{g_bfly_local};
            const uint32_t g_tr1 = AppendGroup(
                hardware_, execution.problem, step, "ntt_transpose1",
                CycleInstructionKind::NTTTranspose1, CycleTransferPath::None,
                1, /*bytes=*/0, step.work_items,
                0, 0, d2,
                &next_instruction_id, &result.program);
            std::vector<uint32_t> d3{g_tr1};
            const uint32_t g_bfly_global = AppendGroup(
                hardware_, execution.problem, step, "ntt_bfly_global",
                CycleInstructionKind::NTTButterflyGlobal, CycleTransferPath::None,
                1, /*bytes=*/0, step.work_items,
                0, 0, d3,
                &next_instruction_id, &result.program);
            std::vector<uint32_t> d4{g_bfly_global};
            const uint32_t g_tr2 = AppendGroup(
                hardware_, execution.problem, step, "ntt_transpose2",
                CycleInstructionKind::NTTTranspose2, CycleTransferPath::None,
                1, /*bytes=*/0, step.work_items,
                0, 0, d4,
                &next_instruction_id, &result.program);
            std::vector<uint32_t> d5{g_tr2};
            terminal = AppendGroup(
                hardware_, execution.problem, step, "ntt_store",
                CycleInstructionKind::NTTStore, CycleTransferPath::None,
                1, /*bytes=*/0, step.work_items,
                0, deltas[1], d5,
                &next_instruction_id, &result.program);
            break;
        }

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
                deltas[0],
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
                    deltas[1],
                    add_deps,
                    &next_instruction_id,
                    &result.program);
            } else {
                // Release on the mul group when no follow-up add group exists.
                result.program.groups[mul_group].live_bytes_delta_on_complete = deltas[1];
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
                deltas[0],
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
                deltas[1],
                sub_deps,
                &next_instruction_id,
                &result.program);
            break;
        }

        case TileExecutionStepType::InterCardSendStep:
            terminal = append_single_group(
                step,
                "inter_card_send",
                CycleInstructionKind::InterCardSend,
                CycleTransferPath::None,
                /*instruction_count=*/1,
                step_bytes,
                /*work_items=*/0,
                deps);
            break;

        case TileExecutionStepType::InterCardRecvStep:
            terminal = append_single_group(
                step,
                "inter_card_recv",
                CycleInstructionKind::InterCardRecv,
                CycleTransferPath::None,
                /*instruction_count=*/1,
                step_bytes,
                /*work_items=*/0,
                deps);
            break;

        case TileExecutionStepType::InterCardReduceStep:
            terminal = append_single_group(
                step,
                "inter_card_reduce",
                CycleInstructionKind::InterCardReduce,
                CycleTransferPath::None,
                /*instruction_count=*/1,
                step_bytes,
                step.work_items,
                deps);
            break;

        case TileExecutionStepType::BarrierStep:
        case TileExecutionStepType::InterCardBarrier:
            terminal = append_single_group(
                step,
                "inter_card_barrier",
                CycleInstructionKind::InterCardSend,
                CycleTransferPath::None,
                /*instruction_count=*/1,
                /*bytes=*/0,
                /*work_items=*/0,
                deps);
            break;

        case TileExecutionStepType::InterCardCommTile:
            terminal = append_single_group(
                step,
                "inter_card_comm",
                CycleInstructionKind::InterCardSend,
                CycleTransferPath::None,
                /*instruction_count=*/1,
                step_bytes,
                /*work_items=*/0,
                deps);
            break;

        case TileExecutionStepType::Merge:
            terminal = append_single_group(
                step,
                "merge",
                CycleInstructionKind::InterCardReduce,
                CycleTransferPath::None,
                /*instruction_count=*/1,
                step_bytes,
                step.work_items,
                deps);
            break;
        }

        if (terminal != invalid_group) {
            step_terminal_group[step.step_id] = terminal;
        }
    }

    result.program.estimated_peak_live_bytes = 0;
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
    const KeySwitchExecution& execution
) const {

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
