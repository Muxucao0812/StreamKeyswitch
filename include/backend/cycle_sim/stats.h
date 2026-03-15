#pragma once

#include "backend/cycle_sim/instruction.h"

#include <array>
#include <cstdint>
#include <vector>

struct CycleGroupTiming {
    uint32_t group_id = 0;
    CycleInstructionKind kind = CycleInstructionKind::LoadHBM;
    CycleTransferPath transfer_path = CycleTransferPath::None;
    TileExecutionStepType source_step_type = TileExecutionStepType::InputHBMToBRAM;
    StageType stage_type = StageType::Dispatch;
    uint32_t ct_tile_index = 0;
    uint32_t limb_tile_index = 0;
    uint32_t digit_tile_index = 0;
    uint64_t start_cycle = 0;
    uint64_t finish_cycle = 0;
    uint64_t bytes = 0;
    uint64_t work_items = 0;
    uint64_t issued_instructions = 0;
    uint64_t completed_instructions = 0;
    uint64_t dependency_wait_cycles = 0;
    uint64_t resource_wait_cycles = 0;
    IntermediateStorageLevel input_storage = IntermediateStorageLevel::BRAM;
    IntermediateStorageLevel output_storage = IntermediateStorageLevel::BRAM;
    bool fused_with_prev = false;
    bool fused_with_next = false;
    bool is_shortcut_path = false;
    uint64_t live_bytes_before = 0;
    uint64_t live_bytes_after = 0;

    uint64_t DurationCycles() const {
        return (finish_cycle > start_cycle) ? (finish_cycle - start_cycle) : 0;
    }
};

struct CycleComponentStats {
    CycleInstructionKind kind = CycleInstructionKind::LoadHBM;
    uint64_t issued_instructions = 0;
    uint64_t completed_instructions = 0;
    uint64_t busy_cycles = 0;
    uint64_t stall_cycles = 0;
    uint64_t max_inflight = 0;
};

struct CycleSimStats {
    uint64_t total_cycles = 0;
    uint64_t hbm_read_bytes = 0;
    uint64_t hbm_write_bytes = 0;
    uint64_t hbm_round_trips = 0;
    uint64_t spill_bytes = 0;
    uint64_t reload_bytes = 0;
    uint64_t peak_bram_live_bytes = 0;
    uint64_t dependency_stall_cycles = 0;
    uint64_t resource_stall_cycles = 0;
    std::array<uint64_t, kCycleInstructionKindCount> instruction_cycles{};
    std::array<uint64_t, kCycleInstructionKindCount> instruction_counts{};
    std::array<uint64_t, kCycleInstructionKindCount> instruction_bytes{};
    std::array<uint64_t, kTileExecutionStepTypeCount> fine_step_cycles{};
    std::array<uint64_t, kTileExecutionStepTypeCount> fine_step_counts{};
    std::vector<CycleGroupTiming> group_timings;
    std::vector<CycleComponentStats> component_stats;
};
