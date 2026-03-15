#pragma once

#include "backend/model/keyswitch_execution_model.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

enum class CycleInstructionKind : uint8_t {
    LoadHBM = 0,
    StoreHBM,
    Decompose,
    NTT,
    INTT,
    EweMul,
    EweAdd,
    EweSub,
    BConv
};

enum class CycleTransferPath : uint8_t {
    None = 0,
    HostToHBM,
    HBMToSPM,
    SPMToHBM
};

constexpr std::size_t kCycleInstructionKindCount = 9;

inline std::size_t ToIndex(CycleInstructionKind kind) {
    return static_cast<std::size_t>(kind);
}

inline const char* ToString(CycleInstructionKind kind) {
    switch (kind) {
    case CycleInstructionKind::LoadHBM:
        return "LoadHBM";
    case CycleInstructionKind::StoreHBM:
        return "StoreHBM";
    case CycleInstructionKind::Decompose:
        return "Decompose";
    case CycleInstructionKind::NTT:
        return "NTT";
    case CycleInstructionKind::INTT:
        return "INTT";
    case CycleInstructionKind::EweMul:
        return "EweMul";
    case CycleInstructionKind::EweAdd:
        return "EweAdd";
    case CycleInstructionKind::EweSub:
        return "EweSub";
    case CycleInstructionKind::BConv:
        return "BConv";
    }

    return "Unknown";
}

struct CycleInstruction {
    uint64_t id = 0;
    uint32_t group_id = 0;
    CycleInstructionKind kind = CycleInstructionKind::LoadHBM;
    CycleTransferPath transfer_path = CycleTransferPath::None;
    TileExecutionStepType source_step_type = TileExecutionStepType::InputHBMToBRAM;
    StageType stage_type = StageType::Dispatch;
    uint32_t ct_tile_index = 0;
    uint32_t limb_tile_index = 0;
    uint32_t digit_tile_index = 0;
    IntermediateStorageLevel input_storage = IntermediateStorageLevel::SRAM;
    IntermediateStorageLevel output_storage = IntermediateStorageLevel::SRAM;
    bool fused_with_prev = false;
    bool fused_with_next = false;
    bool is_shortcut_path = false;
    uint64_t bytes = 0;
    uint64_t work_items = 0;
    uint64_t latency_cycles = 0;
};

struct CycleInstructionGroup {
    uint32_t id = 0;
    std::string name;
    CycleInstructionKind kind = CycleInstructionKind::LoadHBM;
    CycleTransferPath transfer_path = CycleTransferPath::None;
    TileExecutionStepType source_step_type = TileExecutionStepType::InputHBMToBRAM;
    StageType stage_type = StageType::Dispatch;
    uint32_t ct_tile_index = 0;
    uint32_t limb_tile_index = 0;
    uint32_t digit_tile_index = 0;
    IntermediateStorageLevel input_storage = IntermediateStorageLevel::SRAM;
    IntermediateStorageLevel output_storage = IntermediateStorageLevel::SRAM;
    bool fused_with_prev = false;
    bool fused_with_next = false;
    bool is_shortcut_path = false;
    uint64_t bytes = 0;
    uint64_t work_items = 0;
    uint64_t live_bytes_before = 0;
    uint64_t live_bytes_after = 0;
    int64_t live_bytes_delta_on_issue = 0;
    int64_t live_bytes_delta_on_complete = 0;
    int64_t rf_live_bytes_delta_on_issue = 0;
    int64_t rf_live_bytes_delta_on_complete = 0;
    int64_t sram_live_bytes_delta_on_issue = 0;
    int64_t sram_live_bytes_delta_on_complete = 0;
    std::vector<uint32_t> dependencies;
    std::vector<CycleInstruction> instructions;
};

struct CycleProgram {
    KeySwitchMethod method = KeySwitchMethod::Poseidon;
    std::string name;
    uint64_t instruction_count = 0;
    uint64_t estimated_peak_live_bytes = 0;
    std::vector<CycleInstructionGroup> groups;

    bool empty() const {
        return groups.empty();
    }
};
