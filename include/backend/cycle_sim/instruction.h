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
    BConv,

    NTTLoad,
    NTTButterflyLocal,
    NTTTranspose1,
    NTTButterflyGlobal,
    NTTTranspose2,
    NTTStore,

    INTTLoad,
    INTTButterflyLocal,
    INTTTranspose1,
    INTTButterflyGlobal,
    INTTTranspose2,
    INTTStore,

    BConvLoad,
    BConvMAC,
    BConvReduce,
    BConvStore,

    InterCardSend,
    InterCardRecv,
    InterCardReduce
};

enum class CycleTransferPath : uint8_t {
    None = 0,
    HostToHBM,
    HBMToSPM,
    SPMToHBM,
    HBMToHBM
};

enum class CycleOpType : uint8_t {
    DataLoad = 0,
    KeyLoad,
    Spill,
    NTT,
    INTT,
    BConv,
    Multiply,
    Add,
    Sub,
    InterCardComm
};

inline TileExecutionStepType FineStepTypeOf(CycleOpType type) {
    switch (type) {
    case CycleOpType::DataLoad:
        return TileExecutionStepType::InputHBMToBRAM;
    case CycleOpType::KeyLoad:
        return TileExecutionStepType::KeyHBMToBRAM;
    case CycleOpType::Spill:
        return TileExecutionStepType::IntermediateBRAMToHBM;
    case CycleOpType::NTT:
        return TileExecutionStepType::NttTile;
    case CycleOpType::INTT:
        return TileExecutionStepType::InttTile;
    case CycleOpType::BConv:
        return TileExecutionStepType::BasisConvertTile;
    case CycleOpType::Multiply:
        return TileExecutionStepType::KSInnerProdTile;
    case CycleOpType::Add:
        return TileExecutionStepType::CrossDigitReduceTile;
    case CycleOpType::Sub:
        return TileExecutionStepType::FinalSubtractTile;
    case CycleOpType::InterCardComm:
        return TileExecutionStepType::InterCardCommTile;
    }
    return TileExecutionStepType::InputHBMToBRAM;
}

constexpr std::size_t kCycleInstructionKindCount = 28;

enum class ResourceClass : uint8_t {
    ComputeArray = 0,
    SPU,
    HBM,
    Interconnect
};

constexpr std::size_t kResourceClassCount = 4;

inline std::size_t ToIndex(CycleInstructionKind kind) {
    return static_cast<std::size_t>(kind);
}

inline std::size_t ToIndex(ResourceClass rc) {
    return static_cast<std::size_t>(rc);
}

inline const char* ToString(ResourceClass rc) {
    switch (rc) {
    case ResourceClass::ComputeArray:  return "ComputeArray";
    case ResourceClass::SPU:           return "SPU";
    case ResourceClass::HBM:           return "HBM";
    case ResourceClass::Interconnect:  return "Interconnect";
    }
    return "Unknown";
}

inline ResourceClass ResourceClassOf(CycleInstructionKind kind) {
    switch (kind) {
    case CycleInstructionKind::LoadHBM:
    case CycleInstructionKind::StoreHBM:
    case CycleInstructionKind::NTTLoad:
    case CycleInstructionKind::NTTStore:
    case CycleInstructionKind::INTTLoad:
    case CycleInstructionKind::INTTStore:
    case CycleInstructionKind::BConvLoad:
    case CycleInstructionKind::BConvStore:
        return ResourceClass::HBM;

    case CycleInstructionKind::NTTTranspose1:
    case CycleInstructionKind::NTTTranspose2:
    case CycleInstructionKind::INTTTranspose1:
    case CycleInstructionKind::INTTTranspose2:
        return ResourceClass::SPU;

    case CycleInstructionKind::NTT:
    case CycleInstructionKind::INTT:
    case CycleInstructionKind::NTTButterflyLocal:
    case CycleInstructionKind::NTTButterflyGlobal:
    case CycleInstructionKind::INTTButterflyLocal:
    case CycleInstructionKind::INTTButterflyGlobal:
    case CycleInstructionKind::EweMul:
    case CycleInstructionKind::EweAdd:
    case CycleInstructionKind::EweSub:
    case CycleInstructionKind::BConv:
    case CycleInstructionKind::BConvMAC:
    case CycleInstructionKind::BConvReduce:
    case CycleInstructionKind::Decompose:
        return ResourceClass::ComputeArray;

    case CycleInstructionKind::InterCardSend:
    case CycleInstructionKind::InterCardRecv:
    case CycleInstructionKind::InterCardReduce:
        return ResourceClass::Interconnect;
    }
    return ResourceClass::ComputeArray;
}

inline CycleInstructionKind BaseKind(CycleInstructionKind kind) {
    switch (kind) {
    case CycleInstructionKind::NTTLoad:
    case CycleInstructionKind::INTTLoad:
    case CycleInstructionKind::BConvLoad:
        return CycleInstructionKind::LoadHBM;
    case CycleInstructionKind::NTTStore:
    case CycleInstructionKind::INTTStore:
    case CycleInstructionKind::BConvStore:
        return CycleInstructionKind::StoreHBM;
    case CycleInstructionKind::NTTButterflyLocal:
    case CycleInstructionKind::NTTButterflyGlobal:
    case CycleInstructionKind::NTTTranspose1:
    case CycleInstructionKind::NTTTranspose2:
        return CycleInstructionKind::NTT;
    case CycleInstructionKind::INTTButterflyLocal:
    case CycleInstructionKind::INTTButterflyGlobal:
    case CycleInstructionKind::INTTTranspose1:
    case CycleInstructionKind::INTTTranspose2:
        return CycleInstructionKind::INTT;
    case CycleInstructionKind::BConvMAC:
        return CycleInstructionKind::BConv;
    case CycleInstructionKind::BConvReduce:
        return CycleInstructionKind::EweAdd;
    case CycleInstructionKind::InterCardSend:
    case CycleInstructionKind::InterCardRecv:
    case CycleInstructionKind::InterCardReduce:
        return kind;
    default:
        return kind;
    }
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
    case CycleInstructionKind::NTTLoad:
        return "NTTLoad";
    case CycleInstructionKind::NTTButterflyLocal:
        return "NTTButterflyLocal";
    case CycleInstructionKind::NTTTranspose1:
        return "NTTTranspose1";
    case CycleInstructionKind::NTTButterflyGlobal:
        return "NTTButterflyGlobal";
    case CycleInstructionKind::NTTTranspose2:
        return "NTTTranspose2";
    case CycleInstructionKind::NTTStore:
        return "NTTStore";
    case CycleInstructionKind::INTTLoad:
        return "INTTLoad";
    case CycleInstructionKind::INTTButterflyLocal:
        return "INTTButterflyLocal";
    case CycleInstructionKind::INTTTranspose1:
        return "INTTTranspose1";
    case CycleInstructionKind::INTTButterflyGlobal:
        return "INTTButterflyGlobal";
    case CycleInstructionKind::INTTTranspose2:
        return "INTTTranspose2";
    case CycleInstructionKind::INTTStore:
        return "INTTStore";
    case CycleInstructionKind::BConvLoad:
        return "BConvLoad";
    case CycleInstructionKind::BConvMAC:
        return "BConvMAC";
    case CycleInstructionKind::BConvReduce:
        return "BConvReduce";
    case CycleInstructionKind::BConvStore:
        return "BConvStore";
    case CycleInstructionKind::InterCardSend:
        return "InterCardSend";
    case CycleInstructionKind::InterCardRecv:
        return "InterCardRecv";
    case CycleInstructionKind::InterCardReduce:
        return "InterCardReduce";
    }

    return "Unknown";
}

struct CycleInstruction {
    uint64_t id = 0;
    uint32_t group_id = 0;
    CycleInstructionKind kind = CycleInstructionKind::LoadHBM;
    CycleTransferPath transfer_path = CycleTransferPath::None;
    CycleOpType type = CycleOpType::DataLoad;
    uint32_t ct_tile_index = 0;
    uint32_t limb_tile_index = 0;
    uint32_t digit_tile_index = 0;
    IntermediateStorageLevel input_storage = IntermediateStorageLevel::BRAM;
    IntermediateStorageLevel output_storage = IntermediateStorageLevel::BRAM;
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
    CycleOpType type = CycleOpType::DataLoad;
    uint32_t ct_tile_index = 0;
    uint32_t limb_tile_index = 0;
    uint32_t digit_tile_index = 0;
    IntermediateStorageLevel input_storage = IntermediateStorageLevel::BRAM;
    IntermediateStorageLevel output_storage = IntermediateStorageLevel::BRAM;
    bool fused_with_prev = false;
    bool fused_with_next = false;
    bool is_shortcut_path = false;
    uint64_t bytes = 0;
    uint64_t work_items = 0;
    uint64_t live_bytes_before = 0;
    uint64_t live_bytes_after = 0;
    int64_t live_bytes_delta_on_issue = 0;
    int64_t live_bytes_delta_on_complete = 0;
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
