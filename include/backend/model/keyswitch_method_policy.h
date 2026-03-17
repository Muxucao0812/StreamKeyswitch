#pragma once

#include "model/request.h"

#include <cstdint>

enum class IterationAxis : uint8_t {
    ByDigit = 0,
    ByLimb,
    ByOutput
};

enum class ModDownStrategy : uint8_t {
    FullAfterReduce = 0,
    LimbStreaming,
    ShortcutStreaming
};

enum class TransferGranularity : uint8_t {
    Tile = 0,
    Limb,
    Digit,
    Output
};


enum class ReductionMode : uint8_t {
    Centralized = 0,      // 所有 partial 完成后统一 reduce
    Streaming,            // partial 生成后立即进入 reduce
    Hierarchical          // 先局部 reduce，再全局 reduce
};

enum class KeySwitchProcessingGranularity : uint8_t {
    Tile = 0,
    Digit,
    Limb
};

enum class IntermediateStorageLevel : uint8_t {
    BRAM = 0,
    HBM
};

enum class StageConnectionMode : uint8_t {
    DirectForward = 0,
    BufferInBRAM,
    SpillToHBM
};

struct KeySwitchMethodPolicy {
    KeySwitchMethod method = KeySwitchMethod::Poseidon;
    KeySwitchProcessingGranularity granularity = KeySwitchProcessingGranularity::Tile;

    bool fuse_modup_chain = false;
    bool fuse_moddown_chain = false;
    bool fuse_cross_stage = false;

    IntermediateStorageLevel input_pref_storage = IntermediateStorageLevel::BRAM;
    IntermediateStorageLevel key_pref_storage   = IntermediateStorageLevel::BRAM;

    IntermediateStorageLevel modup_output_storage = IntermediateStorageLevel::BRAM;
    IntermediateStorageLevel innerprod_output_storage = IntermediateStorageLevel::BRAM;
    IntermediateStorageLevel reduction_output_storage = IntermediateStorageLevel::BRAM;
    IntermediateStorageLevel moddown_temp_storage = IntermediateStorageLevel::BRAM;

    StageConnectionMode modup_to_innerprod = StageConnectionMode::BufferInBRAM;
    StageConnectionMode innerprod_to_reduction = StageConnectionMode::BufferInBRAM;
    StageConnectionMode reduction_to_moddown = StageConnectionMode::BufferInBRAM;
    StageConnectionMode moddown_to_subtract = StageConnectionMode::DirectForward;

    ReductionMode reduction_mode = ReductionMode::Centralized;

    bool requires_large_bram = false;
    bool supports_moddown_shortcut = false;
    bool supports_fused_final_subtract = false;
    bool supports_partial_reduction_overlap = false;

    IterationAxis modup_axis = IterationAxis::ByDigit;
    IterationAxis innerprod_axis = IterationAxis::ByDigit;
    IterationAxis moddown_axis = IterationAxis::ByDigit;

    ModDownStrategy moddown_strategy = ModDownStrategy::FullAfterReduce;
    TransferGranularity spill_granularity = TransferGranularity::Tile;

    bool overlap_reduce_with_innerprod = false;
    bool persist_partials_in_bram = false;
    bool persist_modup_outputs_in_bram = false;
    bool direct_forward_to_moddown = false;
};
inline KeySwitchMethodPolicy ResolveMethodPolicy(KeySwitchMethod method) {
    KeySwitchMethodPolicy policy;
    policy.method = method;

    switch (method) {
    case KeySwitchMethod::Poseidon:
        policy.granularity = KeySwitchProcessingGranularity::Tile;

        policy.modup_axis = IterationAxis::ByDigit;
        policy.innerprod_axis = IterationAxis::ByDigit;
        policy.moddown_axis = IterationAxis::ByOutput;

        policy.fuse_modup_chain = false;
        policy.fuse_moddown_chain = false;
        policy.fuse_cross_stage = false;

        policy.input_pref_storage = IntermediateStorageLevel::BRAM;
        policy.key_pref_storage = IntermediateStorageLevel::HBM;

        policy.modup_output_storage = IntermediateStorageLevel::HBM;
        policy.innerprod_output_storage = IntermediateStorageLevel::HBM;
        policy.reduction_output_storage = IntermediateStorageLevel::HBM;
        policy.moddown_temp_storage = IntermediateStorageLevel::BRAM;

        policy.modup_to_innerprod = StageConnectionMode::SpillToHBM;
        policy.innerprod_to_reduction = StageConnectionMode::SpillToHBM;
        policy.reduction_to_moddown = StageConnectionMode::SpillToHBM;
        policy.moddown_to_subtract = StageConnectionMode::BufferInBRAM;

        policy.spill_granularity = TransferGranularity::Tile;
        policy.reduction_mode = ReductionMode::Centralized;
        policy.moddown_strategy = ModDownStrategy::FullAfterReduce;

        policy.requires_large_bram = false;
        policy.supports_moddown_shortcut = false;
        policy.supports_fused_final_subtract = false;
        policy.supports_partial_reduction_overlap = false;

        policy.overlap_reduce_with_innerprod = false;
        policy.persist_partials_in_bram = false;
        policy.persist_modup_outputs_in_bram = false;
        policy.direct_forward_to_moddown = false;
        return policy;

    case KeySwitchMethod::OLA:
        policy.granularity = KeySwitchProcessingGranularity::Limb;

        policy.modup_axis = IterationAxis::ByLimb;
        policy.innerprod_axis = IterationAxis::ByLimb;
        policy.moddown_axis = IterationAxis::ByLimb;

        policy.fuse_modup_chain = true;
        policy.fuse_moddown_chain = false;
        policy.fuse_cross_stage = false;

        policy.input_pref_storage = IntermediateStorageLevel::BRAM;
        policy.key_pref_storage = IntermediateStorageLevel::BRAM;

        policy.modup_output_storage = IntermediateStorageLevel::BRAM;
        policy.innerprod_output_storage = IntermediateStorageLevel::HBM;
        policy.reduction_output_storage = IntermediateStorageLevel::BRAM;
        policy.moddown_temp_storage = IntermediateStorageLevel::BRAM;

        policy.modup_to_innerprod = StageConnectionMode::BufferInBRAM;
        policy.innerprod_to_reduction = StageConnectionMode::SpillToHBM;
        policy.reduction_to_moddown = StageConnectionMode::BufferInBRAM;
        policy.moddown_to_subtract = StageConnectionMode::DirectForward;

        policy.spill_granularity = TransferGranularity::Limb;
        policy.reduction_mode = ReductionMode::Streaming;
        policy.moddown_strategy = ModDownStrategy::LimbStreaming;

        policy.requires_large_bram = false;
        policy.supports_moddown_shortcut = false;
        policy.supports_fused_final_subtract = false;
        policy.supports_partial_reduction_overlap = true;

        policy.overlap_reduce_with_innerprod = true;
        policy.persist_partials_in_bram = false;
        policy.persist_modup_outputs_in_bram = true;
        policy.direct_forward_to_moddown = false;
        return policy;

    case KeySwitchMethod::FAB:
        policy.granularity = KeySwitchProcessingGranularity::Digit;

        policy.modup_axis = IterationAxis::ByDigit;
        policy.innerprod_axis = IterationAxis::ByDigit;
        policy.moddown_axis = IterationAxis::ByOutput;

        policy.fuse_modup_chain = true;
        policy.fuse_moddown_chain = false;
        policy.fuse_cross_stage = true;

        policy.input_pref_storage = IntermediateStorageLevel::BRAM;
        policy.key_pref_storage = IntermediateStorageLevel::BRAM;

        policy.modup_output_storage = IntermediateStorageLevel::BRAM;
        policy.innerprod_output_storage = IntermediateStorageLevel::BRAM;
        policy.reduction_output_storage = IntermediateStorageLevel::BRAM;
        policy.moddown_temp_storage = IntermediateStorageLevel::BRAM;

        policy.modup_to_innerprod = StageConnectionMode::BufferInBRAM;
        policy.innerprod_to_reduction = StageConnectionMode::BufferInBRAM;
        policy.reduction_to_moddown = StageConnectionMode::BufferInBRAM;
        policy.moddown_to_subtract = StageConnectionMode::BufferInBRAM;

        policy.spill_granularity = TransferGranularity::Digit;
        policy.reduction_mode = ReductionMode::Streaming;
        policy.moddown_strategy = ModDownStrategy::FullAfterReduce;

        policy.requires_large_bram = true;
        policy.supports_moddown_shortcut = false;
        policy.supports_fused_final_subtract = false;
        policy.supports_partial_reduction_overlap = true;

        policy.overlap_reduce_with_innerprod = true;
        policy.persist_partials_in_bram = true;
        policy.persist_modup_outputs_in_bram = true;
        policy.direct_forward_to_moddown = false;
        return policy;

    case KeySwitchMethod::FAST:
        policy.granularity = KeySwitchProcessingGranularity::Limb;

        policy.modup_axis = IterationAxis::ByLimb;
        policy.innerprod_axis = IterationAxis::ByLimb;
        policy.moddown_axis = IterationAxis::ByLimb;

        policy.fuse_modup_chain = true;
        policy.fuse_moddown_chain = false;
        policy.fuse_cross_stage = false;

        policy.input_pref_storage = IntermediateStorageLevel::BRAM;
        policy.key_pref_storage = IntermediateStorageLevel::BRAM;

        policy.modup_output_storage = IntermediateStorageLevel::BRAM;
        policy.innerprod_output_storage = IntermediateStorageLevel::HBM;
        policy.reduction_output_storage = IntermediateStorageLevel::HBM;
        policy.moddown_temp_storage = IntermediateStorageLevel::BRAM;

        policy.modup_to_innerprod = StageConnectionMode::BufferInBRAM;
        policy.innerprod_to_reduction = StageConnectionMode::SpillToHBM;
        policy.reduction_to_moddown = StageConnectionMode::SpillToHBM;
        policy.moddown_to_subtract = StageConnectionMode::BufferInBRAM;

        policy.spill_granularity = TransferGranularity::Limb;
        policy.reduction_mode = ReductionMode::Centralized;
        policy.moddown_strategy = ModDownStrategy::FullAfterReduce;

        policy.requires_large_bram = false;
        policy.supports_moddown_shortcut = false;
        policy.supports_fused_final_subtract = false;
        policy.supports_partial_reduction_overlap = false;

        policy.overlap_reduce_with_innerprod = false;
        policy.persist_partials_in_bram = false;
        policy.persist_modup_outputs_in_bram = true;
        policy.direct_forward_to_moddown = false;
        return policy;

    case KeySwitchMethod::HERA:
        policy.granularity = KeySwitchProcessingGranularity::Limb;

        policy.modup_axis = IterationAxis::ByLimb;
        policy.innerprod_axis = IterationAxis::ByLimb;
        policy.moddown_axis = IterationAxis::ByLimb;

        policy.fuse_modup_chain = true;
        policy.fuse_moddown_chain = true;
        policy.fuse_cross_stage = true;

        policy.input_pref_storage = IntermediateStorageLevel::BRAM;
        policy.key_pref_storage = IntermediateStorageLevel::BRAM;

        policy.modup_output_storage = IntermediateStorageLevel::BRAM;
        policy.innerprod_output_storage = IntermediateStorageLevel::BRAM;
        policy.reduction_output_storage = IntermediateStorageLevel::BRAM;
        policy.moddown_temp_storage = IntermediateStorageLevel::BRAM;

        policy.modup_to_innerprod = StageConnectionMode::DirectForward;
        policy.innerprod_to_reduction = StageConnectionMode::BufferInBRAM;
        policy.reduction_to_moddown = StageConnectionMode::BufferInBRAM;
        policy.moddown_to_subtract = StageConnectionMode::DirectForward;

        policy.spill_granularity = TransferGranularity::Limb;
        policy.reduction_mode = ReductionMode::Streaming;
        policy.moddown_strategy = ModDownStrategy::ShortcutStreaming;

        policy.requires_large_bram = false;
        policy.supports_moddown_shortcut = true;
        policy.supports_fused_final_subtract = false;
        policy.supports_partial_reduction_overlap = true;

        policy.overlap_reduce_with_innerprod = true;
        policy.persist_partials_in_bram = true;
        policy.persist_modup_outputs_in_bram = true;
        policy.direct_forward_to_moddown = true;
        return policy;

    case KeySwitchMethod::Cinnamon:
        policy.granularity = KeySwitchProcessingGranularity::Limb;

        policy.modup_axis = IterationAxis::ByLimb;
        policy.innerprod_axis = IterationAxis::ByLimb;
        policy.moddown_axis = IterationAxis::ByLimb;

        policy.fuse_modup_chain = true;
        policy.fuse_moddown_chain = false;
        policy.fuse_cross_stage = false;

        policy.input_pref_storage = IntermediateStorageLevel::BRAM;
        policy.key_pref_storage = IntermediateStorageLevel::BRAM;

        policy.modup_output_storage = IntermediateStorageLevel::BRAM;
        policy.innerprod_output_storage = IntermediateStorageLevel::HBM;
        policy.reduction_output_storage = IntermediateStorageLevel::HBM;
        policy.moddown_temp_storage = IntermediateStorageLevel::BRAM;

        policy.modup_to_innerprod = StageConnectionMode::BufferInBRAM;
        policy.innerprod_to_reduction = StageConnectionMode::SpillToHBM;
        policy.reduction_to_moddown = StageConnectionMode::SpillToHBM;
        policy.moddown_to_subtract = StageConnectionMode::BufferInBRAM;

        policy.spill_granularity = TransferGranularity::Limb;
        policy.reduction_mode = ReductionMode::Centralized;
        policy.moddown_strategy = ModDownStrategy::FullAfterReduce;

        policy.requires_large_bram = false;
        policy.supports_moddown_shortcut = false;
        policy.supports_fused_final_subtract = false;
        policy.supports_partial_reduction_overlap = false;

        policy.overlap_reduce_with_innerprod = false;
        policy.persist_partials_in_bram = false;
        policy.persist_modup_outputs_in_bram = true;
        policy.direct_forward_to_moddown = false;
        return policy;

    case KeySwitchMethod::Auto:
    default:
        return ResolveMethodPolicy(KeySwitchMethod::Poseidon);
    }
}
