#pragma once

#include "model/request.h"

#include <cstdint>


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
    RF = 0,
    SRAM,
    HBM
};

enum class StageConnectionMode : uint8_t {
    DirectForward = 0,
    BufferInRF,
    BufferInSRAM,
    SpillToHBM
};

struct KeySwitchMethodPolicy {
    KeySwitchMethod method = KeySwitchMethod::Poseidon;
    KeySwitchProcessingGranularity granularity = KeySwitchProcessingGranularity::Tile;

    bool fuse_modup_chain = false;
    bool fuse_moddown_chain = false;
    bool fuse_cross_stage = false;

    IntermediateStorageLevel input_pref_storage = IntermediateStorageLevel::SRAM;
    IntermediateStorageLevel key_pref_storage   = IntermediateStorageLevel::SRAM;

    IntermediateStorageLevel modup_output_storage = IntermediateStorageLevel::SRAM;
    IntermediateStorageLevel innerprod_output_storage = IntermediateStorageLevel::SRAM;
    IntermediateStorageLevel reduction_output_storage = IntermediateStorageLevel::SRAM;
    IntermediateStorageLevel moddown_temp_storage = IntermediateStorageLevel::SRAM;

    StageConnectionMode modup_to_innerprod = StageConnectionMode::BufferInSRAM;
    StageConnectionMode innerprod_to_reduction = StageConnectionMode::BufferInSRAM;
    StageConnectionMode reduction_to_moddown = StageConnectionMode::BufferInSRAM;
    StageConnectionMode moddown_to_subtract = StageConnectionMode::DirectForward;

    ReductionMode reduction_mode = ReductionMode::Centralized;

    bool requires_large_rf = false;
    bool supports_moddown_shortcut = false;
    bool supports_fused_final_subtract = false;
    bool supports_partial_reduction_overlap = false;

    bool prefer_digit_locality = false;
    bool prefer_limb_streaming = false;
};

inline KeySwitchMethodPolicy ResolveMethodPolicy(KeySwitchMethod method) {
    KeySwitchMethodPolicy policy;
    policy.method = method;

    switch (method) {
    case KeySwitchMethod::Poseidon:
        policy.granularity = KeySwitchProcessingGranularity::Tile;

        policy.fuse_modup_chain = false;
        policy.fuse_moddown_chain = false;
        policy.fuse_cross_stage = false;

        policy.input_pref_storage = IntermediateStorageLevel::SRAM;
        policy.key_pref_storage = IntermediateStorageLevel::HBM;

        policy.modup_output_storage = IntermediateStorageLevel::HBM;
        policy.innerprod_output_storage = IntermediateStorageLevel::HBM;
        policy.reduction_output_storage = IntermediateStorageLevel::HBM;
        policy.moddown_temp_storage = IntermediateStorageLevel::SRAM;

        policy.modup_to_innerprod = StageConnectionMode::SpillToHBM;
        policy.innerprod_to_reduction = StageConnectionMode::SpillToHBM;
        policy.reduction_to_moddown = StageConnectionMode::SpillToHBM;
        policy.moddown_to_subtract = StageConnectionMode::BufferInSRAM;
        
        policy.reduction_mode = ReductionMode::Centralized;

        policy.requires_large_rf = false;
        policy.supports_moddown_shortcut = false;
        policy.supports_fused_final_subtract = false;
        policy.supports_partial_reduction_overlap = false;

        policy.prefer_digit_locality = false;
        policy.prefer_limb_streaming = false;

        return policy;

    case KeySwitchMethod::OLA:
        policy.granularity = KeySwitchProcessingGranularity::Limb;

        policy.fuse_modup_chain = true;
        policy.fuse_moddown_chain = false;
        policy.fuse_cross_stage = false;

        policy.input_pref_storage = IntermediateStorageLevel::SRAM;
        policy.key_pref_storage = IntermediateStorageLevel::SRAM;

        policy.modup_output_storage = IntermediateStorageLevel::SRAM;
        policy.innerprod_output_storage = IntermediateStorageLevel::HBM;
        policy.reduction_output_storage = IntermediateStorageLevel::SRAM;
        policy.moddown_temp_storage = IntermediateStorageLevel::SRAM;

        policy.modup_to_innerprod = StageConnectionMode::BufferInSRAM;
        policy.innerprod_to_reduction = StageConnectionMode::SpillToHBM;
        policy.reduction_to_moddown = StageConnectionMode::BufferInSRAM;
        policy.moddown_to_subtract = StageConnectionMode::DirectForward;

        policy.reduction_mode = ReductionMode::Streaming;

        policy.requires_large_rf = false;
        policy.supports_moddown_shortcut = false;
        policy.supports_fused_final_subtract = false;
        policy.supports_partial_reduction_overlap = true;


        policy.prefer_digit_locality = false;
        policy.prefer_limb_streaming = true;
        return policy;

    case KeySwitchMethod::FAB:
        policy.granularity = KeySwitchProcessingGranularity::Digit;

        policy.fuse_modup_chain = true;
        policy.fuse_moddown_chain = false;
        policy.fuse_cross_stage = true;

        policy.input_pref_storage = IntermediateStorageLevel::SRAM;
        policy.key_pref_storage = IntermediateStorageLevel::RF;

        policy.modup_output_storage = IntermediateStorageLevel::RF;
        policy.innerprod_output_storage = IntermediateStorageLevel::RF;
        policy.reduction_output_storage = IntermediateStorageLevel::SRAM;
        policy.moddown_temp_storage = IntermediateStorageLevel::SRAM;

        policy.modup_to_innerprod = StageConnectionMode::BufferInRF;
        policy.innerprod_to_reduction = StageConnectionMode::BufferInRF;
        policy.reduction_to_moddown = StageConnectionMode::BufferInSRAM;
        policy.moddown_to_subtract = StageConnectionMode::BufferInSRAM;

        policy.reduction_mode = ReductionMode::Streaming;

        policy.requires_large_rf = true;
        policy.supports_moddown_shortcut = false;
        policy.supports_fused_final_subtract = false;
        policy.supports_partial_reduction_overlap = true;

        policy.prefer_digit_locality = true;
        policy.prefer_limb_streaming = false;
        return policy;

    case KeySwitchMethod::FAST:
        policy.granularity = KeySwitchProcessingGranularity::Limb;

        policy.fuse_modup_chain = true;
        policy.fuse_moddown_chain = false;
        policy.fuse_cross_stage = false;

        policy.input_pref_storage = IntermediateStorageLevel::SRAM;
        policy.key_pref_storage = IntermediateStorageLevel::SRAM;

        policy.modup_output_storage = IntermediateStorageLevel::SRAM;
        policy.innerprod_output_storage = IntermediateStorageLevel::HBM;
        policy.reduction_output_storage = IntermediateStorageLevel::HBM;
        policy.moddown_temp_storage = IntermediateStorageLevel::SRAM;

        policy.modup_to_innerprod = StageConnectionMode::BufferInSRAM;
        policy.innerprod_to_reduction = StageConnectionMode::SpillToHBM;
        policy.reduction_to_moddown = StageConnectionMode::SpillToHBM;
        policy.moddown_to_subtract = StageConnectionMode::BufferInSRAM;

        policy.reduction_mode = ReductionMode::Centralized;

        policy.requires_large_rf = false;
        policy.supports_moddown_shortcut = false;
        policy.supports_fused_final_subtract = false;
        policy.supports_partial_reduction_overlap = false;

        policy.prefer_digit_locality = false;
        policy.prefer_limb_streaming = true;
        return policy;

    case KeySwitchMethod::HERA:
        policy.granularity = KeySwitchProcessingGranularity::Limb;

        policy.fuse_modup_chain = true;
        policy.fuse_moddown_chain = true;
        policy.fuse_cross_stage = true;

        policy.input_pref_storage = IntermediateStorageLevel::SRAM;
        policy.key_pref_storage = IntermediateStorageLevel::SRAM;

        policy.modup_output_storage = IntermediateStorageLevel::RF;
        policy.innerprod_output_storage = IntermediateStorageLevel::SRAM;
        policy.reduction_output_storage = IntermediateStorageLevel::SRAM; 
        policy.moddown_temp_storage = IntermediateStorageLevel::RF;

        policy.modup_to_innerprod = StageConnectionMode::DirectForward;
        policy.innerprod_to_reduction = StageConnectionMode::BufferInSRAM;
        policy.reduction_to_moddown = StageConnectionMode::BufferInSRAM;
        policy.moddown_to_subtract = StageConnectionMode::DirectForward;
        
        policy.reduction_mode = ReductionMode::Streaming;

        policy.requires_large_rf = false;
        policy.supports_moddown_shortcut = true;
        policy.supports_fused_final_subtract = false;
        policy.supports_partial_reduction_overlap = true;

        policy.prefer_digit_locality = false;
        policy.prefer_limb_streaming = true;
        return policy;

    case KeySwitchMethod::Cinnamon:
        policy.granularity = KeySwitchProcessingGranularity::Limb;

        policy.granularity = KeySwitchProcessingGranularity::Limb;

        policy.fuse_modup_chain = true;
        policy.fuse_moddown_chain = false;
        policy.fuse_cross_stage = false;

        policy.input_pref_storage = IntermediateStorageLevel::SRAM;
        policy.key_pref_storage = IntermediateStorageLevel::SRAM;

        policy.modup_output_storage = IntermediateStorageLevel::SRAM;
        policy.innerprod_output_storage = IntermediateStorageLevel::HBM;
        policy.reduction_output_storage = IntermediateStorageLevel::HBM;
        policy.moddown_temp_storage = IntermediateStorageLevel::SRAM;

        policy.modup_to_innerprod = StageConnectionMode::BufferInSRAM;
        policy.innerprod_to_reduction = StageConnectionMode::SpillToHBM;
        policy.reduction_to_moddown = StageConnectionMode::SpillToHBM;
        policy.moddown_to_subtract = StageConnectionMode::BufferInSRAM;

        policy.reduction_mode = ReductionMode::Centralized;

        policy.requires_large_rf = false;
        policy.supports_moddown_shortcut = false;
        policy.supports_fused_final_subtract = false;
        policy.supports_partial_reduction_overlap = false;

        policy.prefer_digit_locality = false;
        policy.prefer_limb_streaming = true;
        return policy;
    case KeySwitchMethod::Auto:
    default:
        return ResolveMethodPolicy(KeySwitchMethod::Poseidon);
    }
}

