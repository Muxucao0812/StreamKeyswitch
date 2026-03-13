#pragma once

#include <cstdint>

enum class KeySwitchFallbackReason : uint8_t {
    None,
    NoAssignedCard,
    TilePlanInvalid,
    BrambudgetOverflow,
    UnsupportedMethod,
    UnsupportedConfig,
    DegradedToSingleBoard,
    LegacyStageFallback
};

inline const char* ToString(KeySwitchFallbackReason reason) {
    switch (reason) {
    case KeySwitchFallbackReason::None:
        return "none";
    case KeySwitchFallbackReason::NoAssignedCard:
        return "no_assigned_card";
    case KeySwitchFallbackReason::TilePlanInvalid:
        return "tile_plan_invalid";
    case KeySwitchFallbackReason::BrambudgetOverflow:
        return "bram_budget_overflow";
    case KeySwitchFallbackReason::UnsupportedMethod:
        return "unsupported_method";
    case KeySwitchFallbackReason::UnsupportedConfig:
        return "unsupported_config";
    case KeySwitchFallbackReason::DegradedToSingleBoard:
        return "degraded_to_single_board";
    case KeySwitchFallbackReason::LegacyStageFallback:
        return "legacy_stage_fallback";
    }

    return "unknown";
}

inline bool IsExplicitFallback(KeySwitchFallbackReason reason) {
    return reason != KeySwitchFallbackReason::None
        && reason != KeySwitchFallbackReason::DegradedToSingleBoard;
}
