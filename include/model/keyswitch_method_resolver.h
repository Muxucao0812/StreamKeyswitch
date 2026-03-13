#pragma once

#include "model/request.h"

#include <cstddef>

inline KeySwitchMethod ResolveKeySwitchMethodForAssignedCards(
    KeySwitchMethod requested_method,
    size_t assigned_cards) {

    if (requested_method == KeySwitchMethod::Auto) {
        return (assigned_cards > 1)
            ? KeySwitchMethod::ScaleOutLimb
            : KeySwitchMethod::SingleBoardClassic;
    }
    return requested_method;
}

