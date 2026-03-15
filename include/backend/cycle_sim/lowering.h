#pragma once

#include "backend/cycle_sim/instruction.h"
#include "backend/hw/hardware_model.h"
#include "backend/model/keyswitch_execution_model.h"
#include "model/keyswitch_reason.h"

#include <string>

struct CycleLoweringResult {
    bool valid = false;
    KeySwitchFallbackReason fallback_reason = KeySwitchFallbackReason::None;
    std::string fallback_reason_message;
    CycleProgram program;
};

class SingleBoardCycleLowerer {
public:
    SingleBoardCycleLowerer(
        const HardwareModel& hardware,
        KeySwitchMethod method);

    CycleLoweringResult Lower(const KeySwitchExecution& execution) const;

private:
    const HardwareModel& hardware_;
    KeySwitchMethod method_;
};

class PoseidonCycleLowerer {
public:
    explicit PoseidonCycleLowerer(const HardwareModel& hardware);

    CycleLoweringResult Lower(const KeySwitchExecution& execution) const;

private:
    SingleBoardCycleLowerer impl_;
};

class CycleLowererSelector {
public:
    explicit CycleLowererSelector(const HardwareModel& hardware);

    CycleLoweringResult Lower(const KeySwitchExecution& execution) const;

private:
    const HardwareModel& hardware_;
};
