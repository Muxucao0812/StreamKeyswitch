#pragma once

#include "backend/hw/hardware_model.h"
#include "backend/cycle_sim/instruction.h"

CycleProgram BuildCinnamonInputBroadcastProgram(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware);
