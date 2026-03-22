#pragma once

#include "backend/hw/hardware_model.h"
#include "backend/cycle_sim/instruction.h"

CycleProgram BuildCinnamonOutputAggregationProgram(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware);
