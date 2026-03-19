#pragma once

#include "backend/cycle_sim/instruction.h"
#include "backend/hw/hardware_model.h"

CycleProgram BuildOutputCentricProgram(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware
);
