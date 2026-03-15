#pragma once

#include "backend/cycle_sim/arch.h"
#include "backend/cycle_sim/stats.h"
#include "backend/hw/hardware_model.h"

class CycleDriver {
public:
    explicit CycleDriver(const HardwareModel& hardware);

    CycleSimStats Run(const CycleProgram& program) const;

private:
    const HardwareModel& hardware_;
};
