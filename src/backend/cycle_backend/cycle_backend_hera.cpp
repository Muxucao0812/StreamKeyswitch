#include "backend/cycle_backend/cycle_backend_hera.h"

CycleProgram BuildHERAProgram(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware
) {
    (void)problem;
    (void)hardware;
    return CycleProgram{};
}
