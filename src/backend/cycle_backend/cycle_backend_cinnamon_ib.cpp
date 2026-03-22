#include "backend/cycle_backend/cycle_backend_cinnamon_ib.h"

#include "backend/cycle_backend/cycle_backend_primitives.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

CycleProgram BuildCinnamonInputBroadcastProgram(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware
) {

    CycleProgramBuilder builder(
        problem,
        hardware,
        problem.method,
        "cinnamon_input_broadcast_keyswitch"
    );

    const uint32_t active_cards = std::max<uint32_t>(1, problem.active_cards);
    const uint32_t ct

    builder.program.estimated_peak_live_bytes = builder.bram.Peak();
    return std::move(builder.program);
}
