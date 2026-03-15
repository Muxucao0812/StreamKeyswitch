#pragma once

#include "backend/execution_backend.h"
#include "backend/hw/hardware_model.h"
#include "backend/model/keyswitch_execution_model.h"
#include "backend/primitive_simulator.h"

#include <cstdint>
#include <iosfwd>

struct CycleBackendStats {
    uint64_t estimate_calls = 0;
    uint64_t fallback_count = 0;
};

class CycleBackend : public ExecutionBackend {
public:
    ExecutionResult Estimate(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state
    ) const override;

    CycleBackendStats GetStats() const;
    void PrintStats(std::ostream& os) const;

    
    ExecutionResult EstimateMethod(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state,
        KeySwitchMethod method
    ) const;

private:
    KeySwitchMethod ResolveKeySwitchMethod(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;


private:
    KeySwitchExecutionModel execution_model_;
    HardwareModel hw_model_;
    PrimitiveSimulatorStub primitive_simulator_;
    mutable CycleBackendStats stats_;
};
