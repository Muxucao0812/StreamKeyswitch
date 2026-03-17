#pragma once

#include "backend/execution_backend.h"
#include "backend/hw/hardware_model.h"
#include "backend/model/keyswitch_execution_model.h"
#include "backend/primitive_simulator.h"

#include <cstdint>
#include <iosfwd>
#include <unordered_set>

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
    void SetDebugDumpOptions(bool dump_logical_graph, bool dump_runtime_plan);

    
    ExecutionResult EstimateMethod(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state,
        KeySwitchMethod method
    ) const;

private:
    bool ShouldDumpLogicalGraph(KeySwitchMethod method) const;
    bool ShouldDumpRuntimePlan(KeySwitchMethod method) const;

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
    bool dump_logical_graph_ = false;
    bool dump_runtime_plan_ = false;
    mutable std::unordered_set<uint8_t> dumped_logical_methods_;
    mutable std::unordered_set<uint8_t> dumped_runtime_methods_;
};
