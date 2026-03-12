#pragma once

#include "backend/execution_backend.h"
#include "backend/primitive_simulator.h"
#include "model/stage.h"

#include <cstdint>
#include <iosfwd>
#include <vector>

struct CycleBackendStats {
    uint64_t estimate_calls = 0;
    uint64_t primitive_sim_calls = 0;
    uint64_t primitive_ops_total = 0;
    uint64_t fallback_count = 0;
};

class CycleBackend : public ExecutionBackend {
public:
    ExecutionResult Estimate(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const override;

    CycleBackendStats GetStats() const;
    void PrintStats(std::ostream& os) const;

private:
    std::vector<Stage> BuildStages(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

    PrimitiveTrace BuildPrimitiveTrace(
        const std::vector<Stage>& stages,
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

    bool ResidentKeyHit(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

private:
    PrimitiveSimulatorStub primitive_simulator_;
    mutable CycleBackendStats stats_;
};
