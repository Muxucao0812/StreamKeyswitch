#pragma once

#include "backend/execution_backend.h"
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
        const SystemState& state) const override;

    CycleBackendStats GetStats() const;
    void PrintStats(std::ostream& os) const;

private:
    KeySwitchMethod ResolveKeySwitchMethod(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

    ExecutionResult EstimatePoseidon(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

    ExecutionResult EstimateFAB(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

    ExecutionResult EstimateFAST(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

    ExecutionResult EstimateOLA(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

    ExecutionResult EstimateHERA(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

    ExecutionResult EstimateCinnamon(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

private:
    PrimitiveSimulatorStub primitive_simulator_;
    mutable CycleBackendStats stats_;
};
