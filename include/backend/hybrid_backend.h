#pragma once

#include "backend/execution_backend.h"
#include "backend/primitive_simulator.h"
#include "model/stage.h"

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

struct HybridBackendStats {
    uint64_t estimate_calls = 0;
    uint64_t coarse_estimate_calls = 0;

    uint64_t primitive_sim_calls = 0;
    uint64_t primitive_ops_total = 0;

    uint64_t primitive_stage_attempts = 0;
    uint64_t primitive_stage_covered = 0;
    uint64_t primitive_stage_fallback = 0;

    uint64_t multiply_primitive_covered = 0;
    uint64_t merge_primitive_covered = 0;
};

class HybridBackend : public ExecutionBackend {
public:
    explicit HybridBackend(
        bool use_table_coarse = false,
        std::string profile_table_path = "");

    ExecutionResult Estimate(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const override;

    HybridBackendStats GetStats() const;
    void PrintStats(std::ostream& os) const;

    const std::string& CoarseSource() const;
    bool UseTableCoarse() const;

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

    bool UsePrimitiveForStage(
        StageType stage_type,
        const ExecutionPlan& plan) const;

private:
    bool use_table_coarse_ = false;
    std::string coarse_source_;

    std::unique_ptr<ExecutionBackend> coarse_backend_;
    PrimitiveSimulatorStub primitive_simulator_;

    mutable HybridBackendStats stats_;
};
