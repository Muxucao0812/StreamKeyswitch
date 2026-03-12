#pragma once

#include "backend/execution_backend.h"
#include "model/stage.h"

#include <vector>

class AnalyticalBackend : public ExecutionBackend {
public:
    std::vector<StageType> StageSequenceForTest(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

    ExecutionResult Estimate(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const override;

private:
    std::vector<Stage> BuildStages(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

    Time EstimateStage(
        const Stage& stage,
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;
};
