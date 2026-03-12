#pragma once
#include "backend/execution_backend.h"


enum class StageType {
    KeyLoad,
    Dispatch,
    Decompose,
    Multiply,
    BasisConvert,
    Merge,

    // 
    NTT,
    INTT,
    Add
}

struct Stage {
    StageType type;
    uint64_t bytes = 0;
    uint32_t work_uints = 0;
}

class AnalyticalBackend : public ExecutionBackend {
    public:
        ExecutionResult Estimate(
            const Request& req,
            const ExecutionPlan& plan,
            const SystemState& state) const override;

    private:
        std::vector<Stage> BuildStages(
            const Request& req,
            const ExecutionPlan& plan,
            const SystemState& state) const;
    };