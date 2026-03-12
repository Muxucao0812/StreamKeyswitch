#pragma once
#include "model/request.h"
#include "model/system_state.h"
#include "model/execution_result.h"

class ExecutionBackend {
public:
    virtual ~ExecutionBackend() = default;

    virtual ExecutionResult Estimate(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const = 0;
};