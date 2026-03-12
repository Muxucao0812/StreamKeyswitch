#pragma once
#include "model/request.h"
#include "model/system_state.h"
#include "model/execution_result.h"
#include <optional>
#include <vector>

class ExecutionBackend;

class Scheduler {
public:
    virtual ~Scheduler() = default;

    virtual void OnRequestArrival(const Request& req) = 0;

    virtual std::optional<ExecutionPlan> TrySchedule(
        const SystemState& state,
        const ExecutionBackend& backend) = 0;

    virtual void OnTaskFinished(
        const Request& req,
        const ExecutionPlan& plan,
        const ExecutionResult& result) = 0;

    virtual bool Empty() const = 0;
};