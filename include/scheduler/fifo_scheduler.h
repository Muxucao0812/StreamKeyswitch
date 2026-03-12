#pragma once
#include "scheduler/scheduler.h"
#include <queue>

class FIFOScheduler : public Scheduler {
public:
    void OnRequestArrival(const Request& req) override;
    std::optional<ExecutionPlan> TrySchedule(
        const SystemState& state,
        const ExecutionBackend& backend) override;
    void OnTaskFinished(
        const Request& req,
        const ExecutionPlan& plan,
        const ExecutionResult& result) override;
    bool Empty() const override;

private:
    std::queue<Request> queue_;
};