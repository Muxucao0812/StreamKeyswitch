#pragma once

#include "scheduler/scheduler.h"
#include <deque>

class AffinityScheduler : public Scheduler {
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
    std::deque<Request> queue_;
};
