#pragma once

#include "scheduler/scheduler.h"
#include <deque>

class PoolScheduler : public Scheduler {
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
    uint32_t DecideCardCount(const Request& req) const;

    std::optional<uint32_t> ChoosePool(
        const Request& req,
        const SystemState& state) const;

private:
    std::deque<Request> queue_;
};
