#pragma once

#include "scheduler/scheduler.h"
#include <deque>

struct ScoreWeights {
    double waiting_time = 1.0;
    double switch_cost = 1.0;
    double queue_pressure = 1.0;
    double priority = 1.0;
};

class ScoreScheduler : public Scheduler {
public:
    explicit ScoreScheduler(ScoreWeights weights = {});

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
    double ComputeScore(
        const Request& req,
        const CardState& card,
        Time now,
        size_t queue_index,
        size_t queue_size) const;

private:
    ScoreWeights weights_;
    std::deque<Request> queue_;
};
