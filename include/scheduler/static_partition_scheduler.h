#pragma once

#include "scheduler/scheduler.h"
#include <cstdint>
#include <deque>

class StaticPartitionScheduler : public Scheduler {
public:
    explicit StaticPartitionScheduler(uint32_t num_pools = 2);

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
    uint32_t PoolForUser(UserId user_id) const;

private:
    uint32_t num_pools_ = 1;
    std::deque<Request> queue_;
};
