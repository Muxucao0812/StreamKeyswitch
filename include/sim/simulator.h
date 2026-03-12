#pragma once

#include "backend/execution_backend.h"
#include "scheduler/scheduler.h"
#include "sim/event.h"
#include "sim/metrics.h"
#include "model/system_state.h"
#include <cstdint>
#include <memory>
#include <ostream>
#include <queue>
#include <unordered_map>
#include <vector>

class Simulator {
public:
    Simulator(
        SystemState initial_state,
        std::unique_ptr<Scheduler> scheduler,
        std::unique_ptr<ExecutionBackend> backend);

    void LoadWorkload(const std::vector<Request>& requests);
    void Run();

    SimulationMetrics CollectMetrics() const;
    void Report(std::ostream& os) const;
    void Report() const;

private:
    void HandleArrival(const Event& ev);
    void HandleTaskFinish(const Event& ev);
    void TryDispatch();

private:
    SystemState state_;
    std::unique_ptr<Scheduler> scheduler_;
    std::unique_ptr<ExecutionBackend> backend_;

    std::priority_queue<Event, std::vector<Event>, EventCompare> events_;
    std::unordered_map<RequestId, Request> request_table_;

    EventId next_event_id_ = 1;
    uint64_t reload_count_ = 0;

    struct CompletedRecord {
        Request request;
        ExecutionPlan plan;
        ExecutionResult result;
        Time start_time = 0;
        Time finish_time = 0;
        Time queue_time = 0;
        Time service_time = 0;
        uint32_t reload_count = 0;
    };
    std::vector<CompletedRecord> completed_;
};
