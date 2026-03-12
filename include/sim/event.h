#pragma once
#include "common/types.h"
#include "model/request.h"
#include "model/execution_result.h"
#include <vector>

struct Event {
    EventId event_id = 0;
    EventType type = EventType::RequestArrival;
    Time timestamp = 0;

    Request request;
    ExecutionPlan plan;
    ExecutionResult result;
};

struct EventCompare {
    bool operator()(const Event& a, const Event& b) const {
        return a.timestamp > b.timestamp;
    }
};