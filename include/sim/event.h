#pragma once

#include "common/types.h"
#include "model/execution_result.h"
#include "model/request.h"

struct Event {
    EventId event_id = 0;
    EventType type = EventType::RequestArrival;
    Time timestamp = 0;
    Time dispatch_start_time = 0;
    uint32_t reload_count = 0;

    Request request;
    ExecutionPlan plan;
    ExecutionResult result;
};

inline uint32_t EventTypePriority(EventType type) {
    switch (type) {
    case EventType::TaskFinish:
        return 0;
    case EventType::RequestArrival:
        return 1;
    }
    return 100;
}

struct EventCompare {
    bool operator()(const Event& a, const Event& b) const {
        if (a.timestamp != b.timestamp) {
            return a.timestamp > b.timestamp;
        }

        const uint32_t a_priority = EventTypePriority(a.type);
        const uint32_t b_priority = EventTypePriority(b.type);
        if (a_priority != b_priority) {
            return a_priority > b_priority;
        }

        return a.event_id > b.event_id;
    }
};
