#pragma once

#include <cstdint>

using Time = uint64_t;
using RequestId = uint64_t;
using UserId = uint32_t;
using CardId = uint32_t;
using EventId = uint64_t;

enum class EventType {
    RequestArrival,
    TaskFinish
};

enum class TaskType {
    KeySwitch
};

enum class BackendType {
    Analytical,
    Table,
    Cycle
};

enum class SchedulerType {
    FIFO,
    Affinity,
    StaticPartition,
    Score,
    Pool,
    Hierarchical
};
