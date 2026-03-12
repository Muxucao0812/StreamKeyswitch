#pragma once
#include <cstdint>
#include <string>

using Time = uint64_t;
using RequestId = uint64_t;
using UserId = uint32_t;
using CardId = uint32_t;
using EventId = uint64_t;

enum class EventType {
    RequestArrival,
    DispatchReady,
    TaskStart,
    TaskFinish,
    KeyLoadFinish,
    MergeFinish
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
    RoundRobin,
    StaticPartition,
    Hierarchical
};