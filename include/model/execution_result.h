#pragma once

#include "common/types.h"
#include <vector>

struct ExecutionPlan {
    RequestId request_id = 0;
    std::vector<CardId> assigned_cards;
};

struct ExecutionBreakdown {
    Time queue_time = 0;
    Time key_load_time = 0;
    Time dispatch_time = 0;
    Time decompose_time = 0;
    Time multiply_time = 0;
    Time basis_convert_time = 0;
    Time merge_time = 0;
};

struct ExecutionResult {
    Time total_latency = 0;
    Time peak_memory_bytes = 0;
    double energy_nj = 0.0;
    ExecutionBreakdown breakdown;
};
