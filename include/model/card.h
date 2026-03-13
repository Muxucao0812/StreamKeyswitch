#pragma once

#include "common/types.h"
#include <optional>

struct CardState {
    CardId card_id = 0;
    uint32_t pool_id = 0;

    Time available_time = 0;
    bool busy = false;

    std::optional<UserId> resident_user;

    uint64_t memory_capacity_bytes = 0;
    uint64_t memory_used_bytes = 0;
    uint64_t bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;

    // Runtime counters for metrics.
    Time last_start_time = 0;
    Time total_busy_time = 0;
    uint64_t reload_count = 0;
    uint64_t served_requests = 0;
};
