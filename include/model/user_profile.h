#pragma once

#include "common/types.h"

struct UserProfile {
    UserId user_id = 0;
    uint64_t key_bytes = 0;
    uint64_t ct_bytes = 0;
    Time key_load_time = 0;
    bool latency_sensitive = false;
    uint32_t weight = 1;
};
