#pragma once
#include "common/types.h"
#include <optional>

struct KeySwitchProfile {
    uint32_t num_ciphertexts = 1;
    uint32_t num_polys = 2;
    uint32_t num_digits = 1;
    uint32_t num_rns_limbs = 1;
    uint64_t input_bytes = 0;
    uint64_t key_bytes = 0;
    bool multi_card_allowed = false;

    uint32_t preferred_cards = 1;
    uint32_t max_cards = 1
    uint64_t output_bytes = 0
};

struct Request {
    RequestId request_id = 0;
    UserId user_id = 0;
    TaskType task_type = TaskType::KeySwitch;

    Time arrival_time = 0;
    std::optional<Time> deadline;

    KeySwitchProfile ks_profile;
    UserProfile user_profile;
    
    uint32_t priority = 0;
    bool latency_sensitive = false
    uint32_t sla_class = 0;
};


struct UserProfile {
    UserId user_id = 0;
    uint64_t key_bytes = 0;
    Time key_load_time = 0;
    bool latency_sensitive = false;
    uint32_t weight = 1;
}