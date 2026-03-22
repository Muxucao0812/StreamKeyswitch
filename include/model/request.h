#pragma once

#include "common/types.h"
#include "model/user_profile.h"

enum class KeySwitchMethod : uint8_t {
    Auto,
 
    // Single Board
    Poseidon,
    OLA,
    FAB,
    FAST,
    HERA,
    DigitCentric,
    OutputCentric,
    MaxParallel,
    // Multi Board
    Cinnamon,
    CinnamonIB,
    CinnamonOA,
};

enum class PartitionStrategy : uint8_t {
    Auto,
    None,
    ByLimb,
    ByDigit,
    ByCiphertext
};

enum class KeyPlacement : uint8_t {
    Auto,
    ReplicatedPerCard,
    ShardedByPartition,
    StreamFromHBM
};

enum class CollectiveStrategy : uint8_t {
    Auto,
    None,
    GatherToRoot,
    TreeReduce,
    RingAllReduce
};

enum class MultiBoardMode : uint8_t {
    Auto,
    Sequential,
    InputBroadcast,
    OutputAggregation
};

inline bool IsCinnamonMethod(KeySwitchMethod method) {
    switch (method) {
    case KeySwitchMethod::CinnamonIB:
    case KeySwitchMethod::CinnamonOA:
        return true;
    default:
        return false;
    }
}

inline MultiBoardMode ForcedMultiBoardModeForMethod(KeySwitchMethod method) {
    switch (method) {
    case KeySwitchMethod::CinnamonIB:
        return MultiBoardMode::InputBroadcast;
    case KeySwitchMethod::CinnamonOA:
        return MultiBoardMode::OutputAggregation;
    default:
        return MultiBoardMode::Auto;
    }
}

struct KeySwitchProfile {
    uint32_t num_ciphertexts = 1;
    uint32_t num_polys = 2;
    uint32_t num_digits = 1;
    uint32_t num_rns_limbs = 1;
    uint32_t poly_modulus_degree = 65536;
    uint32_t bytes_per_coeff = 8;

    uint64_t input_bytes = 0;
    uint64_t output_bytes = 0;
    uint64_t key_bytes = 0;

    // Legacy scheduler/resource hints, still used for card assignment bounds.
    // Method-aware execution semantics are controlled by fields below.
    uint32_t preferred_cards = 1;
    uint32_t max_cards = 1;

    // Method-aware keyswitch policy controls.
    KeySwitchMethod method = KeySwitchMethod::Poseidon;
    PartitionStrategy partition = PartitionStrategy::Auto;
    KeyPlacement key_placement = KeyPlacement::Auto;
    CollectiveStrategy collective = CollectiveStrategy::Auto;

    // Multi-card behavior controls.
    MultiBoardMode multi_board_mode = MultiBoardMode::Auto;
    bool enable_inter_card_merge = true;
    // 0 means "auto by assigned cards"; >0 requests explicit scale-out card count.
    uint32_t scale_out_cards = 0;
    // Whether local per-card moddown is allowed in keyswitch execution.
    // Current model requires this to be true for supported methods.
    // Setting false is explicitly rejected as UnsupportedConfig.
    bool allow_local_moddown = true;
    // Whether cross-card collective reduce is allowed on scale-out methods.
    bool allow_cross_card_reduce = true;
};

struct Request {
    RequestId request_id = 0;
    UserId user_id = 0;
    Time arrival_time = 0;

    uint32_t priority = 0;
    bool latency_sensitive = false;
    uint32_t sla_class = 0;

    UserProfile user_profile;
    KeySwitchProfile ks_profile;
};
