#pragma once

#include "common/types.h"

struct LinkState {
    uint32_t link_id = 0;
    Time available_time = 0;
    double bandwidth_bytes_per_ns = 0.0;

    CardId src_card = 0;
    CardId dst_card = 0;
};
