#pragma once

#include "common/types.h"

enum class StageType {
    KeyLoad,
    Dispatch,
    Decompose,
    Multiply,
    BasisConvert,
    Merge
};

struct Stage {
    StageType type = StageType::Dispatch;
    uint64_t bytes = 0;
    uint32_t work_units = 0;
};
