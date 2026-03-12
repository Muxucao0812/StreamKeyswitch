#pragma once

#include "common/types.h"
#include <vector>

namespace stats {

double Mean(const std::vector<Time>& values);
Time Percentile(const std::vector<Time>& values, double percentile);
Time Max(const std::vector<Time>& values);

} // namespace stats
