#include "common/stats.h"

#include <algorithm>
#include <cmath>

namespace stats {

double Mean(const std::vector<Time>& values) {
    if (values.empty()) {
        return 0.0;
    }

    long double sum = 0.0;
    for (const Time value : values) {
        sum += static_cast<long double>(value);
    }
    return static_cast<double>(sum / values.size());
}

Time Percentile(const std::vector<Time>& values, double percentile) {
    if (values.empty()) {
        return 0;
    }

    std::vector<Time> sorted = values;
    std::sort(sorted.begin(), sorted.end());

    if (percentile <= 0.0) {
        return sorted.front();
    }
    if (percentile >= 1.0) {
        return sorted.back();
    }

    const double rank = percentile * static_cast<double>(sorted.size());
    size_t idx = static_cast<size_t>(std::ceil(rank));
    if (idx == 0) {
        idx = 1;
    }
    return sorted.at(idx - 1);
}

Time Max(const std::vector<Time>& values) {
    if (values.empty()) {
        return 0;
    }
    return *std::max_element(values.begin(), values.end());
}

} // namespace stats
