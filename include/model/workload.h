#pragma once
#include "model/request.h"
#include <vector>

class WorkloadBuilder {
public:
    std::vector<Request> GenerateSimple(
        uint32_t num_users,
        uint32_t requests_per_user,
        Time inter_arrival) const;
};