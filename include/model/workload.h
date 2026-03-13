#pragma once

#include "model/he_params.h"
#include "model/request.h"
#include "model/user_profile.h"
#include <random>
#include <vector>

class WorkloadBuilder {
public:
    explicit WorkloadBuilder(uint64_t seed = 20260312ULL);
    WorkloadBuilder(uint64_t seed, const HEParams& he_params);

    void ResetSeed(uint64_t seed);
    void SetHEParams(const HEParams& he_params);
    void SetDefaultKeySwitchMethod(KeySwitchMethod method);

    std::vector<Request> GenerateSynthetic(
        uint32_t num_users,
        uint32_t requests_per_user,
        Time inter_arrival,
        Time start_time = 0);

    std::vector<Request> GenerateBurst(
        uint32_t num_users,
        uint32_t bursts,
        uint32_t requests_per_user_per_burst,
        Time intra_burst_gap,
        Time inter_burst_gap,
        Time start_time = 0);

    // Keep for backward compatibility with previous step.
    std::vector<Request> GenerateSimple(
        uint32_t num_users,
        uint32_t requests_per_user,
        Time inter_arrival);

private:
    std::vector<UserProfile> BuildUserProfiles(uint32_t num_users);
    Request BuildRequest(
        RequestId request_id,
        const UserProfile& user_profile,
        Time arrival_time,
        uint32_t sequence_idx);

    Time NextJitter(Time max_jitter);
    uint32_t NextRange(uint32_t min_value, uint32_t max_value);
    bool NextBernoulli(double p);

private:
    HEParams he_params_;
    KeySwitchMethod keyswitch_method_ = KeySwitchMethod::Auto;
    std::mt19937_64 rng_;
};
