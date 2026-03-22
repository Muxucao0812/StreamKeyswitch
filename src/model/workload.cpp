#include "model/workload.h"
#include "model/request_sizing.h"

#include <algorithm>
#include <cstddef>
#include <random>

WorkloadBuilder::WorkloadBuilder(uint64_t seed)
    : he_params_(HEParams::BuiltInDefault()),
      rng_(seed) {}

WorkloadBuilder::WorkloadBuilder(uint64_t seed, const HEParams& he_params)
    : he_params_(he_params),
      rng_(seed) {}

void WorkloadBuilder::ResetSeed(uint64_t seed) {
    rng_.seed(seed);
}

void WorkloadBuilder::SetHEParams(const HEParams& he_params) {
    he_params_ = he_params;
}

void WorkloadBuilder::SetDefaultKeySwitchMethod(KeySwitchMethod method) {
    keyswitch_method_ = method;
}

Time WorkloadBuilder::NextJitter(Time max_jitter) {
    if (max_jitter == 0) {
        return 0;
    }
    std::uniform_int_distribution<Time> dist(0, max_jitter);
    return dist(rng_);
}

uint32_t WorkloadBuilder::NextRange(uint32_t min_value, uint32_t max_value) {
    std::uniform_int_distribution<uint32_t> dist(min_value, max_value);
    return dist(rng_);
}

bool WorkloadBuilder::NextBernoulli(double p) {
    std::bernoulli_distribution dist(p);
    return dist(rng_);
}

std::vector<UserProfile> WorkloadBuilder::BuildUserProfiles(uint32_t num_users) {
    std::vector<UserProfile> profiles;
    profiles.reserve(static_cast<size_t>(num_users));

    const uint64_t shared_key_bytes =   he_params_.ComputeKeyBytes();
    const uint64_t shared_ct_bytes  =   he_params_.ComputeCiphertextBytes(
                                            /*ciphertext_count=*/1,
                                            he_params_.num_polys,
                                            he_params_.num_rns_limbs
                                        );
    const Time shared_key_load_time =   he_params_.ComputeKeyLoadTime();

    for (uint32_t user = 0; user < num_users; ++user) {
        UserProfile profile;
        profile.user_id = user;
        profile.key_bytes = shared_key_bytes;
        profile.ct_bytes = shared_ct_bytes;
        // SSD -> PCIE -> HBM： Load Time
        profile.key_load_time = shared_key_load_time;
        profile.latency_sensitive = (user % 3 == 0);
        profile.weight = 1 + (user % 4);
        profiles.push_back(profile);
    }

    return profiles;
}

Request WorkloadBuilder::BuildRequest(
    RequestId request_id,
    const UserProfile& user_profile,
    Time arrival_time,
    uint32_t sequence_idx) {
    (void)sequence_idx;

    Request req;
    req.request_id = request_id;
    req.user_id = user_profile.user_id;
    req.arrival_time = arrival_time;

    req.user_profile = user_profile;
    req.latency_sensitive = user_profile.latency_sensitive;
    req.priority = user_profile.latency_sensitive ? 0 : 1;
    req.sla_class = user_profile.latency_sensitive ? 0 : 1;

    // Generate FHE parameters directly from configured HE params
    // (no per-request perturbation).
    req.ks_profile.num_ciphertexts = 1;
    req.ks_profile.num_polys = he_params_.num_polys;
    req.ks_profile.poly_modulus_degree = he_params_.poly_modulus_degree;
    req.ks_profile.bytes_per_coeff = he_params_.bytes_per_coeff;
    req.ks_profile.num_digits = he_params_.num_digits;
    req.ks_profile.num_rns_limbs = he_params_.num_rns_limbs;

    req.ks_profile.input_bytes = he_params_.ComputeCiphertextBytes(
        req.ks_profile.num_ciphertexts,
        req.ks_profile.num_polys,
        req.ks_profile.num_rns_limbs);
    req.ks_profile.output_bytes = he_params_.ComputeCiphertextBytes(
        req.ks_profile.num_ciphertexts,
        req.ks_profile.num_polys,
        req.ks_profile.num_rns_limbs);
    req.ks_profile.key_bytes = he_params_.ComputeKeyBytes(
        req.ks_profile.num_digits,
        req.ks_profile.num_rns_limbs);
    req.ks_profile.method = keyswitch_method_;

    const uint32_t cards_by_working_set = RecommendCardCountForRequest(req);
    req.ks_profile.preferred_cards = cards_by_working_set;

    return req;
}

std::vector<Request> WorkloadBuilder::GenerateSynthetic(
    uint32_t num_users,
    uint32_t requests_per_user,
    Time inter_arrival,
    Time start_time) {

    const auto profiles = BuildUserProfiles(num_users);

    std::vector<Request> out;
    out.reserve(static_cast<size_t>(num_users) * requests_per_user);

    RequestId rid = 0;
    Time t = start_time;

    for (uint32_t round = 0; round < requests_per_user; ++round) {
        for (const auto& profile : profiles) {
            const Time jitter = NextJitter(inter_arrival / 4);
            out.push_back(BuildRequest(rid++, profile, t + jitter, round));
            t += inter_arrival;
        }
    }

    return out;
}

std::vector<Request> WorkloadBuilder::GenerateBurst(
    uint32_t num_users,
    uint32_t bursts,
    uint32_t requests_per_user_per_burst,
    Time intra_burst_gap,
    Time inter_burst_gap,
    Time start_time) {

    auto profiles = BuildUserProfiles(num_users);

    std::vector<Request> out;
    out.reserve(static_cast<size_t>(num_users) * bursts * requests_per_user_per_burst);

    RequestId rid = 0;
    uint32_t seq = 0;

    for (uint32_t burst = 0; burst < bursts; ++burst) {
        const Time burst_start = start_time + burst * inter_burst_gap;

        for (uint32_t k = 0; k < requests_per_user_per_burst; ++k) {
            std::shuffle(profiles.begin(), profiles.end(), rng_);
            const Time base_arrival =
                burst_start + k * intra_burst_gap + NextJitter(intra_burst_gap / 3);
            for (const auto& profile : profiles) {
                out.push_back(BuildRequest(rid++, profile, base_arrival, seq++));
            }
        }
    }

    return out;
}

std::vector<Request> WorkloadBuilder::GenerateSimple(
    uint32_t num_users,
    uint32_t requests_per_user,
    Time inter_arrival) {
    return GenerateSynthetic(num_users, requests_per_user, inter_arrival, 0);
}
