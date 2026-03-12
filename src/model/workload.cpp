#include "model/workload.h"

std::vector<Request> WorkloadBuilder::GenerateSimple(
    uint32_t num_users,
    uint32_t requests_per_user,
    Time inter_arrival) const {

    std::vector<Request> out;
    RequestId rid = 0;
    Time t = 0;

    for (uint32_t u = 0; u < num_users; ++u) {
        for (uint32_t i = 0; i < requests_per_user; ++i) {
            Request req;
            req.request_id = rid++;
            req.user_id = u;
            req.arrival_time = t;
            req.ks_profile.num_ciphertexts = 1 + (i % 4);
            req.ks_profile.num_polys = 2;
            req.ks_profile.num_digits = 2 + (i % 3);
            req.ks_profile.num_rns_limbs = 4;
            req.ks_profile.input_bytes = 4096 * (1 + i % 4);
            req.ks_profile.key_bytes = 8192;
            req.ks_profile.multi_card_allowed = true;
            out.push_back(req);

            t += inter_arrival;
        }
    }
    return out;
}