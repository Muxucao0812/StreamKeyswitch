#include "backend/analytical_backend.h"
#include <algorithm>

ExecutionResult AnalyticalBackend::Estimate(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    ExecutionResult result{};
    const auto& p = req.ks_profile;
    const size_t k = std::max<size_t>(1, plan.assigned_cards.size());

    Time key_load = 0;
    for (auto card_id : plan.assigned_cards) {
        const auto& card = state.cards.at(card_id);
        if (!card.resident_user.has_value() || card.resident_user.value() != req.user_id) {
            key_load = std::max<Time>(key_load, p.key_bytes / 1000 + 500); // demo model
        }
    }

    Time base_compute =
        1000
        + 200 * p.num_ciphertexts
        + 100 * p.num_polys
        + 150 * p.num_digits
        + 180 * p.num_rns_limbs;

    Time compute = static_cast<Time>(base_compute / k + 50 * (k - 1));
    Time merge = (k > 1) ? (200 + 80 * k) : 0;
    Time dispatch = p.input_bytes / 1000;

    result.breakdown.key_load_time = key_load;
    result.breakdown.dispatch_time = dispatch;
    result.breakdown.compute_time = compute;
    result.breakdown.merge_time = merge;
    result.total_latency = key_load + dispatch + compute + merge;
    result.peak_memory_bytes = p.input_bytes + p.key_bytes;
    result.energy_nj = 0.5 * result.total_latency;

    return result;
}