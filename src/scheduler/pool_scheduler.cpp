#include "scheduler/pool_scheduler.h"

#include "backend/execution_backend.h"

#include <algorithm>
#include <cstddef>
#include <vector>

void PoolScheduler::OnRequestArrival(const Request& req) {
    queue_.push_back(req);
}

uint32_t PoolScheduler::DecideCardCount(const Request& req) const {
    uint32_t k = 1;
    if (req.ks_profile.input_bytes <= 4096) {
        k = 1;
    } else if (req.ks_profile.input_bytes <= 8192) {
        k = 2;
    } else {
        k = 4;
    }

    const uint32_t max_cards =
        (req.ks_profile.max_cards == 0) ? k : req.ks_profile.max_cards;
    return std::max<uint32_t>(1, std::min(k, max_cards));
}

std::optional<uint32_t> PoolScheduler::ChoosePool(
    const Request& req,
    const SystemState& state) const {

    if (state.pools.empty()) {
        return std::nullopt;
    }

    std::vector<uint32_t> matched_pools;
    matched_pools.reserve(state.pools.size());

    const bool want_latency_pool = req.latency_sensitive;
    for (const auto& pool : state.pools) {
        if (pool.latency_sensitive_pool == want_latency_pool) {
            matched_pools.push_back(pool.pool_id);
        }
    }

    if (matched_pools.empty()) {
        for (const auto& pool : state.pools) {
            matched_pools.push_back(pool.pool_id);
        }
    }

    const size_t idx = static_cast<size_t>(req.user_id) % matched_pools.size();
    return matched_pools[idx];
}

std::optional<ExecutionPlan> PoolScheduler::TrySchedule(
    const SystemState& state,
    const ExecutionBackend& /*backend*/) {

    if (queue_.empty()) {
        return std::nullopt;
    }

    for (size_t req_idx = 0; req_idx < queue_.size(); ++req_idx) {
        const Request& req = queue_[req_idx];
        const auto pool_id_opt = ChoosePool(req, state);
        if (!pool_id_opt.has_value()) {
            return std::nullopt;
        }
        const uint32_t target_pool_id = pool_id_opt.value();
        const uint32_t required_cards = DecideCardCount(req);

        std::vector<CardId> preferred;
        std::vector<CardId> fallback;
        preferred.reserve(state.cards.size());
        fallback.reserve(state.cards.size());

        for (const auto& card : state.cards) {
            if (card.pool_id != target_pool_id) {
                continue;
            }
            if (card.busy || state.now < card.available_time) {
                continue;
            }

            if (card.resident_user.has_value() && card.resident_user.value() == req.user_id) {
                preferred.push_back(card.card_id);
            } else {
                fallback.push_back(card.card_id);
            }
        }

        const size_t total_candidates = preferred.size() + fallback.size();
        if (total_candidates < required_cards) {
            continue;
        }

        ExecutionPlan plan;
        plan.request_id = req.request_id;

        for (const CardId card_id : preferred) {
            if (plan.assigned_cards.size() == required_cards) {
                break;
            }
            plan.assigned_cards.push_back(card_id);
        }
        for (const CardId card_id : fallback) {
            if (plan.assigned_cards.size() == required_cards) {
                break;
            }
            plan.assigned_cards.push_back(card_id);
        }

        queue_.erase(
            queue_.begin()
            + static_cast<std::deque<Request>::difference_type>(req_idx));
        return plan;
    }

    return std::nullopt;
}

void PoolScheduler::OnTaskFinished(
    const Request&,
    const ExecutionPlan&,
    const ExecutionResult&) {}

bool PoolScheduler::Empty() const {
    return queue_.empty();
}
