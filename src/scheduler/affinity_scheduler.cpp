#include "scheduler/affinity_scheduler.h"

#include "backend/execution_backend.h"
#include "model/request_sizing.h"
#include <algorithm>
#include <cstddef>
#include <vector>

void AffinityScheduler::OnRequestArrival(const Request& req) {
    queue_.push_back(req);
}

std::optional<ExecutionPlan> AffinityScheduler::TrySchedule(
    const SystemState& state,
    const ExecutionBackend& /*backend*/) {

    if (queue_.empty()) {
        return std::nullopt;
    }

    std::vector<CardId> idle_cards;
    idle_cards.reserve(state.cards.size());
    for (const auto& card : state.cards) {
        if (!card.busy && state.now >= card.available_time) {
            idle_cards.push_back(card.card_id);
        }
    }

    if (idle_cards.empty()) {
        return std::nullopt;
    }

    for (size_t req_idx = 0; req_idx < queue_.size(); ++req_idx) {
        const Request& req = queue_[req_idx];

        std::vector<CardId> preferred;
        preferred.reserve(idle_cards.size());
        std::vector<CardId> fallback;
        fallback.reserve(idle_cards.size());

        for (const CardId card_id : idle_cards) {
            const auto& card = state.cards.at(card_id);
            if (card.resident_user.has_value() && card.resident_user.value() == req.user_id) {
                preferred.push_back(card_id);
            } else {
                fallback.push_back(card_id);
            }
        }

        const uint32_t required_cards = DecideCardCountForRequest(req);
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

    ExecutionPlan fallback;
    fallback.request_id = queue_.front().request_id;
    const uint32_t required_cards = DecideCardCountForRequest(queue_.front());
    if (idle_cards.size() < required_cards) {
        return std::nullopt;
    }
    fallback.assigned_cards.insert(
        fallback.assigned_cards.end(),
        idle_cards.begin(),
        idle_cards.begin() + required_cards);
    queue_.pop_front();
    return fallback;
}

void AffinityScheduler::OnTaskFinished(
    const Request&,
    const ExecutionPlan&,
    const ExecutionResult&) {}

bool AffinityScheduler::Empty() const {
    return queue_.empty();
}
