#include "scheduler/score_scheduler.h"

#include "backend/execution_backend.h"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <vector>

namespace {

uint32_t DecideCardCount(const Request& req) {
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

} // namespace

ScoreScheduler::ScoreScheduler(ScoreWeights weights)
    : weights_(weights) {}

void ScoreScheduler::OnRequestArrival(const Request& req) {
    queue_.push_back(req);
}

double ScoreScheduler::ComputeScore(
    const Request& req,
    const CardState& card,
    Time now,
    size_t queue_index,
    size_t queue_size) const {

    const Time waiting_time = (now >= req.arrival_time) ? (now - req.arrival_time) : 0;

    Time switch_cost = 0;
    const bool need_switch =
        (!card.resident_user.has_value())
        || (card.resident_user.value() != req.user_id);
    if (need_switch) {
        switch_cost = (req.user_profile.key_load_time > 0)
            ? req.user_profile.key_load_time
            : (500 + req.ks_profile.key_bytes / 1024);
    }

    const double queue_pressure =
        static_cast<double>(queue_size - queue_index);

    const double priority_score = 1.0 / (1.0 + static_cast<double>(req.priority));

    return
        weights_.waiting_time * static_cast<double>(waiting_time)
        - weights_.switch_cost * static_cast<double>(switch_cost)
        - weights_.queue_pressure * queue_pressure
        + weights_.priority * priority_score;
}

std::optional<ExecutionPlan> ScoreScheduler::TrySchedule(
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

    double best_score = -std::numeric_limits<double>::infinity();
    size_t best_req_idx = 0;
    CardId best_card_id = idle_cards.front();
    uint32_t best_required_cards = 1;
    bool found = false;

    for (size_t req_idx = 0; req_idx < queue_.size(); ++req_idx) {
        const Request& req = queue_[req_idx];
        const uint32_t required_cards = DecideCardCount(req);

        if (idle_cards.size() < required_cards) {
            continue;
        }

        for (const CardId card_id : idle_cards) {
            const auto& card = state.cards.at(card_id);
            const double score = ComputeScore(
                req,
                card,
                state.now,
                req_idx,
                queue_.size());

            if (!found || score > best_score) {
                best_score = score;
                best_req_idx = req_idx;
                best_card_id = card_id;
                best_required_cards = required_cards;
                found = true;
            }
        }
    }

    if (!found) {
        return std::nullopt;
    }

    ExecutionPlan plan;
    plan.request_id = queue_[best_req_idx].request_id;

    std::vector<CardId> ordered_idle_cards;
    ordered_idle_cards.reserve(idle_cards.size());
    ordered_idle_cards.push_back(best_card_id);
    for (const CardId card_id : idle_cards) {
        if (card_id == best_card_id) {
            continue;
        }
        ordered_idle_cards.push_back(card_id);
    }

    const Request& best_req = queue_[best_req_idx];
    std::stable_sort(
        ordered_idle_cards.begin() + 1,
        ordered_idle_cards.end(),
        [&](CardId a, CardId b) {
            const auto& ca = state.cards.at(a);
            const auto& cb = state.cards.at(b);
            const bool a_match =
                ca.resident_user.has_value() && ca.resident_user.value() == best_req.user_id;
            const bool b_match =
                cb.resident_user.has_value() && cb.resident_user.value() == best_req.user_id;
            return a_match && !b_match;
        });

    plan.assigned_cards.insert(
        plan.assigned_cards.end(),
        ordered_idle_cards.begin(),
        ordered_idle_cards.begin() + best_required_cards);

    queue_.erase(
        queue_.begin()
        + static_cast<std::deque<Request>::difference_type>(best_req_idx));
    return plan;
}

void ScoreScheduler::OnTaskFinished(
    const Request&,
    const ExecutionPlan&,
    const ExecutionResult&) {}

bool ScoreScheduler::Empty() const {
    return queue_.empty();
}
