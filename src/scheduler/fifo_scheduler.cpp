#include "scheduler/fifo_scheduler.h"

#include "backend/execution_backend.h"
#include "model/request_sizing.h"
#include <algorithm>
#include <vector>

void FIFOScheduler::OnRequestArrival(const Request& req) {
    queue_.push(req);
}

std::optional<ExecutionPlan> FIFOScheduler::TrySchedule(
    const SystemState& state,
    const ExecutionBackend& /*backend*/) {

    if (queue_.empty()) {
        return std::nullopt;
    }

    const Request& req = queue_.front();
    const uint32_t required_cards = DecideCardCountForRequest(req);

    std::vector<CardId> idle_cards;
    idle_cards.reserve(state.cards.size());
    for (const auto& card : state.cards) {
        if (!card.busy && state.now >= card.available_time) {
            idle_cards.push_back(card.card_id);
        }
    }

    if (idle_cards.size() < required_cards) {
        return std::nullopt;
    }

    ExecutionPlan plan;
    plan.request_id = req.request_id;
    plan.assigned_cards.insert(plan.assigned_cards.end(), idle_cards.begin(), idle_cards.begin() + required_cards);
    queue_.pop();
    return plan;
}

void FIFOScheduler::OnTaskFinished(
    const Request&,
    const ExecutionPlan&,
    const ExecutionResult&) {}

bool FIFOScheduler::Empty() const {
    return queue_.empty();
}
