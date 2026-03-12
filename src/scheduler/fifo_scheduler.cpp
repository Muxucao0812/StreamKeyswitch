#include "scheduler/fifo_scheduler.h"
#include "backend/execution_backend.h"

void FIFOScheduler::OnRequestArrival(const Request& req) {
    queue_.push(req);
}

std::optional<ExecutionPlan> FIFOScheduler::TrySchedule(
    const SystemState& state,
    const ExecutionBackend& /*backend*/) {

    if (queue_.empty()) return std::nullopt;

    for (const auto& card : state.cards) {
        if (!card.busy && state.now >= card.available_time) {
            ExecutionPlan plan;
            plan.request_id = queue_.front().request_id;
            plan.assigned_cards.push_back(card.card_id);
            queue_.pop();
            return plan;
        }
    }
    return std::nullopt;
}

void FIFOScheduler::OnTaskFinished(
    const Request&,
    const ExecutionPlan&,
    const ExecutionResult&) {}

bool FIFOScheduler::Empty() const {
    return queue_.empty();
}
