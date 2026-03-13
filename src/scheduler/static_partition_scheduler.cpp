#include "scheduler/static_partition_scheduler.h"

#include "backend/execution_backend.h"
#include "model/request_sizing.h"
#include <algorithm>
#include <cstddef>
#include <vector>

StaticPartitionScheduler::StaticPartitionScheduler(uint32_t num_pools)
    : num_pools_(num_pools == 0 ? 1 : num_pools) {}

void StaticPartitionScheduler::OnRequestArrival(const Request& req) {
    queue_.push_back(req);
}

uint32_t StaticPartitionScheduler::PoolForUser(UserId user_id) const {
    return user_id % num_pools_;
}

std::optional<ExecutionPlan> StaticPartitionScheduler::TrySchedule(
    const SystemState& state,
    const ExecutionBackend& /*backend*/) {

    if (queue_.empty()) {
        return std::nullopt;
    }

    for (size_t req_idx = 0; req_idx < queue_.size(); ++req_idx) {
        const Request& req = queue_[req_idx];
        const uint32_t target_pool = PoolForUser(req.user_id);
        const uint32_t required_cards = DecideCardCountForRequest(req);

        std::vector<CardId> pool_idle_cards;
        pool_idle_cards.reserve(state.cards.size());

        for (const auto& card : state.cards) {
            if (card.pool_id != target_pool) {
                continue;
            }
            if (card.busy || state.now < card.available_time) {
                continue;
            }
            pool_idle_cards.push_back(card.card_id);
        }

        if (pool_idle_cards.size() < required_cards) {
            continue;
        }

        ExecutionPlan plan;
        plan.request_id = req.request_id;
        plan.assigned_cards.insert(
            plan.assigned_cards.end(),
            pool_idle_cards.begin(),
            pool_idle_cards.begin() + required_cards);

        queue_.erase(
            queue_.begin()
            + static_cast<std::deque<Request>::difference_type>(req_idx));
        return plan;
    }

    return std::nullopt;
}

void StaticPartitionScheduler::OnTaskFinished(
    const Request&,
    const ExecutionPlan&,
    const ExecutionResult&) {}

bool StaticPartitionScheduler::Empty() const {
    return queue_.empty();
}
