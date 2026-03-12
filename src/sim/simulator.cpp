#include "sim/simulator.h"

#include <iostream>
#include <unordered_set>

Simulator::Simulator(
    SystemState initial_state,
    std::unique_ptr<Scheduler> scheduler,
    std::unique_ptr<ExecutionBackend> backend
):  
    state_(std::move(initial_state)),
    scheduler_(std::move(scheduler)),
    backend_(std::move(backend)) {}

void Simulator::LoadWorkload(const std::vector<Request>& requests) {
    for (const auto& req : requests) {
        request_table_[req.request_id] = req;

        Event ev;
        ev.event_id = next_event_id_++;
        ev.type = EventType::RequestArrival;
        ev.timestamp = req.arrival_time;
        ev.request = req;
        events_.push(ev);
    }
}

void Simulator::Run() {
    while (!events_.empty()) {
        Event ev = events_.top();
        events_.pop();

        state_.now = ev.timestamp;

        switch (ev.type) {
        case EventType::RequestArrival:
            HandleArrival(ev);
            break;
        case EventType::TaskFinish:
            HandleTaskFinish(ev);
            break;
        }

        TryDispatch();
    }
}

void Simulator::HandleArrival(const Event& ev) {
    scheduler_->OnRequestArrival(ev.request);
}

void Simulator::TryDispatch() {
    while (true) {
        auto plan_opt = scheduler_->TrySchedule(state_, *backend_);
        if (!plan_opt.has_value()) {
            break;
        }

        const ExecutionPlan& plan = plan_opt.value();
        const Request& req = request_table_.at(plan.request_id);

        ExecutionResult result = backend_->Estimate(req, plan, state_);
        const Time dispatch_start_time = state_.now;
        result.breakdown.queue_time =
            (dispatch_start_time >= req.arrival_time)
            ? (dispatch_start_time - req.arrival_time)
            : 0;
        const Time finish_time = state_.now + result.total_latency;
        uint32_t dispatch_reload_count = 0;

        for (const CardId card_id : plan.assigned_cards) {
            auto& card = state_.cards.at(card_id);

            const bool need_reload =
                (!card.resident_user.has_value())
                || (card.resident_user.value() != req.user_id);
            if (need_reload) {
                ++reload_count_;
                ++dispatch_reload_count;
                ++card.reload_count;
            }

            card.last_start_time = dispatch_start_time;
            card.busy = true;
            card.available_time = finish_time;
            card.resident_user = req.user_id;
            card.memory_used_bytes = result.peak_memory_bytes;
        }

        Event finish_ev;
        finish_ev.event_id = next_event_id_++;
        finish_ev.type = EventType::TaskFinish;
        finish_ev.timestamp = finish_time;
        finish_ev.dispatch_start_time = dispatch_start_time;
        finish_ev.reload_count = dispatch_reload_count;
        finish_ev.request = req;
        finish_ev.plan = plan;
        finish_ev.result = result;
        events_.push(finish_ev);
    }
}

void Simulator::HandleTaskFinish(const Event& ev) {
    for (const CardId card_id : ev.plan.assigned_cards) {
        auto& card = state_.cards.at(card_id);
        card.served_requests += 1;
        if (ev.timestamp >= card.last_start_time) {
            card.total_busy_time += (ev.timestamp - card.last_start_time);
        }
        card.busy = false;
        card.memory_used_bytes = 0;
    }

    scheduler_->OnTaskFinished(ev.request, ev.plan, ev.result);

    completed_.push_back({
        ev.request,
        ev.plan,
        ev.result,
        ev.dispatch_start_time,
        ev.timestamp,
        ev.result.breakdown.queue_time,
        ev.result.total_latency,
        ev.reload_count});
}

SimulationMetrics Simulator::CollectMetrics() const {
    std::vector<RequestMetricsSample> request_samples;
    request_samples.reserve(completed_.size());
    for (const auto& rec : completed_) {
        RequestMetricsSample sample;
        sample.request_id = rec.request.request_id;
        sample.user_id = rec.request.user_id;
        sample.arrival_time = rec.request.arrival_time;
        sample.start_time = rec.start_time;
        sample.finish_time = rec.finish_time;
        sample.latency = rec.result.total_latency;
        sample.queue_time = rec.queue_time;
        sample.service_time = rec.service_time;
        sample.reload_count = rec.reload_count;
        request_samples.push_back(sample);
    }

    std::unordered_set<UserId> users_set;
    users_set.reserve(request_table_.size());
    for (const auto& entry : request_table_) {
        users_set.insert(entry.second.user_id);
    }

    std::vector<UserId> users;
    users.reserve(users_set.size());
    for (const UserId user_id : users_set) {
        users.push_back(user_id);
    }

    std::vector<CardMetricsSample> card_samples;
    card_samples.reserve(state_.cards.size());
    for (const auto& card : state_.cards) {
        CardMetricsSample sample;
        sample.card_id = card.card_id;
        sample.busy_time = card.total_busy_time;
        sample.reload_count = card.reload_count;
        sample.served_requests = card.served_requests;
        card_samples.push_back(sample);
    }

    return ComputeMetrics(request_samples, users, card_samples);
}

void Simulator::Report(std::ostream& os) const {
    PrintMetrics(os, CollectMetrics());
}

void Simulator::Report() const {
    Report(std::cout);
}
