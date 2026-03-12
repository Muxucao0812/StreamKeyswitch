#include "sim/simulator.h"
#include <iostream>

Simulator::Simulator(
    SystemState initial_state,
    std::unique_ptr<Scheduler> scheduler,
    std::unique_ptr<ExecutionBackend> backend)
    : state_(std::move(initial_state)),
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
        default:
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
        if (!plan_opt.has_value()) break;

        const auto& plan = plan_opt.value();
        const auto& req = request_table_.at(plan.request_id);

        ExecutionResult result = backend_->Estimate(req, plan, state_);

        Time finish_time = state_.now + result.total_latency;

        for (auto card_id : plan.assigned_cards) {
            auto& card = state_.cards.at(card_id);
            card.busy = true;
            card.available_time = finish_time;
            card.resident_user = req.user_id;
        }

        Event finish_ev;
        finish_ev.event_id = next_event_id_++;
        finish_ev.type = EventType::TaskFinish;
        finish_ev.timestamp = finish_time;
        finish_ev.request = req;
        finish_ev.plan = plan;
        finish_ev.result = result;
        events_.push(finish_ev);

        // 当前简单实现：一次只发一个
        break;
    }
}

void Simulator::HandleTaskFinish(const Event& ev) {
    for (auto card_id : ev.plan.assigned_cards) {
        auto& card = state_.cards.at(card_id);
        card.busy = false;
        card.memory_used_bytes = 0;
    }

    scheduler_->OnTaskFinished(ev.request, ev.plan, ev.result);

    completed_.push_back({
        ev.request,
        ev.plan,
        ev.result,
        ev.timestamp - ev.result.total_latency,
        ev.timestamp
    });
}

void Simulator::Report() const {
    std::cout << "Simulation finished\n";
    std::cout << "Completed requests: " << completed_.size() << "\n";

    if (completed_.empty()) {
        return;
    }

    Time total_latency = 0;
    Time max_latency = 0;
    Time total_key_load = 0;
    Time total_compute = 0;

    for (const auto& rec : completed_) {
        total_latency += rec.result.total_latency;
        total_key_load += rec.result.breakdown.key_load_time;
        total_compute += rec.result.breakdown.compute_time;
        if (rec.result.total_latency > max_latency) {
            max_latency = rec.result.total_latency;
        }
    }

    const double completed = static_cast<double>(completed_.size());
    std::cout << "Avg latency(ns): " << (total_latency / completed) << "\n";
    std::cout << "Max latency(ns): " << max_latency << "\n";
    std::cout << "Avg key-load(ns): " << (total_key_load / completed) << "\n";
    std::cout << "Avg compute(ns): " << (total_compute / completed) << "\n";
}
