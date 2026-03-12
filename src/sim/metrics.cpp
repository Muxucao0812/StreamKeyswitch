#include "sim/metrics.h"

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <limits>
#include <ostream>
#include <unordered_map>

namespace {

struct UserAggregate {
    size_t completed_requests = 0;
    long double latency_sum = 0.0;
    uint64_t reload_count = 0;
};

double Mean(const std::vector<Time>& values) {
    if (values.empty()) {
        return 0.0;
    }

    long double sum = 0.0;
    for (const Time value : values) {
        sum += static_cast<long double>(value);
    }
    return static_cast<double>(sum / values.size());
}

Time Percentile(const std::vector<Time>& values, double percentile) {
    if (values.empty()) {
        return 0;
    }

    std::vector<Time> sorted = values;
    std::sort(sorted.begin(), sorted.end());

    if (percentile <= 0.0) {
        return sorted.front();
    }
    if (percentile >= 1.0) {
        return sorted.back();
    }

    const double rank = percentile * static_cast<double>(sorted.size());
    size_t idx = static_cast<size_t>(std::ceil(rank));
    if (idx == 0) {
        idx = 1;
    }
    return sorted.at(idx - 1);
}

Time Max(const std::vector<Time>& values) {
    if (values.empty()) {
        return 0;
    }
    return *std::max_element(values.begin(), values.end());
}

double JainFairnessIndex(const std::vector<double>& values) {
    if (values.empty()) {
        return 0.0;
    }

    long double sum = 0.0;
    long double sum_sq = 0.0;
    for (const double value : values) {
        sum += value;
        sum_sq += value * value;
    }

    if (sum_sq == 0.0) {
        return 0.0;
    }

    const long double n = static_cast<long double>(values.size());
    return static_cast<double>((sum * sum) / (n * sum_sq));
}

double Throughput(size_t completed_requests, Time duration) {
    if (duration == 0) {
        return 0.0;
    }
    return static_cast<double>(completed_requests) / static_cast<double>(duration);
}

} // namespace

SimulationMetrics ComputeMetrics(
    const std::vector<RequestMetricsSample>& request_samples,
    const std::vector<UserId>& all_users,
    const std::vector<CardMetricsSample>& card_samples) {

    SimulationMetrics metrics;
    metrics.completed_requests = request_samples.size();

    std::vector<Time> latencies;
    std::vector<Time> queue_times;
    std::vector<Time> service_times;
    latencies.reserve(request_samples.size());
    queue_times.reserve(request_samples.size());
    service_times.reserve(request_samples.size());

    Time start_time = 0;
    Time end_time = 0;
    if (!request_samples.empty()) {
        start_time = std::numeric_limits<Time>::max();
    }

    std::unordered_map<UserId, UserAggregate> user_agg;
    user_agg.reserve(all_users.size() + request_samples.size());
    for (const UserId user_id : all_users) {
        user_agg.emplace(user_id, UserAggregate{});
    }

    for (const auto& sample : request_samples) {
        latencies.push_back(sample.latency);
        queue_times.push_back(sample.queue_time);
        service_times.push_back(sample.service_time);

        start_time = std::min(start_time, sample.arrival_time);
        end_time = std::max(end_time, sample.finish_time);

        auto& agg = user_agg[sample.user_id];
        ++agg.completed_requests;
        agg.latency_sum += sample.latency;
        agg.reload_count += sample.reload_count;
    }

    metrics.mean_latency = Mean(latencies);
    metrics.p95_latency = Percentile(latencies, 0.95);
    metrics.p99_latency = Percentile(latencies, 0.99);
    metrics.max_latency = Max(latencies);
    metrics.mean_queue_time = Mean(queue_times);
    metrics.mean_service_time = Mean(service_times);

    metrics.simulation_start_time = request_samples.empty() ? 0 : start_time;
    metrics.simulation_end_time = request_samples.empty() ? 0 : end_time;
    metrics.simulation_duration =
        (end_time > start_time) ? (end_time - start_time) : 0;

    std::vector<UserId> user_ids;
    user_ids.reserve(user_agg.size());
    for (const auto& item : user_agg) {
        user_ids.push_back(item.first);
    }
    std::sort(user_ids.begin(), user_ids.end());

    std::vector<double> throughputs;
    throughputs.reserve(user_ids.size());

    for (const UserId user_id : user_ids) {
        const auto& agg = user_agg.at(user_id);

        UserMetrics user_metrics;
        user_metrics.user_id = user_id;
        user_metrics.completed_requests = agg.completed_requests;
        user_metrics.mean_latency = (agg.completed_requests == 0)
            ? 0.0
            : static_cast<double>(agg.latency_sum / agg.completed_requests);
        user_metrics.throughput = Throughput(
            agg.completed_requests,
            metrics.simulation_duration);
        user_metrics.reload_count = agg.reload_count;

        throughputs.push_back(user_metrics.throughput);
        metrics.per_user.push_back(user_metrics);
    }

    std::vector<CardMetricsSample> sorted_cards = card_samples;
    std::sort(
        sorted_cards.begin(),
        sorted_cards.end(),
        [](const CardMetricsSample& a, const CardMetricsSample& b) {
            return a.card_id < b.card_id;
        });

    uint64_t card_reload_total = 0;
    for (const auto& card_sample : sorted_cards) {
        CardMetrics card_metrics;
        card_metrics.card_id = card_sample.card_id;
        card_metrics.utilization = (metrics.simulation_duration == 0)
            ? 0.0
            : static_cast<double>(card_sample.busy_time)
                / static_cast<double>(metrics.simulation_duration);
        card_metrics.reload_count = card_sample.reload_count;
        card_metrics.served_requests = card_sample.served_requests;
        metrics.per_card.push_back(card_metrics);

        card_reload_total += card_sample.reload_count;
    }

    metrics.total_reload_count = card_reload_total;
    if (metrics.total_reload_count == 0) {
        for (const auto& sample : request_samples) {
            metrics.total_reload_count += sample.reload_count;
        }
    }

    metrics.total_throughput = Throughput(
        metrics.completed_requests,
        metrics.simulation_duration);
    metrics.jain_fairness_index = JainFairnessIndex(throughputs);

    return metrics;
}

void PrintMetrics(std::ostream& os, const SimulationMetrics& metrics) {
    const std::streamsize old_prec = os.precision();
    const auto old_flags = os.flags();

    os.setf(std::ios::fixed);
    os << std::setprecision(4);

    os << "=== Request Metrics ===\n";
    os << "Completed requests: " << metrics.completed_requests << "\n";
    os << "Mean latency(ns): " << metrics.mean_latency << "\n";
    os << "P95 latency(ns): " << metrics.p95_latency << "\n";
    os << "P99 latency(ns): " << metrics.p99_latency << "\n";
    os << "Max latency(ns): " << metrics.max_latency << "\n";
    os << "Mean queue time(ns): " << metrics.mean_queue_time << "\n";
    os << "Mean service time(ns): " << metrics.mean_service_time << "\n";

    os << "=== User Metrics ===\n";
    for (const auto& user : metrics.per_user) {
        os << "User " << user.user_id
           << ": completed=" << user.completed_requests
           << ", mean_latency(ns)=" << user.mean_latency
           << ", throughput(req/ns)=" << user.throughput
           << ", reload_count=" << user.reload_count
           << "\n";
    }

    os << "=== Card Metrics ===\n";
    for (const auto& card : metrics.per_card) {
        os << "Card " << card.card_id
           << ": utilization=" << card.utilization
           << ", reload_count=" << card.reload_count
           << ", served_requests=" << card.served_requests
           << "\n";
    }

    os << "=== System Metrics ===\n";
    os << "Total throughput(req/ns): " << metrics.total_throughput << "\n";
    os << "Total reload count: " << metrics.total_reload_count << "\n";
    os << "Jain fairness index: " << metrics.jain_fairness_index << "\n";
    os << "Simulation window(ns): ["
       << metrics.simulation_start_time << ", "
       << metrics.simulation_end_time << "]"
       << ", duration=" << metrics.simulation_duration << "\n";

    os.flags(old_flags);
    os.precision(old_prec);
}
