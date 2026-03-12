#pragma once

#include "common/types.h"

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <vector>

struct RequestMetricsSample {
    RequestId request_id = 0;
    UserId user_id = 0;

    Time arrival_time = 0;
    Time start_time = 0;
    Time finish_time = 0;

    Time latency = 0;
    Time queue_time = 0;
    Time service_time = 0;

    uint32_t reload_count = 0;
};

struct UserMetrics {
    UserId user_id = 0;
    size_t completed_requests = 0;
    double mean_latency = 0.0;
    double throughput = 0.0;
    uint64_t reload_count = 0;
};

struct CardMetrics {
    CardId card_id = 0;
    double utilization = 0.0;
    uint64_t reload_count = 0;
    uint64_t served_requests = 0;
};

struct CardMetricsSample {
    CardId card_id = 0;
    Time busy_time = 0;
    uint64_t reload_count = 0;
    uint64_t served_requests = 0;
};

struct SimulationMetrics {
    // Request-level metrics
    size_t completed_requests = 0;
    double mean_latency = 0.0;
    Time p95_latency = 0;
    Time p99_latency = 0;
    Time max_latency = 0;
    double mean_queue_time = 0.0;
    double mean_service_time = 0.0;

    // User-level metrics
    std::vector<UserMetrics> per_user;

    // Card-level metrics
    std::vector<CardMetrics> per_card;

    // System-level metrics
    double total_throughput = 0.0;
    uint64_t total_reload_count = 0;
    double jain_fairness_index = 0.0;

    Time simulation_start_time = 0;
    Time simulation_end_time = 0;
    Time simulation_duration = 0;
};

SimulationMetrics ComputeMetrics(
    const std::vector<RequestMetricsSample>& request_samples,
    const std::vector<UserId>& all_users,
    const std::vector<CardMetricsSample>& card_samples);

void PrintMetrics(std::ostream& os, const SimulationMetrics& metrics);
