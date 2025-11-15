// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cluster/fwd.h"
#include "model/fundamental.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <chrono>
#include <vector>

namespace cluster::autonomous {

/**
 * Predictive auto-scaling based on workload patterns and forecasting.
 */
class predictive_scaler {
public:
    struct scaling_config {
        bool enable_auto_scaling = true;
        size_t min_brokers = 3;
        size_t max_brokers = 100;
        double scale_up_threshold = 0.8;    // 80% resource utilization
        double scale_down_threshold = 0.3;  // 30% resource utilization
        std::chrono::minutes scale_up_cooldown{5};
        std::chrono::minutes scale_down_cooldown{30};
        std::chrono::hours forecast_window{1};
        bool enable_schedule_based = true;
    };

    struct resource_forecast {
        std::chrono::system_clock::time_point timestamp;
        double predicted_cpu;
        double predicted_memory;
        double predicted_throughput;
        double confidence;
        std::chrono::seconds time_to_threshold;
    };

    struct scaling_action {
        enum class type {
            scale_up,
            scale_down,
            no_action
        };

        type action;
        size_t target_broker_count;
        std::string reason;
        double confidence;
        std::chrono::system_clock::time_point execute_at;
    };

    struct cluster_metrics {
        size_t broker_count;
        double avg_cpu;
        double avg_memory;
        double avg_disk;
        double total_throughput;
    };

    explicit predictive_scaler(scaling_config config);

    ss::future<> start();
    ss::future<> stop();

    ss::future<resource_forecast> predict_workload(std::chrono::hours lookahead);
    ss::future<scaling_action> make_scaling_decision(
        const resource_forecast& forecast,
        const cluster_metrics& current);

private:
    ss::future<> scaling_loop();
    ss::future<cluster_metrics> collect_current_metrics();
    bool in_cooldown() const;
    size_t calculate_required_brokers(const resource_forecast& forecast);

    scaling_config _config;
    ss::abort_source _as;
    ss::gate _gate;
    ss::future<> _scaling_fiber;
    std::chrono::system_clock::time_point _last_scale_up;
    std::chrono::system_clock::time_point _last_scale_down;
};

} // namespace cluster::autonomous
