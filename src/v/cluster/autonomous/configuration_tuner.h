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

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <chrono>
#include <string>
#include <variant>
#include <vector>

namespace cluster::autonomous {

/**
 * Self-tuning configuration optimizer that automatically adjusts
 * cluster configuration for optimal performance.
 */
class configuration_tuner {
public:
    struct tuning_config {
        bool enable_auto_tuning = true;
        std::chrono::hours tuning_interval{1};
        double improvement_threshold = 0.05;  // 5% improvement to apply
        std::chrono::minutes experiment_duration{10};
        size_t max_concurrent_experiments = 3;
    };

    using config_value_t = std::variant<int64_t, double, bool, std::string>;

    struct configuration_parameter {
        std::string name;
        config_value_t value;
        config_value_t min_value;
        config_value_t max_value;
        double impact_score;  // Expected impact on performance
    };

    struct tuning_opportunity {
        configuration_parameter parameter;
        double current_performance;
        config_value_t suggested_value;
        std::string rationale;
    };

    struct performance_metrics {
        double throughput;
        double latency_p99;
        double avg_batch_size;
        double compression_ratio;
        double segment_roll_frequency;
    };

    explicit configuration_tuner(tuning_config config);

    ss::future<> start();
    ss::future<> stop();

private:
    ss::future<> tuning_loop();
    ss::future<std::vector<tuning_opportunity>> identify_tuning_opportunities();
    ss::future<> run_tuning_experiment(const tuning_opportunity& opportunity);
    ss::future<> apply_improved_configurations();
    ss::future<performance_metrics> collect_performance_metrics();

    tuning_config _config;
    ss::abort_source _as;
    ss::gate _gate;
    ss::future<> _tuning_fiber;
};

} // namespace cluster::autonomous
