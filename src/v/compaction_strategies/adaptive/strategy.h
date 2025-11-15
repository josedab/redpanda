// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "compaction_strategies/base/types.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

#include <chrono>
#include <vector>

namespace compaction_strategies {

// Performance metrics for adaptive tuning
struct performance_metrics {
    double write_amplification{0.0};
    double space_amplification{0.0};
    double read_amplification{0.0};
    duration average_latency{0};
    double throughput{0.0};
};

// Historical performance data
struct performance_history {
    std::vector<performance_metrics> metrics;
    double write_amplification{0.0};
    double space_amplification{0.0};
    double read_amplification{0.0};
    duration average_latency{0};
    double throughput{0.0};

    friend std::ostream&
    operator<<(std::ostream& o, const performance_history& h) {
        fmt::print(
          o,
          "{{wa: {}, sa: {}, ra: {}, latency: {}ms, throughput: {}}}",
          h.write_amplification,
          h.space_amplification,
          h.read_amplification,
          h.average_latency.count(),
          h.throughput);
        return o;
    }
};

// Strategy effectiveness analysis
struct strategy_effectiveness {
    double write_amp_trend{0.0};
    double space_efficiency{0.0};
    double read_performance{0.0};
    double overall_score{0.0};

    friend std::ostream&
    operator<<(std::ostream& o, const strategy_effectiveness& e) {
        fmt::print(
          o,
          "{{wa_trend: {}, space_eff: {}, read_perf: {}, score: {}}}",
          e.write_amp_trend,
          e.space_efficiency,
          e.read_performance,
          e.overall_score);
        return o;
    }
};

// Strategy parameters for tuning
struct strategy_parameters {
    size_t min_segment_size{1048576}; // 1MB
    size_t max_segment_size{134217728}; // 128MB
    double dead_ratio_threshold{0.5};
    double compaction_frequency{1.0};
    size_t chunk_size{1048576}; // 1MB
    size_t max_concurrent_compactions{2};

    friend std::ostream&
    operator<<(std::ostream& o, const strategy_parameters& p) {
        fmt::print(
          o,
          "{{min_seg: {}, max_seg: {}, dead_ratio: {}, freq: {}, "
          "chunk: {}, max_concurrent: {}}}",
          p.min_segment_size,
          p.max_segment_size,
          p.dead_ratio_threshold,
          p.compaction_frequency,
          p.chunk_size,
          p.max_concurrent_compactions);
        return o;
    }
};

// Adaptive compaction strategy that learns from performance
class adaptive_compaction_strategy {
public:
    adaptive_compaction_strategy()
      : _target_write_amp(5.0)
      , _target_space_amp(1.5)
      , _target_latency(std::chrono::milliseconds(10)) {}

    // Adapt strategy based on performance history
    ss::future<> adapt_strategy(const model::ntp& ntp);

private:
    // Collect historical performance metrics
    ss::future<performance_history>
    collect_performance_history(const model::ntp& ntp);

    // Analyze effectiveness of current strategy
    strategy_effectiveness
    analyze_strategy_effectiveness(const performance_history& history);

    // Determine if strategy change is needed
    bool
    should_change_strategy(const strategy_effectiveness& effectiveness) const;

    // Select optimal strategy based on history
    compaction_strategy
    select_optimal_strategy(const performance_history& history);

    // Apply new strategy
    ss::future<>
    apply_new_strategy(const model::ntp& ntp, compaction_strategy strategy);

    // Tune strategy parameters
    strategy_parameters
    tune_strategy_parameters(const performance_history& history);

    // Apply tuned parameters
    ss::future<>
    apply_tuned_parameters(const model::ntp& ntp, const strategy_parameters& params);

    // Get all available strategies
    std::vector<compaction_strategy> all_strategies() const;

    // Simulate strategy performance
    double simulate_strategy_performance(
      compaction_strategy strategy, const performance_history& history);

    // Calculate trend in metric
    double calculate_trend(const std::vector<double>& values);

    // Calculate space efficiency
    double calculate_space_efficiency(const performance_history& history);

    // Calculate read performance
    double calculate_read_performance(const performance_history& history);

    // Calculate overall score
    double calculate_overall_score(const performance_history& history);

private:
    double _target_write_amp;
    double _target_space_amp;
    duration _target_latency;
};

} // namespace compaction_strategies
