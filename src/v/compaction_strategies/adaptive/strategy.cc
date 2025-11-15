// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compaction_strategies/adaptive/strategy.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <numeric>

namespace compaction_strategies {

ss::future<>
adaptive_compaction_strategy::adapt_strategy(const model::ntp& ntp) {
    // Collect historical metrics
    auto history = co_await collect_performance_history(ntp);

    // Analyze effectiveness of current strategy
    auto effectiveness = analyze_strategy_effectiveness(history);

    // Determine if strategy change is needed
    if (should_change_strategy(effectiveness)) {
        auto new_strategy = select_optimal_strategy(history);
        co_await apply_new_strategy(ntp, new_strategy);
    } else {
        // Tune current strategy parameters
        auto tuned_params = tune_strategy_parameters(history);
        co_await apply_tuned_parameters(ntp, tuned_params);
    }
}

ss::future<performance_history>
adaptive_compaction_strategy::collect_performance_history(
  const model::ntp& ntp) {
    // Placeholder - would collect actual metrics
    performance_history history;
    history.write_amplification = 4.5;
    history.space_amplification = 1.3;
    history.read_amplification = 2.0;
    history.average_latency = std::chrono::milliseconds(8);
    history.throughput = 1000000.0;
    co_return history;
}

strategy_effectiveness
adaptive_compaction_strategy::analyze_strategy_effectiveness(
  const performance_history& history) {
    return {
      .write_amp_trend = calculate_trend({history.write_amplification}),
      .space_efficiency = calculate_space_efficiency(history),
      .read_performance = calculate_read_performance(history),
      .overall_score = calculate_overall_score(history)};
}

bool adaptive_compaction_strategy::should_change_strategy(
  const strategy_effectiveness& effectiveness) const {
    // Change strategy if overall score is low
    return effectiveness.overall_score < 0.6;
}

compaction_strategy
adaptive_compaction_strategy::select_optimal_strategy(
  const performance_history& history) {
    // Score each strategy based on workload
    std::vector<std::pair<compaction_strategy, double>> scores;

    for (auto strategy : all_strategies()) {
        auto score = simulate_strategy_performance(strategy, history);
        scores.emplace_back(strategy, score);
    }

    // Select best performing strategy
    auto best = std::max_element(
      scores.begin(), scores.end(), [](const auto& a, const auto& b) {
          return a.second < b.second;
      });

    return best->first;
}

ss::future<> adaptive_compaction_strategy::apply_new_strategy(
  const model::ntp& ntp, compaction_strategy strategy) {
    // Placeholder - would apply strategy to partition
    co_return;
}

strategy_parameters
adaptive_compaction_strategy::tune_strategy_parameters(
  const performance_history& history) {
    strategy_parameters params;

    // Tune based on observed patterns
    if (history.write_amplification > _target_write_amp) {
        // Increase segment size to reduce write amp
        params.min_segment_size = static_cast<size_t>(
          params.min_segment_size * 1.5);
        params.max_segment_size = static_cast<size_t>(
          params.max_segment_size * 1.5);
    }

    if (history.space_amplification > _target_space_amp) {
        // More aggressive compaction
        params.dead_ratio_threshold *= 0.8;
        params.compaction_frequency *= 1.2;
    }

    if (history.average_latency > _target_latency) {
        // Reduce compaction chunk size
        params.chunk_size = static_cast<size_t>(params.chunk_size * 0.8);
        if (params.max_concurrent_compactions > 1) {
            params.max_concurrent_compactions--;
        }
    }

    return params;
}

ss::future<> adaptive_compaction_strategy::apply_tuned_parameters(
  const model::ntp& ntp, const strategy_parameters& params) {
    // Placeholder - would apply parameters to partition
    co_return;
}

std::vector<compaction_strategy>
adaptive_compaction_strategy::all_strategies() const {
    return {
      compaction_strategy::time_window,
      compaction_strategy::key_based,
      compaction_strategy::size_tiered,
      compaction_strategy::hybrid};
}

double adaptive_compaction_strategy::simulate_strategy_performance(
  compaction_strategy strategy, const performance_history& history) {
    // Simple scoring based on strategy characteristics
    double score = 0.0;

    switch (strategy) {
    case compaction_strategy::time_window:
        score = 0.7; // Good for temporal workloads
        break;
    case compaction_strategy::key_based:
        score = 0.8; // Good for high update rate
        break;
    case compaction_strategy::size_tiered:
        score = 0.6; // Good for write-heavy workloads
        break;
    case compaction_strategy::hybrid:
        score = 0.75; // Balanced approach
        break;
    }

    return score;
}

double adaptive_compaction_strategy::calculate_trend(
  const std::vector<double>& values) {
    if (values.empty()) {
        return 0.0;
    }

    // Simple trend calculation - could use linear regression
    return values.back();
}

double adaptive_compaction_strategy::calculate_space_efficiency(
  const performance_history& history) {
    // Higher efficiency = lower space amplification
    return 1.0 / (1.0 + history.space_amplification);
}

double adaptive_compaction_strategy::calculate_read_performance(
  const performance_history& history) {
    // Lower read amplification = better performance
    return 1.0 / (1.0 + history.read_amplification);
}

double adaptive_compaction_strategy::calculate_overall_score(
  const performance_history& history) {
    auto space_eff = calculate_space_efficiency(history);
    auto read_perf = calculate_read_performance(history);

    // Weighted average of different metrics
    double write_amp_score = std::max(
      0.0, 1.0 - (history.write_amplification / 10.0));
    double latency_score = std::max(
      0.0,
      1.0 - (history.average_latency.count() / 100.0)); // target < 100ms

    return (space_eff * 0.3 + read_perf * 0.2 + write_amp_score * 0.3
            + latency_score * 0.2);
}

} // namespace compaction_strategies
