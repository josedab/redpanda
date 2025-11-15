// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compaction_strategies/hybrid/strategy.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>

namespace compaction_strategies {

ss::future<compaction_plan> hybrid_compaction_strategy::create_hybrid_plan(
  const std::vector<segment_metadata>& segments,
  const hybrid_config& config) {
    compaction_plan plan;

    // Analyze key distribution
    auto key_stats = co_await analyze_key_distribution(segments);

    // Analyze temporal distribution
    auto time_stats = analyze_temporal_distribution(segments);

    // Decide compaction mode
    if (should_use_key_compaction(key_stats, config)) {
        plan = co_await create_key_based_plan(segments, key_stats);
    } else if (should_use_time_compaction(time_stats, config)) {
        plan = create_time_based_plan(segments, time_stats);
    } else {
        // Use hybrid approach
        plan = co_await create_hybrid_plan_impl(
          segments, key_stats, time_stats, config);
    }

    co_return plan;
}

ss::future<compaction_plan>
hybrid_compaction_strategy::create_hybrid_plan_impl(
  const std::vector<segment_metadata>& segments,
  const key_statistics& key_stats,
  const time_statistics& time_stats,
  const hybrid_config& config) {
    compaction_plan plan;

    // Partition segments by strategy
    auto [key_segments, time_segments] = partition_segments(
      segments, key_stats, time_stats);

    // Create key-based compaction for segments with high key overlap
    if (!key_segments.empty()) {
        auto key_plan = co_await create_key_compaction_tasks(
          key_segments, key_stats);
        plan.tasks.insert(
          plan.tasks.end(), key_plan.begin(), key_plan.end());
    }

    // Create time-based compaction for temporal segments
    if (!time_segments.empty()) {
        auto time_plan = create_time_compaction_tasks(
          time_segments, time_stats, config.time_window);
        plan.tasks.insert(
          plan.tasks.end(), time_plan.begin(), time_plan.end());
    }

    // Optimize execution order
    plan = optimize_execution_order(plan);

    co_return plan;
}

ss::future<key_statistics>
hybrid_compaction_strategy::analyze_key_distribution(
  const std::vector<segment_metadata>& segments) {
    // Placeholder implementation - would analyze actual keys
    key_statistics stats;
    stats.total_keys = 10000;
    stats.unique_keys = 8000;
    stats.uniqueness_ratio = 0.8;
    co_return stats;
}

time_statistics hybrid_compaction_strategy::analyze_temporal_distribution(
  const std::vector<segment_metadata>& segments) {
    // Placeholder implementation - would analyze actual timestamps
    time_statistics stats;
    stats.min_timestamp = model::timestamp::now();
    stats.max_timestamp = model::timestamp::now();
    stats.span = duration(0);
    stats.temporal_locality = 0.7;
    return stats;
}

bool hybrid_compaction_strategy::should_use_key_compaction(
  const key_statistics& key_stats, const hybrid_config& config) {
    if (!config.enable_key_compaction) {
        return false;
    }
    return key_stats.uniqueness_ratio < config.key_uniqueness_threshold;
}

bool hybrid_compaction_strategy::should_use_time_compaction(
  const time_statistics& time_stats, const hybrid_config& config) {
    if (!config.enable_time_compaction) {
        return false;
    }
    return time_stats.temporal_locality > 0.7;
}

ss::future<compaction_plan>
hybrid_compaction_strategy::create_key_based_plan(
  const std::vector<segment_metadata>& segments,
  const key_statistics& key_stats) {
    compaction_plan plan;
    plan.tasks = co_await create_key_compaction_tasks(segments, key_stats);
    co_return plan;
}

compaction_plan hybrid_compaction_strategy::create_time_based_plan(
  const std::vector<segment_metadata>& segments,
  const time_statistics& time_stats) {
    compaction_plan plan;
    plan.tasks = create_time_compaction_tasks(
      segments, time_stats, _config.time_window);
    return plan;
}

std::pair<std::vector<segment_metadata>, std::vector<segment_metadata>>
hybrid_compaction_strategy::partition_segments(
  const std::vector<segment_metadata>& segments,
  const key_statistics& key_stats,
  const time_statistics& time_stats) {
    std::vector<segment_metadata> key_candidates;
    std::vector<segment_metadata> time_candidates;

    for (const auto& segment : segments) {
        // Check key overlap with other segments
        auto key_overlap = calculate_key_overlap(segment, segments, key_stats);

        // Check temporal clustering
        auto time_clustering = calculate_temporal_clustering(segment, time_stats);

        // Assign to appropriate strategy
        if (key_overlap > 0.5) {
            key_candidates.push_back(segment);
        } else if (time_clustering > 0.7) {
            time_candidates.push_back(segment);
        }
        // Segments not fitting either criteria are skipped
    }

    return {key_candidates, time_candidates};
}

double hybrid_compaction_strategy::calculate_key_overlap(
  const segment_metadata& segment,
  const std::vector<segment_metadata>& segments,
  const key_statistics& stats) {
    // Placeholder - would calculate actual key overlap
    return 0.3;
}

double hybrid_compaction_strategy::calculate_temporal_clustering(
  const segment_metadata& segment, const time_statistics& stats) {
    // Placeholder - would calculate actual temporal clustering
    return 0.5;
}

ss::future<std::vector<compaction_task>>
hybrid_compaction_strategy::create_key_compaction_tasks(
  const std::vector<segment_metadata>& segments,
  const key_statistics& stats) {
    std::vector<compaction_task> tasks;

    // Group segments with overlapping keys
    auto groups = group_by_key_overlap(segments, stats);

    for (const auto& group : groups) {
        compaction_task task;
        task.type = compaction_type::key_based;
        task.segments = group;
        task.priority = calculate_key_compaction_priority(group, stats);

        // Create key filter
        task.key_filter = create_deduplication_filter(group, stats);

        tasks.push_back(task);
    }

    co_return tasks;
}

std::vector<compaction_task>
hybrid_compaction_strategy::create_time_compaction_tasks(
  const std::vector<segment_metadata>& segments,
  const time_statistics& stats,
  duration window) {
    std::vector<compaction_task> tasks;

    // Group segments by time window
    auto windows = group_by_time_window(segments, window);

    for (const auto& [window_start, window_segments] : windows) {
        compaction_task task;
        task.type = compaction_type::time_window;
        task.segments = window_segments;
        task.priority = calculate_time_compaction_priority(
          window_segments, stats);

        // Set time bounds
        task.time_range = time_range{
          .start = window_start,
          .end = model::timestamp(window_start() + window.count())};

        tasks.push_back(task);
    }

    return tasks;
}

std::vector<std::vector<segment_metadata>>
hybrid_compaction_strategy::group_by_key_overlap(
  const std::vector<segment_metadata>& segments,
  const key_statistics& stats) {
    // Placeholder - simple grouping, all segments in one group
    std::vector<std::vector<segment_metadata>> groups;
    if (!segments.empty()) {
        groups.push_back(segments);
    }
    return groups;
}

absl::flat_hash_map<model::timestamp, std::vector<segment_metadata>>
hybrid_compaction_strategy::group_by_time_window(
  const std::vector<segment_metadata>& segments, duration window) {
    absl::flat_hash_map<model::timestamp, std::vector<segment_metadata>>
      windows;

    // Placeholder - would group by actual time windows
    if (!segments.empty()) {
        windows[model::timestamp::now()] = segments;
    }

    return windows;
}

compaction_priority
hybrid_compaction_strategy::calculate_key_compaction_priority(
  const std::vector<segment_metadata>& group, const key_statistics& stats) {
    // Calculate priority based on key overlap
    if (stats.uniqueness_ratio < 0.5) {
        return compaction_priority::high;
    } else if (stats.uniqueness_ratio < 0.7) {
        return compaction_priority::medium;
    }
    return compaction_priority::low;
}

compaction_priority
hybrid_compaction_strategy::calculate_time_compaction_priority(
  const std::vector<segment_metadata>& group, const time_statistics& stats) {
    // Calculate priority based on temporal locality
    if (stats.temporal_locality > 0.8) {
        return compaction_priority::high;
    } else if (stats.temporal_locality > 0.6) {
        return compaction_priority::medium;
    }
    return compaction_priority::low;
}

size_t hybrid_compaction_strategy::create_deduplication_filter(
  const std::vector<segment_metadata>& group, const key_statistics& stats) {
    // Placeholder - would create actual filter
    return stats.unique_keys;
}

compaction_plan
hybrid_compaction_strategy::optimize_execution_order(compaction_plan plan) {
    // Sort tasks by priority
    std::sort(
      plan.tasks.begin(),
      plan.tasks.end(),
      [](const auto& a, const auto& b) { return a.priority > b.priority; });

    return plan;
}

} // namespace compaction_strategies
