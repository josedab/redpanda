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
#include "model/record.h"

#include <seastar/core/future.hh>

#include <absl/container/flat_hash_map.h>

#include <vector>

namespace compaction_strategies {

// Configuration for hybrid compaction
struct hybrid_config {
    bool enable_key_compaction{true};
    bool enable_time_compaction{true};
    duration time_window{std::chrono::hours(1)};
    double key_uniqueness_threshold{0.8};
    size_t min_segment_size{1048576};
};

// Key statistics for compaction decisions
struct key_statistics {
    size_t total_keys{0};
    size_t unique_keys{0};
    double uniqueness_ratio{0.0};
    absl::flat_hash_map<model::record_key, size_t> key_frequency;
    std::vector<model::record_key> hot_keys;
};

// Temporal statistics for compaction decisions
struct time_statistics {
    model::timestamp min_timestamp;
    model::timestamp max_timestamp;
    duration span{0};
    std::vector<time_bucket> buckets;
    double temporal_locality{0.0};
};

// Hybrid compaction strategy combining time and key-based approaches
class hybrid_compaction_strategy {
public:
    explicit hybrid_compaction_strategy(hybrid_config config = {})
      : _config(std::move(config)) {}

    // Create a hybrid compaction plan
    ss::future<compaction_plan> create_hybrid_plan(
      const std::vector<segment_metadata>& segments,
      const hybrid_config& config);

    ss::future<compaction_plan>
    create_hybrid_plan(const std::vector<segment_metadata>& segments) {
        return create_hybrid_plan(segments, _config);
    }

private:
    // Create the actual hybrid plan
    ss::future<compaction_plan> create_hybrid_plan_impl(
      const std::vector<segment_metadata>& segments,
      const key_statistics& key_stats,
      const time_statistics& time_stats,
      const hybrid_config& config);

    // Analyze key distribution
    ss::future<key_statistics>
    analyze_key_distribution(const std::vector<segment_metadata>& segments);

    // Analyze temporal distribution
    time_statistics
    analyze_temporal_distribution(const std::vector<segment_metadata>& segments);

    // Determine if key-based compaction should be used
    bool should_use_key_compaction(
      const key_statistics& key_stats, const hybrid_config& config);

    // Determine if time-based compaction should be used
    bool should_use_time_compaction(
      const time_statistics& time_stats, const hybrid_config& config);

    // Create key-based compaction plan
    ss::future<compaction_plan> create_key_based_plan(
      const std::vector<segment_metadata>& segments,
      const key_statistics& key_stats);

    // Create time-based compaction plan
    compaction_plan create_time_based_plan(
      const std::vector<segment_metadata>& segments,
      const time_statistics& time_stats);

    // Partition segments by strategy
    std::pair<std::vector<segment_metadata>, std::vector<segment_metadata>>
    partition_segments(
      const std::vector<segment_metadata>& segments,
      const key_statistics& key_stats,
      const time_statistics& time_stats);

    // Calculate key overlap between segments
    double calculate_key_overlap(
      const segment_metadata& segment,
      const std::vector<segment_metadata>& segments,
      const key_statistics& stats);

    // Calculate temporal clustering
    double calculate_temporal_clustering(
      const segment_metadata& segment, const time_statistics& stats);

    // Create key compaction tasks
    ss::future<std::vector<compaction_task>> create_key_compaction_tasks(
      const std::vector<segment_metadata>& segments,
      const key_statistics& stats);

    // Create time compaction tasks
    std::vector<compaction_task> create_time_compaction_tasks(
      const std::vector<segment_metadata>& segments,
      const time_statistics& stats,
      duration window);

    // Group segments by key overlap
    std::vector<std::vector<segment_metadata>> group_by_key_overlap(
      const std::vector<segment_metadata>& segments,
      const key_statistics& stats);

    // Group segments by time window
    absl::flat_hash_map<model::timestamp, std::vector<segment_metadata>>
    group_by_time_window(
      const std::vector<segment_metadata>& segments, duration window);

    // Calculate priority for key compaction
    compaction_priority calculate_key_compaction_priority(
      const std::vector<segment_metadata>& group, const key_statistics& stats);

    // Calculate priority for time compaction
    compaction_priority calculate_time_compaction_priority(
      const std::vector<segment_metadata>& group,
      const time_statistics& stats);

    // Create deduplication filter
    size_t create_deduplication_filter(
      const std::vector<segment_metadata>& group, const key_statistics& stats);

    // Optimize execution order
    compaction_plan optimize_execution_order(compaction_plan plan);

private:
    hybrid_config _config;
};

} // namespace compaction_strategies
