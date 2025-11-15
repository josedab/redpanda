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

#include <algorithm>
#include <chrono>
#include <vector>

namespace compaction_strategies {

// Cluster state for scheduling decisions
struct cluster_state {
    std::vector<model::ntp> partitions;
    resource_availability resource_availability;

    // Constraints for compaction scheduling
    struct constraints {
        size_t max_concurrent_compactions{2};
        size_t max_memory_per_compaction{1073741824}; // 1GB
        double max_cpu_utilization{0.8};
    } constraints;
};

// Intelligent compaction scheduler that analyzes workload patterns
class intelligent_compaction_scheduler {
public:
    explicit intelligent_compaction_scheduler()
      : _max_age(std::chrono::hours(7 * 24))
      , _dead_ratio_weight(0.4)
      , _access_weight(0.2)
      , _age_weight(0.2)
      , _fragmentation_weight(0.2) {}

    // Schedule compactions based on cluster state and workload analysis
    ss::future<std::vector<compaction_decision>>
    schedule_compactions(const cluster_state& state);

private:
    // Collect metadata for segments in a partition
    ss::future<std::vector<segment_metadata>>
    collect_segment_metadata(const model::ntp& partition);

    // Analyze workload characteristics for a partition
    workload_characteristics
    analyze_workload_characteristics(const model::ntp& partition);

    // Identify segments that are candidates for compaction
    std::vector<segment_metadata> identify_compaction_candidates(
      const std::vector<segment_metadata>& segments,
      const workload_characteristics& workload);

    // Calculate compaction score for a segment
    double calculate_compaction_score(
      const segment_metadata& segment,
      const workload_characteristics& workload);

    // Group adjacent segments for efficient compaction
    std::vector<segment_metadata>
    group_adjacent_segments(const std::vector<segment_metadata>& candidates);

    // Make compaction decision for candidate segments
    compaction_decision make_compaction_decision(
      const std::vector<segment_metadata>& candidates,
      const workload_characteristics& workload,
      const resource_availability& resources);

    // Select appropriate compaction strategy based on workload
    compaction_strategy
    select_compaction_strategy(const workload_characteristics& workload);

    // Estimate benefit of compaction
    estimated_benefit estimate_compaction_benefit(
      const std::vector<segment_metadata>& candidates);

    // Calculate priority based on benefit and resources
    compaction_priority calculate_priority(
      const estimated_benefit& benefit,
      const resource_availability& resources);

    // Calculate resource requirements for compaction
    resource_requirements calculate_resource_requirements(
      const std::vector<segment_metadata>& candidates,
      compaction_strategy strategy);

    // Extract segment IDs from metadata
    std::vector<segment_id>
    extract_segment_ids(const std::vector<segment_metadata>& segments);

    // Prioritize compaction decisions
    std::vector<compaction_decision> prioritize_decisions(
      std::vector<compaction_decision> decisions,
      const cluster_state::constraints& constraints);

    // Determine if segment should be compacted based on score
    bool should_compact(double score) const {
        return score > _compaction_threshold;
    }

private:
    duration _max_age;
    double _dead_ratio_weight;
    double _access_weight;
    double _age_weight;
    double _fragmentation_weight;
    static constexpr double _compaction_threshold = 0.5;
};

} // namespace compaction_strategies
