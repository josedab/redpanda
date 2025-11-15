// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compaction_strategies/workload_aware/scheduler.h"

#include <seastar/core/coroutine.hh>

namespace compaction_strategies {

ss::future<std::vector<compaction_decision>>
intelligent_compaction_scheduler::schedule_compactions(
  const cluster_state& state) {
    std::vector<compaction_decision> decisions;

    // Analyze each partition
    for (const auto& partition : state.partitions) {
        auto metadata = co_await collect_segment_metadata(partition);
        auto workload = analyze_workload_characteristics(partition);

        // Score segments for compaction
        auto candidates = identify_compaction_candidates(metadata, workload);

        if (!candidates.empty()) {
            auto decision = make_compaction_decision(
              candidates, workload, state.resource_availability);

            decisions.push_back(decision);
        }
    }

    // Prioritize and schedule
    co_return prioritize_decisions(decisions, state.constraints);
}

ss::future<std::vector<segment_metadata>>
intelligent_compaction_scheduler::collect_segment_metadata(
  const model::ntp& partition) {
    // Placeholder implementation - would integrate with actual storage layer
    std::vector<segment_metadata> segments;
    co_return segments;
}

workload_characteristics
intelligent_compaction_scheduler::analyze_workload_characteristics(
  const model::ntp& partition) {
    // Placeholder implementation - would analyze actual metrics
    workload_characteristics wc;
    wc.pattern = access_pattern::sequential;
    wc.write_rate = 1000.0;
    wc.read_rate = 500.0;
    wc.key_cardinality = 10000.0;
    wc.has_deletes = false;
    return wc;
}

std::vector<segment_metadata>
intelligent_compaction_scheduler::identify_compaction_candidates(
  const std::vector<segment_metadata>& segments,
  const workload_characteristics& workload) {
    std::vector<segment_metadata> candidates;

    for (const auto& segment : segments) {
        auto score = calculate_compaction_score(segment, workload);

        if (should_compact(score)) {
            candidates.push_back(segment);
        }
    }

    // Group adjacent segments for efficiency
    return group_adjacent_segments(candidates);
}

double intelligent_compaction_scheduler::calculate_compaction_score(
  const segment_metadata& segment,
  const workload_characteristics& workload) {
    // Dead data ratio (primary factor)
    double dead_ratio = segment.size_bytes > 0
                          ? static_cast<double>(segment.dead_bytes)
                              / segment.size_bytes
                          : 0.0;

    // Access frequency (inverse relationship)
    double access_factor = 1.0 / (1.0 + segment.access_frequency);

    // Age factor (older segments score higher)
    auto now = std::chrono::system_clock::now();
    auto age = std::chrono::duration_cast<duration>(
      now - segment.creation_time);
    double age_factor = std::min(
      1.0, static_cast<double>(age.count()) / _max_age.count());

    // Fragmentation penalty
    double fragmentation_penalty = segment.fragmentation_ratio;

    // Workload-specific adjustments
    double workload_multiplier = 1.0;

    if (workload.pattern == access_pattern::temporal) {
        // Prioritize time-based compaction
        workload_multiplier = age_factor * 2.0;
    } else if (workload.has_deletes) {
        // Prioritize segments with tombstones
        workload_multiplier = dead_ratio * 2.0;
    }

    return (dead_ratio * _dead_ratio_weight + access_factor * _access_weight
            + age_factor * _age_weight
            + fragmentation_penalty * _fragmentation_weight)
           * workload_multiplier;
}

std::vector<segment_metadata>
intelligent_compaction_scheduler::group_adjacent_segments(
  const std::vector<segment_metadata>& candidates) {
    // Simple implementation - could be enhanced with sophisticated grouping
    return candidates;
}

compaction_decision
intelligent_compaction_scheduler::make_compaction_decision(
  const std::vector<segment_metadata>& candidates,
  const workload_characteristics& workload,
  const resource_availability& resources) {
    // Choose strategy based on workload
    auto strategy = select_compaction_strategy(workload);

    // Estimate benefit
    auto benefit = estimate_compaction_benefit(candidates);

    // Calculate priority
    auto priority = calculate_priority(benefit, resources);

    // Determine resource requirements
    auto requirements = calculate_resource_requirements(candidates, strategy);

    return {
      .ntp = candidates[0].ntp,
      .segments = extract_segment_ids(candidates),
      .priority = priority,
      .benefit = benefit,
      .strategy = strategy,
      .resources = requirements};
}

compaction_strategy
intelligent_compaction_scheduler::select_compaction_strategy(
  const workload_characteristics& workload) {
    if (workload.has_deletes && workload.key_cardinality < 10000) {
        return compaction_strategy::key_based;
    }

    if (workload.pattern == access_pattern::temporal) {
        return compaction_strategy::time_window;
    }

    if (workload.retention.type == retention_type::size_based) {
        return compaction_strategy::size_tiered;
    }

    // Default to hybrid for mixed workloads
    return compaction_strategy::hybrid;
}

estimated_benefit
intelligent_compaction_scheduler::estimate_compaction_benefit(
  const std::vector<segment_metadata>& candidates) {
    size_t total_dead_bytes = 0;
    double total_fragmentation = 0.0;

    for (const auto& seg : candidates) {
        total_dead_bytes += seg.dead_bytes;
        total_fragmentation += seg.fragmentation_ratio;
    }

    double avg_fragmentation = candidates.empty()
                                 ? 0.0
                                 : total_fragmentation / candidates.size();

    return {
      .bytes_reclaimed = total_dead_bytes,
      .fragmentation_reduction = avg_fragmentation,
      .estimated_duration = duration(candidates.size() * 1000) // 1s per
                                                                // segment
    };
}

compaction_priority
intelligent_compaction_scheduler::calculate_priority(
  const estimated_benefit& benefit,
  const resource_availability& resources) {
    // Calculate priority score based on benefit and resource availability
    double score = static_cast<double>(benefit.bytes_reclaimed)
                   / (1024.0 * 1024.0 * 1024.0); // GB

    if (score > 10.0 || benefit.fragmentation_reduction > 0.8) {
        return compaction_priority::critical;
    } else if (score > 5.0 || benefit.fragmentation_reduction > 0.5) {
        return compaction_priority::high;
    } else if (score > 1.0 || benefit.fragmentation_reduction > 0.3) {
        return compaction_priority::medium;
    } else {
        return compaction_priority::low;
    }
}

resource_requirements
intelligent_compaction_scheduler::calculate_resource_requirements(
  const std::vector<segment_metadata>& candidates,
  compaction_strategy strategy) {
    size_t total_size = 0;
    for (const auto& seg : candidates) {
        total_size += seg.size_bytes;
    }

    // Memory: need to hold key-offset map and buffers
    size_t memory = total_size / 10; // 10% of segment size

    // IO: read all segments, write compacted output
    size_t io = total_size * 2;

    // CPU: estimate based on strategy
    size_t cpu_ms = total_size / (1024 * 1024); // 1ms per MB

    if (strategy == compaction_strategy::key_based) {
        cpu_ms *= 2; // Key-based compaction is more CPU intensive
    }

    return {.memory_bytes = memory, .io_bytes = io, .cpu_time_ms = cpu_ms};
}

std::vector<segment_id>
intelligent_compaction_scheduler::extract_segment_ids(
  const std::vector<segment_metadata>& segments) {
    std::vector<segment_id> ids;
    ids.reserve(segments.size());
    for (const auto& seg : segments) {
        ids.push_back(seg.id);
    }
    return ids;
}

std::vector<compaction_decision>
intelligent_compaction_scheduler::prioritize_decisions(
  std::vector<compaction_decision> decisions,
  const cluster_state::constraints& constraints) {
    // Sort by priority (highest first)
    std::sort(
      decisions.begin(),
      decisions.end(),
      [](const auto& a, const auto& b) { return a.priority > b.priority; });

    // Limit to max concurrent compactions
    if (decisions.size() > constraints.max_concurrent_compactions) {
        decisions.resize(constraints.max_concurrent_compactions);
    }

    return decisions;
}

} // namespace compaction_strategies
