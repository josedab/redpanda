// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <chrono>
#include <cstddef>

namespace compaction_strategies {

// Advanced compaction configuration options
struct advanced_compaction_config {
    // Workload-aware scheduling
    bool workload_aware_enabled{true};

    // Incremental compaction
    bool incremental_enabled{true};
    size_t incremental_chunk_size{1048576}; // 1MB
    std::chrono::seconds incremental_max_runtime{60};

    // Hybrid strategy
    bool hybrid_strategy_enabled{true};

    // Cloud-native compaction
    bool cloud_native_enabled{true};
    size_t cloud_min_segment_size{134217728}; // 128MB

    // Adaptive tuning
    bool adaptive_tuning_enabled{true};

    // Compaction thresholds
    double dead_ratio_threshold{0.5};
    size_t min_segments_for_compaction{3};
    size_t max_concurrent_compactions{2};

    friend std::ostream&
    operator<<(std::ostream& o, const advanced_compaction_config& c) {
        fmt::print(
          o,
          "{{workload_aware: {}, incremental: {}, hybrid: {}, cloud: {}, "
          "adaptive: {}, "
          "dead_ratio: {}, min_segs: {}, max_concurrent: {}}}",
          c.workload_aware_enabled,
          c.incremental_enabled,
          c.hybrid_strategy_enabled,
          c.cloud_native_enabled,
          c.adaptive_tuning_enabled,
          c.dead_ratio_threshold,
          c.min_segments_for_compaction,
          c.max_concurrent_compactions);
        return o;
    }
};

} // namespace compaction_strategies
