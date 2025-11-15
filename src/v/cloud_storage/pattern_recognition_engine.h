/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/access_pattern_collector.h"

#include <chrono>
#include <optional>

namespace cloud_storage {

enum class pattern_type {
    sequential_forward,
    sequential_backward,
    strided,
    random,
    temporal_periodic,
    replay_pattern,
    unknown
};

struct pattern_parameters {
    size_t stride = 0;
    size_t lookahead = 0;
    std::chrono::milliseconds period{0};
    double phase = 0.0;
};

struct recognized_pattern {
    pattern_type type;
    double confidence;
    pattern_parameters params;
    std::chrono::milliseconds estimated_duration{0};
};

/// Engine for recognizing access patterns
class pattern_recognition_engine {
public:
    pattern_recognition_engine();

    /// Recognize pattern from extracted features
    recognized_pattern
    recognize(const access_pattern_collector::pattern_features& features);

private:
    recognized_pattern refine_sequential_pattern(
      const access_pattern_collector::pattern_features& features);

    recognized_pattern refine_strided_pattern(
      const access_pattern_collector::pattern_features& features);

    recognized_pattern refine_periodic_pattern(
      const access_pattern_collector::pattern_features& features);

    size_t calculate_optimal_lookahead(
      const access_pattern_collector::pattern_features& features) const;

    bool validate_stride_pattern(size_t stride) const;

    static constexpr size_t prefetch_depth = 4;
    static constexpr double sequential_stride_threshold = 0.1;
    static constexpr double sequential_locality_threshold = 0.9;
    static constexpr double strided_variance_ratio = 0.1;
};

} // namespace cloud_storage
