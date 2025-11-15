/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/pattern_recognition_engine.h"

#include <algorithm>
#include <cmath>

namespace cloud_storage {

pattern_recognition_engine::pattern_recognition_engine() = default;

recognized_pattern pattern_recognition_engine::recognize(
  const access_pattern_collector::pattern_features& features) {
    // Not enough data for pattern recognition
    if (features.sequence_length < 10) {
        return {
          .type = pattern_type::unknown, .confidence = 0.0, .params = {}};
    }

    // Decision tree classification
    // First, check for sequential patterns (most common)
    if (features.spatial_locality > sequential_locality_threshold
        && features.stride_stddev < sequential_stride_threshold) {
        return refine_sequential_pattern(features);
    }

    // Check for strided patterns
    if (features.stride_mean > 0
        && features.stride_stddev
             < features.stride_mean * strided_variance_ratio) {
        return refine_strided_pattern(features);
    }

    // Check for periodic patterns
    if (features.temporal_locality > 0.7) {
        return refine_periodic_pattern(features);
    }

    // Random access pattern
    return {.type = pattern_type::random, .confidence = 0.5, .params = {}};
}

recognized_pattern pattern_recognition_engine::refine_sequential_pattern(
  const access_pattern_collector::pattern_features& features) {
    // Determine direction
    pattern_type type = features.dominant_direction == read_direction::forward
                          ? pattern_type::sequential_forward
                          : pattern_type::sequential_backward;

    // High confidence for clear sequential patterns
    double confidence = std::min(
      0.95, features.spatial_locality * (1.0 - features.stride_stddev));

    return {
      .type = type,
      .confidence = confidence,
      .params = {
        .stride = static_cast<size_t>(std::abs(features.stride_mean)),
        .lookahead = calculate_optimal_lookahead(features)}};
}

recognized_pattern pattern_recognition_engine::refine_strided_pattern(
  const access_pattern_collector::pattern_features& features) {
    auto stride = static_cast<size_t>(std::abs(features.stride_mean));

    // Validate stride consistency
    if (!validate_stride_pattern(stride)) {
        return {.type = pattern_type::unknown, .confidence = 0.4, .params = {}};
    }

    // Calculate confidence based on stride consistency
    double variance_ratio = features.stride_stddev / features.stride_mean;
    double confidence = std::max(0.0, 0.85 * (1.0 - variance_ratio * 10.0));

    return {
      .type = pattern_type::strided,
      .confidence = confidence,
      .params = {.stride = stride, .lookahead = stride * prefetch_depth}};
}

recognized_pattern pattern_recognition_engine::refine_periodic_pattern(
  const access_pattern_collector::pattern_features& features) {
    // Simple periodic pattern detection based on temporal locality
    // In a production system, this would use FFT or autocorrelation
    double confidence = features.temporal_locality;

    if (confidence < 0.5) {
        return {.type = pattern_type::unknown, .confidence = 0.2, .params = {}};
    }

    // Estimate period from inter-arrival time
    auto period = std::chrono::milliseconds(
      static_cast<int64_t>(features.inter_arrival_mean / 1000.0));

    return {
      .type = pattern_type::temporal_periodic,
      .confidence = confidence,
      .params = {.period = period, .phase = 0.0}};
}

size_t pattern_recognition_engine::calculate_optimal_lookahead(
  const access_pattern_collector::pattern_features& features) const {
    // Base lookahead on stride and confidence
    auto base_stride = static_cast<size_t>(std::abs(features.stride_mean));

    // Adjust based on spatial locality
    double locality_factor = std::max(1.0, features.spatial_locality * 4.0);

    return static_cast<size_t>(base_stride * locality_factor * prefetch_depth);
}

bool pattern_recognition_engine::validate_stride_pattern(size_t stride) const {
    // Stride should be reasonable (not too small, not too large)
    constexpr size_t min_stride = 1024;        // 1KB
    constexpr size_t max_stride = 100 * 1024 * 1024; // 100MB

    return stride >= min_stride && stride <= max_stride;
}

} // namespace cloud_storage
