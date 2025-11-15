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

#include "metrics/metrics.h"
#include "seastarx.h"

#include <seastar/core/metrics.hh>

namespace cloud_storage {

/// Metrics for intelligent prefetching system
class prefetch_metrics {
public:
    explicit prefetch_metrics(const model::ntp& ntp);

    void setup_metrics(ss::metrics::metric_groups& groups);

    // Pattern detection metrics
    void record_pattern_detected() { _patterns_detected++; }
    void record_pattern_detection_latency(std::chrono::microseconds latency) {
        _pattern_detection_latency_us = latency.count();
    }
    void set_active_patterns(size_t count) { _active_patterns = count; }

    // Prediction metrics
    void record_prediction(double confidence) {
        _predictions_made++;
        _prediction_confidence = confidence;
    }
    void update_prediction_accuracy(double accuracy) {
        _prediction_accuracy = accuracy;
    }

    // Prefetch metrics
    void record_prefetch_hit() { _prefetch_hits++; }
    void record_prefetch_miss() { _prefetch_misses++; }
    void record_prefetch_eviction() { _prefetch_evictions++; }
    void set_prefetch_queue_size(size_t size) { _prefetch_queue_size = size; }
    void record_prefetch_latency(std::chrono::milliseconds latency) {
        _prefetch_latency_ms = latency.count();
    }

    // Resource metrics
    void set_prefetch_memory_usage(size_t bytes) {
        _prefetch_memory_usage = bytes;
    }
    void set_prefetch_bandwidth_usage(size_t bytes_per_sec) {
        _prefetch_bandwidth_usage = bytes_per_sec;
    }

private:
    model::ntp _ntp;

    // Pattern detection metrics
    uint64_t _patterns_detected = 0;
    uint64_t _pattern_detection_latency_us = 0;
    uint64_t _active_patterns = 0;

    // Prediction metrics
    uint64_t _predictions_made = 0;
    double _prediction_confidence = 0.0;
    double _prediction_accuracy = 0.0;

    // Prefetch metrics
    uint64_t _prefetch_hits = 0;
    uint64_t _prefetch_misses = 0;
    uint64_t _prefetch_evictions = 0;
    uint64_t _prefetch_queue_size = 0;
    uint64_t _prefetch_latency_ms = 0;

    // Resource metrics
    uint64_t _prefetch_memory_usage = 0;
    uint64_t _prefetch_bandwidth_usage = 0;
};

} // namespace cloud_storage
