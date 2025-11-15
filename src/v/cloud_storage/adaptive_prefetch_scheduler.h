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

#include "cloud_storage/markov_predictor.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/flat_hash_set.h>

#include <chrono>
#include <functional>

namespace cloud_storage {

class prefetch_aware_cache_manager;
class remote;

/// Priority level for prefetch requests
enum class priority : uint8_t {
    low = 0,
    medium = 1,
    high = 2,
};

/// Resource constraints for prefetching
struct resource_constraints {
    size_t memory_budget;
    size_t max_concurrent;
    double memory_pressure;
};

/// Adaptive scheduler for prefetch operations
class adaptive_prefetch_scheduler {
public:
    struct prefetch_request {
        model::ntp ntp;
        size_t segment_id;
        priority priority;
        std::chrono::steady_clock::time_point deadline;
        size_t size_estimate;
        double confidence;

        bool operator<(const prefetch_request& other) const {
            // Higher priority first
            if (priority != other.priority) {
                return static_cast<uint8_t>(priority)
                       < static_cast<uint8_t>(other.priority);
            }
            // Earlier deadline first
            return deadline > other.deadline;
        }
    };

    struct prefetch_stats {
        size_t hits = 0;
        size_t misses = 0;
        size_t evictions_before_use = 0;
        double accuracy = 0.0;
        size_t bytes_prefetched = 0;
        size_t bytes_used = 0;
    };

    using fetch_function = ss::noncopyable_function<
      ss::future<>(size_t segment_id, priority prio)>;

    adaptive_prefetch_scheduler(
      prefetch_aware_cache_manager& cache,
      fetch_function fetch_fn);

    /// Schedule prefetch based on prediction
    ss::future<> schedule_prefetch(
      const markov_predictor::prediction& pred,
      const resource_constraints& constraints);

    /// Get current statistics
    const prefetch_stats& get_stats() const { return _stats; }

    /// Record a hit (prefetched data was used)
    void record_hit() { _stats.hits++; }

    /// Record a miss (needed data was not prefetched)
    void record_miss() { _stats.misses++; }

private:
    std::vector<prefetch_request> build_prefetch_requests(
      const markov_predictor::prediction& pred);

    std::vector<prefetch_request> apply_admission_control(
      std::vector<prefetch_request> requests,
      const resource_constraints& constraints);

    ss::future<> schedule_single_prefetch(const prefetch_request& req);

    ss::future<> execute_prefetch(const prefetch_request& req);

    priority calculate_priority(double probability) const;

    std::chrono::steady_clock::time_point
    calculate_deadline(size_t position) const;

    size_t calculate_total_size(
      const std::vector<prefetch_request>& requests) const;

    prefetch_stats _stats;
    absl::flat_hash_set<size_t> _active_prefetches;
    prefetch_aware_cache_manager& _cache_manager;
    fetch_function _fetch_fn;
};

} // namespace cloud_storage
