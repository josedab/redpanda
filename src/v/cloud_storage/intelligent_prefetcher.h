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
#include "cloud_storage/adaptive_prefetch_scheduler.h"
#include "cloud_storage/markov_predictor.h"
#include "cloud_storage/pattern_recognition_engine.h"
#include "cloud_storage/prefetch_aware_cache_manager.h"
#include "cloud_storage/prefetch_metrics.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

namespace cloud_storage {

/// Main coordinator for intelligent prefetching system
class intelligent_prefetcher {
public:
    intelligent_prefetcher(
      model::ntp ntp,
      size_t cache_size,
      adaptive_prefetch_scheduler::fetch_function fetch_fn);

    /// Record an access event and potentially trigger prefetch
    ss::future<> record_access(
      model::offset offset,
      size_t segment_id,
      size_t size_bytes);

    /// Get prefetch statistics
    const adaptive_prefetch_scheduler::prefetch_stats& get_stats() const {
        return _scheduler.get_stats();
    }

    /// Enable/disable prefetching
    void set_enabled(bool enabled) { _enabled = enabled; }

    /// Check if segment is in prefetch cache
    bool is_cached(size_t segment_id) const {
        return _cache_manager.contains(segment_id);
    }

private:
    ss::future<> maybe_trigger_prefetch();

    model::ntp _ntp;
    bool _enabled = true;

    access_pattern_collector _pattern_collector;
    pattern_recognition_engine _pattern_engine;
    markov_predictor _predictor;
    prefetch_aware_cache_manager _cache_manager;
    adaptive_prefetch_scheduler _scheduler;
    prefetch_metrics _metrics;

    size_t _access_count = 0;
    static constexpr size_t min_samples_for_prediction = 10;
};

} // namespace cloud_storage
