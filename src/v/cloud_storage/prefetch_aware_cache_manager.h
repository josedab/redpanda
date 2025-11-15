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

#include "cloud_storage/adaptive_prefetch_scheduler.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/flat_hash_map.h>

#include <chrono>
#include <optional>

namespace cloud_storage {

/// Simple segment data representation
struct segment_data {
    size_t segment_id;
    size_t size_bytes;
    // In a real implementation, this would contain actual segment data
};

/// Cache manager with prefetch awareness
class prefetch_aware_cache_manager {
public:
    struct cache_entry {
        size_t id;
        ss::lw_shared_ptr<const segment_data> data;
        std::chrono::steady_clock::time_point last_access;
        std::chrono::steady_clock::time_point prefetch_time;
        bool was_prefetched;
        bool was_used;
        priority priority;
        size_t access_count;
    };

    struct cache_metrics {
        size_t hits = 0;
        size_t misses = 0;
    };

    struct prefetch_metrics {
        size_t total_prefetched = 0;
        size_t useful_prefetches = 0;
        size_t wasted_prefetches = 0;
    };

    explicit prefetch_aware_cache_manager(size_t max_size);

    /// Insert data into cache
    ss::future<> insert(
      size_t id,
      ss::lw_shared_ptr<const segment_data> data,
      priority prio,
      bool is_prefetch = true);

    /// Get data from cache
    std::optional<ss::lw_shared_ptr<const segment_data>> get(size_t id);

    /// Check if segment is in cache
    bool contains(size_t id) const { return _entries.contains(id); }

    /// Get current cache size
    size_t size() const { return _current_size; }

    /// Get cache metrics
    const cache_metrics& get_cache_metrics() const { return _metrics; }

    /// Get prefetch metrics
    const prefetch_metrics& get_prefetch_metrics() const {
        return _prefetch_metrics;
    }

private:
    ss::future<> evict_least_valuable();

    static double calculate_recency_score(const cache_entry& entry);

    double calculate_value(const cache_entry& entry) const;

    size_t _max_size;
    size_t _current_size = 0;
    absl::flat_hash_map<size_t, cache_entry> _entries;
    cache_metrics _metrics;
    prefetch_metrics _prefetch_metrics;
};

} // namespace cloud_storage
