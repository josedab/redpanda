/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/prefetch_aware_cache_manager.h"

#include <algorithm>
#include <cmath>

namespace cloud_storage {

prefetch_aware_cache_manager::prefetch_aware_cache_manager(size_t max_size)
  : _max_size(max_size) {}

ss::future<> prefetch_aware_cache_manager::insert(
  size_t id,
  ss::lw_shared_ptr<const segment_data> data,
  priority prio,
  bool is_prefetch) {
    // Check if already in cache
    if (_entries.contains(id)) {
        co_return;
    }

    // Evict until we have space
    while (_current_size + data->size_bytes > _max_size) {
        co_await evict_least_valuable();
    }

    // Insert new entry
    using clock = std::chrono::steady_clock;
    _entries[id] = {
      .id = id,
      .data = data,
      .last_access = clock::now(),
      .prefetch_time = is_prefetch ? clock::now() : clock::time_point{},
      .was_prefetched = is_prefetch,
      .was_used = false,
      .priority = prio,
      .access_count = 0};

    _current_size += data->size_bytes;

    if (is_prefetch) {
        _prefetch_metrics.total_prefetched++;
    }
}

std::optional<ss::lw_shared_ptr<const segment_data>>
prefetch_aware_cache_manager::get(size_t id) {
    auto it = _entries.find(id);
    if (it == _entries.end()) {
        _metrics.misses++;
        return std::nullopt;
    }

    // Update access statistics
    using clock = std::chrono::steady_clock;
    it->second.last_access = clock::now();
    it->second.access_count++;

    if (it->second.was_prefetched && !it->second.was_used) {
        it->second.was_used = true;
        _prefetch_metrics.useful_prefetches++;
    }

    _metrics.hits++;
    return it->second.data;
}

ss::future<> prefetch_aware_cache_manager::evict_least_valuable() {
    if (_entries.empty()) {
        co_return;
    }

    // Find entry with minimum value
    auto victim = std::min_element(
      _entries.begin(),
      _entries.end(),
      [this](const auto& a, const auto& b) {
          return calculate_value(a.second) < calculate_value(b.second);
      });

    if (victim != _entries.end()) {
        _current_size -= victim->second.data->size_bytes;

        if (victim->second.was_prefetched && !victim->second.was_used) {
            _prefetch_metrics.wasted_prefetches++;
        }

        _entries.erase(victim);
    }
}

double prefetch_aware_cache_manager::calculate_recency_score(
  const cache_entry& entry) {
    using clock = std::chrono::steady_clock;
    auto age = clock::now() - entry.last_access;
    auto age_seconds = std::chrono::duration_cast<std::chrono::seconds>(age)
                         .count();

    // Exponential decay: more recent accesses have higher scores
    return std::exp(-static_cast<double>(age_seconds) / 60.0); // 60s half-life
}

double
prefetch_aware_cache_manager::calculate_value(const cache_entry& entry) const {
    // Calculate value score for cache entry
    double recency_score = calculate_recency_score(entry);
    double frequency_score = static_cast<double>(entry.access_count);
    double prefetch_score = entry.was_prefetched ? 0.5 : 1.0;
    double usage_score = entry.was_used ? 1.0 : 0.2;
    double priority_score = static_cast<double>(entry.priority) + 1.0;

    return recency_score * frequency_score * prefetch_score * usage_score
           * priority_score;
}

} // namespace cloud_storage
