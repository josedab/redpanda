// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "model/fundamental.h"
#include "model/record.h"

#include <absl/container/flat_hash_map.h>

#include <chrono>
#include <optional>
#include <vector>

namespace redpanda::compute {

// Intelligent caching layer for compute nodes
class compute_cache {
public:
    explicit compute_cache(size_t max_size_bytes);
    ~compute_cache();

    // Cache entry metadata
    struct cache_entry {
        model::ntp ntp;
        model::offset base_offset;
        std::vector<model::record_batch> batches;
        std::chrono::steady_clock::time_point last_access;
        size_t access_count = 0;
        size_t size_bytes = 0;
    };

    // Get records from cache
    std::optional<std::vector<model::record_batch>> get_records(
      model::ntp ntp, model::offset start_offset, size_t max_bytes);

    // Put records into cache
    void put_records(
      model::ntp ntp,
      model::offset base_offset,
      const std::vector<model::record_batch>& batches);

    // Invalidate cache entries for a partition
    void invalidate(const model::ntp& ntp);

    // Get cache statistics
    struct stats {
        size_t total_entries = 0;
        size_t current_size_bytes = 0;
        size_t max_size_bytes = 0;
        size_t hits = 0;
        size_t misses = 0;
        double hit_rate = 0.0;
    };
    stats get_stats() const;

    // Clear all cache entries
    void clear();

private:
    struct cache_key {
        model::ntp ntp;
        model::offset base_offset;

        bool operator==(const cache_key& other) const {
            return ntp == other.ntp && base_offset == other.base_offset;
        }

        template<typename H>
        friend H AbslHashValue(H h, const cache_key& key) {
            return H::combine(std::move(h), key.ntp, key.base_offset);
        }
    };

    cache_key make_cache_key(const model::ntp& ntp, model::offset offset) const;

    std::optional<std::vector<model::record_batch>> extract_records(
      const cache_entry& entry, model::offset start_offset, size_t max_bytes)
      const;

    size_t calculate_size(const std::vector<model::record_batch>& batches)
      const;

    void evict_lru();

    absl::flat_hash_map<cache_key, cache_entry> _entries;
    size_t _max_size;
    size_t _current_size = 0;
    size_t _hits = 0;
    size_t _misses = 0;
};

} // namespace redpanda::compute
