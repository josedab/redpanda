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

#include "model/fundamental.h"
#include "utils/fragmented_vector.h"

#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>

#include <chrono>
#include <optional>
#include <vector>

namespace cloud_storage {

enum class read_direction {
    forward,
    backward,
    random
};

enum class access_type {
    sequential,
    random,
    replay,
    strided
};

/// Statistics calculator for online mean and variance computation
class online_statistics {
public:
    void update(double value) {
        _count++;
        double delta = value - _mean;
        _mean += delta / _count;
        double delta2 = value - _mean;
        _m2 += delta * delta2;
    }

    double mean() const { return _count > 0 ? _mean : 0.0; }

    double variance() const {
        return _count > 1 ? _m2 / (_count - 1) : 0.0;
    }

    double stddev() const { return std::sqrt(variance()); }

    size_t count() const { return _count; }

private:
    size_t _count = 0;
    double _mean = 0.0;
    double _m2 = 0.0;
};

/// Collects and analyzes access patterns for predictive prefetching
class access_pattern_collector {
public:
    struct access_event {
        model::ntp ntp;
        model::offset offset;
        size_t size_bytes;
        std::chrono::steady_clock::time_point timestamp;
        read_direction direction;
        ss::sstring client_id;
        access_type type;
    };

    struct pattern_features {
        double inter_arrival_mean;
        double inter_arrival_stddev;
        double stride_mean;
        double stride_stddev;
        double temporal_locality;
        double spatial_locality;
        size_t sequence_length;
        read_direction dominant_direction;
    };

    access_pattern_collector();

    /// Record a new access event
    void record_access(const access_event& event);

    /// Extract features from collected access history
    pattern_features extract_features() const;

    /// Get access history for analysis
    const fragmented_vector<access_event>& get_history() const {
        return _access_history;
    }

    /// Clear history
    void clear() {
        _access_history.clear();
        _last_access.reset();
    }

private:
    void update_statistics(const access_event& event);
    double calculate_temporal_locality() const;
    double calculate_spatial_locality() const;
    double calculate_inter_arrival_mean() const;
    double calculate_inter_arrival_stddev() const;
    double calculate_stride_mean() const;
    double calculate_stride_stddev() const;
    read_direction determine_dominant_direction() const;

    static constexpr size_t max_history_size = 1000;
    static constexpr size_t min_pattern_size = 10;

    fragmented_vector<access_event> _access_history;
    std::optional<access_event> _last_access;
    online_statistics _inter_arrival_stats;
    online_statistics _stride_stats;
    double _temporal_clustering_coefficient = 0.0;
    double _spatial_clustering_coefficient = 0.0;
};

} // namespace cloud_storage
