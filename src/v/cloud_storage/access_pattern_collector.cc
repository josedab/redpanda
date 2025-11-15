/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/access_pattern_collector.h"

#include <algorithm>
#include <cmath>

namespace cloud_storage {

access_pattern_collector::access_pattern_collector() = default;

void access_pattern_collector::record_access(const access_event& event) {
    // Maintain maximum history size
    if (_access_history.size() >= max_history_size) {
        // Remove oldest entry
        _access_history.erase(_access_history.begin());
    }

    _access_history.push_back(event);
    update_statistics(event);
}

void access_pattern_collector::update_statistics(const access_event& event) {
    if (!_last_access) {
        _last_access = event;
        return;
    }

    // Update inter-arrival time statistics
    auto delta_time = event.timestamp - _last_access->timestamp;
    _inter_arrival_stats.update(
      static_cast<double>(
        std::chrono::duration_cast<std::chrono::microseconds>(delta_time)
          .count()));

    // Update stride statistics (offset difference)
    if (event.ntp == _last_access->ntp) {
        auto stride = static_cast<double>(
          event.offset() - _last_access->offset());
        _stride_stats.update(stride);
    }

    _last_access = event;
}

access_pattern_collector::pattern_features
access_pattern_collector::extract_features() const {
    return {
      .inter_arrival_mean = calculate_inter_arrival_mean(),
      .inter_arrival_stddev = calculate_inter_arrival_stddev(),
      .stride_mean = calculate_stride_mean(),
      .stride_stddev = calculate_stride_stddev(),
      .temporal_locality = calculate_temporal_locality(),
      .spatial_locality = calculate_spatial_locality(),
      .sequence_length = _access_history.size(),
      .dominant_direction = determine_dominant_direction()};
}

double access_pattern_collector::calculate_inter_arrival_mean() const {
    return _inter_arrival_stats.mean();
}

double access_pattern_collector::calculate_inter_arrival_stddev() const {
    return _inter_arrival_stats.stddev();
}

double access_pattern_collector::calculate_stride_mean() const {
    return _stride_stats.mean();
}

double access_pattern_collector::calculate_stride_stddev() const {
    return _stride_stats.stddev();
}

double access_pattern_collector::calculate_temporal_locality() const {
    if (_access_history.size() < 2) {
        return 0.0;
    }

    // Calculate temporal clustering coefficient
    // High value means accesses are clustered in time
    double sum_reciprocal_gaps = 0.0;
    for (size_t i = 1; i < _access_history.size(); ++i) {
        auto gap = _access_history[i].timestamp
                   - _access_history[i - 1].timestamp;
        auto gap_us = std::chrono::duration_cast<std::chrono::microseconds>(
                        gap)
                        .count();
        if (gap_us > 0) {
            sum_reciprocal_gaps += 1.0 / static_cast<double>(gap_us);
        }
    }

    return sum_reciprocal_gaps / (_access_history.size() - 1);
}

double access_pattern_collector::calculate_spatial_locality() const {
    if (_access_history.size() < 2) {
        return 0.0;
    }

    // Calculate how close subsequent accesses are in offset space
    // Group by NTP and calculate locality
    absl::flat_hash_map<model::ntp, std::vector<model::offset>> ntp_offsets;
    for (const auto& event : _access_history) {
        ntp_offsets[event.ntp].push_back(event.offset);
    }

    double total_locality = 0.0;
    size_t count = 0;

    for (const auto& [ntp, offsets] : ntp_offsets) {
        if (offsets.size() < 2) {
            continue;
        }

        // Calculate average offset distance
        double sum_distances = 0.0;
        for (size_t i = 1; i < offsets.size(); ++i) {
            sum_distances += std::abs(
              static_cast<double>(offsets[i]() - offsets[i - 1]()));
        }
        double avg_distance = sum_distances / (offsets.size() - 1);

        // Convert to locality (closer = higher locality)
        // Use exponential decay: locality = exp(-distance/scale)
        constexpr double scale = 1024.0 * 1024.0; // 1MB scale
        total_locality += std::exp(-avg_distance / scale);
        count++;
    }

    return count > 0 ? total_locality / count : 0.0;
}

read_direction
access_pattern_collector::determine_dominant_direction() const {
    if (_access_history.size() < 2) {
        return read_direction::random;
    }

    int forward_count = 0;
    int backward_count = 0;

    for (size_t i = 1; i < _access_history.size(); ++i) {
        if (_access_history[i].ntp != _access_history[i - 1].ntp) {
            continue;
        }

        auto diff = _access_history[i].offset()
                    - _access_history[i - 1].offset();
        if (diff > 0) {
            forward_count++;
        } else if (diff < 0) {
            backward_count++;
        }
    }

    if (forward_count > backward_count * 2) {
        return read_direction::forward;
    } else if (backward_count > forward_count * 2) {
        return read_direction::backward;
    } else {
        return read_direction::random;
    }
}

} // namespace cloud_storage
