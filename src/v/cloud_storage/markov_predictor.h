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

#include <absl/container/flat_hash_map.h>

#include <vector>

namespace cloud_storage {

/// Markov chain predictor for segment access prediction
class markov_predictor {
public:
    struct state {
        model::offset offset;
        size_t segment_id;

        bool operator==(const state& other) const {
            return offset == other.offset && segment_id == other.segment_id;
        }

        template<typename H>
        friend H AbslHashValue(H h, const state& s) {
            return H::combine(std::move(h), s.offset(), s.segment_id);
        }
    };

    struct prediction {
        std::vector<state> next_states;
        std::vector<double> probabilities;
        double confidence;
    };

    markov_predictor();

    /// Update the Markov chain with a state transition
    void update(const state& from, const state& to);

    /// Predict the next k most likely states
    prediction predict(const state& current, size_t k = 3);

    /// Clear all learned transitions
    void clear();

private:
    void apply_decay();

    double calculate_confidence(const std::vector<double>& probs) const;

    static constexpr size_t decay_interval = 1000;
    static constexpr double decay_factor = 0.95;
    static constexpr double min_count_threshold = 0.01;

    absl::flat_hash_map<state, absl::flat_hash_map<state, double>>
      _transition_counts;
    absl::flat_hash_map<state, double> _state_counts;
    size_t _update_count = 0;
};

} // namespace cloud_storage
