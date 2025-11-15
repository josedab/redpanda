/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/markov_predictor.h"

#include <algorithm>
#include <cmath>

namespace cloud_storage {

markov_predictor::markov_predictor() = default;

void markov_predictor::update(const state& from, const state& to) {
    _transition_counts[from][to]++;
    _state_counts[from]++;

    // Periodically decay old transitions
    if (++_update_count % decay_interval == 0) {
        apply_decay();
    }
}

markov_predictor::prediction
markov_predictor::predict(const state& current, size_t k) {
    auto it = _transition_counts.find(current);
    if (it == _transition_counts.end()) {
        return {.confidence = 0.0};
    }

    // Calculate transition probabilities
    std::vector<std::pair<state, double>> transitions;
    double total_count = _state_counts[current];

    if (total_count == 0.0) {
        return {.confidence = 0.0};
    }

    for (const auto& [next_state, count] : it->second) {
        double probability = count / total_count;
        transitions.emplace_back(next_state, probability);
    }

    // Sort by probability and take top-k
    size_t num_to_take = std::min(k, transitions.size());
    std::partial_sort(
      transitions.begin(),
      transitions.begin() + num_to_take,
      transitions.end(),
      [](const auto& a, const auto& b) { return a.second > b.second; });

    // Build prediction
    prediction pred;
    pred.next_states.reserve(num_to_take);
    pred.probabilities.reserve(num_to_take);

    for (size_t i = 0; i < num_to_take; ++i) {
        pred.next_states.push_back(transitions[i].first);
        pred.probabilities.push_back(transitions[i].second);
    }

    // Calculate confidence based on entropy
    pred.confidence = calculate_confidence(pred.probabilities);

    return pred;
}

void markov_predictor::clear() {
    _transition_counts.clear();
    _state_counts.clear();
    _update_count = 0;
}

void markov_predictor::apply_decay() {
    // Decay all transition counts
    for (auto& [from_state, transitions] : _transition_counts) {
        auto it = transitions.begin();
        while (it != transitions.end()) {
            it->second *= decay_factor;
            if (it->second < min_count_threshold) {
                it = transitions.erase(it);
            } else {
                ++it;
            }
        }
    }

    // Decay state counts
    for (auto& [state, count] : _state_counts) {
        count *= decay_factor;
    }
}

double markov_predictor::calculate_confidence(
  const std::vector<double>& probs) const {
    if (probs.empty()) {
        return 0.0;
    }

    // Use normalized entropy as confidence measure
    // Low entropy (concentrated distribution) = high confidence
    double entropy = 0.0;
    for (double p : probs) {
        if (p > 0) {
            entropy -= p * std::log2(p);
        }
    }

    double max_entropy = std::log2(static_cast<double>(probs.size()));
    if (max_entropy == 0.0) {
        return 1.0;
    }

    return 1.0 - (entropy / max_entropy);
}

} // namespace cloud_storage
