/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/adaptive_prefetch_scheduler.h"

#include "cloud_storage/prefetch_aware_cache_manager.h"

#include <algorithm>

namespace cloud_storage {

adaptive_prefetch_scheduler::adaptive_prefetch_scheduler(
  prefetch_aware_cache_manager& cache, fetch_function fetch_fn)
  : _cache_manager(cache)
  , _fetch_fn(std::move(fetch_fn)) {}

ss::future<> adaptive_prefetch_scheduler::schedule_prefetch(
  const markov_predictor::prediction& pred,
  const resource_constraints& constraints) {
    // Convert prediction to prefetch requests
    auto requests = build_prefetch_requests(pred);

    // Apply admission control
    requests = apply_admission_control(std::move(requests), constraints);

    // Schedule all requests
    for (const auto& req : requests) {
        co_await schedule_single_prefetch(req);
    }

    // Update statistics
    _stats.bytes_prefetched += calculate_total_size(requests);
}

std::vector<adaptive_prefetch_scheduler::prefetch_request>
adaptive_prefetch_scheduler::build_prefetch_requests(
  const markov_predictor::prediction& pred) {
    std::vector<prefetch_request> requests;
    requests.reserve(pred.next_states.size());

    for (size_t i = 0; i < pred.next_states.size(); ++i) {
        requests.push_back({
          .segment_id = pred.next_states[i].segment_id,
          .priority = calculate_priority(pred.probabilities[i]),
          .deadline = calculate_deadline(i),
          .size_estimate = 16 * 1024 * 1024, // 16MB estimate
          .confidence = pred.confidence * pred.probabilities[i],
        });
    }

    return requests;
}

std::vector<adaptive_prefetch_scheduler::prefetch_request>
adaptive_prefetch_scheduler::apply_admission_control(
  std::vector<prefetch_request> requests,
  const resource_constraints& constraints) {
    // Sort by utility (priority * confidence / size)
    std::sort(
      requests.begin(), requests.end(), [](const auto& a, const auto& b) {
          auto utility_a = static_cast<double>(a.priority) * a.confidence
                           / a.size_estimate;
          auto utility_b = static_cast<double>(b.priority) * b.confidence
                           / b.size_estimate;
          return utility_a > utility_b;
      });

    // Apply constraints
    size_t total_size = 0;
    auto it = requests.begin();

    while (it != requests.end()) {
        if (total_size + it->size_estimate > constraints.memory_budget
            || _active_prefetches.size() >= constraints.max_concurrent) {
            break;
        }

        total_size += it->size_estimate;
        ++it;
    }

    requests.erase(it, requests.end());
    return requests;
}

ss::future<>
adaptive_prefetch_scheduler::schedule_single_prefetch(
  const prefetch_request& req) {
    // Check if already in cache
    if (_cache_manager.contains(req.segment_id)) {
        _stats.hits++;
        co_return;
    }

    // Check if already being prefetched
    if (_active_prefetches.contains(req.segment_id)) {
        co_return;
    }

    // Schedule prefetch
    co_await execute_prefetch(req);
}

ss::future<>
adaptive_prefetch_scheduler::execute_prefetch(const prefetch_request& req) {
    _active_prefetches.insert(req.segment_id);

    try {
        co_await _fetch_fn(req.segment_id, req.priority);
        _active_prefetches.erase(req.segment_id);
    } catch (...) {
        _stats.misses++;
        _active_prefetches.erase(req.segment_id);
        throw;
    }
}

priority
adaptive_prefetch_scheduler::calculate_priority(double probability) const {
    if (probability > 0.7) {
        return priority::high;
    } else if (probability > 0.4) {
        return priority::medium;
    } else {
        return priority::low;
    }
}

std::chrono::steady_clock::time_point
adaptive_prefetch_scheduler::calculate_deadline(size_t position) const {
    // Earlier positions get earlier deadlines
    auto delay = std::chrono::milliseconds(position * 100);
    return std::chrono::steady_clock::now() + delay;
}

size_t adaptive_prefetch_scheduler::calculate_total_size(
  const std::vector<prefetch_request>& requests) const {
    size_t total = 0;
    for (const auto& req : requests) {
        total += req.size_estimate;
    }
    return total;
}

} // namespace cloud_storage
