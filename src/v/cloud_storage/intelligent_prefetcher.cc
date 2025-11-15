/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/intelligent_prefetcher.h"

#include "config/configuration.h"

namespace cloud_storage {

intelligent_prefetcher::intelligent_prefetcher(
  model::ntp ntp,
  size_t cache_size,
  adaptive_prefetch_scheduler::fetch_function fetch_fn)
  : _ntp(std::move(ntp))
  , _cache_manager(cache_size)
  , _scheduler(_cache_manager, std::move(fetch_fn))
  , _metrics(_ntp) {}

ss::future<> intelligent_prefetcher::record_access(
  model::offset offset, size_t segment_id, size_t size_bytes) {
    if (!_enabled) {
        co_return;
    }

    // Record the access event
    access_pattern_collector::access_event event{
      .ntp = _ntp,
      .offset = offset,
      .size_bytes = size_bytes,
      .timestamp = std::chrono::steady_clock::now(),
      .direction = read_direction::forward, // Simplified for now
      .client_id = "default",
      .type = access_type::sequential,
    };

    _pattern_collector.record_access(event);
    _access_count++;

    // Update Markov chain
    if (_access_count > 1) {
        markov_predictor::state from{offset, segment_id};
        markov_predictor::state to{
          model::offset(offset() + size_bytes), segment_id + 1};
        _predictor.update(from, to);
    }

    // Maybe trigger prefetch
    if (_access_count >= min_samples_for_prediction) {
        co_await maybe_trigger_prefetch();
    }
}

ss::future<> intelligent_prefetcher::maybe_trigger_prefetch() {
    // Extract features and recognize pattern
    auto features = _pattern_collector.extract_features();
    auto pattern = _pattern_engine.recognize(features);

    _metrics.record_pattern_detected();
    _metrics.record_prediction(pattern.confidence);

    // Check confidence threshold
    auto threshold
      = config::shard_local_cfg().cloud_storage_confidence_threshold();
    if (pattern.confidence < threshold) {
        co_return;
    }

    // Get prediction from Markov chain
    auto last_event = _pattern_collector.get_history().back();
    markov_predictor::state current{
      last_event.offset,
      0}; // Simplified segment ID

    auto depth = config::shard_local_cfg().cloud_storage_prefetch_depth();
    auto prediction = _predictor.predict(current, depth);

    // Schedule prefetches
    resource_constraints constraints{
      .memory_budget
      = config::shard_local_cfg().cloud_storage_prefetch_memory_budget(),
      .max_concurrent
      = config::shard_local_cfg().cloud_storage_prefetch_max_concurrent(),
      .memory_pressure = 0.0,
    };

    co_await _scheduler.schedule_prefetch(prediction, constraints);
}

} // namespace cloud_storage
