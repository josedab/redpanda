/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "raft/parallel/parallel_replication_pipeline.h"

#include <seastar/core/coroutine.hh>

namespace raft::parallel {

ss::future<std::vector<result<replicate_result>>>
parallel_replication_pipeline::replicate_parallel(
  std::vector<model::record_batch> batches,
  replicate_options opts,
  replicate_fn replicator) {
    // Build dependency graph
    _dependency_graph.clear();
    std::vector<batch_id> batch_ids;
    batch_ids.reserve(batches.size());

    for (const auto& batch : batches) {
        auto id = _dependency_graph.analyze_batch(batch);
        batch_ids.push_back(id);
    }

    // Identify parallel groups
    auto groups = _dependency_graph.identify_parallel_groups();

    // Results vector (will be filled in order)
    std::vector<result<replicate_result>> results;
    results.resize(batches.size());

    // Process each level in parallel
    for (const auto& level : groups) {
        auto level_results = co_await process_level_parallel(
          level, batches, opts, replicator);

        // Map results back to original positions
        for (size_t i = 0; i < level.size(); ++i) {
            auto batch_id = level[i];
            // Find the index of this batch_id in the original batch_ids vector
            auto it = std::find(batch_ids.begin(), batch_ids.end(), batch_id);
            if (it != batch_ids.end()) {
                size_t idx = std::distance(batch_ids.begin(), it);
                results[idx] = std::move(level_results[i]);
            }
        }
    }

    co_return results;
}

ss::future<std::vector<result<replicate_result>>>
parallel_replication_pipeline::process_level_parallel(
  const batch_group& level,
  const std::vector<model::record_batch>& batches,
  replicate_options opts,
  replicate_fn& replicator) {
    // Limit parallelism
    size_t parallelism = std::min(level.size(), _config.max_parallel_operations);
    auto units = co_await _parallelism_sem.get_units(parallelism);

    // Execute batches in parallel
    std::vector<ss::future<result<replicate_result>>> futures;
    futures.reserve(level.size());

    for (auto batch_id : level) {
        // Find the batch metadata
        auto* metadata = _dependency_graph.get_metadata(batch_id);
        if (!metadata) {
            futures.push_back(
              ss::make_ready_future<result<replicate_result>>(
                errc::invalid_configuration_update));
            continue;
        }

        // Find the actual batch by matching offset
        // This is a simplified approach - in production, we'd maintain
        // a better mapping between batch_id and the actual batch
        std::optional<model::record_batch> found_batch;
        for (const auto& batch : batches) {
            if (batch.base_offset() == metadata->base_offset) {
                found_batch = batch.copy();
                break;
            }
        }

        if (!found_batch) {
            futures.push_back(
              ss::make_ready_future<result<replicate_result>>(
                errc::invalid_configuration_update));
            continue;
        }

        futures.push_back(replicate_single(
          std::move(*found_batch), opts, replicator));
    }

    // Wait for all futures
    auto results = co_await ss::when_all_succeed(futures.begin(), futures.end());

    co_return results;
}

ss::future<result<replicate_result>>
parallel_replication_pipeline::replicate_single(
  model::record_batch batch,
  replicate_options opts,
  replicate_fn& replicator) {
    // Check for duplicates (idempotency)
    auto dup_result = co_await check_idempotency(batch);
    if (dup_result.has_value()) {
        co_return *dup_result;
    }

    // Assign logical timestamp
    auto ts = _linearization_coordinator.assign_timestamp();

    // Perform replication
    auto result = co_await replicator(std::move(batch), opts);

    if (result.has_value()) {
        // Update idempotency tracking
        auto producer_id = batch.header().producer_id;
        auto producer_epoch = batch.header().producer_epoch;
        if (producer_id >= 0) {
            producer_identity pid{producer_id, producer_epoch};
            co_await _idempotency_tracker.update_producer_state(
              pid,
              batch.header().base_sequence,
              result.value().last_offset,
              result.value().last_term);
        }

        // Submit for linearization
        co_await _linearization_coordinator.submit_commit(
          result.value().last_offset,
          result.value().last_term,
          ts);
    }

    co_return result;
}

ss::future<std::optional<result<replicate_result>>>
parallel_replication_pipeline::check_idempotency(
  const model::record_batch& batch) {
    auto producer_id = batch.header().producer_id;
    auto producer_epoch = batch.header().producer_epoch;

    if (producer_id < 0) {
        // No producer ID - not idempotent
        co_return std::nullopt;
    }

    producer_identity pid{producer_id, producer_epoch};
    auto duplicate_offset = _idempotency_tracker.is_duplicate(
      pid, batch.header().base_sequence);

    if (duplicate_offset.has_value()) {
        // This is a duplicate - return the previous result
        auto state = _idempotency_tracker.get_producer_state(pid);
        if (state.has_value()) {
            co_return result<replicate_result>(replicate_result{
              .last_offset = state->last_offset,
              .last_term = state->last_term,
            });
        }
    }

    co_return std::nullopt;
}

bool parallel_replication_pipeline::should_use_parallel(
  const model::record_batch& batch) const {
    // Don't use parallel replication for small batches
    if (batch.size_bytes() < static_cast<int32_t>(_config.batch_size_threshold)) {
        return false;
    }

    // Don't use parallel for control batches
    if (batch.header().attrs.is_control()) {
        return false;
    }

    return true;
}

void parallel_replication_pipeline::update_config(config new_cfg) {
    _config = new_cfg;
    // Note: We can't resize the semaphore, so this only affects future
    // operations
}

void parallel_replication_pipeline::set_current_term(model::term_id term) {
    _linearization_coordinator.update_epoch(term);
}

void parallel_replication_pipeline::clear() {
    _dependency_graph.clear();
    _linearization_coordinator.clear();
    _idempotency_tracker.clear();
}

parallel_replication_pipeline::stats
parallel_replication_pipeline::get_stats() const {
    return stats{
      .pending_commits = _linearization_coordinator.pending_count(),
      .tracked_producers = _idempotency_tracker.tracked_producers(),
      .active_parallel_ops = _config.max_parallel_operations
                             - _parallelism_sem.available_units(),
    };
}

} // namespace raft::parallel
