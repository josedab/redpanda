// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compaction_strategies/incremental/engine.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

namespace compaction_strategies {

ss::future<> checkpoint_manager::save(const checkpoint& cp) {
    // Placeholder - would persist to disk
    co_return;
}

ss::future<std::optional<checkpoint>>
checkpoint_manager::load(segment_id segment) {
    // Placeholder - would load from disk
    co_return std::nullopt;
}

ss::future<> incremental_compaction_engine::compact_incrementally(
  const compaction_request& request, compaction_options opts) {
    // Initialize state
    auto state = initialize_compaction_state(request);
    _current_state = state;

    // Process in chunks
    while (!state.is_complete()) {
        // Check if we should pause
        if (should_pause(state, opts)) {
            co_await pause_compaction(state);
            continue;
        }

        // Process next chunk
        auto chunk_size = calculate_chunk_size(state, opts);
        co_await process_chunk(state, chunk_size);

        // Update progress
        update_progress(state);

        // Yield to foreground operations
        co_await ss::yield();

        // Check resource availability
        if (!has_sufficient_resources(opts)) {
            co_await wait_for_resources(opts.min_resources);
        }
    }

    // Finalize compaction
    co_await finalize_compaction(state);
    _current_state.reset();
}

compaction_state incremental_compaction_engine::initialize_compaction_state(
  const compaction_request& request) {
    compaction_state state;
    state.source_segment = request.segments.empty()
                             ? segment_id(0)
                             : request.segments[0];
    state.bytes_processed = 0;
    state.bytes_total = 1048576; // Placeholder - would get actual size
    state.last_offset = model::offset(0);
    state.is_paused = false;
    state.started_at = std::chrono::steady_clock::now();
    state.last_progress = state.started_at;
    return state;
}

ss::future<>
incremental_compaction_engine::process_chunk(compaction_state& state, size_t chunk_size) {
    // Placeholder implementation - would:
    // 1. Read chunk from source
    // 2. Filter dead records
    // 3. Write to new segment
    // 4. Update state

    state.bytes_processed += chunk_size;
    state.last_offset = state.last_offset + model::offset(100);
    state.last_progress = std::chrono::steady_clock::now();

    co_return;
}

void incremental_compaction_engine::update_progress(compaction_state& state) {
    // Update progress metrics
    state.last_progress = std::chrono::steady_clock::now();
}

bool incremental_compaction_engine::should_pause(
  const compaction_state& state, const compaction_options& opts) {
    auto now = std::chrono::steady_clock::now();

    // Pause if running too long
    if (now - state.started_at > opts.max_run_duration) {
        return true;
    }

    // Pause if foreground load is high
    if (_metrics.foreground_load() > opts.pause_threshold) {
        return true;
    }

    // Pause if memory pressure
    if (_memory_monitor.pressure() > 0.8) {
        return true;
    }

    return false;
}

ss::future<>
incremental_compaction_engine::pause_compaction(compaction_state& state) {
    state.is_paused = true;

    // Save state for resumption
    co_await save_checkpoint(state);

    // Calculate backoff
    auto backoff = calculate_backoff(state);

    // Wait before resuming
    co_await ss::sleep(backoff);

    state.is_paused = false;
}

size_t incremental_compaction_engine::calculate_chunk_size(
  const compaction_state& state, const compaction_options& opts) {
    // Base chunk size
    size_t base_size = opts.base_chunk_size;

    // Adjust based on progress rate
    auto progress_rate = calculate_progress_rate(state);
    if (progress_rate < opts.min_progress_rate) {
        // Reduce chunk size if too slow
        base_size /= 2;
    } else if (progress_rate > opts.max_progress_rate) {
        // Increase chunk size if too fast
        base_size *= 2;
    }

    // Apply limits
    return std::clamp(base_size, opts.min_chunk_size, opts.max_chunk_size);
}

double incremental_compaction_engine::calculate_progress_rate(
  const compaction_state& state) {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
      now - state.last_progress);

    if (elapsed.count() == 0) {
        return 0.0;
    }

    return static_cast<double>(state.bytes_processed) / elapsed.count();
}

duration incremental_compaction_engine::calculate_backoff(
  const compaction_state& state) {
    // Simple exponential backoff
    return duration(1000); // 1 second
}

bool incremental_compaction_engine::has_sufficient_resources(
  const compaction_options& opts) {
    // Check if resources are available
    return _memory_monitor.pressure() < 0.8;
}

ss::future<>
incremental_compaction_engine::wait_for_resources(size_t min_resources) {
    // Wait for resources to become available
    co_await ss::sleep(std::chrono::milliseconds(100));
}

ss::future<>
incremental_compaction_engine::save_checkpoint(const compaction_state& state) {
    // Persist state for crash recovery
    checkpoint cp{
      .segment_id = state.source_segment,
      .last_offset = state.last_offset,
      .bytes_processed = state.bytes_processed,
      .timestamp = std::chrono::steady_clock::now()};

    co_await _checkpoint_manager.save(cp);
}

ss::future<>
incremental_compaction_engine::resume_from_checkpoint(segment_id segment) {
    auto checkpoint = co_await _checkpoint_manager.load(segment);

    if (checkpoint) {
        compaction_state state;
        state.source_segment = checkpoint->segment_id;
        state.last_offset = checkpoint->last_offset;
        state.bytes_processed = checkpoint->bytes_processed;
        state.started_at = std::chrono::steady_clock::now();
        state.last_progress = state.started_at;

        compaction_request request;
        request.segments.push_back(segment);

        // Resume compaction
        co_await compact_incrementally(request);
    }
}

ss::future<>
incremental_compaction_engine::finalize_compaction(compaction_state& state) {
    // Finalize compaction - would:
    // 1. Close output segments
    // 2. Update metadata
    // 3. Delete old segments
    co_return;
}

} // namespace compaction_strategies
