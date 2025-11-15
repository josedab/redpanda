// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "compaction_strategies/base/types.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

#include <chrono>

namespace compaction_strategies {

// Compaction request for incremental processing
struct compaction_request {
    model::ntp ntp;
    std::vector<segment_id> segments;
};

// Options for incremental compaction
struct compaction_options {
    duration max_run_duration{std::chrono::seconds(60)};
    double pause_threshold{0.8}; // Pause if foreground load > 80%
    size_t base_chunk_size{1048576}; // 1MB
    size_t min_chunk_size{65536}; // 64KB
    size_t max_chunk_size{16777216}; // 16MB
    double min_progress_rate{1000.0}; // bytes/sec
    double max_progress_rate{100000000.0}; // bytes/sec
    size_t min_resources{1048576}; // 1MB
};

// Checkpoint manager for crash recovery
class checkpoint_manager {
public:
    ss::future<> save(const checkpoint& cp);
    ss::future<std::optional<checkpoint>> load(segment_id segment);
};

// Memory monitor for resource management
class memory_monitor {
public:
    double pressure() const { return _pressure; }

private:
    double _pressure{0.0};
};

// Metrics collector for compaction
class metrics_collector {
public:
    double foreground_load() const { return _foreground_load; }

private:
    double _foreground_load{0.0};
};

// Incremental compaction engine that processes data in chunks
class incremental_compaction_engine {
public:
    incremental_compaction_engine()
      : _checkpoint_manager()
      , _memory_monitor()
      , _metrics() {}

    // Compact incrementally with pause/resume support
    ss::future<> compact_incrementally(
      const compaction_request& request, compaction_options opts = {});

    // Resume from a checkpoint
    ss::future<> resume_from_checkpoint(segment_id segment);

    // Check if compaction is paused
    bool is_paused() const { return _current_state.has_value() && _current_state->is_paused; }

private:
    // Initialize compaction state
    compaction_state initialize_compaction_state(const compaction_request& request);

    // Process a chunk of data
    ss::future<> process_chunk(compaction_state& state, size_t chunk_size);

    // Update progress tracking
    void update_progress(compaction_state& state);

    // Check if compaction should pause
    bool
    should_pause(const compaction_state& state, const compaction_options& opts);

    // Pause compaction and wait
    ss::future<> pause_compaction(compaction_state& state);

    // Calculate optimal chunk size
    size_t calculate_chunk_size(
      const compaction_state& state, const compaction_options& opts);

    // Calculate progress rate
    double calculate_progress_rate(const compaction_state& state);

    // Calculate backoff duration
    duration calculate_backoff(const compaction_state& state);

    // Check if sufficient resources are available
    bool has_sufficient_resources(const compaction_options& opts);

    // Wait for resources to become available
    ss::future<> wait_for_resources(size_t min_resources);

    // Save checkpoint for crash recovery
    ss::future<> save_checkpoint(const compaction_state& state);

    // Finalize compaction
    ss::future<> finalize_compaction(compaction_state& state);

private:
    checkpoint_manager _checkpoint_manager;
    memory_monitor _memory_monitor;
    metrics_collector _metrics;
    std::optional<compaction_state> _current_state;
};

} // namespace compaction_strategies
