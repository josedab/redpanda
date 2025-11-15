// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "compaction_strategies/adaptive/strategy.h"
#include "compaction_strategies/base/config.h"
#include "compaction_strategies/base/metrics.h"
#include "compaction_strategies/cloud_native/compaction.h"
#include "compaction_strategies/hybrid/strategy.h"
#include "compaction_strategies/incremental/engine.h"
#include "compaction_strategies/workload_aware/scheduler.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

namespace compaction_strategies {

/**
 * Example integration showing how to use the advanced compaction strategies
 * in the Redpanda storage layer.
 *
 * This would typically be integrated into storage::disk_log_impl or
 * storage::log_manager.
 */
class advanced_compaction_manager {
public:
    explicit advanced_compaction_manager(advanced_compaction_config config)
      : _config(std::move(config))
      , _scheduler()
      , _incremental_engine()
      , _hybrid_strategy()
      , _cloud_compactor()
      , _adaptive_strategy()
      , _metrics() {}

    // Main entry point for compaction
    ss::future<> compact_partition(const model::ntp& ntp) {
        // Step 1: Collect segment metadata
        cluster_state state;
        state.partitions.push_back(ntp);

        // Step 2: Schedule compactions using workload-aware scheduler
        auto decisions = co_await _scheduler.schedule_compactions(state);

        // Step 3: Execute compactions based on decisions
        for (const auto& decision : decisions) {
            co_await execute_compaction(decision);
        }

        // Step 4: Adapt strategy based on performance
        if (_config.adaptive_tuning_enabled) {
            co_await _adaptive_strategy.adapt_strategy(ntp);
        }

        // Step 5: Update metrics
        update_metrics(decisions);
    }

    // Execute a single compaction decision
    ss::future<> execute_compaction(const compaction_decision& decision) {
        if (!_config.incremental_enabled) {
            // Use traditional compaction
            co_return co_await execute_traditional_compaction(decision);
        }

        // Use incremental compaction
        compaction_request request;
        request.ntp = decision.ntp;
        request.segments = decision.segments;

        compaction_options opts;
        opts.base_chunk_size = _config.incremental_chunk_size;
        opts.max_run_duration = _config.incremental_max_runtime;

        co_await _incremental_engine.compact_incrementally(request, opts);
    }

    // Compact cloud segments
    ss::future<>
    compact_cloud_partition(const model::ntp& ntp, const std::vector<cloud_segment_metadata>& segments) {
        if (!_config.cloud_native_enabled) {
            co_return;
        }

        cloud_ntp cntp;
        cntp.ntp = ntp;

        cloud_config config;
        config.min_segment_size = _config.cloud_min_segment_size;

        co_await _cloud_compactor.compact_cloud_segments(cntp, segments, config);
    }

    // Create hybrid compaction plan
    ss::future<compaction_plan>
    create_compaction_plan(const std::vector<segment_metadata>& segments) {
        if (!_config.hybrid_strategy_enabled) {
            // Return empty plan
            co_return compaction_plan{};
        }

        co_return co_await _hybrid_strategy.create_hybrid_plan(segments);
    }

    // Get metrics
    metrics::advanced_compaction_metrics& metrics() { return _metrics; }

private:
    ss::future<>
    execute_traditional_compaction(const compaction_decision& decision) {
        // Placeholder - would call existing compaction logic
        co_return;
    }

    void update_metrics(const std::vector<compaction_decision>& decisions) {
        _metrics.scheduling().set_pending_compactions(decisions.size());

        for (const auto& decision : decisions) {
            double score = static_cast<double>(decision.benefit.bytes_reclaimed)
                           / 1073741824.0; // GB
            _metrics.scheduling().record_compaction_score(score);
        }

        _metrics.scheduling().increment_decisions_made();
    }

private:
    advanced_compaction_config _config;
    intelligent_compaction_scheduler _scheduler;
    incremental_compaction_engine _incremental_engine;
    hybrid_compaction_strategy _hybrid_strategy;
    cloud_native_compaction _cloud_compactor;
    adaptive_compaction_strategy _adaptive_strategy;
    metrics::advanced_compaction_metrics _metrics;
};

} // namespace compaction_strategies
