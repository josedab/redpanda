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

#pragma once

#include "model/record.h"
#include "raft/errc.h"
#include "raft/fundamental.h"
#include "raft/replicate.h"
#include "raft/parallel/dependency_graph.h"
#include "raft/parallel/linearization_coordinator.h"
#include "raft/parallel/idempotency_tracker.h"

#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/abort_source.hh>

#include <vector>
#include <functional>

namespace raft {
class consensus;
} // namespace raft

namespace raft::parallel {

// Function signature for the actual replication implementation
// (delegates to consensus::do_replicate or similar)
using replicate_fn = ss::noncopyable_function<ss::future<result<replicate_result>>(
  model::record_batch,
  replicate_options)>;

/**
 * Parallel replication pipeline that orchestrates concurrent replication
 * of independent batches while maintaining correctness guarantees.
 *
 * Key responsibilities:
 * 1. Analyze dependencies between batches
 * 2. Execute independent batches in parallel
 * 3. Maintain linearizability through the linearization coordinator
 * 4. Ensure exactly-once semantics through idempotency tracking
 * 5. Respect resource limits (max parallel operations)
 */
class parallel_replication_pipeline {
public:
    struct config {
        size_t max_parallel_operations{16};
        dependency_graph::detection_mode detection_mode{
          dependency_graph::detection_mode::auto_mode};
        bool speculation_enabled{false};
        size_t batch_size_threshold{1024};  // bytes
    };

    explicit parallel_replication_pipeline(config cfg)
      : _config(cfg)
      , _parallelism_sem(_config.max_parallel_operations)
      , _dependency_graph(_config.detection_mode) {}

    // Replicate multiple batches in parallel
    // Returns results in the same order as input batches
    ss::future<std::vector<result<replicate_result>>> replicate_parallel(
      std::vector<model::record_batch> batches,
      replicate_options opts,
      replicate_fn replicator);

    // Check if parallel replication should be used for a batch
    bool should_use_parallel(const model::record_batch& batch) const;

    // Update configuration
    void update_config(config new_cfg);

    // Set the current term for linearization
    void set_current_term(model::term_id term);

    // Clear state (e.g., on leadership change)
    void clear();

    // Get statistics
    struct stats {
        size_t pending_commits;
        size_t tracked_producers;
        size_t active_parallel_ops;
    };
    stats get_stats() const;

private:
    // Process a single level of the dependency graph in parallel
    ss::future<std::vector<result<replicate_result>>> process_level_parallel(
      const batch_group& level,
      const std::vector<model::record_batch>& batches,
      replicate_options opts,
      replicate_fn& replicator);

    // Replicate a single batch
    ss::future<result<replicate_result>> replicate_single(
      model::record_batch batch,
      replicate_options opts,
      replicate_fn& replicator);

    // Check for duplicate and update idempotency tracking
    ss::future<std::optional<result<replicate_result>>> check_idempotency(
      const model::record_batch& batch);

    config _config;
    ss::semaphore _parallelism_sem;
    dependency_graph _dependency_graph;
    linearization_coordinator _linearization_coordinator;
    idempotency_tracker _idempotency_tracker;
};

} // namespace raft::parallel
