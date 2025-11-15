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

#include "model/fundamental.h"
#include "raft/fundamental.h"

#include <seastar/core/future.hh>

#include <absl/container/flat_hash_map.h>

#include <vector>
#include <optional>

namespace raft::parallel {

// Logical timestamp for ordering operations
struct logical_timestamp {
    model::term_id epoch;
    uint64_t sequence;
    model::partition_id partition;

    auto operator<=>(const logical_timestamp&) const = default;
};

// A pending commit waiting to be linearized
struct pending_commit {
    model::offset offset;
    model::term_id term;
    logical_timestamp logical_ts;
    ss::promise<> completion;
};

/**
 * Linearization coordinator ensures that commits happen in the correct
 * order despite parallel execution of replication operations.
 *
 * This maintains linearizability by:
 * 1. Assigning logical timestamps to operations
 * 2. Buffering commits and sorting by logical timestamp
 * 3. Committing in order
 */
class linearization_coordinator {
public:
    linearization_coordinator()
      : _current_epoch(model::term_id(0))
      , _sequence_counter(0)
      , _last_committed_offset(model::offset(-1)) {}

    // Assign a logical timestamp to an operation
    logical_timestamp assign_timestamp(
      model::partition_id partition = model::partition_id(0));

    // Submit a commit to be linearized
    // Returns a future that completes when the commit is linearized
    ss::future<> submit_commit(
      model::offset offset,
      model::term_id term,
      logical_timestamp ts);

    // Process pending commits in order
    // This should be called periodically or when new commits arrive
    ss::future<> process_pending_commits();

    // Update the current epoch (term)
    void update_epoch(model::term_id new_epoch);

    // Get the last committed offset
    model::offset last_committed_offset() const {
        return _last_committed_offset;
    }

    // Get statistics
    size_t pending_count() const { return _pending_commits.size(); }

    // Clear all state (used during leadership changes)
    void clear();

private:
    // Current epoch (term)
    model::term_id _current_epoch;

    // Sequence counter for timestamps
    uint64_t _sequence_counter;

    // Last committed offset (for monotonicity check)
    model::offset _last_committed_offset;

    // Pending commits indexed by logical timestamp
    absl::flat_hash_map<logical_timestamp, pending_commit> _pending_commits;
};

} // namespace raft::parallel
