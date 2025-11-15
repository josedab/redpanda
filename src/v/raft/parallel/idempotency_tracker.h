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
#include <seastar/core/semaphore.hh>

#include <absl/container/flat_hash_map.h>

#include <optional>

namespace raft::parallel {

// Producer identity
struct producer_identity {
    int64_t id;
    int32_t epoch;

    auto operator<=>(const producer_identity&) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const producer_identity& pid) {
        return H::combine(std::move(h), pid.id, pid.epoch);
    }
};

// Producer state for idempotency tracking
struct producer_state {
    int32_t last_sequence;
    model::offset last_offset;
    model::term_id last_term;
};

/**
 * Idempotency tracker ensures exactly-once semantics by tracking
 * producer epochs and sequence numbers.
 *
 * This prevents duplicate processing when:
 * 1. Retries occur due to network issues
 * 2. Leadership changes happen during replication
 * 3. Client retries requests
 */
class idempotency_tracker {
public:
    idempotency_tracker() = default;

    // Check if a request is a duplicate
    // Returns std::nullopt if not a duplicate, otherwise returns the
    // offset of the previous write
    std::optional<model::offset> is_duplicate(
      const producer_identity& pid, int32_t sequence) const;

    // Update producer state after successful replication
    // This method acquires a per-producer lock to ensure thread safety
    ss::future<> update_producer_state(
      const producer_identity& pid,
      int32_t sequence,
      model::offset offset,
      model::term_id term);

    // Get producer state
    std::optional<producer_state> get_producer_state(
      const producer_identity& pid) const;

    // Remove stale producer state (e.g., during cleanup)
    void remove_producer(const producer_identity& pid);

    // Clear all state (used during leadership changes or snapshot restore)
    void clear();

    // Get statistics
    size_t tracked_producers() const { return _producer_state.size(); }

private:
    // Get or create a lock for a producer
    ss::semaphore& get_producer_lock(const producer_identity& pid);

    // Producer state indexed by producer identity
    absl::flat_hash_map<producer_identity, producer_state> _producer_state;

    // Per-producer locks for concurrent updates
    absl::flat_hash_map<producer_identity, std::unique_ptr<ss::semaphore>>
      _producer_locks;
};

} // namespace raft::parallel
