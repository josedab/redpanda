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

#include "raft/parallel/idempotency_tracker.h"

namespace raft::parallel {

std::optional<model::offset> idempotency_tracker::is_duplicate(
  const producer_identity& pid, int32_t sequence) const {
    auto it = _producer_state.find(pid);
    if (it == _producer_state.end()) {
        // Producer not tracked yet - not a duplicate
        return std::nullopt;
    }

    const auto& state = it->second;

    // If the sequence is less than or equal to the last seen sequence,
    // this is a duplicate
    if (sequence <= state.last_sequence) {
        return state.last_offset;
    }

    return std::nullopt;
}

ss::future<> idempotency_tracker::update_producer_state(
  const producer_identity& pid,
  int32_t sequence,
  model::offset offset,
  model::term_id term) {
    // Acquire per-producer lock
    auto& lock = get_producer_lock(pid);
    auto units = co_await lock.get_units(1);

    // Update or create producer state
    auto it = _producer_state.find(pid);
    if (it == _producer_state.end()) {
        // New producer
        _producer_state.emplace(
          pid,
          producer_state{
            .last_sequence = sequence,
            .last_offset = offset,
            .last_term = term,
          });
    } else {
        // Existing producer - update if sequence is newer
        auto& state = it->second;
        if (sequence > state.last_sequence) {
            state.last_sequence = sequence;
            state.last_offset = offset;
            state.last_term = term;
        }
    }
}

std::optional<producer_state> idempotency_tracker::get_producer_state(
  const producer_identity& pid) const {
    auto it = _producer_state.find(pid);
    if (it == _producer_state.end()) {
        return std::nullopt;
    }
    return it->second;
}

void idempotency_tracker::remove_producer(const producer_identity& pid) {
    _producer_state.erase(pid);
    _producer_locks.erase(pid);
}

void idempotency_tracker::clear() {
    _producer_state.clear();
    _producer_locks.clear();
}

ss::semaphore& idempotency_tracker::get_producer_lock(
  const producer_identity& pid) {
    auto it = _producer_locks.find(pid);
    if (it == _producer_locks.end()) {
        // Create new lock
        auto [new_it, _] = _producer_locks.emplace(
          pid, std::make_unique<ss::semaphore>(1));
        return *new_it->second;
    }
    return *it->second;
}

} // namespace raft::parallel
