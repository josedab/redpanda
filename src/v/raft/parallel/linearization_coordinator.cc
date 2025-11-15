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

#include "raft/parallel/linearization_coordinator.h"

#include <algorithm>

namespace raft::parallel {

logical_timestamp linearization_coordinator::assign_timestamp(
  model::partition_id partition) {
    return logical_timestamp{
      .epoch = _current_epoch,
      .sequence = _sequence_counter++,
      .partition = partition,
    };
}

ss::future<> linearization_coordinator::submit_commit(
  model::offset offset, model::term_id term, logical_timestamp ts) {
    // Create a pending commit
    pending_commit commit{
      .offset = offset,
      .term = term,
      .logical_ts = ts,
      .completion = ss::promise<>(),
    };

    auto fut = commit.completion.get_future();
    _pending_commits.emplace(ts, std::move(commit));

    // Trigger processing
    co_await process_pending_commits();

    co_return co_await std::move(fut);
}

ss::future<> linearization_coordinator::process_pending_commits() {
    // Sort pending commits by logical timestamp
    std::vector<logical_timestamp> sorted_timestamps;
    sorted_timestamps.reserve(_pending_commits.size());

    for (const auto& [ts, _] : _pending_commits) {
        sorted_timestamps.push_back(ts);
    }

    std::sort(sorted_timestamps.begin(), sorted_timestamps.end());

    // Process commits in order until we hit one that's not ready
    // For now, we assume all commits in the map are ready to commit
    // In a real implementation, we'd check if prerequisites are met
    for (const auto& ts : sorted_timestamps) {
        auto it = _pending_commits.find(ts);
        if (it == _pending_commits.end()) {
            continue;
        }

        auto& commit = it->second;

        // Verify monotonicity
        if (commit.offset <= _last_committed_offset) {
            // This shouldn't happen - indicates a bug
            commit.completion.set_exception(
              std::runtime_error(fmt::format(
                "Commit offset {} is not greater than last committed offset {}",
                commit.offset,
                _last_committed_offset)));
            _pending_commits.erase(it);
            continue;
        }

        // Update last committed offset
        _last_committed_offset = commit.offset;

        // Complete the commit
        commit.completion.set_value();

        // Remove from pending
        _pending_commits.erase(it);

        // Yield to prevent starvation
        co_await ss::coroutine::maybe_yield();
    }
}

void linearization_coordinator::update_epoch(model::term_id new_epoch) {
    _current_epoch = new_epoch;
    _sequence_counter = 0;
}

void linearization_coordinator::clear() {
    // Fail all pending commits
    for (auto& [_, commit] : _pending_commits) {
        commit.completion.set_exception(
          std::runtime_error("Linearization coordinator cleared"));
    }
    _pending_commits.clear();
    _sequence_counter = 0;
    _last_committed_offset = model::offset(-1);
}

} // namespace raft::parallel
