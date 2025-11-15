// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cdc/state_manager.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>

namespace redpanda::cdc {

static ss::logger logger{"cdc_state"};

ss::future<> cdc_state_manager::checkpoint(const connector_state& state) {
    vlog(logger.info, "Checkpointing CDC connector: {}, events_processed={}",
         state.connector_id, state.events_processed);

    // TODO: Serialize and store state in internal topic

    // Update in-memory state
    _states[state.connector_id] = state;

    vlog(logger.debug, "Checkpoint completed for connector: {}",
         state.connector_id);
    co_return;
}

ss::future<connector_state> cdc_state_manager::restore(
    const ss::sstring& connector_id) {

    vlog(logger.info, "Restoring CDC connector state: {}", connector_id);

    // Try to load from checkpoint
    if (auto checkpoint = co_await load_checkpoint(connector_id)) {
        // TODO: Deserialize checkpoint data into connector_state

        connector_state state;
        state.connector_id = connector_id;
        state.events_processed = 0;
        state.bytes_processed = 0;
        state.last_checkpoint = std::chrono::system_clock::now();

        _states[connector_id] = state;

        vlog(logger.info, "Restored connector {} from checkpoint", connector_id);
        co_return state;
    }

    // No checkpoint found, start fresh
    connector_state fresh_state;
    fresh_state.connector_id = connector_id;
    fresh_state.events_processed = 0;
    fresh_state.bytes_processed = 0;
    fresh_state.last_checkpoint = std::chrono::system_clock::now();

    _states[connector_id] = fresh_state;

    vlog(logger.info, "No checkpoint found for connector {}, starting fresh",
         connector_id);
    co_return fresh_state;
}

ss::future<> cdc_state_manager::handle_connector_failure(
    const ss::sstring& connector_id) {

    vlog(logger.warn, "CDC connector {} failed, initiating recovery",
         connector_id);

    // Load last checkpoint
    auto state = co_await restore(connector_id);

    // Determine recovery strategy
    auto strategy = determine_recovery_strategy(state);

    vlog(logger.info, "Using recovery strategy: {} for connector: {}",
         static_cast<int>(strategy), connector_id);

    switch (strategy) {
    case recovery_strategy::resume_from_checkpoint:
        co_await resume_from_checkpoint(state);
        break;

    case recovery_strategy::restart_snapshot:
        co_await restart_snapshot(state);
        break;

    case recovery_strategy::skip_to_latest:
        co_await skip_to_latest(state);
        break;
    }

    // Restart connector
    co_await restart_connector(connector_id);
}

ss::future<> cdc_state_manager::store_checkpoint(
    const ss::sstring& connector_id,
    const json::Value& data) {

    // TODO: Produce to internal CDC state topic
    vlog(logger.debug, "Storing checkpoint for connector: {}", connector_id);
    co_return;
}

ss::future<std::optional<json::Value>> cdc_state_manager::load_checkpoint(
    const ss::sstring& connector_id) {

    // TODO: Read from internal CDC state topic
    vlog(logger.debug, "Loading checkpoint for connector: {}", connector_id);
    co_return std::nullopt;
}

recovery_strategy cdc_state_manager::determine_recovery_strategy(
    const connector_state& state) {

    // If snapshot was in progress, restart it
    for (const auto& [table_name, table_state] : state.tables) {
        if (table_state.snapshot == snapshot_state::in_progress) {
            return recovery_strategy::restart_snapshot;
        }
    }

    // If checkpoint is recent, resume from it
    auto age = std::chrono::system_clock::now() - state.last_checkpoint;
    if (age < max_checkpoint_age) {
        return recovery_strategy::resume_from_checkpoint;
    }

    // Otherwise, skip to latest
    return recovery_strategy::skip_to_latest;
}

ss::future<> cdc_state_manager::resume_from_checkpoint(
    const connector_state& state) {

    vlog(logger.info, "Resuming connector {} from checkpoint",
         state.connector_id);
    // TODO: Implement checkpoint resume logic
    co_return;
}

ss::future<> cdc_state_manager::restart_snapshot(
    const connector_state& state) {

    vlog(logger.info, "Restarting snapshot for connector {}",
         state.connector_id);
    // TODO: Implement snapshot restart logic
    co_return;
}

ss::future<> cdc_state_manager::skip_to_latest(
    const connector_state& state) {

    vlog(logger.info, "Skipping to latest for connector {}",
         state.connector_id);
    // TODO: Implement skip to latest logic
    co_return;
}

ss::future<> cdc_state_manager::restart_connector(
    const ss::sstring& connector_id) {

    vlog(logger.info, "Restarting connector: {}", connector_id);
    // TODO: Implement connector restart logic
    co_return;
}

} // namespace redpanda::cdc
