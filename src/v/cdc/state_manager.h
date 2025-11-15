// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cdc/types.h"
#include "json/json.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>

#include <chrono>

namespace redpanda::cdc {

/// Snapshot state
enum class snapshot_state {
    not_started,
    in_progress,
    completed
};

/// Table state
struct table_state {
    ss::sstring table_name;
    snapshot_state snapshot;
    int64_t rows_processed;
    std::optional<ss::sstring> last_key;
};

/// Connector state
struct connector_state {
    ss::sstring connector_id;
    ss::sstring source_type;
    json::Value source_position;  // Database-specific position
    std::chrono::system_clock::time_point last_checkpoint;
    int64_t events_processed;
    int64_t bytes_processed;
    absl::flat_hash_map<ss::sstring, table_state> tables;
};

/// Recovery strategy
enum class recovery_strategy {
    resume_from_checkpoint,
    restart_snapshot,
    skip_to_latest
};

/// CDC connector state management
class cdc_state_manager {
public:
    cdc_state_manager() = default;

    /// Checkpoint connector state
    ss::future<> checkpoint(const connector_state& state);

    /// Restore connector state
    ss::future<connector_state> restore(const ss::sstring& connector_id);

    /// Handle connector failure and recovery
    ss::future<> handle_connector_failure(const ss::sstring& connector_id);

private:
    ss::future<> store_checkpoint(
        const ss::sstring& connector_id,
        const json::Value& data);

    ss::future<std::optional<json::Value>> load_checkpoint(
        const ss::sstring& connector_id);

    recovery_strategy determine_recovery_strategy(
        const connector_state& state);

    ss::future<> resume_from_checkpoint(const connector_state& state);
    ss::future<> restart_snapshot(const connector_state& state);
    ss::future<> skip_to_latest(const connector_state& state);
    ss::future<> restart_connector(const ss::sstring& connector_id);

    absl::flat_hash_map<ss::sstring, connector_state> _states;

    static constexpr std::chrono::hours max_checkpoint_age{24};
    static constexpr const char* cdc_state_topic = "__redpanda_cdc_state";
};

} // namespace redpanda::cdc
