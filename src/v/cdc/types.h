// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "json/json.h"

#include <seastar/core/sstring.hh>

#include <chrono>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace redpanda::cdc {

/// Capture mode for CDC sources
enum class capture_mode {
    snapshot,                    // Full table snapshot
    incremental,                 // Only changes
    snapshot_and_incremental     // Snapshot then incremental
};

/// Change event operation type
enum class operation {
    insert,
    update,
    delete_,
    truncate,
    schema_change
};

/// Change event representing a database modification
struct change_event {
    operation op;
    ss::sstring database;
    ss::sstring schema;
    ss::sstring table;
    std::optional<json::Value> before;        // Before image
    std::optional<json::Value> after;         // After image
    std::optional<json::Value> key;           // Primary key
    std::chrono::system_clock::time_point timestamp;
    std::optional<ss::sstring> transaction_id;
    int64_t sequence_number;
    std::map<ss::sstring, ss::sstring> metadata;
};

/// Checkpoint representing CDC source position
struct checkpoint {
    json::Value position;       // Database-specific position
    int64_t sequence;
    std::chrono::system_clock::time_point timestamp;
};

/// Health status of CDC source
struct health_status {
    enum class state {
        healthy,
        degraded,
        unhealthy
    };

    state status;
    std::optional<ss::sstring> message;
    std::chrono::system_clock::time_point last_check;
};

/// Schema metadata
struct schema_metadata {
    ss::sstring database;
    ss::sstring schema;
    ss::sstring table;
    json::Value columns;
    json::Value primary_keys;
};

/// Source configuration
struct source_config {
    ss::sstring connection_string;
    std::vector<ss::sstring> tables;
    capture_mode mode = capture_mode::incremental;
    bool include_schema_changes = true;
    bool include_before_image = true;
    std::optional<ss::sstring> start_position;
    std::map<ss::sstring, ss::sstring> properties;
};

} // namespace redpanda::cdc
