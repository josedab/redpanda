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

namespace redpanda::cdc {

/// Schema change event type
enum class schema_change_type {
    add_column,
    drop_column,
    alter_column,
    rename_column,
    add_table,
    drop_table,
    rename_table
};

/// Schema change event
struct schema_change_event {
    schema_change_type type;
    ss::sstring database;
    ss::sstring schema;
    ss::sstring table;
    std::optional<json::Value> old_definition;
    std::optional<json::Value> new_definition;
    std::chrono::system_clock::time_point timestamp;
    int32_t version;
};

/// Cached schema information
struct cached_schema {
    json::Value schema;
    int32_t version;
    std::chrono::system_clock::time_point timestamp;
};

/// CDC schema manager for handling schema evolution
class cdc_schema_manager {
public:
    cdc_schema_manager() = default;

    /// Handle a schema change event
    ss::future<> handle_schema_change(const schema_change_event& event);

    /// Evolve schema based on change event
    ss::future<json::Value> evolve_schema(const schema_change_event& event);

    /// Get current schema for a table
    ss::future<json::Value> get_current_schema(
        const ss::sstring& database,
        const ss::sstring& schema,
        const ss::sstring& table);

    /// Check if schema change is compatible
    bool is_compatible(
        const json::Value& current,
        const json::Value& evolved);

private:
    ss::sstring make_cache_key(
        const ss::sstring& database,
        const ss::sstring& schema,
        const ss::sstring& table) const;

    absl::flat_hash_map<ss::sstring, cached_schema> _schema_cache;
};

} // namespace redpanda::cdc
