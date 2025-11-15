// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cdc/schema_manager.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>

namespace redpanda::cdc {

static ss::logger logger{"cdc_schema"};

ss::future<> cdc_schema_manager::handle_schema_change(
    const schema_change_event& event) {

    vlog(logger.info, "Handling schema change: type={}, table={}.{}.{}",
         static_cast<int>(event.type), event.database, event.schema, event.table);

    // Evolve the schema
    auto new_schema = co_await evolve_schema(event);

    // Update cache
    auto key = make_cache_key(event.database, event.schema, event.table);
    _schema_cache[key] = cached_schema{
        .schema = std::move(new_schema),
        .version = event.version,
        .timestamp = event.timestamp
    };

    vlog(logger.info, "Schema change applied successfully, new version: {}",
         event.version);
}

ss::future<json::Value> cdc_schema_manager::evolve_schema(
    const schema_change_event& event) {

    // Get current schema
    auto current = co_await get_current_schema(
        event.database, event.schema, event.table);

    // TODO: Implement schema evolution logic based on change type
    // For now, return a placeholder

    json::Value evolved = current;

    // Validate compatibility
    if (!is_compatible(current, evolved)) {
        throw std::runtime_error("Schema change breaks compatibility");
    }

    co_return evolved;
}

ss::future<json::Value> cdc_schema_manager::get_current_schema(
    const ss::sstring& database,
    const ss::sstring& schema,
    const ss::sstring& table) {

    auto key = make_cache_key(database, schema, table);

    if (auto it = _schema_cache.find(key); it != _schema_cache.end()) {
        co_return it->second.schema;
    }

    // Return empty schema if not found
    co_return json::Value{};
}

bool cdc_schema_manager::is_compatible(
    const json::Value& current,
    const json::Value& evolved) {

    // TODO: Implement proper compatibility checking
    // For now, always return true
    return true;
}

ss::sstring cdc_schema_manager::make_cache_key(
    const ss::sstring& database,
    const ss::sstring& schema,
    const ss::sstring& table) const {

    return fmt::format("{}.{}.{}", database, schema, table);
}

} // namespace redpanda::cdc
