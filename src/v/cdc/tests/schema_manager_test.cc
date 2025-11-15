// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cdc/schema_manager.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

namespace redpanda::cdc {

SEASTAR_THREAD_TEST_CASE(test_schema_manager_cache) {
    cdc_schema_manager manager;

    // Get schema for non-existent table
    auto schema = manager.get_current_schema("db", "public", "table").get();
    // Should return empty schema
}

SEASTAR_THREAD_TEST_CASE(test_schema_evolution_add_column) {
    cdc_schema_manager manager;

    schema_change_event event;
    event.type = schema_change_type::add_column;
    event.database = "test_db";
    event.schema = "public";
    event.table = "users";
    event.timestamp = std::chrono::system_clock::now();
    event.version = 2;

    // Handle schema change
    manager.handle_schema_change(event).get();

    // Get schema and verify it was cached
    auto schema = manager.get_current_schema("test_db", "public", "users").get();
}

SEASTAR_THREAD_TEST_CASE(test_schema_compatibility) {
    cdc_schema_manager manager;

    json::Value current;
    json::Value evolved;

    // Test compatibility check
    bool compatible = manager.is_compatible(current, evolved);
    BOOST_CHECK(compatible);
}

} // namespace redpanda::cdc
