// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cdc/transform_engine.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

namespace redpanda::cdc {

SEASTAR_THREAD_TEST_CASE(test_transform_engine_add_metadata) {
    cdc_transform_engine engine;

    transform_config config;
    config.name = "add_metadata";
    config.type = transform_type::add_metadata;

    engine.add_transform(config);

    // Create test event
    change_event event;
    event.op = operation::insert;
    event.database = "test_db";
    event.schema = "public";
    event.table = "users";
    event.timestamp = std::chrono::system_clock::now();

    // Apply transforms
    auto result = engine.apply_transforms(event).get();
    BOOST_REQUIRE(result.has_value());

    // Verify metadata was added
    BOOST_CHECK(result->metadata.contains("source_database"));
    BOOST_CHECK_EQUAL(result->metadata["source_database"], "test_db");
}

SEASTAR_THREAD_TEST_CASE(test_filter_engine) {
    cdc_filter_engine filter("table = 'users'");

    change_event event;
    event.table = "users";

    // Should match
    bool matches = filter.matches(event).get();
    BOOST_CHECK(matches);
}

SEASTAR_THREAD_TEST_CASE(test_transform_pipeline) {
    cdc_transform_engine engine;

    // Add multiple transforms
    transform_config metadata_config;
    metadata_config.name = "metadata";
    metadata_config.type = transform_type::add_metadata;
    engine.add_transform(metadata_config);

    transform_config debezium_config;
    debezium_config.name = "debezium";
    debezium_config.type = transform_type::debezium_format;
    engine.add_transform(debezium_config);

    change_event event;
    event.op = operation::insert;
    event.database = "test";
    event.table = "table1";
    event.timestamp = std::chrono::system_clock::now();

    // Apply all transforms
    auto result = engine.apply_transforms(event).get();
    BOOST_REQUIRE(result.has_value());
}

} // namespace redpanda::cdc
