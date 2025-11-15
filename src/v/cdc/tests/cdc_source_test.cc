// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cdc/cdc_source.h"
#include "cdc/postgres_cdc_source.h"
#include "cdc/mysql_cdc_source.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

namespace redpanda::cdc {

SEASTAR_THREAD_TEST_CASE(test_cdc_source_factory_postgres) {
    auto source = cdc_source_factory::create("postgresql");
    BOOST_REQUIRE(source != nullptr);
    BOOST_CHECK_EQUAL(source->type(), "postgresql");
}

SEASTAR_THREAD_TEST_CASE(test_cdc_source_factory_mysql) {
    auto source = cdc_source_factory::create("mysql");
    BOOST_REQUIRE(source != nullptr);
    BOOST_CHECK_EQUAL(source->type(), "mysql");
}

SEASTAR_THREAD_TEST_CASE(test_cdc_source_factory_unknown) {
    BOOST_CHECK_THROW(
        cdc_source_factory::create("unknown"),
        std::invalid_argument);
}

SEASTAR_THREAD_TEST_CASE(test_postgres_cdc_source_lifecycle) {
    postgres_cdc_source source;

    source_config config;
    config.connection_string = "postgresql://test";
    config.tables = {"test_table"};
    config.mode = capture_mode::incremental;

    // Start should succeed
    source.start(config).get();

    // Check health
    auto health = source.check_health().get();
    BOOST_CHECK(health.status == health_status::state::healthy);

    // Pause
    source.pause().get();
    health = source.check_health().get();
    BOOST_CHECK(health.status == health_status::state::degraded);

    // Resume
    source.resume().get();
    health = source.check_health().get();
    BOOST_CHECK(health.status == health_status::state::healthy);

    // Stop
    source.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_mysql_cdc_source_lifecycle) {
    mysql_cdc_source source;

    source_config config;
    config.connection_string = "mysql://test";
    config.tables = {"test_table"};
    config.mode = capture_mode::incremental;

    // Start should succeed
    source.start(config).get();

    // Check health
    auto health = source.check_health().get();
    BOOST_CHECK(health.status == health_status::state::healthy);

    // Stop
    source.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_checkpoint_restore) {
    postgres_cdc_source source;

    // Create a checkpoint
    checkpoint cp;
    cp.sequence = 12345;
    cp.timestamp = std::chrono::system_clock::now();

    // Restore from checkpoint
    source.restore_from_checkpoint(cp).get();

    // Get checkpoint back
    auto restored = source.get_checkpoint().get();
    BOOST_CHECK_EQUAL(restored.sequence, 12345);
}

} // namespace redpanda::cdc
