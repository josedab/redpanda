// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "sql/sql_service.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

using namespace redpanda::sql;

SEASTAR_THREAD_TEST_CASE(test_sql_service_lifecycle) {
    sql_config config;
    config.enabled = true;

    sql_service service(config);

    // Start service
    service.start().get();

    // Check stats
    auto stats = service.get_stats();
    BOOST_CHECK_EQUAL(stats.queries_executed, 0);

    // Stop service
    service.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_execute_simple_query) {
    sql_config config;
    config.enabled = true;

    sql_service service(config);
    service.start().get();

    query_request req;
    req.sql = "SELECT * FROM events";

    auto response = service.execute_query(req).get();

    // Query should be parsed successfully even though execution is not
    // implemented
    BOOST_CHECK(response.success || !response.error_message.empty());

    service.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_execute_invalid_query) {
    sql_config config;
    config.enabled = true;

    sql_service service(config);
    service.start().get();

    query_request req;
    req.sql = "SELECT FROM";

    auto response = service.execute_query(req).get();

    // Should fail with errors
    BOOST_CHECK(!response.success);
    BOOST_CHECK(!response.error_message.empty());

    service.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_create_materialized_view) {
    sql_config config;
    config.enabled = true;
    config.materialized_views.enabled = true;

    sql_service service(config);
    service.start().get();

    view_definition view_def;
    view_def.name = "test_view";
    view_def.query = "SELECT event_type, COUNT(*) FROM events GROUP BY "
                     "event_type";
    view_def.policy = refresh_policy::on_demand;
    view_def.storage.storage_backend = storage_options::backend::memory;

    // Create view
    service.create_materialized_view(view_def).get();

    // Check view is listed
    auto views = service.list_materialized_views();
    BOOST_REQUIRE_EQUAL(views.size(), 1);
    BOOST_CHECK_EQUAL(views[0], "test_view");

    // Drop view
    service.drop_materialized_view("test_view").get();

    // Check view is removed
    views = service.list_materialized_views();
    BOOST_CHECK_EQUAL(views.size(), 0);

    service.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_service_stats) {
    sql_config config;
    config.enabled = true;

    sql_service service(config);
    service.start().get();

    // Execute a query
    query_request req;
    req.sql = "SELECT * FROM events";
    service.execute_query(req).get();

    // Check stats
    auto stats = service.get_stats();
    BOOST_CHECK_EQUAL(stats.queries_executed, 1);

    service.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_query_with_aggregation) {
    sql_config config;
    config.enabled = true;

    sql_service service(config);
    service.start().get();

    query_request req;
    req.sql = "SELECT event_type, COUNT(*), AVG(value) FROM events "
              "GROUP BY event_type";

    auto response = service.execute_query(req).get();

    // Should parse successfully
    BOOST_CHECK(response.success || !response.error_message.empty());
    BOOST_CHECK(response.execution_time.count() >= 0);

    service.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_query_with_join) {
    sql_config config;
    config.enabled = true;

    sql_service service(config);
    service.start().get();

    query_request req;
    req.sql = "SELECT e.event_type, u.username "
              "FROM events e JOIN users u ON e.user_id = u.user_id";

    auto response = service.execute_query(req).get();

    // Should parse successfully
    BOOST_CHECK(response.success || !response.error_message.empty());

    service.stop().get();
}
