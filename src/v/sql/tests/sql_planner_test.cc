// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "sql/parser/sql_parser.h"
#include "sql/planner/query_planner.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

using namespace redpanda::sql;

SEASTAR_THREAD_TEST_CASE(test_planner_simple_scan) {
    sql_parser parser;
    auto parse_result = parser.parse("SELECT * FROM users").get();

    BOOST_REQUIRE(parse_result.errors.empty());
    BOOST_REQUIRE(parse_result.ast != nullptr);

    query_planner planner;
    auto logical_plan = planner.create_logical_plan(*parse_result.ast).get();

    BOOST_REQUIRE(logical_plan != nullptr);
    // Top-level should be a projection
    BOOST_CHECK_EQUAL(logical_plan->operation, logical_plan::op_type::project);
}

SEASTAR_THREAD_TEST_CASE(test_planner_with_filter) {
    sql_parser parser;
    auto parse_result = parser.parse("SELECT * FROM users WHERE id > 10")
                          .get();

    BOOST_REQUIRE(parse_result.errors.empty());

    query_planner planner;
    auto logical_plan = planner.create_logical_plan(*parse_result.ast).get();

    BOOST_REQUIRE(logical_plan != nullptr);

    // Should have filter in the plan
    bool has_filter = false;
    auto* current = logical_plan.get();
    while (current) {
        if (current->operation == logical_plan::op_type::filter) {
            has_filter = true;
            break;
        }
        if (current->inputs.empty()) {
            break;
        }
        current = current->inputs[0].get();
    }

    BOOST_CHECK(has_filter);
}

SEASTAR_THREAD_TEST_CASE(test_planner_with_aggregate) {
    sql_parser parser;
    auto parse_result = parser
                          .parse(
                            "SELECT event_type, COUNT(*) FROM events GROUP "
                            "BY event_type")
                          .get();

    BOOST_REQUIRE(parse_result.errors.empty());

    query_planner planner;
    auto logical_plan = planner.create_logical_plan(*parse_result.ast).get();

    BOOST_REQUIRE(logical_plan != nullptr);

    // Should have aggregate in the plan
    bool has_aggregate = false;
    auto* current = logical_plan.get();
    while (current) {
        if (current->operation == logical_plan::op_type::aggregate) {
            has_aggregate = true;
            break;
        }
        if (current->inputs.empty()) {
            break;
        }
        current = current->inputs[0].get();
    }

    BOOST_CHECK(has_aggregate);
}

SEASTAR_THREAD_TEST_CASE(test_physical_plan_scan) {
    sql_parser parser;
    auto parse_result = parser.parse("SELECT * FROM events").get();

    query_planner planner;
    auto logical_plan = planner.create_logical_plan(*parse_result.ast).get();
    auto physical_plan = planner.create_physical_plan(*logical_plan).get();

    BOOST_REQUIRE(physical_plan != nullptr);
}

SEASTAR_THREAD_TEST_CASE(test_cost_estimation) {
    sql_parser parser;
    auto parse_result = parser.parse("SELECT * FROM events WHERE id > 100")
                          .get();

    optimization_rules rules;
    rules.predicate_pushdown = true;

    query_planner planner(rules);
    auto logical_plan = planner.create_logical_plan(*parse_result.ast).get();

    BOOST_REQUIRE(logical_plan != nullptr);

    // Cost should be estimated
    // Note: The actual values depend on statistics
    BOOST_CHECK(logical_plan->cost.total_cost() >= 0);
}

SEASTAR_THREAD_TEST_CASE(test_planner_order_by) {
    sql_parser parser;
    auto parse_result = parser
                          .parse("SELECT * FROM events ORDER BY timestamp")
                          .get();

    query_planner planner;
    auto logical_plan = planner.create_logical_plan(*parse_result.ast).get();

    BOOST_REQUIRE(logical_plan != nullptr);

    // Should have sort in the plan
    bool has_sort = false;
    auto* current = logical_plan.get();
    while (current) {
        if (current->operation == logical_plan::op_type::sort) {
            has_sort = true;
            break;
        }
        if (current->inputs.empty()) {
            break;
        }
        current = current->inputs[0].get();
    }

    BOOST_CHECK(has_sort);
}
