// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "sql/parser/sql_parser.h"
#include "sql/parser/tokenizer.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

using namespace redpanda::sql;

SEASTAR_THREAD_TEST_CASE(test_tokenizer_keywords) {
    tokenizer tok("SELECT * FROM users WHERE id = 42");
    auto tokens = tok.tokenize();

    BOOST_REQUIRE_EQUAL(tokens.size(), 9); // Including EOF
    BOOST_CHECK_EQUAL(tokens[0].token_type, token::type::SELECT);
    BOOST_CHECK_EQUAL(tokens[1].token_type, token::type::STAR);
    BOOST_CHECK_EQUAL(tokens[2].token_type, token::type::FROM);
    BOOST_CHECK_EQUAL(tokens[3].token_type, token::type::IDENTIFIER);
    BOOST_CHECK_EQUAL(tokens[4].token_type, token::type::WHERE);
    BOOST_CHECK_EQUAL(tokens[5].token_type, token::type::IDENTIFIER);
    BOOST_CHECK_EQUAL(tokens[6].token_type, token::type::EQUAL);
    BOOST_CHECK_EQUAL(tokens[7].token_type, token::type::INTEGER_LITERAL);
}

SEASTAR_THREAD_TEST_CASE(test_tokenizer_operators) {
    tokenizer tok("a + b - c * d / e");
    auto tokens = tok.tokenize();

    BOOST_CHECK_EQUAL(tokens[1].token_type, token::type::PLUS);
    BOOST_CHECK_EQUAL(tokens[3].token_type, token::type::MINUS);
    BOOST_CHECK_EQUAL(tokens[5].token_type, token::type::STAR);
    BOOST_CHECK_EQUAL(tokens[7].token_type, token::type::SLASH);
}

SEASTAR_THREAD_TEST_CASE(test_tokenizer_string_literal) {
    tokenizer tok("SELECT 'hello world' FROM users");
    auto tokens = tok.tokenize();

    BOOST_REQUIRE_EQUAL(tokens[1].token_type, token::type::STRING_LITERAL);
    BOOST_CHECK_EQUAL(tokens[1].lexeme, "'hello world'");
}

SEASTAR_THREAD_TEST_CASE(test_tokenizer_comments) {
    tokenizer tok(R"(
        SELECT * -- single line comment
        FROM users /* multi
        line
        comment */
        WHERE id = 1
    )");
    auto tokens = tok.tokenize();

    // Comments should be skipped
    BOOST_CHECK_EQUAL(tokens[0].token_type, token::type::SELECT);
    BOOST_CHECK_EQUAL(tokens[1].token_type, token::type::STAR);
    BOOST_CHECK_EQUAL(tokens[2].token_type, token::type::FROM);
}

SEASTAR_THREAD_TEST_CASE(test_parser_simple_select) {
    sql_parser parser;
    auto result = parser.parse("SELECT id, name FROM users").get();

    BOOST_CHECK(result.errors.empty());
    BOOST_CHECK(result.ast != nullptr);
    BOOST_CHECK_EQUAL(result.ast->node_type, ast_node::type::select);

    auto* select = dynamic_cast<select_node*>(result.ast.get());
    BOOST_REQUIRE(select != nullptr);
    BOOST_CHECK_EQUAL(select->columns.size(), 2);
}

SEASTAR_THREAD_TEST_CASE(test_parser_select_with_where) {
    sql_parser parser;
    auto result = parser
                    .parse("SELECT * FROM events WHERE timestamp > 1000")
                    .get();

    BOOST_CHECK(result.errors.empty());
    BOOST_CHECK(result.ast != nullptr);

    auto* select = dynamic_cast<select_node*>(result.ast.get());
    BOOST_REQUIRE(select != nullptr);

    // Check that WHERE clause is present
    bool has_where = false;
    for (const auto& child : select->children) {
        if (child->node_type == ast_node::type::where) {
            has_where = true;
            break;
        }
    }
    BOOST_CHECK(has_where);
}

SEASTAR_THREAD_TEST_CASE(test_parser_select_with_group_by) {
    sql_parser parser;
    auto result = parser
                    .parse(
                      "SELECT event_type, COUNT(*) FROM events GROUP BY "
                      "event_type")
                    .get();

    BOOST_CHECK(result.errors.empty());
    BOOST_CHECK(result.ast != nullptr);
    BOOST_CHECK(result.metadata.has_aggregates);
}

SEASTAR_THREAD_TEST_CASE(test_parser_select_distinct) {
    sql_parser parser;
    auto result = parser.parse("SELECT DISTINCT user_id FROM events").get();

    BOOST_CHECK(result.errors.empty());

    auto* select = dynamic_cast<select_node*>(result.ast.get());
    BOOST_REQUIRE(select != nullptr);
    BOOST_CHECK(select->distinct);
}

SEASTAR_THREAD_TEST_CASE(test_parser_invalid_sql) {
    sql_parser parser;
    auto result = parser.parse("SELECT FROM").get();

    // Should have errors
    BOOST_CHECK(!result.errors.empty());
}

SEASTAR_THREAD_TEST_CASE(test_parser_function_calls) {
    sql_parser parser;
    auto result = parser
                    .parse(
                      "SELECT COUNT(*), AVG(value), MAX(timestamp) FROM "
                      "events")
                    .get();

    BOOST_CHECK(result.errors.empty());
    BOOST_CHECK(result.metadata.has_aggregates);
}

SEASTAR_THREAD_TEST_CASE(test_parser_order_by) {
    sql_parser parser;
    auto result = parser
                    .parse(
                      "SELECT * FROM events ORDER BY timestamp, user_id")
                    .get();

    BOOST_CHECK(result.errors.empty());

    auto* select = dynamic_cast<select_node*>(result.ast.get());
    BOOST_REQUIRE(select != nullptr);

    // Check for ORDER BY clause
    bool has_order_by = false;
    for (const auto& child : select->children) {
        if (child->node_type == ast_node::type::order_by) {
            has_order_by = true;
            break;
        }
    }
    BOOST_CHECK(has_order_by);
}
