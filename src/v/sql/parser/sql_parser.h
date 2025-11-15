// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "sql/parser/ast.h"
#include "sql/parser/tokenizer.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <string>
#include <string_view>
#include <vector>

namespace redpanda::sql {

struct parse_error {
    size_t line;
    size_t column;
    ss::sstring message;
};

struct parse_metadata {
    std::vector<ss::sstring> referenced_tables;
    std::vector<ss::sstring> referenced_columns;
    bool has_aggregates{false};
    bool has_windows{false};
    bool has_joins{false};
};

// SQL Parser using recursive descent parsing
class sql_parser {
public:
    struct parse_result {
        std::unique_ptr<ast_node> ast;
        std::vector<parse_error> errors;
        parse_metadata metadata;
    };

    sql_parser() = default;

    // Parse SQL statement
    ss::future<parse_result> parse(std::string_view sql);

private:
    // Parser state
    struct parser_state {
        token_stream* tokens;
        size_t current_pos{0};
        std::vector<parse_error> errors;
        parse_metadata metadata;

        const token& peek() const;
        const token& consume();
        bool match(token::type t);
        bool check(token::type t) const;
        void error(const ss::sstring& message);
    };

    // Recursive descent parser functions
    std::unique_ptr<ast_node> parse_statement(parser_state& state);
    std::unique_ptr<ast_node> parse_select(parser_state& state);
    std::unique_ptr<ast_node> parse_create(parser_state& state);
    std::unique_ptr<ast_node> parse_create_stream(parser_state& state);
    std::unique_ptr<ast_node> parse_create_table(parser_state& state);
    std::unique_ptr<ast_node> parse_create_materialized_view(
      parser_state& state);

    std::unique_ptr<ast_node> parse_select_list(parser_state& state);
    std::unique_ptr<ast_node> parse_from_clause(parser_state& state);
    std::unique_ptr<ast_node> parse_where_clause(parser_state& state);
    std::unique_ptr<ast_node> parse_group_by(parser_state& state);
    std::unique_ptr<ast_node> parse_having(parser_state& state);
    std::unique_ptr<ast_node> parse_order_by(parser_state& state);
    std::unique_ptr<ast_node> parse_window(parser_state& state);

    std::unique_ptr<ast_node> parse_expression(
      parser_state& state,
      int precedence = 0);
    std::unique_ptr<ast_node> parse_primary(parser_state& state);
    std::unique_ptr<ast_node> parse_function_call(parser_state& state);
    std::unique_ptr<ast_node> parse_literal(parser_state& state);
    std::unique_ptr<ast_node> parse_identifier(parser_state& state);

    // Helper functions
    int get_operator_precedence(const ss::sstring& op);
    bool is_aggregate_function(const ss::sstring& name);
    parse_metadata extract_metadata(const ast_node* ast);
    std::vector<parse_error> validate_syntax(const ast_node* ast);
};

} // namespace redpanda::sql
