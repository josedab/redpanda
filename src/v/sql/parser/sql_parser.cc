// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "sql/parser/sql_parser.h"

#include <seastar/core/coroutine.hh>

namespace redpanda::sql {

ss::future<sql_parser::parse_result> sql_parser::parse(std::string_view sql) {
    co_return co_await ss::smp::submit_to(
      0, [sql = std::string(sql)]() -> parse_result {
          // Tokenize SQL
          tokenizer tok(sql);
          auto tokens = tok.tokenize();

          // Create token stream
          token_stream stream(std::move(tokens));

          // Build AST
          parser_state state;
          state.tokens = &stream;

          auto ast = parse_statement(state);

          // Extract metadata
          auto metadata = extract_metadata(ast.get());

          // Validate syntax
          auto errors = validate_syntax(ast.get());
          errors.insert(errors.end(), state.errors.begin(), state.errors.end());

          return parse_result{
            .ast = std::move(ast),
            .errors = std::move(errors),
            .metadata = std::move(metadata)};
      });
}

const token& sql_parser::parser_state::peek() const {
    return tokens->peek();
}

const token& sql_parser::parser_state::consume() {
    return tokens->consume();
}

bool sql_parser::parser_state::match(token::type t) {
    if (check(t)) {
        consume();
        return true;
    }
    return false;
}

bool sql_parser::parser_state::check(token::type t) const {
    return peek().token_type == t;
}

void sql_parser::parser_state::error(const ss::sstring& message) {
    auto& tok = peek();
    errors.push_back(parse_error{
      .line = tok.line,
      .column = tok.column,
      .message = message});
}

std::unique_ptr<ast_node> sql_parser::parse_statement(parser_state& state) {
    // Check what kind of statement this is
    if (state.check(token::type::SELECT)) {
        return parse_select(state);
    } else if (state.check(token::type::CREATE)) {
        return parse_create(state);
    } else {
        state.error("Expected SELECT or CREATE statement");
        return nullptr;
    }
}

std::unique_ptr<ast_node> sql_parser::parse_select(parser_state& state) {
    auto select = std::make_unique<select_node>();

    // Consume SELECT keyword
    state.consume();

    // Check for DISTINCT
    if (state.match(token::type::DISTINCT)) {
        select->distinct = true;
    }

    // Parse select list
    do {
        if (state.check(token::type::STAR)) {
            state.consume();
            select->columns.push_back("*");
        } else if (state.check(token::type::IDENTIFIER)) {
            auto& tok = state.consume();
            select->columns.push_back(std::string(tok.lexeme));
        } else {
            // Expression
            auto expr = parse_expression(state);
            if (expr) {
                select->add_child(std::move(expr));
            }
        }
    } while (state.match(token::type::COMMA));

    // Parse FROM clause
    if (state.match(token::type::FROM)) {
        auto from = parse_from_clause(state);
        if (from) {
            select->add_child(std::move(from));
        }
    }

    // Parse WHERE clause
    if (state.check(token::type::WHERE)) {
        auto where = parse_where_clause(state);
        if (where) {
            select->add_child(std::move(where));
        }
    }

    // Parse GROUP BY clause
    if (state.check(token::type::GROUP)) {
        auto group_by = parse_group_by(state);
        if (group_by) {
            select->add_child(std::move(group_by));
            state.metadata.has_aggregates = true;
        }
    }

    // Parse HAVING clause
    if (state.check(token::type::HAVING)) {
        auto having = parse_having(state);
        if (having) {
            select->add_child(std::move(having));
        }
    }

    // Parse ORDER BY clause
    if (state.check(token::type::ORDER)) {
        auto order_by = parse_order_by(state);
        if (order_by) {
            select->add_child(std::move(order_by));
        }
    }

    return select;
}

std::unique_ptr<ast_node> sql_parser::parse_from_clause(parser_state& state) {
    auto from = std::make_unique<from_node>();

    // Parse table name
    if (state.check(token::type::IDENTIFIER)) {
        auto& tok = state.consume();
        from->table_name = std::string(tok.lexeme);
        state.metadata.referenced_tables.emplace_back(tok.lexeme);

        // Check for alias
        if (state.match(token::type::AS)) {
            if (state.check(token::type::IDENTIFIER)) {
                auto& alias_tok = state.consume();
                from->alias = std::string(alias_tok.lexeme);
            }
        }
    } else {
        state.error("Expected table name in FROM clause");
    }

    return from;
}

std::unique_ptr<ast_node> sql_parser::parse_where_clause(parser_state& state) {
    state.consume(); // WHERE keyword

    auto where = std::make_unique<where_node>();
    where->condition = parse_expression(state);

    return where;
}

std::unique_ptr<ast_node> sql_parser::parse_group_by(parser_state& state) {
    state.consume(); // GROUP keyword
    if (!state.match(token::type::BY)) {
        state.error("Expected BY after GROUP");
        return nullptr;
    }

    auto group_by = std::make_unique<group_by_node>();

    do {
        if (state.check(token::type::IDENTIFIER)) {
            auto& tok = state.consume();
            group_by->columns.push_back(std::string(tok.lexeme));
        } else {
            state.error("Expected column name in GROUP BY");
            break;
        }
    } while (state.match(token::type::COMMA));

    return group_by;
}

std::unique_ptr<ast_node> sql_parser::parse_having(parser_state& state) {
    state.consume(); // HAVING keyword

    auto having = std::make_unique<where_node>(); // Reuse where_node
    having->condition = parse_expression(state);

    return having;
}

std::unique_ptr<ast_node> sql_parser::parse_order_by(parser_state& state) {
    state.consume(); // ORDER keyword
    if (!state.match(token::type::BY)) {
        state.error("Expected BY after ORDER");
        return nullptr;
    }

    auto order_by = std::make_unique<ast_node>(ast_node::type::order_by);

    do {
        if (state.check(token::type::IDENTIFIER)) {
            auto& tok = state.consume();
            auto identifier = std::make_unique<identifier_node>();
            identifier->name = std::string(tok.lexeme);
            order_by->add_child(std::move(identifier));
        }
    } while (state.match(token::type::COMMA));

    return order_by;
}

std::unique_ptr<ast_node> sql_parser::parse_expression(
  parser_state& state,
  int precedence) {
    // Parse primary expression
    auto left = parse_primary(state);
    if (!left) {
        return nullptr;
    }

    // Parse binary operators with precedence climbing
    while (true) {
        auto& tok = state.peek();
        int op_precedence = get_operator_precedence(tok.lexeme);

        if (op_precedence <= precedence) {
            break;
        }

        // Consume operator
        auto op = state.consume();

        // Parse right side
        auto right = parse_expression(state, op_precedence);

        // Create binary expression node
        auto expr = std::make_unique<expression_node>();
        expr->etype = expression_node::expr_type::binary_op;
        expr->op = std::string(op.lexeme);
        expr->add_child(std::move(left));
        expr->add_child(std::move(right));

        left = std::move(expr);
    }

    return left;
}

std::unique_ptr<ast_node> sql_parser::parse_primary(parser_state& state) {
    // Check for literal values
    if (state.check(token::type::INTEGER_LITERAL)
        || state.check(token::type::FLOAT_LITERAL)
        || state.check(token::type::STRING_LITERAL)) {
        return parse_literal(state);
    }

    // Check for NULL, TRUE, FALSE
    if (state.check(token::type::NULL_TOKEN)
        || state.check(token::type::TRUE_TOKEN)
        || state.check(token::type::FALSE_TOKEN)) {
        return parse_literal(state);
    }

    // Check for identifier or function call
    if (state.check(token::type::IDENTIFIER)
        || state.check(token::type::COUNT) || state.check(token::type::SUM)
        || state.check(token::type::AVG) || state.check(token::type::MIN)
        || state.check(token::type::MAX)) {
        // Look ahead for parenthesis to detect function call
        size_t saved_pos = state.tokens->position();
        state.consume();
        bool is_function = state.check(token::type::LEFT_PAREN);
        state.tokens->set_position(saved_pos);

        if (is_function) {
            return parse_function_call(state);
        } else {
            return parse_identifier(state);
        }
    }

    // Check for parenthesized expression
    if (state.match(token::type::LEFT_PAREN)) {
        auto expr = parse_expression(state);
        if (!state.match(token::type::RIGHT_PAREN)) {
            state.error("Expected ')' after expression");
        }
        return expr;
    }

    state.error("Expected expression");
    return nullptr;
}

std::unique_ptr<ast_node> sql_parser::parse_function_call(
  parser_state& state) {
    auto func = std::make_unique<function_call_node>();

    // Get function name
    auto& name_tok = state.consume();
    func->function_name = std::string(name_tok.lexeme);
    func->is_aggregate = is_aggregate_function(func->function_name);

    if (func->is_aggregate) {
        state.metadata.has_aggregates = true;
    }

    // Parse argument list
    if (!state.match(token::type::LEFT_PAREN)) {
        state.error("Expected '(' after function name");
        return func;
    }

    // Parse arguments
    if (!state.check(token::type::RIGHT_PAREN)) {
        do {
            auto arg = parse_expression(state);
            if (arg) {
                func->arguments.push_back(std::move(arg));
            }
        } while (state.match(token::type::COMMA));
    }

    if (!state.match(token::type::RIGHT_PAREN)) {
        state.error("Expected ')' after function arguments");
    }

    return func;
}

std::unique_ptr<ast_node> sql_parser::parse_literal(parser_state& state) {
    auto literal = std::make_unique<literal_node>();
    auto& tok = state.consume();

    switch (tok.token_type) {
    case token::type::INTEGER_LITERAL:
        literal->ltype = literal_node::literal_type::integer;
        literal->literal_value = std::stoll(std::string(tok.lexeme));
        break;
    case token::type::FLOAT_LITERAL:
        literal->ltype = literal_node::literal_type::float_val;
        literal->literal_value = std::stod(std::string(tok.lexeme));
        break;
    case token::type::STRING_LITERAL:
        literal->ltype = literal_node::literal_type::string_val;
        // Remove quotes
        literal->literal_value = std::string(
          tok.lexeme.substr(1, tok.lexeme.length() - 2));
        break;
    case token::type::TRUE_TOKEN:
        literal->ltype = literal_node::literal_type::boolean;
        literal->literal_value = true;
        break;
    case token::type::FALSE_TOKEN:
        literal->ltype = literal_node::literal_type::boolean;
        literal->literal_value = false;
        break;
    case token::type::NULL_TOKEN:
        literal->ltype = literal_node::literal_type::null_val;
        break;
    default:
        break;
    }

    return literal;
}

std::unique_ptr<ast_node> sql_parser::parse_identifier(parser_state& state) {
    auto identifier = std::make_unique<identifier_node>();
    auto& tok = state.consume();

    identifier->name = std::string(tok.lexeme);
    state.metadata.referenced_columns.emplace_back(tok.lexeme);

    // Check for table prefix (table.column)
    if (state.match(token::type::DOT)) {
        identifier->table_prefix = identifier->name;
        if (state.check(token::type::IDENTIFIER)) {
            auto& col_tok = state.consume();
            identifier->name = std::string(col_tok.lexeme);
        }
    }

    return identifier;
}

std::unique_ptr<ast_node> sql_parser::parse_create(parser_state& state) {
    state.consume(); // CREATE

    if (state.match(token::type::STREAM)) {
        return parse_create_stream(state);
    } else if (state.match(token::type::TABLE)) {
        return parse_create_table(state);
    } else if (state.match(token::type::MATERIALIZED)) {
        if (state.match(token::type::VIEW)) {
            return parse_create_materialized_view(state);
        }
    }

    state.error("Expected STREAM, TABLE, or MATERIALIZED VIEW after CREATE");
    return nullptr;
}

std::unique_ptr<ast_node> sql_parser::parse_create_stream(
  parser_state& state) {
    auto create = std::make_unique<ast_node>(
      ast_node::type::create_stream);

    // Parse stream name
    if (state.check(token::type::IDENTIFIER)) {
        auto& tok = state.consume();
        create->value = std::string(tok.lexeme);
    }

    // TODO: Parse column definitions and WITH options

    return create;
}

std::unique_ptr<ast_node> sql_parser::parse_create_table(
  parser_state& state) {
    auto create = std::make_unique<ast_node>(ast_node::type::create_table);

    // Parse table name
    if (state.check(token::type::IDENTIFIER)) {
        auto& tok = state.consume();
        create->value = std::string(tok.lexeme);
    }

    // TODO: Parse column definitions and WITH options

    return create;
}

std::unique_ptr<ast_node> sql_parser::parse_create_materialized_view(
  parser_state& state) {
    auto create = std::make_unique<ast_node>(
      ast_node::type::create_materialized_view);

    // Parse view name
    if (state.check(token::type::IDENTIFIER)) {
        auto& tok = state.consume();
        create->value = std::string(tok.lexeme);
    }

    // TODO: Parse AS SELECT and WITH options

    return create;
}

std::unique_ptr<ast_node> sql_parser::parse_window(parser_state& state) {
    auto window = std::make_unique<window_node>();
    state.metadata.has_windows = true;

    // TODO: Implement window parsing

    return window;
}

int sql_parser::get_operator_precedence(const ss::sstring& op) {
    // Operator precedence table
    if (op == "OR")
        return 1;
    if (op == "AND")
        return 2;
    if (op == "NOT")
        return 3;
    if (op == "=" || op == "!=" || op == "<>" || op == "<" || op == "<="
        || op == ">" || op == ">=")
        return 4;
    if (op == "+" || op == "-")
        return 5;
    if (op == "*" || op == "/" || op == "%")
        return 6;
    return 0;
}

bool sql_parser::is_aggregate_function(const ss::sstring& name) {
    std::string upper;
    for (char c : name) {
        upper += std::toupper(c);
    }
    return upper == "COUNT" || upper == "SUM" || upper == "AVG"
           || upper == "MIN" || upper == "MAX";
}

parse_metadata sql_parser::extract_metadata(const ast_node* ast) {
    parse_metadata metadata;
    // TODO: Walk AST and extract metadata
    return metadata;
}

std::vector<parse_error> sql_parser::validate_syntax(const ast_node* ast) {
    std::vector<parse_error> errors;
    // TODO: Validate AST for semantic errors
    return errors;
}

} // namespace redpanda::sql
