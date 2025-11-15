// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <any>
#include <memory>
#include <string>
#include <vector>

namespace redpanda::sql {

// Abstract Syntax Tree nodes
struct ast_node {
    enum class type {
        select,
        from,
        where,
        group_by,
        having,
        order_by,
        window,
        join,
        expression,
        literal,
        identifier,
        function_call,
        create_stream,
        create_table,
        create_materialized_view
    };

    type node_type;
    std::vector<std::unique_ptr<ast_node>> children;
    std::any value;

    ast_node(type t) : node_type(t) {}
    virtual ~ast_node() = default;

    // Helper to add child nodes
    void add_child(std::unique_ptr<ast_node> child) {
        children.push_back(std::move(child));
    }
};

// Specific AST node types

struct select_node : ast_node {
    std::vector<std::string> columns;
    bool distinct{false};

    select_node() : ast_node(type::select) {}
};

struct from_node : ast_node {
    std::string table_name;
    std::string alias;

    from_node() : ast_node(type::from) {}
};

struct where_node : ast_node {
    std::unique_ptr<ast_node> condition;

    where_node() : ast_node(type::where) {}
};

struct group_by_node : ast_node {
    std::vector<std::string> columns;

    group_by_node() : ast_node(type::group_by) {}
};

struct window_node : ast_node {
    enum class window_type {
        tumbling,
        sliding,
        session,
        hopping
    };

    window_type win_type;
    std::string time_column;
    int64_t size_ms;
    int64_t slide_ms{0};

    window_node() : ast_node(type::window) {}
};

struct join_node : ast_node {
    enum class join_type {
        inner,
        left,
        right,
        full,
        cross
    };

    join_type jtype;
    std::unique_ptr<ast_node> left;
    std::unique_ptr<ast_node> right;
    std::unique_ptr<ast_node> condition;

    join_node() : ast_node(type::join) {}
};

struct expression_node : ast_node {
    enum class expr_type {
        binary_op,
        unary_op,
        column_ref,
        literal_value,
        function_call,
        case_when
    };

    expr_type etype;
    std::string op;

    expression_node() : ast_node(type::expression) {}
};

struct literal_node : ast_node {
    enum class literal_type {
        integer,
        float_val,
        string_val,
        boolean,
        null_val,
        timestamp
    };

    literal_type ltype;
    std::any literal_value;

    literal_node() : ast_node(type::literal) {}
};

struct identifier_node : ast_node {
    std::string name;
    std::string table_prefix;

    identifier_node() : ast_node(type::identifier) {}
};

struct function_call_node : ast_node {
    std::string function_name;
    std::vector<std::unique_ptr<ast_node>> arguments;
    bool is_aggregate{false};

    function_call_node() : ast_node(type::function_call) {}
};

} // namespace redpanda::sql
