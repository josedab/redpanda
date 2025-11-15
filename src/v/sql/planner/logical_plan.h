// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "model/fundamental.h"

#include <seastar/core/sstring.hh>

#include <any>
#include <memory>
#include <vector>

namespace redpanda::sql {

// Cost estimation for query optimization
struct cost_estimate {
    double cpu_cost{0.0};
    double io_cost{0.0};
    double memory_cost{0.0};
    size_t estimated_rows{0};
    size_t estimated_row_size{0};

    double total_cost() const {
        return cpu_cost + io_cost + memory_cost;
    }
};

// Logical plan representation
struct logical_plan {
    enum class op_type {
        table_scan,
        stream_scan,
        filter,
        project,
        join,
        aggregate,
        window,
        sort,
        limit,
        union_all,
        materialize
    };

    op_type operation;
    std::vector<std::unique_ptr<logical_plan>> inputs;
    std::any properties;
    cost_estimate cost;

    logical_plan(op_type op) : operation(op) {}
    virtual ~logical_plan() = default;
};

// Specific logical plan nodes

struct scan_plan : logical_plan {
    ss::sstring table_name;
    model::ntp ntp;
    bool is_stream{false};

    scan_plan() : logical_plan(op_type::table_scan) {}
};

struct filter_plan : logical_plan {
    ss::sstring predicate_expression;
    double selectivity{0.5};

    filter_plan() : logical_plan(op_type::filter) {}
};

struct project_plan : logical_plan {
    std::vector<ss::sstring> columns;
    std::vector<ss::sstring> expressions;

    project_plan() : logical_plan(op_type::project) {}
};

struct join_plan : logical_plan {
    enum class join_type {
        inner,
        left,
        right,
        full,
        cross
    };

    join_type jtype{join_type::inner};
    ss::sstring left_key;
    ss::sstring right_key;
    ss::sstring join_condition;

    join_plan() : logical_plan(op_type::join) {}
};

struct aggregate_plan : logical_plan {
    std::vector<ss::sstring> group_by_columns;
    std::vector<ss::sstring> aggregate_functions;
    std::vector<ss::sstring> aggregate_columns;

    aggregate_plan() : logical_plan(op_type::aggregate) {}
};

struct window_plan : logical_plan {
    enum class window_type {
        tumbling,
        sliding,
        session,
        hopping
    };

    window_type wtype{window_type::tumbling};
    ss::sstring time_column;
    int64_t window_size_ms{0};
    int64_t slide_ms{0};
    std::vector<ss::sstring> partition_by;
    std::vector<ss::sstring> aggregate_functions;

    window_plan() : logical_plan(op_type::window) {}
};

struct sort_plan : logical_plan {
    std::vector<ss::sstring> sort_columns;
    std::vector<bool> ascending;

    sort_plan() : logical_plan(op_type::sort) {}
};

struct limit_plan : logical_plan {
    size_t limit{0};
    size_t offset{0};

    limit_plan() : logical_plan(op_type::limit) {}
};

} // namespace redpanda::sql
