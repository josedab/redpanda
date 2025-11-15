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
#include "sql/planner/logical_plan.h"

#include <seastar/core/sstring.hh>

#include <memory>
#include <vector>

namespace redpanda::sql {

// Physical execution plan
struct physical_plan {
    enum class operator_type {
        stream_scan,
        table_scan,
        filter,
        project,
        hash_join,
        merge_join,
        nested_loop_join,
        hash_aggregate,
        stream_aggregate,
        tumbling_window,
        sliding_window,
        session_window,
        hopping_window,
        sort,
        limit,
        exchange
    };

    operator_type op;
    std::vector<std::unique_ptr<physical_plan>> children;
    std::any properties;
    cost_estimate estimated_cost;

    // Parallelism
    int32_t parallelism{1};
    bool is_distributed{false};

    physical_plan(operator_type o) : op(o) {}
    virtual ~physical_plan() = default;
};

// Specific physical plan nodes

struct scan_physical_plan : physical_plan {
    ss::sstring table_name;
    model::ntp ntp;
    bool is_stream{false};
    ss::sstring predicate; // Pushed-down predicate

    scan_physical_plan(bool stream)
      : physical_plan(
        stream ? operator_type::stream_scan : operator_type::table_scan)
      , is_stream(stream) {}
};

struct filter_physical_plan : physical_plan {
    ss::sstring predicate;

    filter_physical_plan() : physical_plan(operator_type::filter) {}
};

struct project_physical_plan : physical_plan {
    std::vector<ss::sstring> projections;

    project_physical_plan() : physical_plan(operator_type::project) {}
};

struct join_physical_plan : physical_plan {
    enum class join_type {
        inner,
        left,
        right,
        full
    };

    join_type jtype{join_type::inner};
    ss::sstring join_condition;
    ss::sstring left_key;
    ss::sstring right_key;

    join_physical_plan(operator_type op) : physical_plan(op) {}
};

struct aggregate_physical_plan : physical_plan {
    std::vector<ss::sstring> group_by;
    std::vector<ss::sstring> aggregates;

    aggregate_physical_plan(operator_type op) : physical_plan(op) {}
};

struct window_physical_plan : physical_plan {
    ss::sstring time_column;
    int64_t window_size_ms{0};
    int64_t slide_ms{0};
    std::vector<ss::sstring> partition_by;
    std::vector<ss::sstring> aggregates;

    window_physical_plan(operator_type op) : physical_plan(op) {}
};

struct sort_physical_plan : physical_plan {
    std::vector<ss::sstring> sort_keys;
    std::vector<bool> ascending;

    sort_physical_plan() : physical_plan(operator_type::sort) {}
};

struct limit_physical_plan : physical_plan {
    size_t limit{0};
    size_t offset{0};

    limit_physical_plan() : physical_plan(operator_type::limit) {}
};

struct exchange_physical_plan : physical_plan {
    enum class exchange_type {
        gather,      // Gather all data to one node
        repartition, // Repartition by key
        broadcast    // Broadcast to all nodes
    };

    exchange_type etype{exchange_type::gather};
    ss::sstring partition_key;

    exchange_physical_plan() : physical_plan(operator_type::exchange) {}
};

} // namespace redpanda::sql
