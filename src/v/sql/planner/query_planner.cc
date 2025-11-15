// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "sql/planner/query_planner.h"

#include <seastar/core/coroutine.hh>

namespace redpanda::sql {

ss::future<std::unique_ptr<logical_plan>>
query_planner::create_logical_plan(const ast_node& ast) {
    // Convert AST to logical plan
    std::unique_ptr<logical_plan> plan;

    switch (ast.node_type) {
    case ast_node::type::select: {
        auto* select = dynamic_cast<const select_node*>(&ast);
        if (select) {
            plan = build_from_select(*select);
        }
        break;
    }
    default:
        // Handle other statement types
        break;
    }

    // Apply optimization rules
    if (plan) {
        plan = co_await optimize(std::move(plan));
    }

    co_return plan;
}

ss::future<std::unique_ptr<physical_plan>>
query_planner::create_physical_plan(const logical_plan& logical) {
    // Select physical operators
    auto physical = select_physical_operator(logical);

    // Add exchange operators for distribution
    physical = add_exchange_operators(std::move(physical));

    co_return physical;
}

ss::future<std::unique_ptr<logical_plan>>
query_planner::optimize(std::unique_ptr<logical_plan> plan) {
    // Apply optimization rules
    if (_rules.predicate_pushdown) {
        plan = apply_predicate_pushdown(std::move(plan));
    }

    if (_rules.join_reordering) {
        plan = apply_join_reordering(std::move(plan));
    }

    if (_rules.expression_simplification) {
        plan = apply_expression_simplification(std::move(plan));
    }

    // Estimate costs
    plan->cost = estimate_cost(*plan);

    co_return plan;
}

std::unique_ptr<logical_plan> query_planner::build_from_select(
  const select_node& node) {
    std::unique_ptr<logical_plan> plan;

    // Start with FROM clause (scan)
    for (const auto& child : node.children) {
        if (child->node_type == ast_node::type::from) {
            auto* from = dynamic_cast<const from_node*>(child.get());
            if (from) {
                plan = build_scan_plan(*from);
            }
            break;
        }
    }

    if (!plan) {
        // No FROM clause, create dummy scan
        plan = std::make_unique<scan_plan>();
    }

    // Add WHERE clause (filter)
    for (const auto& child : node.children) {
        if (child->node_type == ast_node::type::where) {
            auto filter = std::make_unique<filter_plan>();
            filter->inputs.push_back(std::move(plan));
            plan = std::move(filter);
            break;
        }
    }

    // Add GROUP BY (aggregate)
    for (const auto& child : node.children) {
        if (child->node_type == ast_node::type::group_by) {
            auto* group = dynamic_cast<const group_by_node*>(child.get());
            if (group) {
                auto agg = std::make_unique<aggregate_plan>();
                agg->group_by_columns = group->columns;
                agg->inputs.push_back(std::move(plan));
                plan = std::move(agg);
            }
            break;
        }
    }

    // Add ORDER BY (sort)
    for (const auto& child : node.children) {
        if (child->node_type == ast_node::type::order_by) {
            auto sort = std::make_unique<sort_plan>();
            sort->inputs.push_back(std::move(plan));
            plan = std::move(sort);
            break;
        }
    }

    // Add projection
    auto project = std::make_unique<project_plan>();
    project->columns = node.columns;
    project->inputs.push_back(std::move(plan));
    plan = std::move(project);

    return plan;
}

std::unique_ptr<logical_plan> query_planner::build_scan_plan(
  const from_node& node) {
    auto scan = std::make_unique<scan_plan>();
    scan->table_name = node.table_name;
    // TODO: Determine if this is a stream or table
    // TODO: Look up NTP from metadata
    return scan;
}

std::unique_ptr<logical_plan> query_planner::apply_predicate_pushdown(
  std::unique_ptr<logical_plan> plan) {
    // TODO: Implement predicate pushdown optimization
    // Push filters down closer to scans
    return plan;
}

std::unique_ptr<logical_plan> query_planner::apply_join_reordering(
  std::unique_ptr<logical_plan> plan) {
    // TODO: Implement join reordering optimization
    // Use dynamic programming to find optimal join order
    return plan;
}

std::unique_ptr<logical_plan> query_planner::apply_expression_simplification(
  std::unique_ptr<logical_plan> plan) {
    // TODO: Implement expression simplification
    // Simplify constant expressions, eliminate redundant predicates
    return plan;
}

cost_estimate query_planner::estimate_cost(const logical_plan& plan) {
    switch (plan.operation) {
    case logical_plan::op_type::table_scan:
    case logical_plan::op_type::stream_scan:
        return estimate_scan_cost(
          static_cast<const scan_plan&>(plan));
    case logical_plan::op_type::join:
        return estimate_join_cost(static_cast<const join_plan&>(plan));
    case logical_plan::op_type::aggregate:
        return estimate_aggregate_cost(
          static_cast<const aggregate_plan&>(plan));
    default:
        // Default cost estimate
        cost_estimate cost;
        cost.estimated_rows = 1000;
        cost.cpu_cost = 1.0;
        return cost;
    }
}

cost_estimate query_planner::estimate_scan_cost(const scan_plan& plan) {
    cost_estimate cost;
    // TODO: Get actual statistics from catalog
    cost.estimated_rows = 10000;       // Default estimate
    cost.estimated_row_size = 100;     // Default row size
    cost.io_cost = cost.estimated_rows * 0.001; // IO cost per row
    cost.cpu_cost = cost.estimated_rows * 0.0001; // CPU cost per row
    return cost;
}

cost_estimate query_planner::estimate_join_cost(const join_plan& plan) {
    cost_estimate cost;
    // TODO: Implement proper join cost estimation
    // Consider hash join vs nested loop join costs
    cost.estimated_rows = 1000;
    cost.cpu_cost = 10.0;
    cost.memory_cost = 5.0;
    return cost;
}

cost_estimate query_planner::estimate_aggregate_cost(
  const aggregate_plan& plan) {
    cost_estimate cost;
    // TODO: Implement proper aggregate cost estimation
    cost.estimated_rows = 100; // Assuming aggregation reduces rows
    cost.cpu_cost = 5.0;
    cost.memory_cost = 2.0;
    return cost;
}

std::unique_ptr<physical_plan> query_planner::select_physical_operator(
  const logical_plan& logical) {
    std::unique_ptr<physical_plan> physical;

    switch (logical.operation) {
    case logical_plan::op_type::stream_scan: {
        auto* scan = static_cast<const scan_plan*>(&logical);
        auto phys_scan = std::make_unique<scan_physical_plan>(true);
        phys_scan->table_name = scan->table_name;
        phys_scan->ntp = scan->ntp;
        physical = std::move(phys_scan);
        break;
    }
    case logical_plan::op_type::table_scan: {
        auto* scan = static_cast<const scan_plan*>(&logical);
        auto phys_scan = std::make_unique<scan_physical_plan>(false);
        phys_scan->table_name = scan->table_name;
        phys_scan->ntp = scan->ntp;
        physical = std::move(phys_scan);
        break;
    }
    case logical_plan::op_type::filter: {
        auto* filter = static_cast<const filter_plan*>(&logical);
        auto phys_filter = std::make_unique<filter_physical_plan>();
        phys_filter->predicate = filter->predicate_expression;
        physical = std::move(phys_filter);
        break;
    }
    case logical_plan::op_type::project: {
        auto* project = static_cast<const project_plan*>(&logical);
        auto phys_project = std::make_unique<project_physical_plan>();
        phys_project->projections = project->columns;
        physical = std::move(phys_project);
        break;
    }
    case logical_plan::op_type::join: {
        auto* join = static_cast<const join_plan*>(&logical);
        // Choose hash join for most cases
        auto phys_join = std::make_unique<join_physical_plan>(
          physical_plan::operator_type::hash_join);
        phys_join->left_key = join->left_key;
        phys_join->right_key = join->right_key;
        physical = std::move(phys_join);
        break;
    }
    case logical_plan::op_type::aggregate: {
        auto* agg = static_cast<const aggregate_plan*>(&logical);
        auto phys_agg = std::make_unique<aggregate_physical_plan>(
          physical_plan::operator_type::hash_aggregate);
        phys_agg->group_by = agg->group_by_columns;
        phys_agg->aggregates = agg->aggregate_functions;
        physical = std::move(phys_agg);
        break;
    }
    case logical_plan::op_type::window: {
        auto* window = static_cast<const window_plan*>(&logical);
        physical_plan::operator_type op_type;
        switch (window->wtype) {
        case window_plan::window_type::tumbling:
            op_type = physical_plan::operator_type::tumbling_window;
            break;
        case window_plan::window_type::sliding:
            op_type = physical_plan::operator_type::sliding_window;
            break;
        case window_plan::window_type::session:
            op_type = physical_plan::operator_type::session_window;
            break;
        case window_plan::window_type::hopping:
            op_type = physical_plan::operator_type::hopping_window;
            break;
        }
        auto phys_window = std::make_unique<window_physical_plan>(op_type);
        phys_window->time_column = window->time_column;
        phys_window->window_size_ms = window->window_size_ms;
        physical = std::move(phys_window);
        break;
    }
    case logical_plan::op_type::sort: {
        auto* sort = static_cast<const sort_plan*>(&logical);
        auto phys_sort = std::make_unique<sort_physical_plan>();
        phys_sort->sort_keys = sort->sort_columns;
        phys_sort->ascending = sort->ascending;
        physical = std::move(phys_sort);
        break;
    }
    case logical_plan::op_type::limit: {
        auto* limit = static_cast<const limit_plan*>(&logical);
        auto phys_limit = std::make_unique<limit_physical_plan>();
        phys_limit->limit = limit->limit;
        phys_limit->offset = limit->offset;
        physical = std::move(phys_limit);
        break;
    }
    default:
        break;
    }

    // Recursively process children
    if (physical) {
        for (const auto& child : logical.inputs) {
            auto child_physical = select_physical_operator(*child);
            if (child_physical) {
                physical->children.push_back(std::move(child_physical));
            }
        }
    }

    return physical;
}

std::unique_ptr<physical_plan> query_planner::add_exchange_operators(
  std::unique_ptr<physical_plan> plan) {
    // TODO: Add exchange operators for distributed execution
    // Analyze plan and insert gather/repartition/broadcast operators
    return plan;
}

} // namespace redpanda::sql
