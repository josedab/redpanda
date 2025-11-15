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
#include "sql/planner/logical_plan.h"
#include "sql/planner/physical_plan.h"

#include <seastar/core/future.hh>

#include <memory>

namespace redpanda::sql {

// Query planner configuration
struct optimization_rules {
    bool predicate_pushdown{true};
    bool join_reordering{true};
    bool expression_simplification{true};
    bool common_subexpression_elimination{true};
    bool partition_pruning{true};
};

// Query planner and optimizer
class query_planner {
public:
    explicit query_planner(optimization_rules rules = {})
      : _rules(rules) {}

    // Create logical plan from AST
    ss::future<std::unique_ptr<logical_plan>>
    create_logical_plan(const ast_node& ast);

    // Create physical plan from logical plan
    ss::future<std::unique_ptr<physical_plan>>
    create_physical_plan(const logical_plan& logical);

    // Optimize logical plan
    ss::future<std::unique_ptr<logical_plan>>
    optimize(std::unique_ptr<logical_plan> plan);

private:
    // Logical plan construction
    std::unique_ptr<logical_plan> build_from_select(const select_node& node);
    std::unique_ptr<logical_plan> build_from_join(const join_node& node);
    std::unique_ptr<logical_plan> build_scan_plan(const from_node& node);

    // Optimization rules
    std::unique_ptr<logical_plan> apply_predicate_pushdown(
      std::unique_ptr<logical_plan> plan);
    std::unique_ptr<logical_plan> apply_join_reordering(
      std::unique_ptr<logical_plan> plan);
    std::unique_ptr<logical_plan> apply_expression_simplification(
      std::unique_ptr<logical_plan> plan);

    // Cost estimation
    cost_estimate estimate_cost(const logical_plan& plan);
    cost_estimate estimate_scan_cost(const scan_plan& plan);
    cost_estimate estimate_join_cost(const join_plan& plan);
    cost_estimate estimate_aggregate_cost(const aggregate_plan& plan);

    // Physical plan generation
    std::unique_ptr<physical_plan> select_physical_operator(
      const logical_plan& logical);
    std::unique_ptr<physical_plan> add_exchange_operators(
      std::unique_ptr<physical_plan> plan);

    optimization_rules _rules;
};

} // namespace redpanda::sql
