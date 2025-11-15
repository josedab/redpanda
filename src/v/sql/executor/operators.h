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
#include "sql/executor/physical_operator.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>

#include <chrono>
#include <memory>
#include <vector>

namespace redpanda::sql {

// Stream scan operator - reads from Kafka topics
class stream_scan_operator : public physical_operator {
public:
    struct scan_config {
        model::ntp ntp;
        model::offset start_offset;
        std::optional<model::offset> end_offset;
        size_t batch_size{1000};
    };

    stream_scan_operator(
      scan_config config,
      std::optional<ss::sstring> predicate = std::nullopt)
      : _config(std::move(config))
      , _predicate(std::move(predicate)) {}

    ss::future<> open() override;
    ss::future<std::optional<row_batch>> next() override;
    ss::future<> close() override;
    operator_stats get_stats() const override { return _stats; }

private:
    scan_config _config;
    std::optional<ss::sstring> _predicate;
    operator_stats _stats;
    // TODO: Add actual Kafka reader
};

// Filter operator - applies predicates to rows
class filter_operator : public physical_operator {
public:
    filter_operator(
      std::unique_ptr<physical_operator> input,
      ss::sstring predicate)
      : _input(std::move(input))
      , _predicate(std::move(predicate)) {}

    ss::future<> open() override;
    ss::future<std::optional<row_batch>> next() override;
    ss::future<> close() override;
    operator_stats get_stats() const override { return _stats; }

private:
    bool evaluate_predicate(const row_batch& batch, size_t row_index);

    std::unique_ptr<physical_operator> _input;
    ss::sstring _predicate;
    operator_stats _stats;
};

// Project operator - selects and transforms columns
class project_operator : public physical_operator {
public:
    project_operator(
      std::unique_ptr<physical_operator> input,
      std::vector<ss::sstring> projections)
      : _input(std::move(input))
      , _projections(std::move(projections)) {}

    ss::future<> open() override;
    ss::future<std::optional<row_batch>> next() override;
    ss::future<> close() override;
    operator_stats get_stats() const override { return _stats; }

private:
    std::unique_ptr<physical_operator> _input;
    std::vector<ss::sstring> _projections;
    operator_stats _stats;
};

// Hash join operator
class hash_join_operator : public physical_operator {
public:
    enum class join_type {
        inner,
        left,
        right,
        full
    };

    hash_join_operator(
      std::unique_ptr<physical_operator> left,
      std::unique_ptr<physical_operator> right,
      ss::sstring left_key,
      ss::sstring right_key,
      join_type type = join_type::inner)
      : _left(std::move(left))
      , _right(std::move(right))
      , _left_key(std::move(left_key))
      , _right_key(std::move(right_key))
      , _join_type(type) {}

    ss::future<> open() override;
    ss::future<std::optional<row_batch>> next() override;
    ss::future<> close() override;
    operator_stats get_stats() const override { return _stats; }

private:
    ss::future<> build_hash_table();
    row_batch probe_hash_table(const row_batch& left_batch);

    std::unique_ptr<physical_operator> _left;
    std::unique_ptr<physical_operator> _right;
    ss::sstring _left_key;
    ss::sstring _right_key;
    join_type _join_type;
    operator_stats _stats;

    // Hash table for right side
    absl::flat_hash_map<int64_t, std::vector<row_batch>> _hash_table;
    bool _hash_table_built{false};
};

// Hash aggregate operator
class hash_aggregate_operator : public physical_operator {
public:
    struct aggregate_spec {
        enum class function {
            count,
            sum,
            avg,
            min,
            max
        };

        function func;
        ss::sstring column;
        ss::sstring alias;
    };

    hash_aggregate_operator(
      std::unique_ptr<physical_operator> input,
      std::vector<ss::sstring> group_by,
      std::vector<aggregate_spec> aggregates)
      : _input(std::move(input))
      , _group_by(std::move(group_by))
      , _aggregates(std::move(aggregates)) {}

    ss::future<> open() override;
    ss::future<std::optional<row_batch>> next() override;
    ss::future<> close() override;
    operator_stats get_stats() const override { return _stats; }

private:
    struct aggregate_state {
        int64_t count{0};
        double sum{0.0};
        double min_val{std::numeric_limits<double>::max()};
        double max_val{std::numeric_limits<double>::lowest()};
    };

    ss::future<> compute_aggregates();
    row_batch materialize_results();

    std::unique_ptr<physical_operator> _input;
    std::vector<ss::sstring> _group_by;
    std::vector<aggregate_spec> _aggregates;
    operator_stats _stats;

    absl::flat_hash_map<int64_t, std::vector<aggregate_state>> _agg_states;
    bool _computed{false};
    bool _emitted{false};
};

// Tumbling window operator
class tumbling_window_operator : public physical_operator {
public:
    tumbling_window_operator(
      std::unique_ptr<physical_operator> input,
      ss::sstring time_column,
      std::chrono::milliseconds window_size,
      std::vector<hash_aggregate_operator::aggregate_spec> aggregates)
      : _input(std::move(input))
      , _time_column(std::move(time_column))
      , _window_size(window_size)
      , _aggregates(std::move(aggregates)) {}

    ss::future<> open() override;
    ss::future<std::optional<row_batch>> next() override;
    ss::future<> close() override;
    operator_stats get_stats() const override { return _stats; }

private:
    struct window_state {
        int64_t window_start;
        int64_t window_end;
        std::vector<hash_aggregate_operator::aggregate_spec::function>
          agg_values;
    };

    std::unique_ptr<physical_operator> _input;
    ss::sstring _time_column;
    std::chrono::milliseconds _window_size;
    std::vector<hash_aggregate_operator::aggregate_spec> _aggregates;
    operator_stats _stats;

    std::vector<window_state> _windows;
};

// Sort operator
class sort_operator : public physical_operator {
public:
    sort_operator(
      std::unique_ptr<physical_operator> input,
      std::vector<ss::sstring> sort_keys,
      std::vector<bool> ascending)
      : _input(std::move(input))
      , _sort_keys(std::move(sort_keys))
      , _ascending(std::move(ascending)) {}

    ss::future<> open() override;
    ss::future<std::optional<row_batch>> next() override;
    ss::future<> close() override;
    operator_stats get_stats() const override { return _stats; }

private:
    ss::future<> sort_all_data();

    std::unique_ptr<physical_operator> _input;
    std::vector<ss::sstring> _sort_keys;
    std::vector<bool> _ascending;
    operator_stats _stats;

    std::vector<row_batch> _sorted_batches;
    size_t _current_batch{0};
    bool _sorted{false};
};

// Limit operator
class limit_operator : public physical_operator {
public:
    limit_operator(
      std::unique_ptr<physical_operator> input,
      size_t limit,
      size_t offset = 0)
      : _input(std::move(input))
      , _limit(limit)
      , _offset(offset) {}

    ss::future<> open() override;
    ss::future<std::optional<row_batch>> next() override;
    ss::future<> close() override;
    operator_stats get_stats() const override { return _stats; }

private:
    std::unique_ptr<physical_operator> _input;
    size_t _limit;
    size_t _offset;
    size_t _rows_emitted{0};
    size_t _rows_skipped{0};
    operator_stats _stats;
};

} // namespace redpanda::sql
