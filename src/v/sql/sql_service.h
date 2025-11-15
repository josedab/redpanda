// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "sql/materialized_view/view_manager.h"
#include "sql/parser/sql_parser.h"
#include "sql/planner/query_planner.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

#include <chrono>
#include <memory>

namespace redpanda::sql {

// SQL engine configuration
struct sql_config {
    // Parser settings
    struct parser_config {
        size_t max_query_length{1048576}; // 1MB
        std::chrono::milliseconds timeout{5000};
    } parser;

    // Planner settings
    struct planner_config {
        bool enable_cost_based_optimization{true};
        bool enable_rule_based_optimization{true};
        double statistics_sample_rate{0.01};
        size_t join_reorder_threshold{12};
    } planner;

    // Executor settings
    struct executor_config {
        size_t max_memory_per_query{1073741824}; // 1GB
        int32_t parallelism{0};                  // 0 = auto
        bool enable_compilation{true};
        size_t compilation_threshold{100};
        bool spill_to_disk{true};
        ss::sstring spill_directory{"/var/lib/redpanda/sql/spill"};
    } executor;

    // Materialized views settings
    struct materialized_views_config {
        bool enabled{true};
        ss::sstring storage_directory{"/var/lib/redpanda/sql/views"};
        size_t max_views{1000};
        std::chrono::milliseconds refresh_interval{5000};
        bool incremental_maintenance{true};
    } materialized_views;

    // Performance settings
    struct performance_config {
        bool enable_simd{true};
        bool enable_adaptive_execution{true};
        size_t cache_size{268435456};       // 256MB
        size_t buffer_pool_size{536870912}; // 512MB
    } performance;

    // Security settings
    struct security_config {
        bool enable_row_level_security{true};
        bool enable_column_level_security{true};
        bool audit_queries{true};
    } security;

    bool enabled{true};
};

// Query request
struct query_request {
    ss::sstring sql;
    enum class format {
        json,
        arrow,
        protobuf
    } response_format{format::json};
};

// Query response
struct query_response {
    bool success{false};
    ss::sstring error_message;
    query_result result;
    std::chrono::microseconds execution_time{0};

    struct statistics {
        size_t rows_scanned{0};
        size_t rows_returned{0};
        size_t bytes_scanned{0};
    } stats;
};

// SQL subsystem integration
class sql_service : public ss::peering_sharded_service<sql_service> {
public:
    explicit sql_service(sql_config config)
      : _config(std::move(config)) {}

    // Start the SQL service
    ss::future<> start();

    // Stop the SQL service
    ss::future<> stop();

    // Execute SQL query
    ss::future<query_response> execute_query(query_request req);

    // Create materialized view
    ss::future<> create_materialized_view(view_definition def);

    // Drop materialized view
    ss::future<> drop_materialized_view(ss::sstring name);

    // List materialized views
    std::vector<ss::sstring> list_materialized_views() const;

    // Get service statistics
    struct service_stats {
        size_t queries_executed{0};
        size_t queries_failed{0};
        size_t active_views{0};
        std::chrono::microseconds total_query_time{0};
    };

    service_stats get_stats() const { return _stats; }

private:
    ss::future<> maintenance_loop();
    ss::future<> update_statistics();
    ss::future<> cleanup_expired_results();

    query_response make_error_response(const ss::sstring& error);
    query_response make_error_response(const std::vector<parse_error>& errors);

    sql_config _config;
    sql_parser _parser;
    query_planner _planner;
    view_manager _view_manager;

    service_stats _stats;
    ss::abort_source _as;
    ss::future<> _maintenance_fiber = ss::now();
};

} // namespace redpanda::sql
