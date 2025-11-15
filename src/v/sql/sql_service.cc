// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "sql/sql_service.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <chrono>

namespace redpanda::sql {

ss::future<> sql_service::start() {
    if (!_config.enabled) {
        co_return;
    }

    // Start view manager if enabled
    if (_config.materialized_views.enabled) {
        co_await _view_manager.start();
    }

    // Start background maintenance
    _maintenance_fiber = maintenance_loop();

    co_return;
}

ss::future<> sql_service::stop() {
    _as.request_abort();
    co_await std::move(_maintenance_fiber);

    // Stop view manager
    if (_config.materialized_views.enabled) {
        co_await _view_manager.stop();
    }

    co_return;
}

ss::future<query_response> sql_service::execute_query(query_request req) {
    auto start_time = std::chrono::high_resolution_clock::now();

    query_response response;

    try {
        // Parse SQL
        auto parse_result = co_await _parser.parse(req.sql);
        if (!parse_result.errors.empty()) {
            co_return make_error_response(parse_result.errors);
        }

        if (!parse_result.ast) {
            co_return make_error_response("Failed to parse SQL query");
        }

        // Create logical plan
        auto logical_plan = co_await _planner.create_logical_plan(
          *parse_result.ast);
        if (!logical_plan) {
            co_return make_error_response("Failed to create query plan");
        }

        // Create physical plan
        auto physical_plan = co_await _planner.create_physical_plan(
          *logical_plan);
        if (!physical_plan) {
            co_return make_error_response(
              "Failed to create physical execution plan");
        }

        // TODO: Execute query using execution engine
        // For now, return empty result
        response.success = true;

        _stats.queries_executed++;
    } catch (const std::exception& e) {
        response = make_error_response(
          ss::sstring("Query execution failed: ") + e.what());
        _stats.queries_failed++;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    response.execution_time = std::chrono::duration_cast<
      std::chrono::microseconds>(end_time - start_time);
    _stats.total_query_time += response.execution_time;

    co_return response;
}

ss::future<> sql_service::create_materialized_view(view_definition def) {
    if (!_config.materialized_views.enabled) {
        throw std::runtime_error("Materialized views are not enabled");
    }

    co_await _view_manager.create_view(std::move(def));
}

ss::future<> sql_service::drop_materialized_view(ss::sstring name) {
    if (!_config.materialized_views.enabled) {
        throw std::runtime_error("Materialized views are not enabled");
    }

    co_await _view_manager.drop_view(std::move(name));
}

std::vector<ss::sstring> sql_service::list_materialized_views() const {
    if (!_config.materialized_views.enabled) {
        return {};
    }

    return _view_manager.list_views();
}

ss::future<> sql_service::maintenance_loop() {
    while (!_as.abort_requested()) {
        try {
            // Update statistics
            co_await update_statistics();

            // Clean up expired results
            co_await cleanup_expired_results();

            // Sleep for maintenance interval
            co_await ss::sleep_abortable(std::chrono::seconds(60), _as);
        } catch (const ss::sleep_aborted&) {
            break;
        } catch (const std::exception& e) {
            // Log error but continue
        }
    }
}

ss::future<> sql_service::update_statistics() {
    // Update view count
    if (_config.materialized_views.enabled) {
        _stats.active_views = _view_manager.list_views().size();
    }

    co_return;
}

ss::future<> sql_service::cleanup_expired_results() {
    // TODO: Implement cleanup of cached query results
    co_return;
}

query_response sql_service::make_error_response(const ss::sstring& error) {
    query_response response;
    response.success = false;
    response.error_message = error;
    return response;
}

query_response sql_service::make_error_response(
  const std::vector<parse_error>& errors) {
    query_response response;
    response.success = false;

    ss::sstring combined_errors;
    for (const auto& err : errors) {
        combined_errors += ss::format(
          "Line {}, Column {}: {}\n", err.line, err.column, err.message);
    }

    response.error_message = std::move(combined_errors);
    return response;
}

} // namespace redpanda::sql
