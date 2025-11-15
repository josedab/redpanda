// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "model/timestamp.h"
#include "sql/executor/physical_operator.h"
#include "sql/planner/logical_plan.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>

#include <chrono>
#include <memory>
#include <optional>
#include <vector>

namespace redpanda::sql {

// Refresh policy for materialized views
enum class refresh_policy {
    continuous, // Update on every write
    periodic,   // Update at intervals
    on_demand   // Manual refresh
};

// Storage options for materialized views
struct storage_options {
    enum class backend {
        memory,
        disk
    };

    backend storage_backend{backend::memory};
    ss::sstring directory;
    size_t max_size_bytes{0}; // 0 = unlimited
};

// Partition specification
struct partition_spec {
    std::vector<ss::sstring> partition_columns;
    size_t num_partitions{0};
};

// Materialized view definition
struct view_definition {
    ss::sstring name;
    ss::sstring query;
    refresh_policy policy{refresh_policy::on_demand};
    storage_options storage;
    std::optional<partition_spec> partitioning;
    std::chrono::milliseconds refresh_interval{5000};
};

// Query request for materialized views
struct query_request {
    std::vector<ss::sstring> filters;
    std::vector<ss::sstring> projection;
    std::optional<size_t> limit;
};

// Query result
struct query_result {
    std::vector<row_batch> batches;
    size_t total_rows{0};

    void add_batch(row_batch batch) {
        total_rows += batch.row_count;
        batches.push_back(std::move(batch));
    }
};

// Storage backend interface
class view_storage {
public:
    virtual ~view_storage() = default;

    // Write batch to storage
    virtual ss::future<> write(const row_batch& batch) = 0;

    // Query stored data
    virtual ss::future<query_result> query(const query_request& req) = 0;

    // Compact storage
    virtual ss::future<> compact() = 0;

    // Clear all data
    virtual ss::future<> clear() = 0;
};

// In-memory storage implementation
class memory_view_storage : public view_storage {
public:
    ss::future<> write(const row_batch& batch) override;
    ss::future<query_result> query(const query_request& req) override;
    ss::future<> compact() override;
    ss::future<> clear() override;

private:
    std::vector<row_batch> _data;
    size_t _row_count{0};
};

// Disk-based storage implementation
class disk_view_storage : public view_storage {
public:
    explicit disk_view_storage(storage_options config)
      : _config(std::move(config)) {}

    ss::future<> write(const row_batch& batch) override;
    ss::future<query_result> query(const query_request& req) override;
    ss::future<> compact() override;
    ss::future<> clear() override;

private:
    storage_options _config;
    std::vector<ss::sstring> _segment_files;
};

// Materialized view state
struct view_state {
    view_definition definition;
    std::unique_ptr<logical_plan> plan;
    model::timestamp last_refresh;
    bool is_refreshing{false};
};

// Materialized view manager
class materialized_view {
public:
    materialized_view() = default;

    // Create materialized view
    ss::future<> create(const view_definition& def);

    // Drop materialized view
    ss::future<> drop();

    // Refresh the view
    ss::future<> refresh();

    // Query the view
    ss::future<query_result> query(const query_request& req);

    // Start continuous refresh
    ss::future<> start_continuous_refresh();

    // Stop continuous refresh
    ss::future<> stop_continuous_refresh();

private:
    bool needs_refresh() const;
    ss::future<> refresh_continuously();
    ss::future<> incremental_update();

    std::unique_ptr<view_state> _state;
    std::unique_ptr<view_storage> _storage;
    ss::abort_source _as;
    ss::future<> _refresh_fiber = ss::now();
};

// Manager for all materialized views
class view_manager {
public:
    view_manager() = default;

    // Create a new materialized view
    ss::future<> create_view(const view_definition& def);

    // Drop a materialized view
    ss::future<> drop_view(const ss::sstring& name);

    // Refresh a specific view
    ss::future<> refresh_view(const ss::sstring& name);

    // Refresh all views
    ss::future<> refresh_all_views();

    // Query a materialized view
    ss::future<query_result> query_view(
      const ss::sstring& name,
      const query_request& req);

    // List all views
    std::vector<ss::sstring> list_views() const;

    // Start the view manager
    ss::future<> start();

    // Stop the view manager
    ss::future<> stop();

private:
    ss::future<> maintenance_loop();

    absl::flat_hash_map<ss::sstring, std::unique_ptr<materialized_view>>
      _views;
    ss::abort_source _as;
    ss::future<> _maintenance_fiber = ss::now();
};

} // namespace redpanda::sql
