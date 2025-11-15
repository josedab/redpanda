// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cdc/cdc_source.h"
#include "cdc/schema_manager.h"
#include "cdc/state_manager.h"
#include "cdc/transform_engine.h"
#include "json/json.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>

namespace redpanda::cdc {

/// Create connector request
struct create_connector_request {
    ss::sstring name;
    ss::sstring source_type;
    json::Value config;
    std::optional<json::Value> transforms;
    std::optional<ss::sstring> filter;
};

/// Connector status
struct connector_status {
    ss::sstring name;
    ss::sstring state;  // running, paused, failed
    connector_metrics metrics;
    std::optional<ss::sstring> error;
    std::chrono::system_clock::time_point last_activity;
};

/// Connector metrics
struct connector_metrics {
    int64_t events_total;
    int64_t bytes_total;
    double events_per_second;
    double bytes_per_second;
    std::chrono::microseconds average_latency;
    absl::flat_hash_map<ss::sstring, int64_t> events_by_table;
};

/// REST API for CDC management
class cdc_admin_api {
public:
    cdc_admin_api();
    ~cdc_admin_api();

    /// Create a new CDC connector
    ss::future<json::Value> create_connector(
        const create_connector_request& request);

    /// List all connectors
    ss::future<json::Value> list_connectors();

    /// Get connector details
    ss::future<json::Value> get_connector(const ss::sstring& name);

    /// Delete a connector
    ss::future<json::Value> delete_connector(const ss::sstring& name);

    /// Pause a connector
    ss::future<json::Value> pause_connector(const ss::sstring& name);

    /// Resume a connector
    ss::future<json::Value> resume_connector(const ss::sstring& name);

    /// Get connector status
    ss::future<connector_status> get_connector_status(const ss::sstring& name);

private:
    ss::future<> start_pipeline(const ss::sstring& connector_name);
    ss::future<> produce_event(
        const ss::sstring& connector_name,
        const change_event& event);
    ss::future<> create_output_topic(const ss::sstring& connector_name);
    bool should_checkpoint(const ss::sstring& connector_name);
    json::Value metrics_to_json(const connector_metrics& metrics);
    std::unique_ptr<cdc_source> get_connector_checked(const ss::sstring& name);

    absl::flat_hash_map<ss::sstring, std::unique_ptr<cdc_source>> _connectors;
    absl::flat_hash_map<ss::sstring, ss::abort_source> _pipelines;
    absl::flat_hash_map<ss::sstring, std::unique_ptr<cdc_transform_engine>> _transform_engines;
    absl::flat_hash_map<ss::sstring, connector_metrics> _metrics;
    cdc_state_manager _state_manager;
    cdc_schema_manager _schema_manager;
    ss::abort_source _as;
};

} // namespace redpanda::cdc
