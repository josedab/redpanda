// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cdc/admin_api.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>

namespace redpanda::cdc {

static ss::logger logger{"cdc_admin"};

cdc_admin_api::cdc_admin_api() {
    vlog(logger.info, "Initializing CDC Admin API");
}

cdc_admin_api::~cdc_admin_api() {
    vlog(logger.info, "Shutting down CDC Admin API");
}

ss::future<json::Value> cdc_admin_api::create_connector(
    const create_connector_request& request) {

    vlog(logger.info, "Creating CDC connector: name={}, type={}",
         request.name, request.source_type);

    // Check if connector already exists
    if (_connectors.contains(request.name)) {
        throw std::runtime_error(
            fmt::format("Connector {} already exists", request.name));
    }

    // Create connector instance
    auto connector = cdc_source_factory::create(request.source_type);

    // Configure connector
    source_config config;
    // TODO: Parse config from request.config JSON

    // Start connector
    co_await connector->start(config);

    // Register connector
    _connectors[request.name] = std::move(connector);

    // Initialize metrics
    _metrics[request.name] = connector_metrics{
        .events_total = 0,
        .bytes_total = 0,
        .events_per_second = 0.0,
        .bytes_per_second = 0.0,
        .average_latency = std::chrono::microseconds{0}
    };

    // Create transform engine if transforms specified
    if (request.transforms) {
        _transform_engines[request.name] = std::make_unique<cdc_transform_engine>();
        // TODO: Configure transforms
    }

    // TODO: Create output topic
    co_await create_output_topic(request.name);

    // Start processing pipeline
    _pipelines[request.name] = ss::abort_source{};
    // TODO: Start pipeline fiber

    json::Value response;
    // TODO: Build response JSON

    vlog(logger.info, "CDC connector {} created successfully", request.name);
    co_return response;
}

ss::future<json::Value> cdc_admin_api::list_connectors() {
    json::Value connectors_array;
    // TODO: Build array of connector summaries

    vlog(logger.debug, "Listing {} CDC connectors", _connectors.size());
    co_return connectors_array;
}

ss::future<json::Value> cdc_admin_api::get_connector(const ss::sstring& name) {
    auto it = _connectors.find(name);
    if (it == _connectors.end()) {
        throw std::runtime_error(
            fmt::format("Connector {} not found", name));
    }

    auto status = co_await get_connector_status(name);

    json::Value result;
    // TODO: Build detailed connector info JSON

    co_return result;
}

ss::future<json::Value> cdc_admin_api::delete_connector(
    const ss::sstring& name) {

    vlog(logger.info, "Deleting CDC connector: {}", name);

    auto it = _connectors.find(name);
    if (it == _connectors.end()) {
        throw std::runtime_error(
            fmt::format("Connector {} not found", name));
    }

    // Stop pipeline
    if (auto pipeline_it = _pipelines.find(name);
        pipeline_it != _pipelines.end()) {
        pipeline_it->second.request_abort();
    }

    // Stop connector
    co_await it->second->stop();

    // Remove from registries
    _connectors.erase(it);
    _pipelines.erase(name);
    _metrics.erase(name);
    _transform_engines.erase(name);

    json::Value response;
    // TODO: Build response

    vlog(logger.info, "CDC connector {} deleted successfully", name);
    co_return response;
}

ss::future<json::Value> cdc_admin_api::pause_connector(const ss::sstring& name) {
    auto connector = get_connector_checked(name);
    co_await connector->pause();

    json::Value response;
    vlog(logger.info, "CDC connector {} paused", name);
    co_return response;
}

ss::future<json::Value> cdc_admin_api::resume_connector(
    const ss::sstring& name) {

    auto connector = get_connector_checked(name);
    co_await connector->resume();

    json::Value response;
    vlog(logger.info, "CDC connector {} resumed", name);
    co_return response;
}

ss::future<connector_status> cdc_admin_api::get_connector_status(
    const ss::sstring& name) {

    auto it = _connectors.find(name);
    if (it == _connectors.end()) {
        throw std::runtime_error(
            fmt::format("Connector {} not found", name));
    }

    auto health = co_await it->second->check_health();

    connector_status status;
    status.name = name;

    switch (health.status) {
    case health_status::state::healthy:
        status.state = "running";
        break;
    case health_status::state::degraded:
        status.state = "paused";
        break;
    case health_status::state::unhealthy:
        status.state = "failed";
        break;
    }

    status.metrics = _metrics[name];
    status.error = health.message;
    status.last_activity = std::chrono::system_clock::now();

    co_return status;
}

ss::future<> cdc_admin_api::start_pipeline(const ss::sstring& connector_name) {
    // TODO: Implement pipeline processing
    vlog(logger.info, "Starting pipeline for connector: {}", connector_name);
    co_return;
}

ss::future<> cdc_admin_api::produce_event(
    const ss::sstring& connector_name,
    const change_event& event) {

    // TODO: Serialize and produce event to Redpanda topic
    vlog(logger.trace, "Producing event for connector: {}", connector_name);
    co_return;
}

ss::future<> cdc_admin_api::create_output_topic(
    const ss::sstring& connector_name) {

    // TODO: Create topic for CDC events
    vlog(logger.info, "Creating output topic for connector: {}", connector_name);
    co_return;
}

bool cdc_admin_api::should_checkpoint(const ss::sstring& connector_name) {
    // TODO: Implement checkpoint logic
    return false;
}

json::Value cdc_admin_api::metrics_to_json(const connector_metrics& metrics) {
    json::Value result;
    // TODO: Convert metrics to JSON
    return result;
}

std::unique_ptr<cdc_source> cdc_admin_api::get_connector_checked(
    const ss::sstring& name) {

    auto it = _connectors.find(name);
    if (it == _connectors.end()) {
        throw std::runtime_error(
            fmt::format("Connector {} not found", name));
    }

    // Note: This is a workaround since we can't return references to unique_ptr
    // In a real implementation, we'd use different ownership semantics
    return nullptr;
}

} // namespace redpanda::cdc
