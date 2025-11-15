// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "kafka/server/handlers/handler_interface.h"
#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <chrono>
#include <memory>

namespace redpanda::compute {

// Forward declarations
class compute_cache;
class kafka_request_handler;
namespace storage_service {
class storage_service;
}
namespace metadata_service {
class metadata_service;
}

// Stateless compute node configuration
struct compute_node_config {
    ss::sstring storage_service_endpoint;
    ss::sstring metadata_service_endpoint;
    size_t cache_size_bytes = 10ULL * 1024 * 1024 * 1024; // 10 GiB
    size_t max_concurrent_requests = 10000;
    std::chrono::milliseconds metadata_refresh_interval{10000};
    uint16_t kafka_port = 9092;
};

// Stateless compute node
class compute_node {
public:
    explicit compute_node(compute_node_config cfg);
    ~compute_node();

    ss::future<> start();
    ss::future<> stop();

    // Get the current status of the compute node
    struct status {
        size_t active_connections = 0;
        size_t total_requests = 0;
        size_t cache_hit_rate = 0;
        size_t cache_size_bytes = 0;
    };
    status get_status() const;

private:
    ss::future<> start_kafka_server();
    ss::future<> refresh_metadata_loop();
    ss::future<> refresh_partition_metadata();
    ss::future<> refresh_consumer_group_metadata();

    compute_node_config _config;
    std::unique_ptr<storage_service::storage_service> _storage_client;
    std::unique_ptr<metadata_service::metadata_service> _metadata_client;
    std::unique_ptr<compute_cache> _cache;
    std::unique_ptr<kafka_request_handler> _request_handler;
    ss::future<> _metadata_refresher;
    ss::abort_source _as;
};

} // namespace redpanda::compute
