// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "net/unresolved_address.h"
#include "service_mesh/service_registry.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <atomic>
#include <optional>

namespace redpanda::service_mesh {

// Load balancing strategy
enum class load_balancing_strategy {
    round_robin,
    least_connections,
    random,
    consistent_hash,
};

// No instances available exception
class no_instances_available_exception : public std::runtime_error {
public:
    explicit no_instances_available_exception(const ss::sstring& service_name)
      : std::runtime_error(
        fmt::format("No instances available for service: {}", service_name)) {}
};

// Client-side load balancer
class load_balancer {
public:
    explicit load_balancer(
      service_registry& registry,
      load_balancing_strategy strategy = load_balancing_strategy::round_robin);
    ~load_balancer();

    // Get an endpoint for a service, optionally using a routing key
    ss::future<net::unresolved_address> get_endpoint(
      ss::sstring service_name,
      std::optional<ss::sstring> routing_key = std::nullopt);

    // Set the load balancing strategy
    void set_strategy(load_balancing_strategy strategy);

    // Get the current strategy
    load_balancing_strategy get_strategy() const { return _strategy; }

private:
    service_instance
    select_round_robin(const std::vector<service_instance>& instances);

    ss::future<service_instance>
    select_least_connections(const std::vector<service_instance>& instances);

    service_instance
    select_random(const std::vector<service_instance>& instances);

    service_instance select_consistent_hash(
      const std::vector<service_instance>& instances,
      const std::optional<ss::sstring>& routing_key);

    net::unresolved_address parse_endpoint(const ss::sstring& endpoint);

    service_registry& _registry;
    load_balancing_strategy _strategy;
    std::atomic<size_t> _round_robin_counter{0};
};

} // namespace redpanda::service_mesh
