// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>

#include <chrono>
#include <map>
#include <vector>

namespace redpanda::service_mesh {

// Health status of a service instance
enum class health_status {
    healthy,
    unhealthy,
    unknown,
};

// Service instance representation
struct service_instance {
    ss::sstring service_name;
    ss::sstring instance_id;
    ss::sstring endpoint;
    health_status status;
    std::map<ss::sstring, ss::sstring> metadata;
    std::chrono::system_clock::time_point last_heartbeat;

    bool operator==(const service_instance& other) const {
        return service_name == other.service_name
               && instance_id == other.instance_id;
    }
};

// Forward declaration of consensus store
class consensus_store;

// Service registry for dynamic discovery
class service_registry {
public:
    service_registry();
    ~service_registry();

    ss::future<> start();
    ss::future<> stop();

    // Register a service instance
    ss::future<> register_service(
      ss::sstring service_name,
      ss::sstring instance_id,
      ss::sstring endpoint,
      std::map<ss::sstring, ss::sstring> metadata = {});

    // Unregister a service instance
    ss::future<> unregister_service(
      ss::sstring service_name, ss::sstring instance_id);

    // Discover all healthy instances of a service
    ss::future<std::vector<service_instance>>
    discover_service(ss::sstring service_name);

    // Update the health status of a service instance
    ss::future<> update_health_status(
      ss::sstring service_name,
      ss::sstring instance_id,
      health_status status);

private:
    ss::future<> heartbeat_loop(service_instance instance);

    std::unique_ptr<consensus_store> _store;
    absl::flat_hash_map<ss::sstring, ss::future<>> _heartbeat_tasks;
    ss::abort_source _as;
};

} // namespace redpanda::service_mesh
