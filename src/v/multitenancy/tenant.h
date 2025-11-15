// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "multitenancy/resource_domain.h"
#include "multitenancy/types.h"

#include <seastar/core/future.hh>

#include <atomic>
#include <memory>

namespace redpanda::multitenancy {

// Usage statistics for a tenant
struct usage_statistics {
    std::atomic<size_t> cpu_time_us{0};
    std::atomic<size_t> memory_bytes{0};
    std::atomic<size_t> disk_bytes{0};
    std::atomic<size_t> network_bytes_in{0};
    std::atomic<size_t> network_bytes_out{0};

    // Request counts
    std::atomic<size_t> produce_requests{0};
    std::atomic<size_t> fetch_requests{0};
    std::atomic<size_t> errors{0};
};

// Core tenant abstraction
class tenant {
public:
    tenant(tenant_id id, resource_limits limits);

    const tenant_id& id() const { return _id; }
    const resource_limits& limits() const { return _limits; }
    const usage_statistics& usage() const { return _usage; }
    resource_domain& domain() { return _domain; }

    // Update resource limits
    seastar::future<> update_limits(resource_limits new_limits);

    // Check if quota allows the request
    bool check_produce_quota(size_t estimated_size) const;
    bool check_fetch_quota(size_t estimated_size) const;

    // Update request statistics
    void record_produce_request();
    void record_fetch_request();
    void record_error();

private:
    void validate_resource_limits(const resource_limits& limits);

    tenant_id _id;
    resource_limits _limits;
    usage_statistics _usage;
    resource_domain _domain;
};

using tenant_ptr = std::shared_ptr<tenant>;

} // namespace redpanda::multitenancy
