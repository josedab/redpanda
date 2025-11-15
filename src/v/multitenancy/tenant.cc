// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "multitenancy/tenant.h"

#include <stdexcept>

namespace redpanda::multitenancy {

tenant::tenant(tenant_id id, resource_limits limits)
  : _id(std::move(id))
  , _limits(limits)
  , _domain(limits) {
    validate_resource_limits(limits);
}

seastar::future<> tenant::update_limits(resource_limits new_limits) {
    // Validate limits
    validate_resource_limits(new_limits);

    // Update resource domain
    co_await _domain.reconfigure(new_limits);

    _limits = new_limits;
}

bool tenant::check_produce_quota(size_t estimated_size) const {
    // Check if current usage plus estimated size is within limits
    auto current_net = _usage.network_bytes_out.load();
    return current_net + estimated_size <= _limits.network.egress_bytes_per_second;
}

bool tenant::check_fetch_quota(size_t estimated_size) const {
    // Check if current usage plus estimated size is within limits
    auto current_net = _usage.network_bytes_in.load();
    return current_net + estimated_size <= _limits.network.ingress_bytes_per_second;
}

void tenant::record_produce_request() { _usage.produce_requests++; }

void tenant::record_fetch_request() { _usage.fetch_requests++; }

void tenant::record_error() { _usage.errors++; }

void tenant::validate_resource_limits(const resource_limits& limits) {
    // Validate CPU limits
    if (limits.cpu.quota_per_second.count() <= 0) {
        throw std::invalid_argument("CPU quota must be positive");
    }

    // Validate memory limits
    if (limits.memory.hard_limit_bytes <= 0) {
        throw std::invalid_argument("Memory hard limit must be positive");
    }
    if (limits.memory.soft_limit_bytes > limits.memory.hard_limit_bytes) {
        throw std::invalid_argument(
          "Memory soft limit must not exceed hard limit");
    }

    // Validate disk limits
    if (limits.disk.max_storage_bytes <= 0) {
        throw std::invalid_argument("Disk storage limit must be positive");
    }

    // Validate network limits
    if (limits.network.max_connections <= 0) {
        throw std::invalid_argument("Max connections must be positive");
    }
}

} // namespace redpanda::multitenancy
