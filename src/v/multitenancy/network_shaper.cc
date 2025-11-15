// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "multitenancy/network_shaper.h"

namespace redpanda::multitenancy {

network_shaper::network_shaper(const network_quota& quota)
  : _quota(quota)
  , _ingress_limiter(quota.ingress_bytes_per_second)
  , _egress_limiter(quota.egress_bytes_per_second) {}

seastar::future<> network_shaper::shape_ingress(size_t bytes) {
    co_await _ingress_limiter.acquire(bytes);
    _stats.ingress_bytes += bytes;
}

seastar::future<> network_shaper::shape_egress(size_t bytes) {
    co_await _egress_limiter.acquire(bytes);
    _stats.egress_bytes += bytes;
}

seastar::future<network_shaper::connection_permit>
network_shaper::acquire_connection() {
    co_await _connection_available.wait([this] {
        return _active_connections < _quota.max_connections;
    });

    _active_connections++;
    co_return connection_permit(*this);
}

void network_shaper::release_connection() {
    _active_connections--;
    _connection_available.signal();
}

network_shaper::connection_permit::connection_permit(network_shaper& shaper)
  : _shaper(&shaper) {}

network_shaper::connection_permit::~connection_permit() {
    if (_shaper) {
        _shaper->release_connection();
    }
}

} // namespace redpanda::multitenancy
