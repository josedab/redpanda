// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "multitenancy/token_bucket.h"
#include "multitenancy/types.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>

#include <atomic>

namespace redpanda::multitenancy {

// Network bandwidth and connection management
class network_shaper {
public:
    explicit network_shaper(const network_quota& quota);

    // Shape incoming traffic
    seastar::future<> shape_ingress(size_t bytes);

    // Shape outgoing traffic
    seastar::future<> shape_egress(size_t bytes);

    // Connection management
    class connection_permit {
    public:
        explicit connection_permit(network_shaper& shaper);
        ~connection_permit();

        connection_permit(const connection_permit&) = delete;
        connection_permit& operator=(const connection_permit&) = delete;
        connection_permit(connection_permit&&) = default;
        connection_permit& operator=(connection_permit&&) = default;

    private:
        network_shaper* _shaper;
    };

    seastar::future<connection_permit> acquire_connection();

    // Get statistics
    size_t ingress_bytes() const { return _stats.ingress_bytes; }
    size_t egress_bytes() const { return _stats.egress_bytes; }
    size_t active_connections() const { return _active_connections; }

private:
    void release_connection();

    network_quota _quota;
    token_bucket _ingress_limiter;
    token_bucket _egress_limiter;
    std::atomic<size_t> _active_connections{0};
    seastar::condition_variable _connection_available;

    struct {
        std::atomic<size_t> ingress_bytes{0};
        std::atomic<size_t> egress_bytes{0};
    } _stats;
};

} // namespace redpanda::multitenancy
