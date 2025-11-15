// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "multitenancy/types.h"

#include <absl/container/flat_hash_map.h>

#include <atomic>
#include <memory>

namespace redpanda::multitenancy {

// Per-tenant memory management
class memory_manager {
public:
    explicit memory_manager(const memory_quota& quota);

    // Scoped memory context for requests
    class memory_context {
    public:
        explicit memory_context(memory_manager& mgr);
        ~memory_context();

        memory_context(const memory_context&) = delete;
        memory_context& operator=(const memory_context&) = delete;
        memory_context(memory_context&&) = default;
        memory_context& operator=(memory_context&&) = default;

    private:
        memory_manager& _mgr;
    };

    memory_context enter_context();

    // Get current memory usage
    size_t current_usage() const { return _current_usage; }

    // Check if allocation would exceed limits
    bool can_allocate(size_t size) const;

    // Track allocation
    void track_allocation(size_t size);

    // Track deallocation
    void track_deallocation(size_t size);

    // Update usage statistics
    void update_usage_stats();

private:
    void reclaim_memory(size_t needed);
    void apply_memory_backpressure();

    memory_quota _quota;
    std::atomic<size_t> _current_usage{0};
};

} // namespace redpanda::multitenancy
