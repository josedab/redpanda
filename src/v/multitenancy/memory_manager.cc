// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "multitenancy/memory_manager.h"

#include <thread>

namespace redpanda::multitenancy {

memory_manager::memory_manager(const memory_quota& quota)
  : _quota(quota) {}

memory_manager::memory_context::memory_context(memory_manager& mgr)
  : _mgr(mgr) {}

memory_manager::memory_context::~memory_context() {
    // Update usage statistics
    _mgr.update_usage_stats();
}

memory_manager::memory_context memory_manager::enter_context() {
    return memory_context(*this);
}

bool memory_manager::can_allocate(size_t size) const {
    return _current_usage + size <= _quota.hard_limit_bytes;
}

void memory_manager::track_allocation(size_t size) {
    // Check against hard limit
    if (_current_usage + size > _quota.hard_limit_bytes) {
        throw std::bad_alloc();
    }

    // Check against soft limit
    if (_current_usage + size > _quota.soft_limit_bytes) {
        // Try to reclaim memory
        reclaim_memory(size);

        // Still over soft limit? Apply backpressure
        if (_current_usage + size > _quota.soft_limit_bytes) {
            apply_memory_backpressure();
        }
    }

    _current_usage += size;
}

void memory_manager::track_deallocation(size_t size) {
    if (_current_usage >= size) {
        _current_usage -= size;
    }
}

void memory_manager::update_usage_stats() {
    // This would update metrics/statistics
    // For now, this is a placeholder
}

void memory_manager::reclaim_memory(size_t needed) {
    // This would evict from cache, shrink buffers, etc.
    // For now, this is a placeholder
}

void memory_manager::apply_memory_backpressure() {
    // Calculate backpressure delay based on memory pressure
    auto pressure = static_cast<double>(_current_usage)
                    / static_cast<double>(_quota.soft_limit_bytes);
    auto delay_ms = static_cast<int>((pressure - 1.0) * 100);

    if (delay_ms > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    }
}

} // namespace redpanda::multitenancy
