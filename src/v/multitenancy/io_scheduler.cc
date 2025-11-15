// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "multitenancy/io_scheduler.h"

namespace redpanda::multitenancy {

io_scheduler::io_scheduler(const disk_quota& quota)
  : _quota(quota)
  , _read_limiter(quota.read_bytes_per_second, quota.read_iops)
  , _write_limiter(quota.write_bytes_per_second, quota.write_iops) {}

io_scheduler::io_context::io_context(io_scheduler& scheduler)
  : _scheduler(scheduler) {}

io_scheduler::io_context::~io_context() {
    // Cleanup if needed
}

io_scheduler::io_context io_scheduler::enter_context() {
    return io_context(*this);
}

void io_scheduler::update_io_stats(io_direction direction, size_t bytes) {
    if (direction == io_direction::read) {
        _stats.read_bytes += bytes;
        _stats.read_ios++;
    } else {
        _stats.write_bytes += bytes;
        _stats.write_ios++;
    }
}

io_scheduler::io_limiter::io_limiter(size_t bytes_per_sec, size_t iops)
  : _bandwidth_bucket(bytes_per_sec, bytes_per_sec / 10) // 100ms burst
  , _iops_bucket(iops, iops / 10) {}

seastar::future<> io_scheduler::io_limiter::acquire(size_t bytes) {
    // Wait for bandwidth tokens
    co_await _bandwidth_bucket.acquire(bytes);

    // Wait for IOPS token
    co_await _iops_bucket.acquire(1);
}

void io_scheduler::io_limiter::release(size_t bytes) {
    _bandwidth_bucket.release(bytes);
    _iops_bucket.release(1);
}

} // namespace redpanda::multitenancy
