// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "multitenancy/cpu_scheduler.h"

#include <seastar/core/sleep.hh>

namespace redpanda::multitenancy {

cpu_scheduler::cpu_scheduler(const cpu_quota& quota)
  : _quota(quota)
  , _token_bucket(
    quota.quota_per_second.count(), quota.burst_quota.count()) {}

seastar::future<> cpu_scheduler::acquire_cpu_time(duration estimated_time) {
    // Check if we have tokens
    if (!_token_bucket.try_consume(estimated_time.count())) {
        // Add to wait queue with virtual runtime
        auto vruntime = calculate_vruntime(estimated_time);

        wait_entry entry{
          .vruntime = vruntime,
          .promise = seastar::promise<>(),
        };

        auto fut = entry.promise.get_future();
        _wait_queue.emplace(std::move(entry));

        // Wait for our turn
        co_await std::move(fut);

        // Try to consume tokens again
        co_await _token_bucket.acquire(estimated_time.count());
    }

    // Track active requests
    _active_requests++;

    if (_active_requests > _quota.max_concurrent_requests) {
        // Apply backpressure
        co_await apply_backpressure();
    }
}

void cpu_scheduler::release_cpu_time(duration actual_time) {
    _active_requests--;

    // Wake up waiters
    process_wait_queue();
}

std::chrono::nanoseconds
cpu_scheduler::calculate_vruntime(duration requested_time) {
    // vruntime = actual_runtime * (nice_0_weight / weight)
    auto weight = _quota.cpu_shares;
    auto nice_0_weight = 1024.0; // Default nice value weight

    auto vruntime_us = requested_time.count() * (nice_0_weight / weight);
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::microseconds(static_cast<int64_t>(vruntime_us)));
}

void cpu_scheduler::process_wait_queue() {
    while (!_wait_queue.empty() && _token_bucket.available() > 0) {
        auto entry = std::move(const_cast<wait_entry&>(_wait_queue.top()));
        _wait_queue.pop();
        entry.promise.set_value();
    }
}

seastar::future<> cpu_scheduler::apply_backpressure() {
    // Calculate backpressure delay based on overload
    auto overload = _active_requests - _quota.max_concurrent_requests;
    auto delay = std::chrono::milliseconds(overload * 10);
    return seastar::sleep(delay);
}

} // namespace redpanda::multitenancy
