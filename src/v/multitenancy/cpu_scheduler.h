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
#include <chrono>
#include <queue>

namespace redpanda::multitenancy {

// Hierarchical CPU scheduler with CFS-like behavior
class cpu_scheduler {
public:
    using duration = std::chrono::microseconds;

    explicit cpu_scheduler(const cpu_quota& quota);

    // Fair queuing with priority
    seastar::future<> acquire_cpu_time(duration estimated_time);

    // Release CPU time
    void release_cpu_time(duration actual_time);

    // Get current usage
    size_t active_requests() const { return _active_requests; }

private:
    struct wait_entry {
        std::chrono::nanoseconds vruntime;
        seastar::promise<> promise;

        bool operator<(const wait_entry& other) const {
            return vruntime > other.vruntime; // Min heap
        }
    };

    // CFS-inspired virtual runtime calculation
    std::chrono::nanoseconds calculate_vruntime(duration requested_time);

    // Process waiting requests
    void process_wait_queue();

    // Apply backpressure
    seastar::future<> apply_backpressure();

    cpu_quota _quota;
    token_bucket _token_bucket;
    std::priority_queue<wait_entry> _wait_queue;
    std::atomic<size_t> _active_requests{0};
    seastar::condition_variable _queue_cv;
};

} // namespace redpanda::multitenancy
