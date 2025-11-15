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

#include <seastar/core/future.hh>

#include <atomic>
#include <functional>

namespace redpanda::multitenancy {

// Hierarchical I/O scheduler with tenant priorities
class io_scheduler {
public:
    explicit io_scheduler(const disk_quota& quota);

    // Schedule I/O request with tenant priority
    template<typename Op>
    seastar::future<size_t> schedule_io(
      io_direction direction, size_t size, Op&& op) {
        io_limiter* limiter = (direction == io_direction::read)
                                ? &_read_limiter
                                : &_write_limiter;

        // Wait for tokens
        co_await limiter->acquire(size);

        try {
            // Execute I/O operation
            auto bytes_transferred = co_await op();

            // Update statistics
            update_io_stats(direction, bytes_transferred);

            co_return bytes_transferred;

        } catch (...) {
            // Return unused tokens on error
            limiter->release(size);
            throw;
        }
    }

    // Scoped I/O context for requests
    class io_context {
    public:
        explicit io_context(io_scheduler& scheduler);
        ~io_context();

        io_context(const io_context&) = delete;
        io_context& operator=(const io_context&) = delete;
        io_context(io_context&&) = default;
        io_context& operator=(io_context&&) = default;

    private:
        io_scheduler& _scheduler;
    };

    io_context enter_context();

private:
    class io_limiter {
    public:
        io_limiter(size_t bytes_per_sec, size_t iops);

        seastar::future<> acquire(size_t bytes);
        void release(size_t bytes);

    private:
        token_bucket _bandwidth_bucket;
        token_bucket _iops_bucket;
    };

    void update_io_stats(io_direction direction, size_t bytes);

    disk_quota _quota;
    io_limiter _read_limiter;
    io_limiter _write_limiter;

    struct {
        std::atomic<size_t> read_bytes{0};
        std::atomic<size_t> write_bytes{0};
        std::atomic<size_t> read_ios{0};
        std::atomic<size_t> write_ios{0};
    } _stats;
};

} // namespace redpanda::multitenancy
