// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "multitenancy/cpu_scheduler.h"
#include "multitenancy/io_scheduler.h"
#include "multitenancy/memory_manager.h"
#include "multitenancy/network_shaper.h"
#include "multitenancy/types.h"

#include <seastar/core/future.hh>

#include <chrono>
#include <functional>

namespace redpanda::multitenancy {

// Resource domain provides isolation
class resource_domain {
public:
    explicit resource_domain(const resource_limits& limits);

    // Execute work within tenant domain
    template<typename Func>
    seastar::future<std::invoke_result_t<Func>>
    execute(Func&& func, const execution_context& ctx) {
        using result_type = std::invoke_result_t<Func>;

        // Account for CPU
        co_await _cpu_scheduler.acquire_cpu_time(ctx.estimated_cpu_time);

        // Set memory context
        auto mem_guard = _memory_manager.enter_context();

        // Set I/O context
        auto io_guard = _io_scheduler.enter_context();

        // Execute with accounting
        auto start = std::chrono::steady_clock::now();

        try {
            if constexpr (std::is_same_v<result_type, void>) {
                co_await func();
                // Update usage statistics
                auto duration = std::chrono::duration_cast<
                  std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - start);
                update_usage_stats(duration, ctx);
                co_return;
            } else {
                auto result = co_await func();

                // Update usage statistics
                auto duration = std::chrono::duration_cast<
                  std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - start);
                update_usage_stats(duration, ctx);

                co_return result;
            }

        } catch (...) {
            // Still account for resources on error
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - start);
            _cpu_scheduler.release_cpu_time(duration);
            throw;
        }
    }

    // Reconfigure resource limits
    seastar::future<> reconfigure(const resource_limits& new_limits);

    // Get components
    cpu_scheduler& cpu() { return _cpu_scheduler; }
    memory_manager& memory() { return _memory_manager; }
    io_scheduler& io() { return _io_scheduler; }
    network_shaper& network() { return _network_shaper; }

private:
    void update_usage_stats(
      std::chrono::microseconds duration, const execution_context& ctx);

    cpu_scheduler _cpu_scheduler;
    memory_manager _memory_manager;
    io_scheduler _io_scheduler;
    network_shaper _network_shaper;
};

} // namespace redpanda::multitenancy
