// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "metrics/metrics.h"
#include "utils/hdr_hist.h"

#include <seastar/core/future.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/reactor.hh>

#include <absl/container/flat_hash_map.h>

#include <algorithm>
#include <chrono>
#include <string>
#include <string_view>

namespace metrics {

/// Cost information for an operation
struct operation_cost {
    std::chrono::microseconds cpu_time{0};
    size_t memory_bytes = 0;
    size_t disk_io_bytes = 0;
    size_t network_io_bytes = 0;

    operation_cost& operator+=(const operation_cost& other) {
        cpu_time += other.cpu_time;
        memory_bytes += other.memory_bytes;
        disk_io_bytes += other.disk_io_bytes;
        network_io_bytes += other.network_io_bytes;
        return *this;
    }
};

/// Operation cost history with histograms
struct operation_cost_history {
    hdr_hist cpu_time_hist;
    hdr_hist memory_hist;
    hdr_hist disk_io_hist;
    hdr_hist network_io_hist;

    uint64_t operation_count = 0;
    operation_cost total_cost;

    operation_cost_history()
      : cpu_time_hist(
        std::chrono::microseconds(1),
        std::chrono::seconds(60))
      , memory_hist(1, 1024 * 1024 * 1024) // 1 byte to 1GB
      , disk_io_hist(1, 1024 * 1024 * 1024)
      , network_io_hist(1, 1024 * 1024 * 1024) {}

    void record(const operation_cost& cost) {
        operation_count++;
        total_cost += cost;
        cpu_time_hist.record(cost.cpu_time.count());
        memory_hist.record(cost.memory_bytes);
        disk_io_hist.record(cost.disk_io_bytes);
        network_io_hist.record(cost.network_io_bytes);
    }
};

/// Tracks costs of operations for billing and optimization
class cost_tracker {
public:
    cost_tracker() = default;

    /// Track the cost of an operation
    template<typename T>
    seastar::future<T> track_operation_cost(
      std::string_view operation,
      seastar::future<T> fut) {
        if (!_enabled) {
            co_return co_await std::move(fut);
        }

        auto start_time = std::chrono::steady_clock::now();
        auto start_cpu = get_thread_cpu_time();
        auto start_mem = get_current_memory();

        try {
            T result = co_await std::move(fut);

            auto end_time = std::chrono::steady_clock::now();
            auto end_cpu = get_thread_cpu_time();
            auto end_mem = get_current_memory();

            operation_cost cost{
              .cpu_time = std::chrono::duration_cast<std::chrono::microseconds>(
                end_cpu - start_cpu),
              .memory_bytes = end_mem > start_mem ? end_mem - start_mem : 0,
              .disk_io_bytes = 0,    // TODO: Track from I/O subsystem
              .network_io_bytes = 0, // TODO: Track from network subsystem
            };

            record_cost(operation, cost);

            co_return result;
        } catch (...) {
            auto end_time = std::chrono::steady_clock::now();
            auto end_cpu = get_thread_cpu_time();
            auto end_mem = get_current_memory();

            operation_cost cost{
              .cpu_time = std::chrono::duration_cast<std::chrono::microseconds>(
                end_cpu - start_cpu),
              .memory_bytes = end_mem > start_mem ? end_mem - start_mem : 0,
            };

            record_cost(operation, cost);
            throw;
        }
    }

    /// Manually record operation cost
    void record_cost(std::string_view operation, const operation_cost& cost) {
        auto& history = _costs[std::string(operation)];
        history.record(cost);
    }

    /// Get top N operations by CPU time
    std::vector<std::pair<std::string, operation_cost>>
    top_operations_by_cpu(size_t n = 10) const {
        return top_operations_by_metric(
          n,
          [](const operation_cost_history& h) {
              return h.total_cost.cpu_time.count();
          });
    }

    /// Get top N operations by memory usage
    std::vector<std::pair<std::string, operation_cost>>
    top_operations_by_memory(size_t n = 10) const {
        return top_operations_by_metric(
          n,
          [](const operation_cost_history& h) {
              return h.total_cost.memory_bytes;
          });
    }

    /// Get cost history for a specific operation
    const operation_cost_history*
    get_operation_history(std::string_view operation) const {
        auto it = _costs.find(std::string(operation));
        return it != _costs.end() ? &it->second : nullptr;
    }

    /// Enable or disable cost tracking
    void set_enabled(bool enabled) { _enabled = enabled; }

    /// Check if cost tracking is enabled
    bool is_enabled() const { return _enabled; }

    /// Setup public metrics
    void setup_public_metrics(public_metric_groups& metrics_groups) {
        namespace sm = seastar::metrics;

        metrics_groups.add_group(
          "operation_cost",
          {
            sm::make_gauge(
              "tracked_operations",
              [this] { return _costs.size(); },
              sm::description("Number of tracked operations")),
            sm::make_counter(
              "total_cpu_time_us",
              [this] {
                  uint64_t total = 0;
                  for (const auto& [_, h] : _costs) {
                      total += h.total_cost.cpu_time.count();
                  }
                  return total;
              },
              sm::description("Total CPU time across all operations")),
            sm::make_counter(
              "total_operations",
              [this] {
                  uint64_t total = 0;
                  for (const auto& [_, h] : _costs) {
                      total += h.operation_count;
                  }
                  return total;
              },
              sm::description("Total number of tracked operations")),
          });
    }

private:
    template<typename F>
    std::vector<std::pair<std::string, operation_cost>>
    top_operations_by_metric(size_t n, F metric_fn) const {
        std::vector<std::pair<std::string, operation_cost>> operations;

        for (const auto& [op, history] : _costs) {
            operations.emplace_back(op, history.total_cost);
        }

        std::partial_sort(
          operations.begin(),
          operations.begin() + std::min(n, operations.size()),
          operations.end(),
          [&metric_fn](const auto& a, const auto& b) {
              return metric_fn(_costs.at(a.first))
                     > metric_fn(_costs.at(b.first));
          });

        operations.resize(std::min(n, operations.size()));
        return operations;
    }

    static std::chrono::nanoseconds get_thread_cpu_time() {
        // Use Seastar's CPU time tracking
        return std::chrono::nanoseconds(seastar::engine().total_busy_time());
    }

    static size_t get_current_memory() {
        // Use Seastar's memory stats
        return seastar::memory::stats().total_memory();
    }

    bool _enabled = true;
    absl::flat_hash_map<std::string, operation_cost_history> _costs;
};

} // namespace metrics
