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

#include <seastar/core/metrics_registration.hh>
#include <seastar/net/inet_address.hh>

#include <absl/container/flat_hash_map.h>

#include <algorithm>
#include <chrono>
#include <string>

namespace metrics {

/// Client identification for attribution
struct client_id {
    std::string client_id_str;
    std::string user;
    seastar::net::inet_address ip_address;

    bool operator==(const client_id& other) const {
        return client_id_str == other.client_id_str && user == other.user
               && ip_address == other.ip_address;
    }

    std::string to_string() const {
        return fmt::format(
          "{}@{}:{}", client_id_str, user, ip_address.to_string());
    }
};

/// Client metrics data
struct client_metrics_data {
    // Request metrics
    uint64_t requests = 0;
    uint64_t errors = 0;
    hdr_hist request_latency;

    // Bandwidth metrics
    uint64_t bytes_sent = 0;
    uint64_t bytes_received = 0;

    // Resource usage
    uint64_t connection_count = 0;
    uint64_t cpu_time_us = 0;
    uint64_t memory_bytes = 0;

    client_metrics_data()
      : request_latency(
        std::chrono::microseconds(1),
        std::chrono::seconds(30)) {}
};

/// Request type enum
enum class request_type {
    produce,
    fetch,
    metadata,
    offset_commit,
    offset_fetch,
    other
};

inline bool is_produce(request_type type) {
    return type == request_type::produce;
}

inline bool is_fetch(request_type type) {
    return type == request_type::fetch;
}

/// Client attribution metrics system
class client_metrics {
public:
    client_metrics() = default;

    /// Record a client request
    void record_request(
      const client_id& client,
      request_type type,
      size_t bytes,
      std::chrono::microseconds latency,
      bool success) {
        auto& m = get_or_create(client);
        m.requests++;

        if (!success) {
            m.errors++;
        }

        m.request_latency.record(latency.count());

        if (is_produce(type)) {
            m.bytes_received += bytes;
        } else if (is_fetch(type)) {
            m.bytes_sent += bytes;
        }
    }

    /// Record resource usage for a client
    void record_resource_usage(
      const client_id& client,
      std::chrono::microseconds cpu_time,
      size_t memory_bytes) {
        auto& m = get_or_create(client);
        m.cpu_time_us += cpu_time.count();
        m.memory_bytes = memory_bytes; // Current usage, not cumulative
    }

    /// Update connection count for a client
    void update_connection_count(const client_id& client, uint64_t count) {
        auto& m = get_or_create(client);
        m.connection_count = count;
    }

    /// Get top N clients by request count
    std::vector<std::pair<client_id, double>>
    top_clients_by_requests(size_t n = 10) const {
        return top_clients_by_metric(
          n,
          [](const client_metrics_data& m) { return m.requests; });
    }

    /// Get top N clients by bandwidth (sent + received)
    std::vector<std::pair<client_id, double>>
    top_clients_by_bandwidth(size_t n = 10) const {
        return top_clients_by_metric(
          n,
          [](const client_metrics_data& m) {
              return m.bytes_sent + m.bytes_received;
          });
    }

    /// Get top N clients by error rate
    std::vector<std::pair<client_id, double>>
    top_clients_by_errors(size_t n = 10) const {
        return top_clients_by_metric(
          n,
          [](const client_metrics_data& m) { return m.errors; });
    }

    /// Get metrics for a specific client
    const client_metrics_data* get_metrics(const client_id& client) const {
        auto it = _metrics.find(client);
        return it != _metrics.end() ? &it->second : nullptr;
    }

    /// Setup public metrics for client attribution
    void setup_public_metrics(public_metric_groups& metrics_groups) {
        namespace sm = seastar::metrics;

        // Aggregate metrics across all clients
        metrics_groups.add_group(
          "client_attribution",
          {
            sm::make_gauge(
              "total_clients",
              [this] { return _metrics.size(); },
              sm::description("Total number of tracked clients")),
            sm::make_counter(
              "total_requests",
              [this] {
                  uint64_t total = 0;
                  for (const auto& [_, m] : _metrics) {
                      total += m.requests;
                  }
                  return total;
              },
              sm::description("Total requests from all clients")),
            sm::make_counter(
              "total_errors",
              [this] {
                  uint64_t total = 0;
                  for (const auto& [_, m] : _metrics) {
                      total += m.errors;
                  }
                  return total;
              },
              sm::description("Total errors from all clients")),
          });
    }

private:
    template<typename F>
    std::vector<std::pair<client_id, double>>
    top_clients_by_metric(size_t n, F metric_fn) const {
        std::vector<std::pair<client_id, double>> clients;

        for (const auto& [id, m] : _metrics) {
            clients.emplace_back(id, static_cast<double>(metric_fn(m)));
        }

        std::partial_sort(
          clients.begin(),
          clients.begin() + std::min(n, clients.size()),
          clients.end(),
          [](const auto& a, const auto& b) { return a.second > b.second; });

        clients.resize(std::min(n, clients.size()));
        return clients;
    }

    client_metrics_data& get_or_create(const client_id& client) {
        return _metrics[client];
    }

    absl::flat_hash_map<client_id, client_metrics_data> _metrics;
};

} // namespace metrics

// Hash function for client_id
template<>
struct std::hash<metrics::client_id> {
    size_t operator()(const metrics::client_id& id) const {
        size_t h1 = std::hash<std::string>{}(id.client_id_str);
        size_t h2 = std::hash<std::string>{}(id.user);
        size_t h3 = std::hash<std::string>{}(id.ip_address.to_string());
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};
