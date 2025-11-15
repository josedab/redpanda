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
#include "model/fundamental.h"
#include "utils/hdr_hist.h"

#include <seastar/core/metrics_registration.hh>

#include <absl/container/flat_hash_map.h>

#include <chrono>

namespace metrics {

/// Enhanced partition-level metrics with detailed latency tracking
class partition_metrics {
public:
    struct metrics_data {
        // Throughput metrics
        uint64_t bytes_produced = 0;
        uint64_t bytes_consumed = 0;
        uint64_t records_produced = 0;
        uint64_t records_consumed = 0;

        // Latency metrics (using HDR histogram for accuracy)
        hdr_hist produce_latency;
        hdr_hist fetch_latency;
        hdr_hist commit_latency;

        // Storage metrics
        uint64_t log_size_bytes = 0;
        uint64_t segment_count = 0;
        uint64_t index_size_bytes = 0;

        // Error metrics
        uint64_t produce_errors = 0;
        uint64_t fetch_errors = 0;
        uint64_t out_of_order_records = 0;

        metrics_data()
          : produce_latency(
            std::chrono::microseconds(1),
            std::chrono::seconds(30))
          , fetch_latency(
            std::chrono::microseconds(1),
            std::chrono::seconds(30))
          , commit_latency(
            std::chrono::microseconds(1),
            std::chrono::seconds(30)) {}
    };

    partition_metrics() = default;

    /// Record a produce operation
    void record_produce(
      const model::ntp& ntp,
      size_t bytes,
      size_t records,
      std::chrono::microseconds latency) {
        auto& m = get_or_create(ntp);
        m.bytes_produced += bytes;
        m.records_produced += records;
        m.produce_latency.record(latency.count());
    }

    /// Record a fetch operation
    void record_fetch(
      const model::ntp& ntp,
      size_t bytes,
      size_t records,
      std::chrono::microseconds latency) {
        auto& m = get_or_create(ntp);
        m.bytes_consumed += bytes;
        m.records_consumed += records;
        m.fetch_latency.record(latency.count());
    }

    /// Record a commit operation
    void record_commit(
      const model::ntp& ntp,
      std::chrono::microseconds latency) {
        auto& m = get_or_create(ntp);
        m.commit_latency.record(latency.count());
    }

    /// Update storage metrics
    void update_storage_metrics(
      const model::ntp& ntp,
      uint64_t log_size,
      uint64_t segments,
      uint64_t index_size) {
        auto& m = get_or_create(ntp);
        m.log_size_bytes = log_size;
        m.segment_count = segments;
        m.index_size_bytes = index_size;
    }

    /// Record produce error
    void record_produce_error(const model::ntp& ntp) {
        auto& m = get_or_create(ntp);
        m.produce_errors++;
    }

    /// Record fetch error
    void record_fetch_error(const model::ntp& ntp) {
        auto& m = get_or_create(ntp);
        m.fetch_errors++;
    }

    /// Record out-of-order record
    void record_out_of_order(const model::ntp& ntp) {
        auto& m = get_or_create(ntp);
        m.out_of_order_records++;
    }

    /// Get metrics for a specific partition
    const metrics_data* get_metrics(const model::ntp& ntp) const {
        auto it = _metrics.find(ntp);
        return it != _metrics.end() ? &it->second : nullptr;
    }

    /// Setup Prometheus metrics for a partition
    void setup_public_metrics(
      const model::ntp& ntp,
      public_metric_groups& metrics) {
        auto& m = get_or_create(ntp);

        namespace sm = seastar::metrics;

        auto ns_label = metrics::make_namespaced_label("namespace");
        auto topic_label = metrics::make_namespaced_label("topic");
        auto partition_label = metrics::make_namespaced_label("partition");

        std::vector<sm::label_instance> labels{
          ns_label(ntp.ns()),
          topic_label(ntp.tp.topic()),
          partition_label(ntp.tp.partition()),
        };

        metrics.add_group(
          "partition",
          {
            sm::make_counter(
              "bytes_produced_total",
              [&m] { return m.bytes_produced; },
              sm::description("Total bytes produced to partition"),
              labels),
            sm::make_counter(
              "bytes_consumed_total",
              [&m] { return m.bytes_consumed; },
              sm::description("Total bytes consumed from partition"),
              labels),
            sm::make_counter(
              "records_produced_total",
              [&m] { return m.records_produced; },
              sm::description("Total records produced to partition"),
              labels),
            sm::make_counter(
              "records_consumed_total",
              [&m] { return m.records_consumed; },
              sm::description("Total records consumed from partition"),
              labels),
            sm::make_histogram(
              "produce_latency_us",
              [&m] { return m.produce_latency.seastar_histogram_logform(); },
              sm::description("Produce latency in microseconds"),
              labels),
            sm::make_histogram(
              "fetch_latency_us",
              [&m] { return m.fetch_latency.seastar_histogram_logform(); },
              sm::description("Fetch latency in microseconds"),
              labels),
            sm::make_gauge(
              "log_size_bytes",
              [&m] { return m.log_size_bytes; },
              sm::description("Partition log size in bytes"),
              labels),
            sm::make_gauge(
              "segment_count",
              [&m] { return m.segment_count; },
              sm::description("Number of log segments"),
              labels),
            sm::make_counter(
              "produce_errors_total",
              [&m] { return m.produce_errors; },
              sm::description("Total produce errors"),
              labels),
            sm::make_counter(
              "fetch_errors_total",
              [&m] { return m.fetch_errors; },
              sm::description("Total fetch errors"),
              labels),
          });
    }

private:
    metrics_data& get_or_create(const model::ntp& ntp) {
        return _metrics[ntp];
    }

    absl::flat_hash_map<model::ntp, metrics_data> _metrics;
};

} // namespace metrics
