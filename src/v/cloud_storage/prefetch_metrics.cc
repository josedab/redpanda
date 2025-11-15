/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/prefetch_metrics.h"

#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace cloud_storage {

prefetch_metrics::prefetch_metrics(const model::ntp& ntp)
  : _ntp(ntp) {}

void prefetch_metrics::setup_metrics(ss::metrics::metric_groups& groups) {
    namespace sm = ss::metrics;

    auto ns_label = sm::label("namespace");
    auto topic_label = sm::label("topic");
    auto partition_label = sm::label("partition");

    const std::vector<sm::label_instance> labels = {
      ns_label(_ntp.ns()),
      topic_label(_ntp.tp.topic()),
      partition_label(_ntp.tp.partition()),
    };

    groups.add_group(
      prometheus_sanitize::metrics_name("cloud_storage:prefetch"),
      {
        sm::make_counter(
          "pattern_detection_total",
          [this] { return _patterns_detected; },
          sm::description("Total number of access patterns detected"),
          labels),

        sm::make_gauge(
          "pattern_detection_latency_us",
          [this] { return _pattern_detection_latency_us; },
          sm::description(
            "Latency of pattern detection in microseconds (last measurement)"),
          labels),

        sm::make_gauge(
          "active_patterns",
          [this] { return _active_patterns; },
          sm::description("Number of currently active access patterns"),
          labels),

        sm::make_counter(
          "predictions_total",
          [this] { return _predictions_made; },
          sm::description("Total number of predictions made"),
          labels),

        sm::make_gauge(
          "prediction_confidence",
          [this] { return _prediction_confidence; },
          sm::description("Confidence of last prediction (0.0-1.0)"),
          labels),

        sm::make_gauge(
          "prediction_accuracy",
          [this] { return _prediction_accuracy; },
          sm::description("Overall prediction accuracy (0.0-1.0)"),
          labels),

        sm::make_counter(
          "prefetch_hits_total",
          [this] { return _prefetch_hits; },
          sm::description(
            "Total number of prefetch hits (prefetched data was used)"),
          labels),

        sm::make_counter(
          "prefetch_misses_total",
          [this] { return _prefetch_misses; },
          sm::description(
            "Total number of prefetch misses (needed data was not "
            "prefetched)"),
          labels),

        sm::make_counter(
          "prefetch_evictions_total",
          [this] { return _prefetch_evictions; },
          sm::description(
            "Total number of prefetched items evicted before use"),
          labels),

        sm::make_gauge(
          "prefetch_queue_size",
          [this] { return _prefetch_queue_size; },
          sm::description("Current size of prefetch queue"),
          labels),

        sm::make_gauge(
          "prefetch_latency_ms",
          [this] { return _prefetch_latency_ms; },
          sm::description(
            "Latency of prefetch operations in milliseconds (last "
            "measurement)"),
          labels),

        sm::make_gauge(
          "prefetch_memory_usage_bytes",
          [this] { return _prefetch_memory_usage; },
          sm::description("Current memory usage for prefetched data in bytes"),
          labels),

        sm::make_gauge(
          "prefetch_bandwidth_usage_bytes_per_sec",
          [this] { return _prefetch_bandwidth_usage; },
          sm::description("Current bandwidth usage for prefetching in bytes "
                          "per second"),
          labels),
      });
}

} // namespace cloud_storage
