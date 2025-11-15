// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <chrono>
#include <string>

namespace observability {

/// Configuration for the observability system
struct observability_config {
    // Distributed Tracing
    bool tracing_enabled = false;
    double tracing_sampling_rate = 0.01; // 1% default
    std::string tracing_exporter = "otlp";
    std::string tracing_endpoint = "http://localhost:4317";

    // Enhanced Metrics
    bool metrics_partition_level = true;
    bool metrics_client_attribution = true;
    bool metrics_cost_tracking = false; // Disabled by default due to overhead
    std::chrono::seconds metrics_export_interval{10};

    // Live Profiling
    bool profiling_enabled = false;
    bool profiling_cpu_enabled = true;
    bool profiling_memory_enabled = false; // Higher overhead
    std::chrono::milliseconds profiling_sampling_interval{10};

    // Structured Logging
    bool logging_structured = false;
    bool logging_query_enabled = false;
    int logging_retention_days = 7;
    size_t logging_index_size_mb = 1024;
};

/// Get global observability configuration
inline observability_config& get_observability_config() {
    static observability_config config;
    return config;
}

} // namespace observability
