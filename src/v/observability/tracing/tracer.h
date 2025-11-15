// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "observability/tracing/span.h"
#include "observability/tracing/span_recorder.h"
#include "observability/tracing/trace_context.h"

#include <seastar/core/future.hh>

#include <charconv>
#include <memory>
#include <optional>
#include <random>
#include <string_view>

namespace observability::tracing {

/// Configuration for the tracer
struct tracer_config {
    /// Sampling rate (0.0 = never, 1.0 = always)
    double sampling_rate = 1.0;

    /// Whether tracing is enabled
    bool enabled = true;
};

/// Main interface for creating and managing distributed traces
class tracer {
public:
    explicit tracer(
      std::unique_ptr<span_recorder> recorder,
      tracer_config config = {})
      : _recorder(std::move(recorder))
      , _config(config) {}

    /// Start a new span
    seastar::future<span_handle>
    start_span(std::string_view name, const span_options& opts = {});

    /// Inject trace context into RPC headers (W3C Trace Context format)
    void inject_context(
      absl::flat_hash_map<std::string, std::string>& headers,
      const trace_context& ctx);

    /// Extract trace context from RPC headers
    std::optional<trace_context> extract_context(
      const absl::flat_hash_map<std::string, std::string>& headers);

    /// Get the current trace context (if any)
    static std::optional<trace_context> get_current_context();

    /// Set the current trace context
    static void set_current_context(trace_context ctx);

    /// Clear the current trace context
    static void clear_current_context();

    /// Flush all pending spans
    seastar::future<> flush() { return _recorder->flush(); }

    /// Check if tracing is enabled
    bool is_enabled() const { return _config.enabled; }

    /// Get the recorder (for span_handle)
    span_recorder* get_recorder() { return _recorder.get(); }

private:
    /// Generate a new trace ID
    static trace_id generate_trace_id();

    /// Generate a new span ID
    static span_id generate_span_id();

    /// Format trace context as W3C traceparent header
    std::string format_traceparent(const trace_context& ctx);

    /// Parse W3C traceparent header
    std::optional<trace_context>
    parse_traceparent(std::string_view traceparent);

    /// Format trace state as W3C tracestate header
    std::string format_tracestate(const trace_state& state);

    /// Parse W3C tracestate header
    trace_state parse_tracestate(std::string_view tracestate);

    /// Format baggage as W3C baggage header
    std::string format_baggage(const baggage& baggage);

    /// Parse W3C baggage header
    baggage parse_baggage(std::string_view baggage_str);

    /// Decide if a trace should be sampled
    bool should_sample();

    /// Parse hex string to uint64_t
    static std::optional<uint64_t> parse_hex(std::string_view hex);

private:
    std::unique_ptr<span_recorder> _recorder;
    tracer_config _config;
    static thread_local std::optional<trace_context> _current_context;
    static thread_local std::mt19937_64 _rng;
};

} // namespace observability::tracing
