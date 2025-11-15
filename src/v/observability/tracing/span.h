// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "observability/tracing/trace_context.h"

#include <seastar/core/future.hh>

#include <chrono>
#include <memory>
#include <string>
#include <vector>

namespace observability::tracing {

class tracer;
class span_recorder;

/// A span represents a single operation within a distributed trace
struct span {
    span_id id;
    trace_id trace;
    span_id parent;
    std::string name;
    span_kind kind{span_kind::internal};
    std::chrono::system_clock::time_point start_time;
    std::chrono::system_clock::time_point end_time;
    attributes attrs;
    std::vector<event> events;
    std::vector<link> links;
    status_code status{status_code::unset};
    std::string status_message;

    /// Calculate span duration
    std::chrono::microseconds duration() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(
          end_time - start_time);
    }

    bool operator==(const span&) const = default;
};

/// Options for creating a span
struct span_options {
    span_kind kind{span_kind::internal};
    std::optional<span_id> parent;
    attributes initial_attributes;
    std::vector<link> links;
    std::chrono::system_clock::time_point start_time
      = std::chrono::system_clock::now();
};

/// RAII handle for managing span lifecycle
class span_handle {
public:
    span_handle() = default;

    span_handle(span s, span_recorder* recorder)
      : _span(std::make_unique<span>(std::move(s)))
      , _recorder(recorder) {}

    span_handle(span_handle&&) noexcept = default;
    span_handle& operator=(span_handle&&) noexcept = default;

    span_handle(const span_handle&) = delete;
    span_handle& operator=(const span_handle&) = delete;

    /// Destructor automatically finishes the span
    ~span_handle();

    /// Set an attribute on the span
    void set_attribute(std::string_view key, attribute_value value);

    /// Add an event to the span
    void add_event(std::string_view name, const attributes& attrs = {});

    /// Set the span status
    void
    set_status(status_code code, std::string_view description = std::string_view{});

    /// Manually finish the span
    seastar::future<> finish();

    /// Check if span is valid
    bool is_valid() const { return _span != nullptr; }

    /// Get the span ID
    span_id get_span_id() const {
        return _span ? _span->id : span_id{};
    }

    /// Get the trace ID
    trace_id get_trace_id() const {
        return _span ? _span->trace : trace_id{};
    }

private:
    std::unique_ptr<span> _span;
    span_recorder* _recorder{nullptr};
};

} // namespace observability::tracing
