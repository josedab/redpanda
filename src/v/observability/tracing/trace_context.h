// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <absl/container/flat_hash_map.h>
#include <fmt/format.h>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace observability::tracing {

/// Unique identifier for a distributed trace
struct trace_id {
    uint64_t high = 0;
    uint64_t low = 0;

    bool operator==(const trace_id&) const = default;
    bool operator!=(const trace_id&) const = default;

    explicit operator bool() const { return high != 0 || low != 0; }
};

/// Unique identifier for a span within a trace
struct span_id {
    uint64_t value = 0;

    bool operator==(const span_id&) const = default;
    bool operator!=(const span_id&) const = default;

    explicit operator bool() const { return value != 0; }
};

/// Trace flags as defined by W3C Trace Context spec
struct trace_flags {
    uint8_t value = 0;

    static constexpr uint8_t sampled = 0x01;

    bool is_sampled() const { return (value & sampled) != 0; }
    void set_sampled(bool s) {
        if (s) {
            value |= sampled;
        } else {
            value &= ~sampled;
        }
    }

    bool operator==(const trace_flags&) const = default;
};

/// Vendor-specific trace state (W3C Trace Context)
using trace_state = absl::flat_hash_map<std::string, std::string>;

/// Baggage items for cross-process context propagation
using baggage = absl::flat_hash_map<std::string, std::string>;

/// Complete trace context following W3C Trace Context specification
struct trace_context {
    trace_id id;
    span_id parent_span;
    span_id current_span;
    trace_flags flags;
    trace_state state;
    baggage baggage_items;

    bool operator==(const trace_context&) const = default;
};

/// Attribute value for spans (supports multiple types)
struct attribute_value {
    enum class type { string, int64, double_, bool_ };

    type value_type;
    union {
        int64_t int_value;
        double double_value;
        bool bool_value;
    };
    std::string string_value;

    attribute_value() = default;

    explicit attribute_value(std::string_view s)
      : value_type(type::string)
      , string_value(s) {}

    explicit attribute_value(const char* s)
      : value_type(type::string)
      , string_value(s) {}

    explicit attribute_value(int64_t i)
      : value_type(type::int64)
      , int_value(i) {}

    explicit attribute_value(int i)
      : value_type(type::int64)
      , int_value(i) {}

    explicit attribute_value(uint64_t i)
      : value_type(type::int64)
      , int_value(static_cast<int64_t>(i)) {}

    explicit attribute_value(double d)
      : value_type(type::double_)
      , double_value(d) {}

    explicit attribute_value(bool b)
      : value_type(type::bool_)
      , bool_value(b) {}

    bool operator==(const attribute_value&) const = default;
};

/// Map of span attributes
using attributes = absl::flat_hash_map<std::string, attribute_value>;

/// Span kind as defined by OpenTelemetry
enum class span_kind {
    internal,  // Internal operation
    server,    // Server handling RPC
    client,    // Client making RPC
    producer,  // Message producer
    consumer   // Message consumer
};

/// Span status code
enum class status_code {
    unset = 0,
    ok = 1,
    error = 2
};

/// Event within a span
struct event {
    std::string name;
    std::chrono::system_clock::time_point timestamp;
    attributes attributes_;

    bool operator==(const event&) const = default;
};

/// Link to another span
struct link {
    trace_id trace;
    span_id span;
    attributes attributes_;

    bool operator==(const link&) const = default;
};

} // namespace observability::tracing

// Hash functions for use in containers
template<>
struct std::hash<observability::tracing::trace_id> {
    size_t operator()(const observability::tracing::trace_id& id) const {
        return std::hash<uint64_t>{}(id.high) ^ std::hash<uint64_t>{}(id.low);
    }
};

template<>
struct std::hash<observability::tracing::span_id> {
    size_t operator()(const observability::tracing::span_id& id) const {
        return std::hash<uint64_t>{}(id.value);
    }
};

// Formatting support
template<>
struct fmt::formatter<observability::tracing::trace_id> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const observability::tracing::trace_id& id, FormatContext& ctx)
      const {
        return fmt::format_to(ctx.out(), "{:016x}{:016x}", id.high, id.low);
    }
};

template<>
struct fmt::formatter<observability::tracing::span_id> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const observability::tracing::span_id& id, FormatContext& ctx)
      const {
        return fmt::format_to(ctx.out(), "{:016x}", id.value);
    }
};
