// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "observability/tracing/tracer.h"

#include <absl/strings/str_split.h>
#include <fmt/format.h>

#include <random>
#include <sstream>

namespace observability::tracing {

thread_local std::optional<trace_context> tracer::_current_context;
thread_local std::mt19937_64 tracer::_rng{std::random_device{}()};

seastar::future<span_handle>
tracer::start_span(std::string_view name, const span_options& opts) {
    if (!_config.enabled) {
        // Return an invalid span handle
        return seastar::make_ready_future<span_handle>(span_handle{});
    }

    span s;
    s.name = std::string(name);
    s.kind = opts.kind;
    s.start_time = opts.start_time;
    s.attrs = opts.initial_attributes;
    s.links = opts.links;

    // Determine parent and trace ID
    if (opts.parent) {
        // Use provided parent
        s.parent = *opts.parent;
        if (auto ctx = get_current_context()) {
            s.trace = ctx->id;
        } else {
            s.trace = generate_trace_id();
        }
    } else if (auto ctx = get_current_context()) {
        // Use current context
        s.parent = ctx->current_span;
        s.trace = ctx->id;
    } else {
        // Root span - new trace
        s.trace = generate_trace_id();
    }

    // Generate span ID
    s.id = generate_span_id();

    // Determine sampling
    trace_flags flags;
    if (should_sample()) {
        flags.set_sampled(true);
    }

    // Update current context
    trace_context new_ctx{
      .id = s.trace,
      .parent_span = s.parent,
      .current_span = s.id,
      .flags = flags,
    };

    if (auto ctx = get_current_context()) {
        new_ctx.state = ctx->state;
        new_ctx.baggage_items = ctx->baggage_items;
    }

    set_current_context(new_ctx);

    // Record span start
    co_await _recorder->record_start(s);

    co_return span_handle{std::move(s), _recorder.get()};
}

void tracer::inject_context(
  absl::flat_hash_map<std::string, std::string>& headers,
  const trace_context& ctx) {
    // W3C Trace Context format
    headers["traceparent"] = format_traceparent(ctx);

    if (!ctx.state.empty()) {
        headers["tracestate"] = format_tracestate(ctx.state);
    }

    if (!ctx.baggage_items.empty()) {
        headers["baggage"] = format_baggage(ctx.baggage_items);
    }
}

std::optional<trace_context> tracer::extract_context(
  const absl::flat_hash_map<std::string, std::string>& headers) {
    auto it = headers.find("traceparent");
    if (it == headers.end()) {
        return std::nullopt;
    }

    auto ctx = parse_traceparent(it->second);
    if (!ctx) {
        return std::nullopt;
    }

    // Parse tracestate if present
    if (auto ts_it = headers.find("tracestate"); ts_it != headers.end()) {
        ctx->state = parse_tracestate(ts_it->second);
    }

    // Parse baggage if present
    if (auto bg_it = headers.find("baggage"); bg_it != headers.end()) {
        ctx->baggage_items = parse_baggage(bg_it->second);
    }

    return ctx;
}

std::optional<trace_context> tracer::get_current_context() {
    return _current_context;
}

void tracer::set_current_context(trace_context ctx) {
    _current_context = std::move(ctx);
}

void tracer::clear_current_context() { _current_context = std::nullopt; }

trace_id tracer::generate_trace_id() {
    std::uniform_int_distribution<uint64_t> dist;
    return trace_id{.high = dist(_rng), .low = dist(_rng)};
}

span_id tracer::generate_span_id() {
    std::uniform_int_distribution<uint64_t> dist;
    return span_id{.value = dist(_rng)};
}

std::string tracer::format_traceparent(const trace_context& ctx) {
    // W3C Trace Context: version-trace_id-parent_id-trace_flags
    return fmt::format(
      "00-{:016x}{:016x}-{:016x}-{:02x}",
      ctx.id.high,
      ctx.id.low,
      ctx.current_span.value,
      ctx.flags.value);
}

std::optional<trace_context>
tracer::parse_traceparent(std::string_view traceparent) {
    // Parse W3C traceparent format: version-trace_id-parent_id-trace_flags
    std::vector<std::string_view> parts = absl::StrSplit(traceparent, '-');

    if (parts.size() != 4) {
        return std::nullopt;
    }

    // Version must be 00
    if (parts[0] != "00") {
        return std::nullopt;
    }

    trace_context ctx;

    // Parse trace ID (32 hex chars)
    if (parts[1].size() != 32) {
        return std::nullopt;
    }
    auto high = parse_hex(parts[1].substr(0, 16));
    auto low = parse_hex(parts[1].substr(16, 16));
    if (!high || !low) {
        return std::nullopt;
    }
    ctx.id = trace_id{.high = *high, .low = *low};

    // Parse parent span ID (16 hex chars)
    if (parts[2].size() != 16) {
        return std::nullopt;
    }
    auto parent = parse_hex(parts[2]);
    if (!parent) {
        return std::nullopt;
    }
    ctx.parent_span = span_id{.value = *parent};
    ctx.current_span = ctx.parent_span; // Will be updated when starting child

    // Parse trace flags (2 hex chars)
    if (parts[3].size() != 2) {
        return std::nullopt;
    }
    auto flags = parse_hex(parts[3]);
    if (!flags) {
        return std::nullopt;
    }
    ctx.flags = trace_flags{.value = static_cast<uint8_t>(*flags)};

    return ctx;
}

std::string tracer::format_tracestate(const trace_state& state) {
    std::ostringstream ss;
    bool first = true;
    for (const auto& [key, value] : state) {
        if (!first) {
            ss << ",";
        }
        ss << key << "=" << value;
        first = false;
    }
    return ss.str();
}

trace_state tracer::parse_tracestate(std::string_view tracestate) {
    trace_state state;
    std::vector<std::string_view> entries = absl::StrSplit(tracestate, ',');

    for (auto entry : entries) {
        auto pos = entry.find('=');
        if (pos != std::string_view::npos) {
            auto key = entry.substr(0, pos);
            auto value = entry.substr(pos + 1);
            state[std::string(key)] = std::string(value);
        }
    }

    return state;
}

std::string tracer::format_baggage(const baggage& baggage) {
    std::ostringstream ss;
    bool first = true;
    for (const auto& [key, value] : baggage) {
        if (!first) {
            ss << ",";
        }
        ss << key << "=" << value;
        first = false;
    }
    return ss.str();
}

baggage tracer::parse_baggage(std::string_view baggage_str) {
    baggage items;
    std::vector<std::string_view> entries = absl::StrSplit(baggage_str, ',');

    for (auto entry : entries) {
        auto pos = entry.find('=');
        if (pos != std::string_view::npos) {
            auto key = entry.substr(0, pos);
            auto value = entry.substr(pos + 1);
            items[std::string(key)] = std::string(value);
        }
    }

    return items;
}

bool tracer::should_sample() {
    if (_config.sampling_rate >= 1.0) {
        return true;
    }
    if (_config.sampling_rate <= 0.0) {
        return false;
    }

    std::uniform_real_distribution<double> dist(0.0, 1.0);
    return dist(_rng) < _config.sampling_rate;
}

std::optional<uint64_t> tracer::parse_hex(std::string_view hex) {
    uint64_t value = 0;
    auto result = std::from_chars(hex.data(), hex.data() + hex.size(), value, 16);
    if (result.ec != std::errc{} || result.ptr != hex.data() + hex.size()) {
        return std::nullopt;
    }
    return value;
}

} // namespace observability::tracing
