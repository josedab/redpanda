// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "json/document.h"
#include "json/writer.h"
#include "observability/tracing/trace_context.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/util/log.hh>

#include <chrono>
#include <optional>
#include <string>
#include <string_view>

namespace observability::logging {

using log_level = seastar::log_level;

/// A structured log entry with JSON fields
struct log_entry {
    std::chrono::system_clock::time_point timestamp;
    log_level level;
    std::string module;
    std::string message;
    json::Document fields;
    std::optional<tracing::trace_id> trace_id;
    std::optional<tracing::span_id> span_id;

    log_entry()
      : fields(json::Type::kObjectType) {}

    bool operator==(const log_entry&) const = default;
};

/// Structured logger that outputs JSON-formatted logs
class structured_logger {
public:
    explicit structured_logger(
      std::string module_name,
      size_t ring_buffer_size = 10000)
      : _module_name(std::move(module_name))
      , _ring_buffer(ring_buffer_size) {}

    /// Log a message with structured fields
    template<typename... Args>
    void log(
      log_level level,
      std::string_view format,
      Args&&... args) {
        log_entry entry;
        entry.timestamp = std::chrono::system_clock::now();
        entry.level = level;
        entry.module = _module_name;
        entry.message = fmt::format(
          fmt::runtime(format),
          std::forward<Args>(args)...);

        // Add trace context if available
        if (auto ctx = tracing::tracer::get_current_context()) {
            entry.trace_id = ctx->id;
            entry.span_id = ctx->current_span;
        }

        // Copy any persistent fields
        entry.fields.CopyFrom(_fields, entry.fields.GetAllocator());

        // Write log
        write_log(entry);
    }

    /// Add a persistent field that will be included in all subsequent logs
    template<typename T>
    structured_logger& with_field(std::string_view key, T&& value) {
        add_field_to_document(_fields, key, std::forward<T>(value));
        return *this;
    }

    /// Clear all persistent fields
    void clear_fields() {
        _fields = json::Document(json::Type::kObjectType);
    }

    /// Get recent log entries from ring buffer
    std::vector<log_entry> get_recent_logs(size_t count = 100) const {
        std::vector<log_entry> logs;
        size_t to_copy = std::min(count, _ring_buffer.size());

        auto start_it = _ring_buffer.end() - to_copy;
        for (auto it = start_it; it != _ring_buffer.end(); ++it) {
            logs.push_back(*it);
        }

        return logs;
    }

private:
    void write_log(const log_entry& entry) {
        // Store in ring buffer
        if (_ring_buffer.size() >= _ring_buffer.capacity()) {
            _ring_buffer.pop_front();
        }
        _ring_buffer.push_back(entry);

        // Format and output
        auto formatted = format_log_entry(entry);
        std::cout << formatted << std::endl;
    }

    std::string format_log_entry(const log_entry& entry) {
        json::Document doc(json::Type::kObjectType);
        auto& allocator = doc.GetAllocator();

        // Add standard fields
        doc.AddMember(
          "timestamp",
          format_timestamp(entry.timestamp),
          allocator);
        doc.AddMember(
          "level",
          to_string(entry.level),
          allocator);
        doc.AddMember("module", json::Value(entry.module, allocator), allocator);
        doc.AddMember(
          "message",
          json::Value(entry.message, allocator),
          allocator);

        // Add trace context if present
        if (entry.trace_id) {
            doc.AddMember(
              "trace_id",
              format_trace_id(*entry.trace_id),
              allocator);
        }

        if (entry.span_id) {
            doc.AddMember(
              "span_id",
              format_span_id(*entry.span_id),
              allocator);
        }

        // Add custom fields
        for (auto it = entry.fields.MemberBegin();
             it != entry.fields.MemberEnd();
             ++it) {
            json::Value key(it->name, allocator);
            json::Value value(it->value, allocator);
            doc.AddMember(key, value, allocator);
        }

        // Serialize to JSON string
        json::StringBuffer buffer;
        json::Writer<json::StringBuffer> writer(buffer);
        doc.Accept(writer);

        return buffer.GetString();
    }

    static std::string
    format_timestamp(std::chrono::system_clock::time_point tp) {
        auto time_t = std::chrono::system_clock::to_time_t(tp);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    tp.time_since_epoch())
                    % 1000;

        std::ostringstream ss;
        ss << std::put_time(std::gmtime(&time_t), "%Y-%m-%dT%H:%M:%S");
        ss << '.' << std::setfill('0') << std::setw(3) << ms.count() << 'Z';
        return ss.str();
    }

    static std::string to_string(log_level level) {
        switch (level) {
        case log_level::error:
            return "ERROR";
        case log_level::warn:
            return "WARN";
        case log_level::info:
            return "INFO";
        case log_level::debug:
            return "DEBUG";
        case log_level::trace:
            return "TRACE";
        default:
            return "UNKNOWN";
        }
    }

    static std::string format_trace_id(const tracing::trace_id& id) {
        return fmt::format("{:016x}{:016x}", id.high, id.low);
    }

    static std::string format_span_id(const tracing::span_id& id) {
        return fmt::format("{:016x}", id.value);
    }

    template<typename T>
    static void add_field_to_document(
      json::Document& doc,
      std::string_view key,
      T&& value) {
        auto& allocator = doc.GetAllocator();

        if constexpr (std::is_same_v<std::decay_t<T>, std::string>
                      || std::is_same_v<std::decay_t<T>, const char*>
                      || std::is_convertible_v<T, std::string_view>) {
            doc.AddMember(
              json::Value(key.data(), key.size(), allocator),
              json::Value(
                std::string_view(value).data(),
                std::string_view(value).size(),
                allocator),
              allocator);
        } else if constexpr (std::is_integral_v<std::decay_t<T>>) {
            doc.AddMember(
              json::Value(key.data(), key.size(), allocator),
              json::Value(static_cast<int64_t>(value)),
              allocator);
        } else if constexpr (std::is_floating_point_v<std::decay_t<T>>) {
            doc.AddMember(
              json::Value(key.data(), key.size(), allocator),
              json::Value(static_cast<double>(value)),
              allocator);
        } else if constexpr (std::is_same_v<std::decay_t<T>, bool>) {
            doc.AddMember(
              json::Value(key.data(), key.size(), allocator),
              json::Value(value),
              allocator);
        }
    }

private:
    std::string _module_name;
    json::Document _fields{json::Type::kObjectType};
    seastar::circular_buffer<log_entry> _ring_buffer;
};

} // namespace observability::logging
