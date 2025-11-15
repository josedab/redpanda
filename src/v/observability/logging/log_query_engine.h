// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "observability/logging/structured_logger.h"
#include "observability/tracing/trace_context.h"

#include <seastar/core/future.hh>

#include <chrono>
#include <functional>
#include <optional>
#include <regex>
#include <string>
#include <vector>

namespace observability::logging {

/// Time range for log queries
struct time_range {
    std::chrono::system_clock::time_point start;
    std::chrono::system_clock::time_point end;

    static time_range all() {
        return time_range{
          .start = std::chrono::system_clock::time_point::min(),
          .end = std::chrono::system_clock::time_point::max(),
        };
    }

    static time_range last_hours(int hours) {
        auto now = std::chrono::system_clock::now();
        return time_range{
          .start = now - std::chrono::hours(hours),
          .end = now,
        };
    }

    static time_range last_minutes(int minutes) {
        auto now = std::chrono::system_clock::now();
        return time_range{
          .start = now - std::chrono::minutes(minutes),
          .end = now,
        };
    }

    bool contains(std::chrono::system_clock::time_point tp) const {
        return tp >= start && tp <= end;
    }
};

/// Query parameters for searching logs
struct log_query {
    std::optional<time_range> time_range;
    std::optional<log_level> min_level;
    std::optional<std::string> module_filter;
    std::optional<std::string> message_pattern;
    std::optional<tracing::trace_id> trace_filter;

    /// Maximum number of results to return
    size_t max_results = 1000;
};

/// Aggregation types for log analysis
enum class aggregation_type {
    count,
    group_by,
    histogram
};

/// Simple log query engine for in-memory log searching
class log_query_engine {
public:
    explicit log_query_engine(
      const std::vector<std::reference_wrapper<structured_logger>>& loggers)
      : _loggers(loggers) {}

    /// Execute a query and return matching log entries
    seastar::future<std::vector<log_entry>> execute_query(const log_query& q) {
        std::vector<log_entry> results;

        // Scan all loggers
        for (auto& logger_ref : _loggers) {
            auto& logger = logger_ref.get();
            auto recent_logs = logger.get_recent_logs(10000);

            for (const auto& entry : recent_logs) {
                if (matches_query(entry, q)) {
                    results.push_back(entry);

                    if (results.size() >= q.max_results) {
                        co_return results;
                    }
                }
            }
        }

        co_return results;
    }

    /// Count matching log entries
    seastar::future<size_t> count_matching(const log_query& q) {
        size_t count = 0;

        for (auto& logger_ref : _loggers) {
            auto& logger = logger_ref.get();
            auto recent_logs = logger.get_recent_logs(10000);

            for (const auto& entry : recent_logs) {
                if (matches_query(entry, q)) {
                    count++;
                }
            }
        }

        co_return count;
    }

    /// Group log entries by a field (e.g., module, level)
    seastar::future<std::map<std::string, size_t>>
    group_by_module(const log_query& q) {
        std::map<std::string, size_t> groups;

        for (auto& logger_ref : _loggers) {
            auto& logger = logger_ref.get();
            auto recent_logs = logger.get_recent_logs(10000);

            for (const auto& entry : recent_logs) {
                if (matches_query(entry, q)) {
                    groups[entry.module]++;
                }
            }
        }

        co_return groups;
    }

    /// Group log entries by log level
    seastar::future<std::map<log_level, size_t>>
    group_by_level(const log_query& q) {
        std::map<log_level, size_t> groups;

        for (auto& logger_ref : _loggers) {
            auto& logger = logger_ref.get();
            auto recent_logs = logger.get_recent_logs(10000);

            for (const auto& entry : recent_logs) {
                if (matches_query(entry, q)) {
                    groups[entry.level]++;
                }
            }
        }

        co_return groups;
    }

    /// Get error rate over time
    seastar::future<double> calculate_error_rate(const log_query& q) {
        size_t total = 0;
        size_t errors = 0;

        for (auto& logger_ref : _loggers) {
            auto& logger = logger_ref.get();
            auto recent_logs = logger.get_recent_logs(10000);

            for (const auto& entry : recent_logs) {
                if (matches_query(entry, q)) {
                    total++;
                    if (entry.level == log_level::error) {
                        errors++;
                    }
                }
            }
        }

        double error_rate = total > 0 ? static_cast<double>(errors) / total
                                      : 0.0;
        co_return error_rate;
    }

private:
    bool matches_query(const log_entry& entry, const log_query& q) const {
        // Check time range
        if (q.time_range && !q.time_range->contains(entry.timestamp)) {
            return false;
        }

        // Check minimum log level
        if (q.min_level && entry.level < *q.min_level) {
            return false;
        }

        // Check module filter
        if (q.module_filter && entry.module != *q.module_filter) {
            return false;
        }

        // Check message pattern (regex)
        if (q.message_pattern) {
            try {
                std::regex pattern(*q.message_pattern);
                if (!std::regex_search(entry.message, pattern)) {
                    return false;
                }
            } catch (const std::regex_error&) {
                // Invalid regex, skip pattern matching
            }
        }

        // Check trace ID filter
        if (q.trace_filter && entry.trace_id != q.trace_filter) {
            return false;
        }

        return true;
    }

    std::vector<std::reference_wrapper<structured_logger>> _loggers;
};

} // namespace observability::logging
