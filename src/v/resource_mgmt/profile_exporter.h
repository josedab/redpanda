// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <cstdint>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace resource_mgmt {

/// Stack frame in a profile
struct stack_frame {
    std::string function;
    std::string file;
    uint64_t line = 0;
    uint64_t address = 0;

    bool operator==(const stack_frame&) const = default;
};

/// Sample in a CPU profile
struct profile_sample {
    std::vector<stack_frame> stack_trace;
    uint64_t count = 0;

    bool operator==(const profile_sample&) const = default;
};

/// CPU profile data structure
struct cpu_profile {
    std::vector<profile_sample> samples;
    std::chrono::microseconds sample_period{10000}; // 10ms default
    std::chrono::system_clock::time_point start_time;
    std::chrono::microseconds duration{0};

    void add_sample(const std::vector<stack_frame>& stack, uint64_t count) {
        samples.push_back(profile_sample{.stack_trace = stack, .count = count});
    }
};

/// Exports CPU profiles to various formats
class profile_exporter {
public:
    profile_exporter() = default;

    /// Export profile in FlameGraph format
    /// Format: <stack_trace> <count>
    /// Example: "main;foo;bar 100"
    seastar::future<seastar::sstring>
    export_flamegraph(const cpu_profile& profile) {
        std::ostringstream ss;

        for (const auto& sample : profile.samples) {
            // Build stack string from bottom to top
            std::string stack;
            for (auto it = sample.stack_trace.rbegin();
                 it != sample.stack_trace.rend();
                 ++it) {
                if (!stack.empty()) {
                    stack += ";";
                }
                stack += it->function.empty() ? fmt::format(
                           "0x{:x}",
                           it->address)
                                              : it->function;
            }

            ss << stack << " " << sample.count << "\n";
        }

        co_return seastar::sstring(ss.str());
    }

    /// Export profile in collapsed stack format (same as FlameGraph but more compact)
    seastar::future<seastar::sstring>
    export_collapsed(const cpu_profile& profile) {
        // Collapsed format is the same as flamegraph format
        return export_flamegraph(profile);
    }

    /// Export profile in simple text format for debugging
    seastar::future<seastar::sstring>
    export_text(const cpu_profile& profile) {
        std::ostringstream ss;

        ss << "CPU Profile\n";
        ss << "===========\n";
        ss << "Sample period: " << profile.sample_period.count() << " us\n";
        ss << "Duration: " << profile.duration.count() << " us\n";
        ss << "Total samples: " << profile.samples.size() << "\n\n";

        uint64_t total_count = 0;
        for (const auto& sample : profile.samples) {
            total_count += sample.count;
        }

        ss << "Total count: " << total_count << "\n\n";

        // Sort samples by count (descending)
        auto sorted_samples = profile.samples;
        std::sort(
          sorted_samples.begin(),
          sorted_samples.end(),
          [](const auto& a, const auto& b) { return a.count > b.count; });

        // Show top 20 stacks
        size_t to_show = std::min(size_t(20), sorted_samples.size());
        for (size_t i = 0; i < to_show; ++i) {
            const auto& sample = sorted_samples[i];
            double percentage = (static_cast<double>(sample.count) / total_count)
                                * 100.0;

            ss << fmt::format(
              "{} samples ({:.2f}%):\n",
              sample.count,
              percentage);

            for (const auto& frame : sample.stack_trace) {
                ss << "  ";
                if (!frame.function.empty()) {
                    ss << frame.function;
                } else {
                    ss << fmt::format("0x{:x}", frame.address);
                }
                if (!frame.file.empty()) {
                    ss << " (" << frame.file;
                    if (frame.line > 0) {
                        ss << ":" << frame.line;
                    }
                    ss << ")";
                }
                ss << "\n";
            }
            ss << "\n";
        }

        co_return seastar::sstring(ss.str());
    }

    /// Export profile in speedscope JSON format
    /// See: https://github.com/jlfwong/speedscope
    seastar::future<seastar::sstring>
    export_speedscope(const cpu_profile& profile) {
        std::ostringstream ss;

        ss << "{\n";
        ss << "  \"$schema\": "
              "\"https://www.speedscope.app/file-format-schema.json\",\n";
        ss << "  \"profiles\": [\n";
        ss << "    {\n";
        ss << "      \"type\": \"sampled\",\n";
        ss << "      \"name\": \"CPU Profile\",\n";
        ss << "      \"unit\": \"microseconds\",\n";
        ss << "      \"startValue\": 0,\n";
        ss << "      \"endValue\": " << profile.duration.count() << ",\n";
        ss << "      \"samples\": [\n";

        // Build samples
        bool first_sample = true;
        for (const auto& sample : profile.samples) {
            if (!first_sample) {
                ss << ",\n";
            }
            first_sample = false;

            ss << "        [";
            bool first_frame = true;
            for (const auto& frame : sample.stack_trace) {
                if (!first_frame) {
                    ss << ", ";
                }
                first_frame = false;
                ss << "\"" << (frame.function.empty() ? fmt::format("0x{:x}", frame.address) : frame.function)
                   << "\"";
            }
            ss << "]";
        }

        ss << "\n      ],\n";
        ss << "      \"weights\": [";

        // Add weights (counts)
        bool first_weight = true;
        for (const auto& sample : profile.samples) {
            if (!first_weight) {
                ss << ", ";
            }
            first_weight = false;
            ss << sample.count;
        }

        ss << "]\n";
        ss << "    }\n";
        ss << "  ],\n";
        ss << "  \"shared\": {\n";
        ss << "    \"frames\": []\n";
        ss << "  }\n";
        ss << "}\n";

        co_return seastar::sstring(ss.str());
    }
};

} // namespace resource_mgmt
