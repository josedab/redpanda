// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "storage/types.h"

#include <seastar/core/sstring.hh>

#include <chrono>
#include <vector>

namespace compaction_strategies {

using segment_id = named_type<uint64_t, struct segment_id_tag>;
using duration = std::chrono::milliseconds;

// Access patterns for workload analysis
enum class access_pattern : uint8_t {
    sequential = 0,
    random = 1,
    temporal = 2
};

// Compaction strategy types
enum class compaction_strategy : uint8_t {
    time_window = 0,
    key_based = 1,
    size_tiered = 2,
    hybrid = 3
};

// Compaction priority levels
enum class compaction_priority : uint8_t {
    low = 0,
    medium = 1,
    high = 2,
    critical = 3
};

// Compaction task types
enum class compaction_type : uint8_t {
    key_based = 0,
    time_window = 1,
    hybrid = 2
};

// Retention policy types
enum class retention_type : uint8_t {
    time_based = 0,
    size_based = 1,
    hybrid = 2
};

// Segment metadata for compaction decisions
struct segment_metadata {
    model::ntp ntp;
    segment_id id;
    size_t size_bytes{0};
    size_t live_bytes{0};
    size_t dead_bytes{0};
    std::chrono::system_clock::time_point creation_time;
    std::chrono::system_clock::time_point last_access_time;
    size_t access_frequency{0};
    double fragmentation_ratio{0.0};

    friend std::ostream& operator<<(std::ostream& o, const segment_metadata& m) {
        fmt::print(
          o,
          "{{ntp: {}, id: {}, size: {}, live: {}, dead: {}, frag: {}}}",
          m.ntp,
          m.id,
          m.size_bytes,
          m.live_bytes,
          m.dead_bytes,
          m.fragmentation_ratio);
        return o;
    }
};

// Retention policy configuration
struct retention_policy {
    retention_type type{retention_type::time_based};
    std::optional<duration> time_ms;
    std::optional<size_t> size_bytes;

    friend std::ostream& operator<<(std::ostream& o, const retention_policy& p) {
        fmt::print(
          o,
          "{{type: {}, time_ms: {}, size: {}}}",
          static_cast<int>(p.type),
          p.time_ms.has_value() ? p.time_ms.value().count() : -1,
          p.size_bytes.value_or(0));
        return o;
    }
};

// Workload characteristics for strategy selection
struct workload_characteristics {
    access_pattern pattern{access_pattern::sequential};
    double write_rate{0.0};
    double read_rate{0.0};
    retention_policy retention;
    double key_cardinality{0.0};
    bool has_deletes{false};

    friend std::ostream&
    operator<<(std::ostream& o, const workload_characteristics& w) {
        fmt::print(
          o,
          "{{pattern: {}, wr: {}, rr: {}, cardinality: {}, has_deletes: {}}}",
          static_cast<int>(w.pattern),
          w.write_rate,
          w.read_rate,
          w.key_cardinality,
          w.has_deletes);
        return o;
    }
};

// Estimated benefit of compaction
struct estimated_benefit {
    size_t bytes_reclaimed{0};
    double fragmentation_reduction{0.0};
    duration estimated_duration;

    friend std::ostream&
    operator<<(std::ostream& o, const estimated_benefit& b) {
        fmt::print(
          o,
          "{{reclaimed: {}, frag_reduction: {}, duration: {}ms}}",
          b.bytes_reclaimed,
          b.fragmentation_reduction,
          b.estimated_duration.count());
        return o;
    }
};

// Resource requirements for compaction
struct resource_requirements {
    size_t memory_bytes{0};
    size_t io_bytes{0};
    size_t cpu_time_ms{0};

    friend std::ostream&
    operator<<(std::ostream& o, const resource_requirements& r) {
        fmt::print(
          o,
          "{{memory: {}, io: {}, cpu_ms: {}}}",
          r.memory_bytes,
          r.io_bytes,
          r.cpu_time_ms);
        return o;
    }
};

// Resource availability in the cluster
struct resource_availability {
    size_t available_memory{0};
    size_t available_io_bandwidth{0};
    double cpu_utilization{0.0};

    friend std::ostream&
    operator<<(std::ostream& o, const resource_availability& r) {
        fmt::print(
          o,
          "{{memory: {}, io_bandwidth: {}, cpu_util: {}}}",
          r.available_memory,
          r.available_io_bandwidth,
          r.cpu_utilization);
        return o;
    }
};

// Compaction decision produced by scheduler
struct compaction_decision {
    model::ntp ntp;
    std::vector<segment_id> segments;
    compaction_priority priority{compaction_priority::medium};
    estimated_benefit benefit;
    compaction_strategy strategy{compaction_strategy::hybrid};
    resource_requirements resources;

    friend std::ostream&
    operator<<(std::ostream& o, const compaction_decision& d) {
        fmt::print(
          o,
          "{{ntp: {}, segments: {}, priority: {}, strategy: {}}}",
          d.ntp,
          d.segments.size(),
          static_cast<int>(d.priority),
          static_cast<int>(d.strategy));
        return o;
    }
};

// Compaction state for incremental processing
struct compaction_state {
    segment_id source_segment;
    size_t bytes_processed{0};
    size_t bytes_total{0};
    model::offset last_offset{0};
    bool is_paused{false};
    std::chrono::steady_clock::time_point started_at;
    std::chrono::steady_clock::time_point last_progress;

    bool is_complete() const { return bytes_processed >= bytes_total; }

    friend std::ostream& operator<<(std::ostream& o, const compaction_state& s) {
        fmt::print(
          o,
          "{{segment: {}, progress: {}/{}, paused: {}}}",
          s.source_segment,
          s.bytes_processed,
          s.bytes_total,
          s.is_paused);
        return o;
    }
};

// Compaction checkpoint for crash recovery
struct checkpoint {
    segment_id segment_id;
    model::offset last_offset{0};
    size_t bytes_processed{0};
    std::chrono::steady_clock::time_point timestamp;
};

// Time bucket for temporal analysis
struct time_bucket {
    model::timestamp start_time;
    model::timestamp end_time;
    size_t record_count{0};
    size_t size_bytes{0};

    friend std::ostream& operator<<(std::ostream& o, const time_bucket& b) {
        fmt::print(
          o,
          "{{start: {}, end: {}, count: {}, size: {}}}",
          b.start_time,
          b.end_time,
          b.record_count,
          b.size_bytes);
        return o;
    }
};

// Time range for time-based compaction
struct time_range {
    model::timestamp start;
    model::timestamp end;

    friend std::ostream& operator<<(std::ostream& o, const time_range& r) {
        fmt::print(o, "{{start: {}, end: {}}}", r.start, r.end);
        return o;
    }
};

// Compaction task
struct compaction_task {
    compaction_type type{compaction_type::hybrid};
    std::vector<segment_metadata> segments;
    compaction_priority priority{compaction_priority::medium};
    std::optional<time_range> time_range;

    // For key-based compaction
    std::optional<size_t> key_filter;

    friend std::ostream& operator<<(std::ostream& o, const compaction_task& t) {
        fmt::print(
          o,
          "{{type: {}, segments: {}, priority: {}}}",
          static_cast<int>(t.type),
          t.segments.size(),
          static_cast<int>(t.priority));
        return o;
    }
};

// Compaction plan containing multiple tasks
struct compaction_plan {
    std::vector<compaction_task> tasks;

    friend std::ostream& operator<<(std::ostream& o, const compaction_plan& p) {
        fmt::print(o, "{{tasks: {}}}", p.tasks.size());
        return o;
    }
};

} // namespace compaction_strategies
