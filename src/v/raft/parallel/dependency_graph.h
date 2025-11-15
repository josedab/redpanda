/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/record.h"
#include "model/fundamental.h"
#include "raft/fundamental.h"

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <vector>
#include <optional>

namespace raft::parallel {

// Unique identifier for a batch in the dependency graph
using batch_id = named_type<uint64_t, struct batch_id_tag>;

// Type of dependency between batches
enum class dependency_type {
    key_order,       // Same key, must preserve order
    offset_order,    // Strict offset ordering required
    transaction,     // Part of same transaction
};

// Metadata about a batch used for dependency analysis
struct batch_metadata {
    batch_id id;
    model::offset base_offset;
    model::offset last_offset;
    std::vector<iobuf> keys;  // Record keys extracted from batch
    model::partition_id partition;
    model::timestamp timestamp;
    bool is_transactional;
    int64_t producer_id;
    int32_t producer_epoch;

    batch_metadata() = default;

    batch_metadata(
      batch_id id_,
      model::offset base_,
      model::offset last_,
      std::vector<iobuf> keys_,
      model::partition_id partition_,
      model::timestamp ts,
      bool is_tx,
      int64_t pid,
      int32_t epoch)
      : id(id_)
      , base_offset(base_)
      , last_offset(last_)
      , keys(std::move(keys_))
      , partition(partition_)
      , timestamp(ts)
      , is_transactional(is_tx)
      , producer_id(pid)
      , producer_epoch(epoch) {}
};

// An edge in the dependency graph
struct dependency_edge {
    batch_id source;
    batch_id target;
    dependency_type type;
};

// A group of batches that can execute in parallel
using batch_group = std::vector<batch_id>;

/**
 * Dependency graph builder for parallel replication.
 *
 * Analyzes batches to identify dependencies and groups batches
 * into levels that can execute in parallel while maintaining
 * correctness guarantees.
 */
class dependency_graph {
public:
    enum class detection_mode {
        auto_mode,  // Balanced detection
        strict,     // Conservative, more sequential
        relaxed,    // Aggressive, more parallel
    };

    explicit dependency_graph(detection_mode mode = detection_mode::auto_mode)
      : _mode(mode)
      , _next_batch_id(0) {}

    // Analyze a batch and add it to the dependency graph
    batch_id analyze_batch(const model::record_batch& batch);

    // Identify groups of batches that can execute in parallel
    // Returns a vector of batch groups, where each group represents
    // one level of the dependency graph (batches in same group are independent)
    std::vector<batch_group> identify_parallel_groups() const;

    // Get metadata for a batch
    const batch_metadata* get_metadata(batch_id id) const;

    // Clear the graph
    void clear();

    // Get statistics about the graph
    size_t batch_count() const { return _batch_metadata.size(); }
    size_t edge_count() const { return _edges.size(); }

private:
    // Extract keys from a record batch
    std::vector<iobuf> extract_keys(const model::record_batch& batch) const;

    // Check if a batch requires strict ordering
    bool requires_strict_ordering(const model::record_batch& batch) const;

    // Add an edge to the dependency graph
    void add_edge(batch_id source, batch_id target, dependency_type type);

    // Perform topological sort with level identification
    std::vector<batch_group> topological_sort_with_levels() const;

    // Find all batches with no incoming edges
    std::vector<batch_id> find_source_batches() const;

    // Get outgoing edges for a batch
    std::vector<dependency_edge> get_outgoing_edges(batch_id id) const;

    // Get incoming edge count for a batch
    size_t get_incoming_edge_count(batch_id id) const;

    detection_mode _mode;
    uint64_t _next_batch_id;

    // Batch metadata indexed by batch_id
    absl::flat_hash_map<batch_id, batch_metadata> _batch_metadata;

    // Dependency edges
    std::vector<dependency_edge> _edges;

    // Index: record key -> batch_id that last wrote this key
    absl::flat_hash_map<iobuf, batch_id> _key_index;

    // Last batch that required strict ordering
    std::optional<batch_id> _last_ordered_batch;

    // Transactional batches: producer_id -> batch_id
    absl::flat_hash_map<int64_t, std::vector<batch_id>> _transaction_batches;
};

} // namespace raft::parallel
