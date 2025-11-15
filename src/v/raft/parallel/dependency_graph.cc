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

#include "raft/parallel/dependency_graph.h"
#include "model/record.h"

#include <queue>

namespace raft::parallel {

batch_id dependency_graph::analyze_batch(const model::record_batch& batch) {
    // Assign batch ID
    auto id = batch_id(_next_batch_id++);

    // Extract metadata
    auto keys = extract_keys(batch);
    auto metadata = batch_metadata(
      id,
      batch.base_offset(),
      batch.last_offset(),
      std::move(keys),
      model::partition_id(0),  // Will be set by caller if needed
      batch.header().first_timestamp,
      batch.contains_transactional_data(),
      batch.header().producer_id,
      batch.header().producer_epoch);

    // Check key-based dependencies
    for (const auto& key : metadata.keys) {
        auto it = _key_index.find(key);
        if (it != _key_index.end()) {
            // Found a previous batch with same key - add dependency
            add_edge(it->second, id, dependency_type::key_order);
        }
        // Update key index to point to this batch
        _key_index[key.copy()] = id;
    }

    // Check offset-based dependencies
    if (requires_strict_ordering(batch)) {
        if (_last_ordered_batch.has_value()) {
            add_edge(*_last_ordered_batch, id, dependency_type::offset_order);
        }
        _last_ordered_batch = id;
    }

    // Check transaction dependencies
    if (metadata.is_transactional && metadata.producer_id >= 0) {
        auto& tx_batches = _transaction_batches[metadata.producer_id];
        // All batches in a transaction must execute in order
        if (!tx_batches.empty()) {
            add_edge(tx_batches.back(), id, dependency_type::transaction);
        }
        tx_batches.push_back(id);
    }

    // Store metadata
    _batch_metadata.emplace(id, std::move(metadata));

    return id;
}

std::vector<iobuf> dependency_graph::extract_keys(
  const model::record_batch& batch) const {
    std::vector<iobuf> keys;

    // Early return for empty or control batches
    if (batch.empty() || batch.header().attrs.is_control()) {
        return keys;
    }

    try {
        // Iterate through records in the batch and extract keys
        auto copy = batch.copy();
        copy.for_each_record([&keys](model::record rec) {
            if (rec.has_key()) {
                keys.push_back(rec.release_key());
            }
        });
    } catch (...) {
        // If we can't extract keys, return empty vector
        // This will cause conservative dependencies to be created
    }

    return keys;
}

bool dependency_graph::requires_strict_ordering(
  const model::record_batch& batch) const {
    // Control batches always require strict ordering
    if (batch.header().attrs.is_control()) {
        return true;
    }

    // In strict mode, all batches require strict ordering
    if (_mode == detection_mode::strict) {
        return true;
    }

    // In relaxed mode, only control batches require strict ordering
    if (_mode == detection_mode::relaxed) {
        return false;
    }

    // Auto mode: require ordering for control batches and transaction markers
    return batch.header().attrs.is_control()
           || batch.header().type == model::record_batch_type::tx_fence
           || batch.header().type == model::record_batch_type::tx_prepare;
}

void dependency_graph::add_edge(
  batch_id source, batch_id target, dependency_type type) {
    _edges.push_back({source, target, type});
}

std::vector<batch_group> dependency_graph::identify_parallel_groups() const {
    return topological_sort_with_levels();
}

std::vector<batch_group> dependency_graph::topological_sort_with_levels() const {
    std::vector<batch_group> levels;

    // Calculate in-degree for each batch
    absl::flat_hash_map<batch_id, size_t> in_degree;
    for (const auto& [id, _] : _batch_metadata) {
        in_degree[id] = get_incoming_edge_count(id);
    }

    // Find all batches with no incoming edges (sources)
    std::queue<batch_id> current_level;
    for (const auto& [id, degree] : in_degree) {
        if (degree == 0) {
            current_level.push(id);
        }
    }

    // Process level by level
    while (!current_level.empty()) {
        batch_group level;
        size_t level_size = current_level.size();

        // Process all batches in current level
        for (size_t i = 0; i < level_size; ++i) {
            auto id = current_level.front();
            current_level.pop();
            level.push_back(id);

            // Decrease in-degree for all dependent batches
            for (const auto& edge : get_outgoing_edges(id)) {
                auto& degree = in_degree[edge.target];
                if (--degree == 0) {
                    current_level.push(edge.target);
                }
            }
        }

        levels.push_back(std::move(level));
    }

    return levels;
}

std::vector<batch_id> dependency_graph::find_source_batches() const {
    std::vector<batch_id> sources;
    for (const auto& [id, _] : _batch_metadata) {
        if (get_incoming_edge_count(id) == 0) {
            sources.push_back(id);
        }
    }
    return sources;
}

std::vector<dependency_edge> dependency_graph::get_outgoing_edges(
  batch_id id) const {
    std::vector<dependency_edge> outgoing;
    for (const auto& edge : _edges) {
        if (edge.source == id) {
            outgoing.push_back(edge);
        }
    }
    return outgoing;
}

size_t dependency_graph::get_incoming_edge_count(batch_id id) const {
    size_t count = 0;
    for (const auto& edge : _edges) {
        if (edge.target == id) {
            ++count;
        }
    }
    return count;
}

const batch_metadata* dependency_graph::get_metadata(batch_id id) const {
    auto it = _batch_metadata.find(id);
    if (it == _batch_metadata.end()) {
        return nullptr;
    }
    return &it->second;
}

void dependency_graph::clear() {
    _batch_metadata.clear();
    _edges.clear();
    _key_index.clear();
    _last_ordered_batch.reset();
    _transaction_batches.clear();
    _next_batch_id = 0;
}

} // namespace raft::parallel
