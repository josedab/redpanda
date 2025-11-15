// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cloud_storage_clients/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/input_stream.hh>

#include <optional>
#include <vector>

namespace redpanda::storage_service {

// Append operation options
struct append_options {
    int replication_factor = 1;
    std::optional<model::timeout_clock::duration> timeout;
    bool sync = false;
};

// Append operation result
struct append_result {
    model::offset base_offset;
    model::offset last_offset;
    model::timestamp timestamp;
};

// Reader options for fetching data
struct reader_options {
    size_t prefetch_segments = 1;
    std::optional<model::timeout_clock::duration> timeout;
    bool skip_batch_cache = false;
};

// Partition metadata
struct partition_metadata {
    model::ntp ntp;
    model::offset committed_offset;
    model::offset high_water_mark;
    model::term_id leader_epoch;
    int replication_factor = 3;
    size_t log_size_bytes = 0;
    size_t segment_count = 0;
};

// Segment metadata
struct segment_metadata {
    model::ntp ntp;
    model::offset base_offset;
    model::offset committed_offset;
    model::timestamp base_timestamp;
    model::timestamp max_timestamp;
    size_t size_bytes;
    size_t record_count;
    std::optional<model::compression> compression;
    cloud_storage::segment_name name;
};

// Index entry for efficient seeking
struct index_entry {
    model::offset offset;
    size_t filepos;
    model::timestamp timestamp;
};

// Main storage service interface
class storage_service {
public:
    virtual ~storage_service() = default;

    // Append a single batch
    virtual ss::future<append_result> append_batch(
      model::ntp ntp, model::record_batch batch, append_options opts)
      = 0;

    // Append multiple batches
    virtual ss::future<append_result> append_batches(
      model::ntp ntp,
      std::vector<model::record_batch> batches,
      append_options opts)
      = 0;

    // Create a reader for fetching records
    virtual ss::future<ss::input_stream<model::record_batch>> create_reader(
      model::ntp ntp,
      model::offset start_offset,
      model::offset end_offset,
      reader_options opts)
      = 0;

    // Get partition metadata
    virtual ss::future<partition_metadata>
    get_partition_metadata(model::ntp ntp) = 0;

    // List segments for a partition
    virtual ss::future<std::vector<segment_metadata>> list_segments(
      model::ntp ntp,
      std::optional<model::offset> start_offset = std::nullopt)
      = 0;
};

} // namespace redpanda::storage_service
