// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cloud_storage_clients/client.h"
#include "storage_service/storage_service.h"

#include <seastar/core/abort_source.hh>

#include <absl/container/flat_hash_map.h>

namespace redpanda::storage_service {

// Implementation of storage service using cloud object storage
class cloud_storage_service final : public storage_service {
public:
    struct config {
        ss::sstring cloud_provider; // "s3", "gcs", "azure"
        ss::sstring bucket_name;
        size_t segment_size_threshold = 128 * 1024 * 1024; // 128 MiB
        int default_replication_factor = 3;
    };

    explicit cloud_storage_service(config cfg);
    ~cloud_storage_service() override;

    ss::future<> start();
    ss::future<> stop();

    // Storage service interface implementation
    ss::future<append_result> append_batch(
      model::ntp ntp,
      model::record_batch batch,
      append_options opts) override;

    ss::future<append_result> append_batches(
      model::ntp ntp,
      std::vector<model::record_batch> batches,
      append_options opts) override;

    ss::future<ss::input_stream<model::record_batch>> create_reader(
      model::ntp ntp,
      model::offset start_offset,
      model::offset end_offset,
      reader_options opts) override;

    ss::future<partition_metadata>
    get_partition_metadata(model::ntp ntp) override;

    ss::future<std::vector<segment_metadata>> list_segments(
      model::ntp ntp,
      std::optional<model::offset> start_offset = std::nullopt) override;

private:
    class segment_writer;
    class segment_reader;
    class segment_cache;

    ss::future<model::offset>
    allocate_offset(const model::ntp& ntp, size_t record_count);

    ss::future<> update_partition_metadata(
      const model::ntp& ntp, model::offset last_offset);

    ss::future<> update_segment_index(const segment_metadata& metadata);

    segment_writer* get_or_create_writer(const model::ntp& ntp);

    ss::future<> replicate_to_followers(
      const model::ntp& ntp,
      const std::vector<model::record_batch>& batches,
      int replication_factor);

    config _config;
    std::unique_ptr<cloud_storage_clients::client> _cloud_client;
    absl::flat_hash_map<model::ntp, std::unique_ptr<segment_writer>> _writers;
    absl::flat_hash_map<model::ntp, partition_metadata> _metadata;
    ss::abort_source _as;
};

} // namespace redpanda::storage_service
