// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "metadata_service/metadata_service.h"

#include <seastar/core/abort_source.hh>

#include <absl/container/flat_hash_map.h>

namespace redpanda::metadata_service {

// Implementation using distributed consensus store (Raft/etcd/FoundationDB)
class consensus_metadata_service final : public metadata_service {
public:
    struct config {
        ss::sstring consensus_backend; // "raft", "etcd", "foundationdb"
        std::vector<ss::sstring> cluster_endpoints;
        int replication_factor = 5;
        std::chrono::milliseconds snapshot_interval{60000};
    };

    explicit consensus_metadata_service(config cfg);
    ~consensus_metadata_service() override;

    ss::future<> start();
    ss::future<> stop();

    // Metadata service interface implementation
    ss::future<partition_metadata>
    get_partition_metadata(model::ntp ntp) override;

    ss::future<> update_partition_metadata(
      model::ntp ntp, partition_metadata metadata) override;

    ss::future<consumer_group_metadata>
    get_group_metadata(kafka::group_id group_id) override;

    ss::future<> update_group_metadata(
      kafka::group_id group_id, consumer_group_metadata metadata) override;

    ss::future<model::offset>
    get_consumer_offset(kafka::group_id group_id, model::ntp ntp) override;

    ss::future<> commit_consumer_offset(
      kafka::group_id group_id,
      model::ntp ntp,
      model::offset offset,
      std::optional<ss::sstring> metadata = std::nullopt) override;

    ss::future<> watch_partition(
      model::ntp ntp,
      subscription<partition_metadata_update> handler) override;

    ss::future<> watch_consumer_group(
      kafka::group_id group_id,
      subscription<consumer_group_update> handler) override;

private:
    class metadata_store;
    class group_coordinator;
    struct offset_key;
    struct offset_commit_value;

    ss::future<> setup_watches();
    ss::future<> load_metadata();

    config _config;
    std::unique_ptr<metadata_store> _store;
    std::unique_ptr<group_coordinator> _coordinator;
    ss::abort_source _as;
};

} // namespace redpanda::metadata_service
