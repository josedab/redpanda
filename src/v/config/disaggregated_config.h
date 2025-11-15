// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <seastar/core/sstring.hh>

#include <chrono>
#include <cstddef>

namespace config {

// Disaggregated mode configuration
// These settings control the new disaggregated storage and compute architecture
struct disaggregated_config {
    // Feature flag to enable disaggregated mode
    // Default: false (monolithic mode)
    bool enabled = false;

    // Storage service endpoint
    // Format: "host:port"
    // Default: "storage.redpanda.internal:9092"
    ss::sstring storage_service_endpoint = "storage.redpanda.internal:9092";

    // Metadata service endpoint
    // Format: "host:port"
    // Default: "metadata.redpanda.internal:9093"
    ss::sstring metadata_service_endpoint = "metadata.redpanda.internal:9093";

    // Compute node cache size in bytes
    // Default: 10 GiB
    size_t compute_cache_size_mb = 10240;

    // Maximum concurrent connections per compute node
    // Default: 10000
    size_t compute_max_connections = 10000;

    // Metadata refresh interval in milliseconds
    // Default: 10 seconds
    std::chrono::milliseconds compute_metadata_refresh_ms{10000};

    // Storage segment size threshold in MB
    // Default: 128 MiB
    size_t storage_segment_size_mb = 128;

    // Storage replication factor
    // Default: 3
    int storage_replication_factor = 3;

    // Cloud storage provider
    // Options: "s3", "gcs", "azure"
    // Default: "s3"
    ss::sstring storage_cloud_provider = "s3";

    // Cloud storage bucket name
    // Default: "redpanda-segments"
    ss::sstring storage_cloud_bucket = "redpanda-segments";

    // Metadata consensus backend
    // Options: "raft", "etcd", "foundationdb"
    // Default: "raft"
    ss::sstring metadata_consensus_backend = "raft";

    // Metadata replication factor
    // Default: 5
    int metadata_replication_factor = 5;

    // Metadata snapshot interval in milliseconds
    // Default: 60 seconds
    std::chrono::milliseconds metadata_snapshot_interval_ms{60000};

    // Service mesh load balancing strategy
    // Options: "round_robin", "least_connections", "random", "consistent_hash"
    // Default: "round_robin"
    ss::sstring service_mesh_load_balancing = "round_robin";

    // Service mesh heartbeat interval in milliseconds
    // Default: 5 seconds
    std::chrono::milliseconds service_mesh_heartbeat_interval_ms{5000};
};

} // namespace config
