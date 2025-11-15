// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "compaction_strategies/base/types.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>

#include <filesystem>
#include <vector>

namespace compaction_strategies {

// Cloud segment metadata
struct cloud_segment_metadata {
    segment_id id;
    std::filesystem::path path;
    size_t size{0};
};

// Local segment representation
struct local_segment {
    cloud_segment_metadata metadata;
    std::filesystem::path path;
};

// Segment group for optimization
using segment_group = std::vector<cloud_segment_metadata>;

// Cloud storage ntp
struct cloud_ntp {
    model::ntp ntp;

    std::filesystem::path path() const {
        return std::filesystem::path(ntp.path());
    }
};

// Configuration for cloud-native compaction
struct cloud_config {
    size_t min_segment_size{134217728}; // 128MB
    size_t max_segment_size{1073741824}; // 1GB
    bool compact_in_cloud{true};
    bool use_lambda_functions{false};
    size_t parallel_downloads{4};
    size_t parallel_uploads{2};
};

// Lambda request for serverless compaction
struct lambda_request {
    ss::sstring function_name;
    ss::sstring payload;
};

// Lambda result
struct lambda_result {
    bool success{false};
    ss::sstring error_message;
    ss::sstring request_id;
    std::vector<cloud_segment_metadata> compacted_segments;
};

// Lambda client for serverless operations
class lambda_client {
public:
    ss::future<lambda_result> invoke_async(const lambda_request& request);
};

// Cloud-native compaction for tiered storage
class cloud_native_compaction {
public:
    cloud_native_compaction() = default;

    // Compact cloud segments
    ss::future<> compact_cloud_segments(
      const cloud_ntp& ntp,
      const std::vector<cloud_segment_metadata>& segments,
      cloud_config config = {});

private:
    // Compact using lambda functions
    ss::future<> compact_using_lambda(
      const cloud_ntp& ntp,
      const std::vector<cloud_segment_metadata>& segments,
      const cloud_config& config);

    // Compact locally by downloading segments
    ss::future<> compact_locally(
      const cloud_ntp& ntp,
      const std::vector<cloud_segment_metadata>& segments,
      const cloud_config& config);

    // Download segments in parallel
    ss::future<std::vector<local_segment>> download_segments_parallel(
      const std::vector<cloud_segment_metadata>& segments, size_t parallelism);

    // Download a single segment
    ss::future<local_segment>
    download_segment(const cloud_segment_metadata& metadata);

    // Perform local compaction
    ss::future<std::vector<local_segment>>
    perform_local_compaction(const std::vector<local_segment>& segments);

    // Upload segments in parallel
    ss::future<> upload_segments_parallel(
      const std::vector<local_segment>& segments, size_t parallelism);

    // Upload a single segment
    ss::future<> upload_segment(const local_segment& segment);

    // Delete old segments from cloud storage
    ss::future<>
    delete_old_segments(const std::vector<cloud_segment_metadata>& segments);

    // Update cloud manifest
    ss::future<> update_cloud_manifest(
      const cloud_ntp& ntp,
      const std::vector<cloud_segment_metadata>& segments);

    ss::future<> update_cloud_manifest(
      const cloud_ntp& ntp, const std::vector<local_segment>& segments);

    // Wait for lambda completion
    ss::future<> wait_for_lambda_completion(const ss::sstring& request_id);

    // Optimize segment sizes for cloud storage
    std::vector<segment_group> optimize_segment_sizes(
      const std::vector<cloud_segment_metadata>& segments,
      const cloud_config& config);

    // Generate temporary path for downloads
    std::filesystem::path
    generate_temp_path(const segment_id& id) const {
        return std::filesystem::path("/tmp")
               / ("segment_" + std::to_string(id()));
    }

private:
    lambda_client _lambda_client;
};

} // namespace compaction_strategies
