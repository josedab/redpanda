// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compaction_strategies/cloud_native/compaction.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>

namespace compaction_strategies {

ss::future<lambda_result>
lambda_client::invoke_async(const lambda_request& request) {
    // Placeholder - would invoke actual lambda function
    lambda_result result;
    result.success = true;
    result.request_id = "request-123";
    co_return result;
}

ss::future<> cloud_native_compaction::compact_cloud_segments(
  const cloud_ntp& ntp,
  const std::vector<cloud_segment_metadata>& segments,
  cloud_config config) {
    if (config.use_lambda_functions) {
        co_return co_await compact_using_lambda(ntp, segments, config);
    } else {
        co_return co_await compact_locally(ntp, segments, config);
    }
}

ss::future<> cloud_native_compaction::compact_using_lambda(
  const cloud_ntp& ntp,
  const std::vector<cloud_segment_metadata>& segments,
  const cloud_config& config) {
    // Prepare lambda invocation
    lambda_request request;
    request.function_name = "redpanda-compaction";
    request.payload = "{}"; // Would contain actual JSON payload

    // Invoke lambda function
    auto result = co_await _lambda_client.invoke_async(request);

    if (!result.success) {
        throw std::runtime_error(result.error_message.c_str());
    }

    // Wait for completion
    co_await wait_for_lambda_completion(result.request_id);

    // Update metadata
    co_await update_cloud_manifest(ntp, result.compacted_segments);
}

ss::future<> cloud_native_compaction::compact_locally(
  const cloud_ntp& ntp,
  const std::vector<cloud_segment_metadata>& segments,
  const cloud_config& config) {
    // Download segments in parallel
    auto downloaded = co_await download_segments_parallel(
      segments, config.parallel_downloads);

    // Perform compaction
    auto compacted = co_await perform_local_compaction(downloaded);

    // Upload compacted segments
    co_await upload_segments_parallel(compacted, config.parallel_uploads);

    // Clean up old segments
    co_await delete_old_segments(segments);

    // Update manifest
    co_await update_cloud_manifest(ntp, compacted);
}

ss::future<std::vector<local_segment>>
cloud_native_compaction::download_segments_parallel(
  const std::vector<cloud_segment_metadata>& segments, size_t parallelism) {
    ss::semaphore limit(parallelism);
    std::vector<ss::future<local_segment>> futures;

    for (const auto& segment : segments) {
        futures.push_back(ss::with_semaphore(
          limit, 1, [this, segment]() { return download_segment(segment); }));
    }

    co_return co_await ss::when_all_succeed(futures.begin(), futures.end());
}

ss::future<local_segment> cloud_native_compaction::download_segment(
  const cloud_segment_metadata& metadata) {
    // Placeholder - would download from cloud storage
    auto temp_path = generate_temp_path(metadata.id);

    local_segment segment;
    segment.metadata = metadata;
    segment.path = temp_path;

    co_return segment;
}

ss::future<std::vector<local_segment>>
cloud_native_compaction::perform_local_compaction(
  const std::vector<local_segment>& segments) {
    // Placeholder - would perform actual compaction
    std::vector<local_segment> compacted = segments;
    co_return compacted;
}

ss::future<> cloud_native_compaction::upload_segments_parallel(
  const std::vector<local_segment>& segments, size_t parallelism) {
    ss::semaphore limit(parallelism);
    std::vector<ss::future<>> futures;

    for (const auto& segment : segments) {
        futures.push_back(ss::with_semaphore(
          limit, 1, [this, segment]() { return upload_segment(segment); }));
    }

    co_await ss::when_all_succeed(futures.begin(), futures.end());
}

ss::future<>
cloud_native_compaction::upload_segment(const local_segment& segment) {
    // Placeholder - would upload to cloud storage
    co_return;
}

ss::future<> cloud_native_compaction::delete_old_segments(
  const std::vector<cloud_segment_metadata>& segments) {
    // Placeholder - would delete from cloud storage
    co_return;
}

ss::future<> cloud_native_compaction::update_cloud_manifest(
  const cloud_ntp& ntp,
  const std::vector<cloud_segment_metadata>& segments) {
    // Placeholder - would update manifest in cloud storage
    co_return;
}

ss::future<> cloud_native_compaction::update_cloud_manifest(
  const cloud_ntp& ntp, const std::vector<local_segment>& segments) {
    // Placeholder - would update manifest in cloud storage
    co_return;
}

ss::future<> cloud_native_compaction::wait_for_lambda_completion(
  const ss::sstring& request_id) {
    // Placeholder - would poll for lambda completion
    co_await ss::sleep(std::chrono::seconds(1));
}

std::vector<segment_group> cloud_native_compaction::optimize_segment_sizes(
  const std::vector<cloud_segment_metadata>& segments,
  const cloud_config& config) {
    std::vector<segment_group> groups;
    segment_group current_group;
    size_t current_size = 0;

    for (const auto& segment : segments) {
        if (current_size + segment.size > config.max_segment_size) {
            // Start new group
            if (!current_group.empty()) {
                groups.push_back(std::move(current_group));
            }
            current_group.clear();
            current_size = 0;
        }

        current_group.push_back(segment);
        current_size += segment.size;
    }

    // Add last group if it meets minimum size
    if (current_size >= config.min_segment_size) {
        groups.push_back(std::move(current_group));
    }

    return groups;
}

} // namespace compaction_strategies
