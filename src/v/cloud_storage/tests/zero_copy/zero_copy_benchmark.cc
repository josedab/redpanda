// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/zero_copy/direct_io_reader.h"
#include "cloud_storage/zero_copy/scatter_gather_buffer.h"
#include "cloud_storage/zero_copy/zero_copy_decompressor.h"
#include "compression/compression.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/perf_tests.hh>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>

namespace cloud_storage::zero_copy {

namespace {

// Helper to create test data
iobuf create_test_data(size_t size, char fill = 'A') {
    iobuf result;
    std::vector<char> data(size, fill);
    result.append(data.data(), data.size());
    return result;
}

// Helper to create a test file
std::filesystem::path create_test_file(size_t size) {
    auto path = std::filesystem::temp_directory_path()
                / "zero_copy_bench.dat";

    std::ofstream file(path, std::ios::binary);
    std::vector<char> data(size, 'A');
    file.write(data.data(), data.size());
    file.close();

    return path;
}

} // anonymous namespace

PERF_TEST(zero_copy_benchmark, direct_io_read_1mb) {
    direct_io_reader reader(dio_config{.enabled = true});

    // Create a 10MB test file
    auto path = create_test_file(10 * 1024 * 1024);

    reader.open_segment(path).get();

    perf_tests::start_measuring_time();

    // Read 1MB chunks
    for (int i = 0; i < 10; ++i) {
        auto data = reader.read_aligned(i * 1024 * 1024, 1024 * 1024).get();
        perf_tests::do_not_optimize(data);
    }

    perf_tests::stop_measuring_time();

    reader.close().get();
    std::filesystem::remove(path);
}

PERF_TEST(zero_copy_benchmark, regular_io_read_1mb) {
    direct_io_reader reader(dio_config{.enabled = false});

    // Create a 10MB test file
    auto path = create_test_file(10 * 1024 * 1024);

    reader.open_segment(path).get();

    perf_tests::start_measuring_time();

    // Read 1MB chunks
    for (int i = 0; i < 10; ++i) {
        auto data = reader.read_aligned(i * 1024 * 1024, 1024 * 1024).get();
        perf_tests::do_not_optimize(data);
    }

    perf_tests::stop_measuring_time();

    reader.close().get();
    std::filesystem::remove(path);
}

PERF_TEST(zero_copy_benchmark, zstd_decompression_streaming) {
    zero_copy_decompressor decomp;

    // Create and compress test data (1MB)
    auto test_data = create_test_data(1024 * 1024);
    auto compressor = compression::make_stream_compressor(
      compression::type::zstd);
    auto compressed = compressor->compress(test_data.copy()).get();

    perf_tests::start_measuring_time();

    // Decompress using zero-copy streaming
    auto result = decomp.decompress_in_place(
                          std::move(compressed), compression::type::zstd)
                    .get();

    perf_tests::stop_measuring_time();

    perf_tests::do_not_optimize(result);
}

PERF_TEST(zero_copy_benchmark, zstd_decompression_fallback) {
    // Create and compress test data (1MB)
    auto test_data = create_test_data(1024 * 1024);
    auto compressor = compression::make_stream_compressor(
      compression::type::zstd);
    auto compressed = compressor->compress(test_data.copy()).get();

    perf_tests::start_measuring_time();

    // Decompress using fallback path
    auto decompressor = compression::make_stream_compressor(
      compression::type::zstd);
    auto result = decompressor->uncompress(std::move(compressed)).get();

    perf_tests::stop_measuring_time();

    perf_tests::do_not_optimize(result);
}

PERF_TEST(zero_copy_benchmark, scatter_gather_buffer_allocation) {
    scatter_gather_buffer_manager mgr;
    mgr.initialize(256).get();

    perf_tests::start_measuring_time();

    // Allocate 100 buffers
    std::vector<std::unique_ptr<sg_buffer>> buffers;
    for (int i = 0; i < 100; ++i) {
        auto buffer = mgr.allocate_sg_buffer(1048576, 16).get();
        buffers.push_back(std::move(buffer));
    }

    perf_tests::stop_measuring_time();

    perf_tests::do_not_optimize(buffers);

    mgr.shutdown().get();
}

PERF_TEST(zero_copy_benchmark, memory_copy_baseline) {
    // Baseline: measure pure memcpy performance
    std::vector<char> src(1024 * 1024, 'A');
    std::vector<char> dst(1024 * 1024);

    perf_tests::start_measuring_time();

    for (int i = 0; i < 10; ++i) {
        std::memcpy(dst.data(), src.data(), src.size());
        perf_tests::do_not_optimize(dst);
    }

    perf_tests::stop_measuring_time();
}

} // namespace cloud_storage::zero_copy
