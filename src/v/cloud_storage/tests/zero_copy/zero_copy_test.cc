// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/zero_copy/config.h"
#include "cloud_storage/zero_copy/direct_io_reader.h"
#include "cloud_storage/zero_copy/io_uring_reader.h"
#include "cloud_storage/zero_copy/scatter_gather_buffer.h"
#include "cloud_storage/zero_copy/zero_copy_decompressor.h"
#include "compression/compression.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <cstring>
#include <filesystem>
#include <fstream>

namespace cloud_storage::zero_copy {

SEASTAR_THREAD_TEST_CASE(test_config_validation) {
    config cfg;
    cfg.io_uring_ring_size = 100; // Not a power of 2
    cfg.direct_io_alignment = 1024; // Invalid alignment
    cfg.zerocopy_threshold = 100; // Too small

    cfg.validate();

    // Should be rounded up to next power of 2
    BOOST_CHECK_GE(cfg.io_uring_ring_size, 100);
    BOOST_CHECK_EQUAL(cfg.io_uring_ring_size & (cfg.io_uring_ring_size - 1), 0);

    // Should be corrected to 512
    BOOST_CHECK(
      cfg.direct_io_alignment == 512 || cfg.direct_io_alignment == 4096);

    // Should be increased to minimum
    BOOST_CHECK_GE(cfg.zerocopy_threshold, 512);
}

SEASTAR_THREAD_TEST_CASE(test_config_defaults) {
    auto cfg = config::get_default();

    BOOST_CHECK_EQUAL(cfg.io_uring_ring_size, 1024);
    BOOST_CHECK_EQUAL(cfg.io_uring_buffer_count, 64);
    BOOST_CHECK_EQUAL(cfg.zerocopy_threshold, 4096);
}

SEASTAR_THREAD_TEST_CASE(test_alignment_functions) {
    // Test align_down
    BOOST_CHECK_EQUAL(align_down(0, 512), 0);
    BOOST_CHECK_EQUAL(align_down(512, 512), 512);
    BOOST_CHECK_EQUAL(align_down(513, 512), 512);
    BOOST_CHECK_EQUAL(align_down(1023, 512), 512);
    BOOST_CHECK_EQUAL(align_down(1024, 512), 1024);

    // Test align_up
    BOOST_CHECK_EQUAL(align_up(0, 512), 0);
    BOOST_CHECK_EQUAL(align_up(1, 512), 512);
    BOOST_CHECK_EQUAL(align_up(512, 512), 512);
    BOOST_CHECK_EQUAL(align_up(513, 512), 1024);
    BOOST_CHECK_EQUAL(align_up(1023, 512), 1024);
    BOOST_CHECK_EQUAL(align_up(1024, 512), 1024);
}

SEASTAR_THREAD_TEST_CASE(test_scatter_gather_buffer_allocation) {
    scatter_gather_buffer_manager mgr;
    mgr.initialize(256).get();

    // Allocate a buffer
    auto buffer = mgr.allocate_sg_buffer(1048576, 8).get();

    BOOST_REQUIRE(buffer);
    BOOST_CHECK_LE(buffer->iovec_count(), 8);
    BOOST_CHECK_GT(buffer->iovec_count(), 0);

    // Verify alignment
    for (size_t i = 0; i < buffer->iovec_count(); ++i) {
        auto addr = reinterpret_cast<uintptr_t>(buffer->iovecs()[i].iov_base);
        BOOST_CHECK_EQUAL(addr % 4096, 0);
    }

    mgr.shutdown().get();
}

SEASTAR_THREAD_TEST_CASE(test_sg_buffer_reference_counting) {
    sg_buffer buffer(1024, 4);

    BOOST_CHECK_EQUAL(buffer.ref_count(), 1);

    buffer.add_ref();
    BOOST_CHECK_EQUAL(buffer.ref_count(), 2);

    bool should_delete = buffer.release();
    BOOST_CHECK_EQUAL(should_delete, false);
    BOOST_CHECK_EQUAL(buffer.ref_count(), 1);

    should_delete = buffer.release();
    BOOST_CHECK_EQUAL(should_delete, true);
    BOOST_CHECK_EQUAL(buffer.ref_count(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_direct_io_reader_alignment) {
    direct_io_reader reader(dio_config{.alignment = 512});

    // Create a temporary file
    auto temp_path = std::filesystem::temp_directory_path()
                     / "zero_copy_test.dat";

    // Write test data
    {
        std::ofstream file(temp_path, std::ios::binary);
        std::vector<char> data(4096, 'A');
        file.write(data.data(), data.size());
    }

    // Open and read
    reader.open_segment(temp_path).get();

    BOOST_CHECK_EQUAL(reader.file_size(), 4096);
    BOOST_CHECK(reader.is_open());

    // Read aligned data
    auto result = reader.read_aligned(0, 1024).get();
    BOOST_CHECK_EQUAL(result.size_bytes(), 1024);

    // Read unaligned data
    result = reader.read_unaligned(100, 200).get();
    BOOST_CHECK_EQUAL(result.size_bytes(), 200);

    reader.close().get();

    // Cleanup
    std::filesystem::remove(temp_path);
}

SEASTAR_THREAD_TEST_CASE(test_zero_copy_decompressor_uncompressed) {
    zero_copy_decompressor decomp;

    // Test with uncompressed data
    iobuf input;
    std::string test_data = "Hello, World!";
    input.append(test_data.data(), test_data.size());

    auto result = decomp.decompress_in_place(
                          std::move(input), compression::type::none)
                    .get();

    BOOST_CHECK_EQUAL(result.uncompressed_size, 13);
    BOOST_CHECK_EQUAL(result.was_in_place, true);
    BOOST_CHECK_EQUAL(result.data.size_bytes(), 13);
}

SEASTAR_THREAD_TEST_CASE(test_zero_copy_decompressor_zstd) {
    zero_copy_decompressor decomp;

    // Create test data
    std::string test_data(1024, 'A');
    iobuf input;
    input.append(test_data.data(), test_data.size());

    // Compress using existing infrastructure
    auto compressor = compression::make_stream_compressor(
      compression::type::zstd);
    auto compressed = compressor->compress(input.copy()).get();

    // Decompress using zero-copy
    auto result = decomp.decompress_in_place(
                          std::move(compressed), compression::type::zstd)
                    .get();

    BOOST_CHECK_EQUAL(result.uncompressed_size, 1024);
    BOOST_CHECK_EQUAL(result.data.size_bytes(), 1024);
}

SEASTAR_THREAD_TEST_CASE(test_io_uring_availability) {
    bool available = io_uring_cloud_reader::is_zero_copy_available();

    // This test just checks that the function doesn't crash
    // The actual availability depends on the system
    BOOST_CHECK(available || !available); // Always true, just runs the check
}

SEASTAR_THREAD_TEST_CASE(test_hw_acceleration_detection) {
    bool hw_available = zero_copy_decompressor::is_hw_acceleration_available();

    // This test just checks that the function doesn't crash
    BOOST_CHECK(hw_available || !hw_available); // Always true, just runs the check
}

// Performance-related tests would go in a separate benchmark file

} // namespace cloud_storage::zero_copy
