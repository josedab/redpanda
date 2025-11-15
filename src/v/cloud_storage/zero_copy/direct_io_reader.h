// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "bytes/iobuf.h"

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>

#include <cstddef>
#include <filesystem>
#include <optional>

namespace cloud_storage::zero_copy {

/// Configuration for Direct I/O operations
struct dio_config {
    /// Enable Direct I/O (O_DIRECT)
    bool enabled = true;

    /// Alignment requirement for O_DIRECT (typically 512 or 4096)
    size_t alignment = 512;

    /// Default buffer size for reads
    size_t buffer_size = 1048576; // 1MB

    /// Use AIO (Linux async I/O)
    bool use_aio = true;

    /// Use io_uring instead of AIO
    bool use_io_uring = true;
};

/// Direct I/O segment reader for zero-copy file operations
class direct_io_segment_reader {
public:
    direct_io_segment_reader();
    explicit direct_io_segment_reader(dio_config config);
    ~direct_io_segment_reader();

    direct_io_segment_reader(const direct_io_segment_reader&) = delete;
    direct_io_segment_reader& operator=(const direct_io_segment_reader&)
      = delete;
    direct_io_segment_reader(direct_io_segment_reader&&) = default;
    direct_io_segment_reader& operator=(direct_io_segment_reader&&) = default;

    /// Open a segment file for reading
    ss::future<> open_segment(const std::filesystem::path& path);

    /// Close the segment file
    ss::future<> close();

    /// Read data with alignment handling for O_DIRECT
    ss::future<iobuf> read_aligned(size_t offset, size_t size);

    /// Read data without alignment (uses regular I/O)
    ss::future<iobuf> read_unaligned(size_t offset, size_t size);

    /// Get the file size
    size_t file_size() const { return _file_size; }

    /// Check if the file is open
    bool is_open() const { return _file.has_value(); }

    /// Get the current configuration
    const dio_config& config() const { return _config; }

private:
    /// Align offset down to alignment boundary
    size_t align_offset_down(size_t offset) const;

    /// Align offset up to alignment boundary
    size_t align_offset_up(size_t offset) const;

    /// Align size up to alignment boundary
    size_t align_size_up(size_t size) const;

    /// Read using Direct I/O
    ss::future<iobuf>
    read_direct(size_t aligned_offset, size_t aligned_size);

    /// Read using regular I/O (fallback)
    ss::future<iobuf> read_regular(size_t offset, size_t size);

private:
    dio_config _config;
    std::optional<ss::file> _file;
    size_t _file_size = 0;
};

} // namespace cloud_storage::zero_copy
