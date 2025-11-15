// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/zero_copy/direct_io_reader.h"

#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>

#include <fcntl.h>

namespace cloud_storage::zero_copy {

static ss::logger dio_log("direct_io");

direct_io_segment_reader::direct_io_segment_reader()
  : direct_io_segment_reader(dio_config{}) {}

direct_io_segment_reader::direct_io_segment_reader(dio_config config)
  : _config(std::move(config)) {}

direct_io_segment_reader::~direct_io_segment_reader() {
    if (_file.has_value()) {
        vlog(dio_log.warn, "direct_io_segment_reader destroyed with open file");
    }
}

ss::future<>
direct_io_segment_reader::open_segment(const std::filesystem::path& path) {
    if (_file.has_value()) {
        co_await close();
    }

    // Determine open flags
    auto flags = ss::open_flags::ro;
    if (_config.enabled) {
        flags = ss::open_flags::ro | ss::open_flags::direct;
    }

    try {
        _file = co_await ss::open_file_dma(path.string(), flags);
    } catch (const std::exception& e) {
        vlog(
          dio_log.warn,
          "Failed to open file with O_DIRECT ({}), falling back to regular "
          "I/O: {}",
          path.string(),
          e.what());

        // Fall back to regular I/O
        _config.enabled = false;
        _file = co_await ss::open_file_dma(path.string(), ss::open_flags::ro);
    }

    // Get file size
    auto stat = co_await _file->stat();
    _file_size = stat.size;

    vlog(
      dio_log.debug,
      "Opened segment file: {}, size: {}, direct_io: {}",
      path.string(),
      _file_size,
      _config.enabled);

    co_return;
}

ss::future<> direct_io_segment_reader::close() {
    if (_file.has_value()) {
        co_await _file->close();
        _file.reset();
        _file_size = 0;
    }
    co_return;
}

ss::future<iobuf>
direct_io_segment_reader::read_aligned(size_t offset, size_t size) {
    if (!_file.has_value()) {
        throw std::runtime_error("File not open");
    }

    // Clamp to file size
    if (offset >= _file_size) {
        co_return iobuf{};
    }

    size = std::min(size, _file_size - offset);

    if (_config.enabled) {
        // Align offset and size for O_DIRECT
        size_t aligned_offset = align_offset_down(offset);
        size_t end_offset = offset + size;
        size_t aligned_end = align_offset_up(end_offset);
        size_t aligned_size = aligned_end - aligned_offset;

        // Read aligned data
        auto result = co_await read_direct(aligned_offset, aligned_size);

        // Trim to requested range
        size_t skip_bytes = offset - aligned_offset;
        if (skip_bytes > 0) {
            result.trim_front(skip_bytes);
        }
        if (result.size_bytes() > size) {
            result.trim(size);
        }

        co_return std::move(result);
    } else {
        co_return co_await read_regular(offset, size);
    }
}

ss::future<iobuf>
direct_io_segment_reader::read_unaligned(size_t offset, size_t size) {
    if (!_file.has_value()) {
        throw std::runtime_error("File not open");
    }

    // Clamp to file size
    if (offset >= _file_size) {
        co_return iobuf{};
    }

    size = std::min(size, _file_size - offset);

    // Use regular I/O for unaligned reads
    co_return co_await read_regular(offset, size);
}

size_t direct_io_segment_reader::align_offset_down(size_t offset) const {
    return offset & ~(_config.alignment - 1);
}

size_t direct_io_segment_reader::align_offset_up(size_t offset) const {
    return (offset + _config.alignment - 1) & ~(_config.alignment - 1);
}

size_t direct_io_segment_reader::align_size_up(size_t size) const {
    return (size + _config.alignment - 1) & ~(_config.alignment - 1);
}

ss::future<iobuf> direct_io_segment_reader::read_direct(
  size_t aligned_offset, size_t aligned_size) {
    iobuf result;

    // Read in chunks if size is large
    size_t chunk_size = _config.buffer_size;
    size_t remaining = aligned_size;
    size_t current_offset = aligned_offset;

    while (remaining > 0) {
        size_t to_read = std::min(remaining, chunk_size);
        to_read = align_size_up(to_read);

        // Perform DMA read
        auto buffer = co_await _file->dma_read<char>(current_offset, to_read);

        result.append(std::move(buffer));

        current_offset += to_read;
        remaining = remaining > to_read ? remaining - to_read : 0;

        // Yield to allow other tasks to run for large reads
        if (result.size_bytes() % (4 * 1024 * 1024) == 0) {
            co_await ss::coroutine::maybe_yield();
        }
    }

    co_return std::move(result);
}

ss::future<iobuf>
direct_io_segment_reader::read_regular(size_t offset, size_t size) {
    iobuf result;

    // Read in chunks
    size_t chunk_size = _config.buffer_size;
    size_t remaining = size;
    size_t current_offset = offset;

    while (remaining > 0) {
        size_t to_read = std::min(remaining, chunk_size);

        // Perform regular read
        auto buffer = co_await _file->dma_read<char>(current_offset, to_read);

        // Actual bytes read might be less than requested
        size_t bytes_read = buffer.size();
        result.append(std::move(buffer));

        current_offset += bytes_read;
        remaining -= bytes_read;

        // If we got fewer bytes than requested, we've hit EOF
        if (bytes_read < to_read) {
            break;
        }

        // Yield to allow other tasks to run for large reads
        if (result.size_bytes() % (4 * 1024 * 1024) == 0) {
            co_await ss::coroutine::maybe_yield();
        }
    }

    co_return std::move(result);
}

} // namespace cloud_storage::zero_copy
