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
#include "ssx/semaphore.h"

#include <seastar/core/future.hh>
#include <seastar/core/scattered_message.hh>
#include <seastar/core/temporary_buffer.hh>

#include <liburing.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <system_error>

namespace cloud_storage::zero_copy {

/// Configuration for zero-copy io_uring operations
struct zero_copy_config {
    /// Size of the io_uring submission/completion queue
    size_t ring_size = 1024;

    /// Size of each buffer in the buffer ring
    size_t buffer_size = 1048576; // 1MB buffers

    /// Number of buffers in the buffer ring
    size_t buffer_count = 64;

    /// Use huge pages for buffer allocation
    bool use_huge_pages = true;

    /// Use registered buffers for zero-copy
    bool use_registered_buffers = true;

    /// Enable kernel polling (SQPOLL)
    bool use_sqpoll = false;
};

/// Offset range for reads
struct offset_range {
    size_t offset;
    size_t size;
};

/// Registered buffer managed by io_uring
class registered_buffer {
public:
    registered_buffer(
      void* data,
      size_t size,
      uint16_t buffer_id,
      std::function<void()> release_fn)
      : _data(static_cast<char*>(data))
      , _size(size)
      , _buffer_id(buffer_id)
      , _release_fn(std::move(release_fn)) {}

    ~registered_buffer() {
        if (_release_fn) {
            _release_fn();
        }
    }

    registered_buffer(const registered_buffer&) = delete;
    registered_buffer& operator=(const registered_buffer&) = delete;
    registered_buffer(registered_buffer&&) = default;
    registered_buffer& operator=(registered_buffer&&) = default;

    char* data() { return _data; }
    const char* data() const { return _data; }
    size_t size() const { return _size; }
    uint16_t buffer_id() const { return _buffer_id; }

private:
    char* _data;
    size_t _size;
    uint16_t _buffer_id;
    std::function<void()> _release_fn;
};

/// io_uring-based cloud storage reader for zero-copy operations
class io_uring_cloud_reader {
public:
    io_uring_cloud_reader();
    ~io_uring_cloud_reader();

    io_uring_cloud_reader(const io_uring_cloud_reader&) = delete;
    io_uring_cloud_reader& operator=(const io_uring_cloud_reader&) = delete;
    io_uring_cloud_reader(io_uring_cloud_reader&&) = delete;
    io_uring_cloud_reader& operator=(io_uring_cloud_reader&&) = delete;

    /// Initialize the io_uring reader with the given configuration
    ss::future<> initialize(zero_copy_config config);

    /// Shutdown and cleanup resources
    ss::future<> shutdown();

    /// Read from a file descriptor using zero-copy
    ss::future<iobuf> read_zero_copy(int fd, offset_range range);

    /// Read into a pre-allocated buffer using zero-copy
    ss::future<size_t>
    read_zero_copy_fixed(int fd, void* buffer, size_t size, uint64_t offset);

    /// Check if zero-copy is available on this system
    static bool is_zero_copy_available();

private:
    /// Register buffer ring for zero-copy operations
    ss::future<> register_buffer_ring(const zero_copy_config& config);

    /// Wait for io_uring completion
    ss::future<> wait_for_completion(struct io_uring_cqe** cqe);

    /// Get a registered buffer by ID
    std::shared_ptr<registered_buffer> get_registered_buffer(uint16_t buffer_id);

    /// Return a buffer to the ring
    void return_buffer(uint16_t buffer_id);

    /// Create an iobuf from a registered buffer
    iobuf create_iobuf_from_buffer(
      std::shared_ptr<registered_buffer> buffer, size_t size);

private:
    struct io_uring _ring;
    struct io_uring_buf_ring* _buffer_ring = nullptr;
    void* _buffer_base = nullptr;
    size_t _buffer_base_size = 0;
    uint16_t _buffer_group_id = 0;
    zero_copy_config _config;
    bool _initialized = false;
    bool _use_huge_pages = false;

    // Buffer tracking
    std::vector<bool> _buffer_in_use;
    ssx::semaphore _buffer_sem{0};
};

} // namespace cloud_storage::zero_copy
