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

#include <seastar/core/future.hh>

#include <liburing.h>
#include <sys/socket.h>
#include <sys/uio.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <vector>

namespace cloud_storage::zero_copy {

/// Scatter-gather buffer for zero-copy I/O operations
class sg_buffer {
public:
    sg_buffer(size_t total_size, size_t max_fragments);
    ~sg_buffer();

    sg_buffer(const sg_buffer&) = delete;
    sg_buffer& operator=(const sg_buffer&) = delete;
    sg_buffer(sg_buffer&&) = default;
    sg_buffer& operator=(sg_buffer&&) = default;

    /// Get the iovecs for scatter-gather I/O
    struct iovec* iovecs() { return _iovecs.data(); }
    const struct iovec* iovecs() const { return _iovecs.data(); }

    /// Get the number of iovecs
    size_t iovec_count() const { return _iovecs.size(); }

    /// Get the total size of all fragments
    size_t total_size() const { return _total_size; }

    /// Check if buffer is registered with io_uring
    bool is_registered() const { return _is_registered; }

    /// Mark buffer as registered
    void set_registered(bool registered) { _is_registered = registered; }

    /// Increment reference count
    void add_ref() { _ref_count.fetch_add(1, std::memory_order_relaxed); }

    /// Decrement reference count and return true if count reached zero
    bool release() {
        return _ref_count.fetch_sub(1, std::memory_order_release) == 1;
    }

    /// Get current reference count
    size_t ref_count() const {
        return _ref_count.load(std::memory_order_acquire);
    }

    /// Convert to iobuf
    iobuf to_iobuf();

private:
    std::vector<struct iovec> _iovecs;
    size_t _total_size;
    std::atomic<size_t> _ref_count{1};
    bool _is_registered = false;
};

/// Manager for scatter-gather buffers
class scatter_gather_buffer_manager {
public:
    scatter_gather_buffer_manager();
    ~scatter_gather_buffer_manager();

    scatter_gather_buffer_manager(const scatter_gather_buffer_manager&)
      = delete;
    scatter_gather_buffer_manager&
    operator=(const scatter_gather_buffer_manager&) = delete;

    /// Initialize the manager with io_uring
    ss::future<> initialize(size_t ring_size = 256);

    /// Shutdown the manager
    ss::future<> shutdown();

    /// Allocate a scatter-gather buffer
    ss::future<std::unique_ptr<sg_buffer>>
    allocate_sg_buffer(size_t size, size_t max_fragments = 16);

    /// Read into scatter-gather buffer using io_uring
    ss::future<size_t>
    read_into_sg_buffer(int fd, sg_buffer* buffer, size_t offset = 0);

    /// Write from scatter-gather buffer using zero-copy send
    ss::future<size_t>
    write_from_sg_buffer(int fd, sg_buffer* buffer, size_t offset = 0);

private:
    /// Calculate optimal fragment sizes
    std::vector<size_t>
    calculate_optimal_fragments(size_t total_size, size_t max_fragments);

    /// Allocate aligned memory
    void* allocate_aligned_memory(size_t size);

    /// Free aligned memory
    void free_aligned_memory(void* ptr, size_t size);

    /// Register scatter-gather buffer with io_uring
    ss::future<> register_sg_buffer(sg_buffer* buffer);

    /// Wait for io_uring completion
    ss::future<> wait_for_completion(struct io_uring_cqe** cqe);

    /// Wait for zero-copy send completion
    ss::future<> wait_for_zerocopy_completion(int fd);

private:
    struct io_uring _ring;
    bool _initialized = false;
    bool _use_huge_pages = false;
};

/// Align value down to alignment boundary
inline size_t align_down(size_t value, size_t alignment) {
    return value & ~(alignment - 1);
}

/// Align value up to alignment boundary
inline size_t align_up(size_t value, size_t alignment) {
    return (value + alignment - 1) & ~(alignment - 1);
}

} // namespace cloud_storage::zero_copy
