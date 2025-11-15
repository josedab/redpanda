// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/zero_copy/io_uring_reader.h"

#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>

#include <sys/mman.h>

#include <stdexcept>

namespace cloud_storage::zero_copy {

static ss::logger zcopy_log("zero_copy");

io_uring_cloud_reader::io_uring_cloud_reader() = default;

io_uring_cloud_reader::~io_uring_cloud_reader() {
    if (_initialized) {
        vlog(
          zcopy_log.warn,
          "io_uring_cloud_reader destroyed without shutdown");
    }
}

bool io_uring_cloud_reader::is_zero_copy_available() {
    struct io_uring ring;
    struct io_uring_params params = {};

    int ret = io_uring_queue_init_params(8, &ring, &params);
    if (ret < 0) {
        return false;
    }

    io_uring_queue_exit(&ring);
    return true;
}

ss::future<> io_uring_cloud_reader::initialize(zero_copy_config config) {
    if (_initialized) {
        co_return;
    }

    _config = config;

    // Initialize io_uring
    struct io_uring_params params = {};
    if (config.use_sqpoll) {
        params.flags |= IORING_SETUP_SQPOLL;
    }
    params.flags |= IORING_SETUP_CQSIZE;
    params.cq_entries = config.ring_size * 2;

    int ret = io_uring_queue_init_params(
      config.ring_size, &_ring, &params);

    if (ret < 0) {
        vlog(
          zcopy_log.error,
          "Failed to initialize io_uring: {}",
          std::strerror(-ret));
        throw std::system_error(-ret, std::system_category());
    }

    // Register buffer ring for zero-copy
    if (config.use_registered_buffers) {
        co_await register_buffer_ring(config);
    }

    _initialized = true;
    _buffer_sem = ssx::semaphore(config.buffer_count);

    vlog(
      zcopy_log.info,
      "Initialized io_uring reader with {} buffers of {} bytes",
      config.buffer_count,
      config.buffer_size);

    co_return;
}

ss::future<> io_uring_cloud_reader::shutdown() {
    if (!_initialized) {
        co_return;
    }

    // Wait for all buffers to be returned
    co_await _buffer_sem.wait(_config.buffer_count);

    // Unregister buffers
    if (_buffer_ring) {
        io_uring_unregister_buf_ring(&_ring, _buffer_group_id);
        _buffer_ring = nullptr;
    }

    // Free buffer memory
    if (_buffer_base) {
        if (_use_huge_pages) {
            munmap(_buffer_base, _buffer_base_size);
        } else {
            free(_buffer_base);
        }
        _buffer_base = nullptr;
        _buffer_base_size = 0;
    }

    // Cleanup io_uring
    io_uring_queue_exit(&_ring);

    _initialized = false;

    vlog(zcopy_log.info, "Shutdown io_uring reader");

    co_return;
}

ss::future<>
io_uring_cloud_reader::register_buffer_ring(const zero_copy_config& config) {
    // Allocate page-aligned buffers
    size_t total_size = config.buffer_size * config.buffer_count;
    _buffer_base_size = total_size;

    if (config.use_huge_pages) {
        _buffer_base = mmap(
          nullptr,
          total_size,
          PROT_READ | PROT_WRITE,
          MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB,
          -1,
          0);

        if (_buffer_base == MAP_FAILED) {
            vlog(
              zcopy_log.warn,
              "Failed to allocate huge pages, falling back to regular pages");
            _buffer_base = aligned_alloc(4096, total_size);
            _use_huge_pages = false;
        } else {
            _use_huge_pages = true;
        }
    } else {
        _buffer_base = aligned_alloc(4096, total_size);
        _use_huge_pages = false;
    }

    if (!_buffer_base) {
        throw std::bad_alloc();
    }

    // Register with io_uring
    struct io_uring_buf_reg reg = {};
    reg.ring_addr = reinterpret_cast<__u64>(_buffer_base);
    reg.ring_entries = config.buffer_count;
    reg.bgid = _buffer_group_id;

    int ret = io_uring_register_buf_ring(&_ring, &reg, 0);
    if (ret < 0) {
        if (_use_huge_pages) {
            munmap(_buffer_base, total_size);
        } else {
            free(_buffer_base);
        }
        _buffer_base = nullptr;
        throw std::system_error(-ret, std::system_category());
    }

    // Initialize buffer ring
    _buffer_ring = io_uring_setup_buf_ring(
      &_ring, config.buffer_count, _buffer_group_id, 0, &ret);
    if (!_buffer_ring) {
        if (_use_huge_pages) {
            munmap(_buffer_base, total_size);
        } else {
            free(_buffer_base);
        }
        _buffer_base = nullptr;
        throw std::system_error(-ret, std::system_category());
    }

    // Add buffers to ring
    for (size_t i = 0; i < config.buffer_count; ++i) {
        io_uring_buf_ring_add(
          _buffer_ring,
          static_cast<char*>(_buffer_base) + i * config.buffer_size,
          config.buffer_size,
          i,
          io_uring_buf_ring_mask(config.buffer_count),
          i);
    }

    io_uring_buf_ring_advance(_buffer_ring, config.buffer_count);

    _buffer_in_use.resize(config.buffer_count, false);

    vlog(
      zcopy_log.debug,
      "Registered buffer ring with {} buffers, huge_pages={}",
      config.buffer_count,
      _use_huge_pages);

    co_return;
}

ss::future<iobuf> io_uring_cloud_reader::read_zero_copy(int fd, offset_range range) {
    if (!_initialized) {
        throw std::runtime_error("io_uring reader not initialized");
    }

    // Acquire a buffer from the semaphore
    co_await _buffer_sem.wait();

    // Prepare read request
    auto* sqe = io_uring_get_sqe(&_ring);
    if (!sqe) {
        _buffer_sem.signal();
        throw std::runtime_error("Failed to get SQE");
    }

    // Use buffer ring for zero-copy read
    if (_config.use_registered_buffers) {
        io_uring_prep_read(sqe, fd, nullptr, range.size, range.offset);
        sqe->flags |= IOSQE_BUFFER_SELECT;
        sqe->buf_group = _buffer_group_id;
    } else {
        // Fallback to regular read
        auto buffer = static_cast<char*>(_buffer_base);
        io_uring_prep_read(sqe, fd, buffer, range.size, range.offset);
    }

    // Submit the request
    int ret = io_uring_submit(&_ring);
    if (ret < 0) {
        _buffer_sem.signal();
        throw std::system_error(-ret, std::system_category());
    }

    // Wait for completion
    struct io_uring_cqe* cqe;
    co_await wait_for_completion(&cqe);

    if (cqe->res < 0) {
        io_uring_cqe_seen(&_ring, cqe);
        _buffer_sem.signal();
        throw std::system_error(-cqe->res, std::system_category());
    }

    size_t bytes_read = cqe->res;

    // Extract buffer information
    uint16_t buffer_id = 0;
    if (_config.use_registered_buffers) {
        buffer_id = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
    }

    io_uring_cqe_seen(&_ring, cqe);

    // Create iobuf from the buffer
    auto buffer_ptr = get_registered_buffer(buffer_id);
    auto result = create_iobuf_from_buffer(std::move(buffer_ptr), bytes_read);

    co_return std::move(result);
}

ss::future<size_t> io_uring_cloud_reader::read_zero_copy_fixed(
  int fd, void* buffer, size_t size, uint64_t offset) {
    if (!_initialized) {
        throw std::runtime_error("io_uring reader not initialized");
    }

    // Prepare read request
    auto* sqe = io_uring_get_sqe(&_ring);
    if (!sqe) {
        throw std::runtime_error("Failed to get SQE");
    }

    io_uring_prep_read(sqe, fd, buffer, size, offset);

    // Submit the request
    int ret = io_uring_submit(&_ring);
    if (ret < 0) {
        throw std::system_error(-ret, std::system_category());
    }

    // Wait for completion
    struct io_uring_cqe* cqe;
    co_await wait_for_completion(&cqe);

    if (cqe->res < 0) {
        io_uring_cqe_seen(&_ring, cqe);
        throw std::system_error(-cqe->res, std::system_category());
    }

    size_t bytes_read = cqe->res;
    io_uring_cqe_seen(&_ring, cqe);

    co_return bytes_read;
}

ss::future<>
io_uring_cloud_reader::wait_for_completion(struct io_uring_cqe** cqe) {
    // In Seastar, we need to yield to allow other tasks to run
    // This is a simplified implementation - production code would
    // integrate with Seastar's reactor
    while (true) {
        int ret = io_uring_peek_cqe(&_ring, cqe);
        if (ret == 0) {
            co_return;
        }

        if (ret != -EAGAIN) {
            throw std::system_error(-ret, std::system_category());
        }

        // Yield to Seastar reactor
        co_await ss::sleep(std::chrono::microseconds(100));
    }
}

std::shared_ptr<registered_buffer>
io_uring_cloud_reader::get_registered_buffer(uint16_t buffer_id) {
    if (buffer_id >= _config.buffer_count) {
        throw std::out_of_range("Invalid buffer ID");
    }

    _buffer_in_use[buffer_id] = true;

    auto* data = static_cast<char*>(_buffer_base)
                 + buffer_id * _config.buffer_size;

    // Create a shared_ptr with a custom deleter that returns the buffer
    auto release_fn = [this, buffer_id]() { return_buffer(buffer_id); };

    return std::make_shared<registered_buffer>(
      data, _config.buffer_size, buffer_id, std::move(release_fn));
}

void io_uring_cloud_reader::return_buffer(uint16_t buffer_id) {
    if (buffer_id >= _config.buffer_count) {
        return;
    }

    if (!_buffer_in_use[buffer_id]) {
        vlog(
          zcopy_log.warn, "Attempted to return buffer {} that was not in use", buffer_id);
        return;
    }

    _buffer_in_use[buffer_id] = false;

    // Return buffer to the ring
    if (_buffer_ring) {
        auto* data = static_cast<char*>(_buffer_base)
                     + buffer_id * _config.buffer_size;
        io_uring_buf_ring_add(
          _buffer_ring,
          data,
          _config.buffer_size,
          buffer_id,
          io_uring_buf_ring_mask(_config.buffer_count),
          0);
        io_uring_buf_ring_advance(_buffer_ring, 1);
    }

    // Signal that a buffer is available
    _buffer_sem.signal();
}

iobuf io_uring_cloud_reader::create_iobuf_from_buffer(
  std::shared_ptr<registered_buffer> buffer, size_t size) {
    iobuf result;

    // Create a temporary_buffer that shares the registered buffer
    // When the temporary_buffer is destroyed, it will call the deleter
    // which returns the buffer to the ring
    auto deleter = ss::deleter();
    deleter.append(std::move(buffer));

    ss::temporary_buffer<char> temp_buf(
      buffer->data(), size, std::move(deleter));

    result.append(std::move(temp_buf));

    return result;
}

} // namespace cloud_storage::zero_copy
