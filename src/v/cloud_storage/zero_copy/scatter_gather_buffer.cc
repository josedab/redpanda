// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/zero_copy/scatter_gather_buffer.h"

#include "vlog.h"

#include <seastar/core/coroutine.hh>

#include <sys/mman.h>

#include <algorithm>
#include <stdexcept>

namespace cloud_storage::zero_copy {

static ss::logger sg_log("scatter_gather");

// Optimal fragment sizes (power of 2 for better alignment)
static constexpr size_t fragment_sizes[] = {
  4096,    // 4KB
  8192,    // 8KB
  16384,   // 16KB
  32768,   // 32KB
  65536,   // 64KB
  131072,  // 128KB
  262144,  // 256KB
  524288,  // 512KB
  1048576  // 1MB
};

sg_buffer::sg_buffer(size_t total_size, size_t max_fragments)
  : _total_size(total_size) {
    _iovecs.reserve(max_fragments);
}

sg_buffer::~sg_buffer() {
    // Free all allocated memory
    for (auto& iov : _iovecs) {
        if (iov.iov_base) {
            free(iov.iov_base);
        }
    }
}

iobuf sg_buffer::to_iobuf() {
    iobuf result;

    for (const auto& iov : _iovecs) {
        if (iov.iov_base && iov.iov_len > 0) {
            // Copy data into iobuf
            // In a production implementation, we could use zero-copy
            // by creating a custom deleter
            result.append(static_cast<const char*>(iov.iov_base), iov.iov_len);
        }
    }

    return result;
}

scatter_gather_buffer_manager::scatter_gather_buffer_manager() = default;

scatter_gather_buffer_manager::~scatter_gather_buffer_manager() {
    if (_initialized) {
        vlog(
          sg_log.warn,
          "scatter_gather_buffer_manager destroyed without shutdown");
    }
}

ss::future<> scatter_gather_buffer_manager::initialize(size_t ring_size) {
    if (_initialized) {
        co_return;
    }

    // Initialize io_uring
    struct io_uring_params params = {};
    params.flags = IORING_SETUP_CQSIZE;
    params.cq_entries = ring_size * 2;

    int ret = io_uring_queue_init_params(ring_size, &_ring, &params);

    if (ret < 0) {
        vlog(
          sg_log.error,
          "Failed to initialize io_uring for SG manager: {}",
          std::strerror(-ret));
        throw std::system_error(-ret, std::system_category());
    }

    _initialized = true;

    vlog(sg_log.info, "Initialized scatter-gather buffer manager");

    co_return;
}

ss::future<> scatter_gather_buffer_manager::shutdown() {
    if (!_initialized) {
        co_return;
    }

    io_uring_queue_exit(&_ring);
    _initialized = false;

    vlog(sg_log.info, "Shutdown scatter-gather buffer manager");

    co_return;
}

ss::future<std::unique_ptr<sg_buffer>>
scatter_gather_buffer_manager::allocate_sg_buffer(
  size_t size, size_t max_fragments) {
    auto buffer = std::make_unique<sg_buffer>(size, max_fragments);

    // Calculate optimal fragment sizes
    auto fragments = calculate_optimal_fragments(size, max_fragments);

    // Allocate each fragment
    for (size_t fragment_size : fragments) {
        struct iovec iov;
        iov.iov_len = fragment_size;
        iov.iov_base = allocate_aligned_memory(fragment_size);

        if (!iov.iov_base) {
            throw std::bad_alloc();
        }

        buffer->_iovecs.push_back(iov);
    }

    // Register with io_uring if initialized
    if (_initialized) {
        co_await register_sg_buffer(buffer.get());
    }

    vlog(
      sg_log.debug,
      "Allocated SG buffer with {} fragments, total size {}",
      buffer->iovec_count(),
      buffer->total_size());

    co_return std::move(buffer);
}

ss::future<size_t> scatter_gather_buffer_manager::read_into_sg_buffer(
  int fd, sg_buffer* buffer, size_t offset) {
    if (!_initialized) {
        throw std::runtime_error("SG buffer manager not initialized");
    }

    // Prepare readv operation with io_uring
    auto* sqe = io_uring_get_sqe(&_ring);
    if (!sqe) {
        throw std::runtime_error("Failed to get SQE");
    }

    io_uring_prep_readv(
      sqe, fd, buffer->iovecs(), buffer->iovec_count(), offset);

    // Use registered buffers if available
    if (buffer->is_registered()) {
        sqe->flags |= IOSQE_FIXED_FILE;
    }

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

ss::future<size_t> scatter_gather_buffer_manager::write_from_sg_buffer(
  int fd, sg_buffer* buffer, size_t offset) {
    // Use sendmsg with MSG_ZEROCOPY for network sends
    struct msghdr msg = {};
    msg.msg_iov = buffer->iovecs();
    msg.msg_iovlen = buffer->iovec_count();

    // Enable zero-copy send
    int flags = MSG_ZEROCOPY | MSG_NOSIGNAL;

    ssize_t sent = ::sendmsg(fd, &msg, flags);

    if (sent < 0) {
        if (errno == ENOTSUP || errno == EINVAL) {
            // Zero-copy not supported, fall back to regular send
            vlog(
              sg_log.warn, "Zero-copy send not supported, falling back to regular send");
            flags = MSG_NOSIGNAL;
            sent = ::sendmsg(fd, &msg, flags);
        }

        if (sent < 0) {
            throw std::system_error(errno, std::system_category());
        }
    } else {
        // Wait for zero-copy completion notification
        try {
            co_await wait_for_zerocopy_completion(fd);
        } catch (const std::exception& e) {
            vlog(
              sg_log.warn,
              "Failed to wait for zero-copy completion: {}",
              e.what());
        }
    }

    co_return sent;
}

std::vector<size_t>
scatter_gather_buffer_manager::calculate_optimal_fragments(
  size_t total_size, size_t max_fragments) {
    std::vector<size_t> fragments;
    fragments.reserve(max_fragments);

    size_t remaining = total_size;

    // Greedy allocation from largest to smallest
    for (auto it = std::rbegin(fragment_sizes); it != std::rend(fragment_sizes);
         ++it) {
        while (remaining >= *it && fragments.size() < max_fragments) {
            fragments.push_back(*it);
            remaining -= *it;
        }

        if (remaining == 0 || fragments.size() >= max_fragments) {
            break;
        }
    }

    // Add remainder if any
    if (remaining > 0 && fragments.size() < max_fragments) {
        // Align up to page size
        size_t aligned_remaining = align_up(remaining, 4096);
        fragments.push_back(aligned_remaining);
    }

    return fragments;
}

void* scatter_gather_buffer_manager::allocate_aligned_memory(size_t size) {
    // Try huge pages first for large allocations
    if (size >= 2 * 1024 * 1024) { // 2MB or larger
        void* ptr = mmap(
          nullptr,
          size,
          PROT_READ | PROT_WRITE,
          MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB,
          -1,
          0);

        if (ptr != MAP_FAILED) {
            _use_huge_pages = true;
            return ptr;
        }
    }

    // Fall back to regular page-aligned allocation
    return aligned_alloc(4096, size);
}

void scatter_gather_buffer_manager::free_aligned_memory(
  void* ptr, size_t size) {
    if (_use_huge_pages && size >= 2 * 1024 * 1024) {
        munmap(ptr, size);
    } else {
        free(ptr);
    }
}

ss::future<>
scatter_gather_buffer_manager::register_sg_buffer(sg_buffer* buffer) {
    if (!_initialized) {
        co_return;
    }

    // Register buffers with io_uring for zero-copy
    int ret = io_uring_register_buffers(
      &_ring, buffer->iovecs(), buffer->iovec_count());

    if (ret == 0) {
        buffer->set_registered(true);
        vlog(
          sg_log.debug,
          "Registered SG buffer with {} iovecs",
          buffer->iovec_count());
    } else {
        vlog(
          sg_log.warn,
          "Failed to register SG buffer: {}",
          std::strerror(-ret));
    }

    co_return;
}

ss::future<> scatter_gather_buffer_manager::wait_for_completion(
  struct io_uring_cqe** cqe) {
    // In Seastar, we need to yield to allow other tasks to run
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

ss::future<>
scatter_gather_buffer_manager::wait_for_zerocopy_completion(int fd) {
    // Wait for kernel notification of zero-copy completion
    struct msghdr msg = {};
    char control[100];
    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);

    // Poll for completion notification
    for (int i = 0; i < 10; ++i) {
        int ret = ::recvmsg(fd, &msg, MSG_ERRQUEUE | MSG_DONTWAIT);

        if (ret >= 0) {
            // Process completion notification
            for (struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg); cmsg;
                 cmsg = CMSG_NXTHDR(&msg, cmsg)) {
                if (cmsg->cmsg_level == SOL_IP
                    && cmsg->cmsg_type == IP_RECVERR) {
                    auto* serr = reinterpret_cast<struct sock_extended_err*>(
                      CMSG_DATA(cmsg));
                    if (serr->ee_origin == SO_EE_ORIGIN_ZEROCOPY) {
                        // Zero-copy completed
                        co_return;
                    }
                }
            }
        }

        // Yield and retry
        co_await ss::sleep(std::chrono::microseconds(100));
    }

    vlog(sg_log.debug, "Zero-copy completion notification timeout");

    co_return;
}

} // namespace cloud_storage::zero_copy
