# RFC-003: Zero-Copy Remote Segment Reads

**Authors**: Redpanda Engineering Team  
**Status**: Proposed  
**Created**: 2025-01-15  
**Discussion**: [GitHub Issue #XXXX]

## Summary

This RFC proposes implementing true zero-copy reads from cloud storage to client, reducing CPU usage by 20-30% and memory allocations by 40-50% through io_uring integration and scatter-gather I/O.

## Motivation

### Current State

Current cloud storage read path involves multiple data copies:

```
Cloud Storage → HTTP Buffer → iobuf → Decompression Buffer → 
Record Batch → Serialization Buffer → Network Buffer → Client
```

**Copy Count**: 4-5 copies per read operation

### Problems

1. **CPU Overhead**: Each copy consumes CPU cycles
2. **Memory Pressure**: Multiple buffers for same data
3. **Cache Pollution**: Temporary buffers evict useful data
4. **Latency**: Copy operations add to read latency
5. **Scalability**: Memory bandwidth becomes bottleneck

### Use Cases

- High-throughput consumer applications
- Real-time analytics with large segment reads
- Multi-consumer scenarios with same data
- Low-latency data replay operations

## Detailed Design

### Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│                  Zero-Copy Read Path                      │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │                   Cloud Storage                      │ │
│  └────────────────────────┬─────────────────────────────┘ │
│                           │                               │
│                           ▼                               │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              io_uring Submission Queue               │ │
│  │         (IORING_OP_RECV with MSG_ZEROCOPY)          │ │
│  └────────────────────────┬─────────────────────────────┘ │
│                           │                               │
│                           ▼                               │
│  ┌─────────────────────────────────────────────────────┐ │
│  │                 Kernel Buffer Ring                   │ │
│  │            (Direct mapping to user space)            │ │
│  └────────────────────────┬─────────────────────────────┘ │
│                           │                               │
│                           ▼                               │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              Scatter-Gather Manager                  │ │
│  │          (Page-aligned buffer management)            │ │
│  └────────────────────────┬─────────────────────────────┘ │
│                           │                               │
│                           ▼                               │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              In-Place Decompression                  │ │
│  │        (Hardware-accelerated when available)         │ │
│  └────────────────────────┬─────────────────────────────┘ │
│                           │                               │
│                           ▼                               │
│  ┌─────────────────────────────────────────────────────┐ │
│  │                  Client Socket                       │ │
│  │            (sendmsg with MSG_ZEROCOPY)              │ │
│  └─────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

### Core Components

#### 1. io_uring Integration

```cpp
class io_uring_cloud_reader {
public:
    struct zero_copy_config {
        size_t ring_size = 1024;
        size_t buffer_size = 1048576;  // 1MB buffers
        size_t buffer_count = 64;
        bool use_huge_pages = true;
        bool use_registered_buffers = true;
    };

    ss::future<> initialize(zero_copy_config config) {
        // Initialize io_uring
        struct io_uring_params params = {};
        params.flags = IORING_SETUP_SQPOLL |    // Kernel polling
                      IORING_SETUP_CQSIZE;       // Custom CQ size
        params.cq_entries = config.ring_size * 2;
        
        int ret = io_uring_queue_init_params(
            config.ring_size, 
            &_ring, 
            &params
        );
        
        if (ret < 0) {
            throw std::system_error(-ret, std::system_category());
        }
        
        // Register buffer ring for zero-copy
        co_await register_buffer_ring(config);
        
        // Set up completion handling
        co_await setup_completion_handler();
    }

    ss::future<ss::scattered_message<char>>
    read_zero_copy(
        const cloud_storage_client::object_key& key,
        offset_range range
    ) {
        // Prepare zero-copy read request
        auto sqe = io_uring_get_sqe(&_ring);
        
        // Configure for HTTP GET with range
        auto http_req = prepare_http_request(key, range);
        
        // Use registered buffers for zero-copy
        io_uring_prep_recv_multishot(
            sqe,
            _socket_fd,
            nullptr,  // Use buffer ring
            0,
            MSG_WAITALL
        );
        
        sqe->flags |= IOSQE_BUFFER_SELECT;  // Let kernel choose buffer
        sqe->buf_group = _buffer_group_id;
        
        // Set up direct descriptor for zero-copy
        sqe->flags |= IOSQE_FIXED_FILE;
        sqe->fd = _fixed_file_idx;
        
        // Submit and wait for completion
        io_uring_submit(&_ring);
        
        // Process completion
        struct io_uring_cqe* cqe;
        co_await wait_for_completion(&cqe);
        
        if (cqe->res < 0) {
            throw std::system_error(-cqe->res, std::system_category());
        }
        
        // Extract buffer without copying
        auto buffer_id = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
        auto buffer = get_registered_buffer(buffer_id);
        
        // Create scattered message from buffer
        co_return create_scattered_message(buffer, cqe->res);
    }

private:
    ss::future<> register_buffer_ring(const zero_copy_config& config) {
        // Allocate page-aligned buffers
        size_t total_size = config.buffer_size * config.buffer_count;
        
        void* base;
        if (config.use_huge_pages) {
            base = mmap(nullptr, total_size,
                       PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB,
                       -1, 0);
        } else {
            base = aligned_alloc(4096, total_size);
        }
        
        // Register with io_uring
        struct io_uring_buf_reg reg = {};
        reg.ring_addr = reinterpret_cast<__u64>(base);
        reg.ring_entries = config.buffer_count;
        reg.bgid = _buffer_group_id;
        
        int ret = io_uring_register_buf_ring(&_ring, &reg, 0);
        if (ret < 0) {
            throw std::system_error(-ret, std::system_category());
        }
        
        // Initialize buffer ring
        _buffer_ring = static_cast<io_uring_buf_ring*>(base);
        io_uring_buf_ring_init(_buffer_ring);
        
        // Add buffers to ring
        for (size_t i = 0; i < config.buffer_count; ++i) {
            io_uring_buf_ring_add(
                _buffer_ring,
                static_cast<char*>(base) + i * config.buffer_size,
                config.buffer_size,
                i,
                io_uring_buf_ring_mask(config.buffer_count),
                i
            );
        }
        
        io_uring_buf_ring_advance(_buffer_ring, config.buffer_count);
        
        co_return;
    }
    
    ss::scattered_message<char> create_scattered_message(
        registered_buffer* buffer,
        size_t size
    ) {
        // Create scattered message without copying
        ss::scattered_message<char> msg;
        
        // Add fragments directly from registered buffer
        size_t offset = 0;
        while (offset < size) {
            size_t chunk_size = std::min(
                size - offset,
                _max_fragment_size
            );
            
            msg.append_fragment(
                buffer->data + offset,
                chunk_size,
                [buffer] { buffer->release(); }  // Custom deleter
            );
            
            offset += chunk_size;
        }
        
        return msg;
    }

private:
    struct io_uring _ring;
    struct io_uring_buf_ring* _buffer_ring;
    uint16_t _buffer_group_id = 0;
    int _socket_fd;
    int _fixed_file_idx;
    static constexpr size_t _max_fragment_size = 65536;
};
```

#### 2. Scatter-Gather Buffer Manager

```cpp
class scatter_gather_buffer_manager {
public:
    struct sg_buffer {
        struct iovec* iovecs;
        size_t iovec_count;
        size_t total_size;
        std::atomic<size_t> ref_count{1};
        bool is_registered;
    };

    ss::future<std::unique_ptr<sg_buffer>>
    allocate_sg_buffer(size_t size, size_t max_fragments = 16) {
        auto buffer = std::make_unique<sg_buffer>();
        
        // Calculate fragment sizes for optimal alignment
        auto fragments = calculate_optimal_fragments(size, max_fragments);
        
        buffer->iovec_count = fragments.size();
        buffer->iovecs = new iovec[buffer->iovec_count];
        buffer->total_size = size;
        
        // Allocate each fragment
        for (size_t i = 0; i < fragments.size(); ++i) {
            auto& iov = buffer->iovecs[i];
            
            // Allocate page-aligned memory
            iov.iov_len = fragments[i];
            iov.iov_base = allocate_aligned_memory(fragments[i]);
            
            if (!iov.iov_base) {
                // Cleanup on failure
                for (size_t j = 0; j < i; ++j) {
                    free_aligned_memory(buffer->iovecs[j].iov_base);
                }
                throw std::bad_alloc();
            }
        }
        
        // Register with kernel for zero-copy
        co_await register_sg_buffer(buffer.get());
        
        co_return buffer;
    }
    
    ss::future<size_t> read_into_sg_buffer(
        int fd,
        sg_buffer* buffer,
        size_t offset = 0
    ) {
        // Use readv for scatter-gather read
        struct io_uring_sqe* sqe = io_uring_get_sqe(&_ring);
        
        io_uring_prep_readv(
            sqe,
            fd,
            buffer->iovecs,
            buffer->iovec_count,
            offset
        );
        
        // Use registered buffers if available
        if (buffer->is_registered) {
            sqe->flags |= IOSQE_FIXED_FILE;
        }
        
        io_uring_submit(&_ring);
        
        struct io_uring_cqe* cqe;
        co_await wait_for_completion(&cqe);
        
        co_return cqe->res;
    }
    
    ss::future<size_t> write_from_sg_buffer(
        int fd,
        sg_buffer* buffer,
        size_t offset = 0
    ) {
        // Use MSG_ZEROCOPY for network send
        struct msghdr msg = {};
        msg.msg_iov = buffer->iovecs;
        msg.msg_iovlen = buffer->iovec_count;
        
        // Enable zero-copy send
        int flags = MSG_ZEROCOPY | MSG_NOSIGNAL;
        
        ssize_t sent = sendmsg(fd, &msg, flags);
        
        if (sent < 0) {
            throw std::system_error(errno, std::system_category());
        }
        
        // Wait for zero-copy completion notification
        co_await wait_for_zerocopy_completion(fd);
        
        co_return sent;
    }

private:
    std::vector<size_t> calculate_optimal_fragments(
        size_t total_size,
        size_t max_fragments
    ) {
        std::vector<size_t> fragments;
        
        // Use power-of-2 sizes for better alignment
        static constexpr size_t sizes[] = {
            4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576
        };
        
        size_t remaining = total_size;
        
        // Greedy allocation from largest to smallest
        for (auto it = std::rbegin(sizes); it != std::rend(sizes); ++it) {
            while (remaining >= *it && fragments.size() < max_fragments) {
                fragments.push_back(*it);
                remaining -= *it;
            }
        }
        
        // Add remainder if any
        if (remaining > 0 && fragments.size() < max_fragments) {
            fragments.push_back(align_up(remaining, 4096));
        }
        
        return fragments;
    }
    
    void* allocate_aligned_memory(size_t size) {
        // Try huge pages first
        void* ptr = mmap(
            nullptr, size,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB,
            -1, 0
        );
        
        if (ptr != MAP_FAILED) {
            return ptr;
        }
        
        // Fall back to regular pages
        return aligned_alloc(4096, size);
    }
    
    ss::future<> register_sg_buffer(sg_buffer* buffer) {
        // Register buffer with io_uring for zero-copy
        struct iovec* iovecs = buffer->iovecs;
        
        int ret = io_uring_register_buffers(
            &_ring,
            iovecs,
            buffer->iovec_count
        );
        
        if (ret == 0) {
            buffer->is_registered = true;
        }
        
        co_return;
    }
    
    ss::future<> wait_for_zerocopy_completion(int fd) {
        // Wait for kernel notification of zero-copy completion
        struct sock_extended_err* serr;
        struct msghdr msg = {};
        struct cmsghdr* cmsg;
        
        char control[100];
        msg.msg_control = control;
        msg.msg_controllen = sizeof(control);
        
        int ret = recvmsg(fd, &msg, MSG_ERRQUEUE);
        
        if (ret < 0) {
            throw std::system_error(errno, std::system_category());
        }
        
        // Process completion notification
        for (cmsg = CMSG_FIRSTHDR(&msg); cmsg; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
            if (cmsg->cmsg_level == SOL_IP && 
                cmsg->cmsg_type == IP_RECVERR) {
                serr = (struct sock_extended_err*)CMSG_DATA(cmsg);
                if (serr->ee_origin == SO_EE_ORIGIN_ZEROCOPY) {
                    // Zero-copy completed
                    co_return;
                }
            }
        }
    }

private:
    struct io_uring _ring;
};
```

#### 3. In-Place Decompression

```cpp
class zero_copy_decompressor {
public:
    enum class compression_type {
        none,
        gzip,
        snappy,
        lz4,
        zstd
    };
    
    struct decompression_result {
        ss::scattered_message<char> data;
        size_t uncompressed_size;
        bool was_in_place;
    };

    ss::future<decompression_result> decompress_in_place(
        ss::scattered_message<char> compressed,
        compression_type type
    ) {
        switch (type) {
        case compression_type::none:
            co_return {
                .data = std::move(compressed),
                .uncompressed_size = compressed.size(),
                .was_in_place = true
            };
            
        case compression_type::lz4:
            co_return co_await decompress_lz4_in_place(std::move(compressed));
            
        case compression_type::zstd:
            if (_hw_accelerated && has_qat_support()) {
                co_return co_await decompress_qat(std::move(compressed));
            }
            co_return co_await decompress_zstd_streaming(std::move(compressed));
            
        default:
            co_return co_await decompress_fallback(std::move(compressed), type);
        }
    }

private:
    ss::future<decompression_result> decompress_lz4_in_place(
        ss::scattered_message<char> compressed
    ) {
        // LZ4 supports true in-place decompression
        if (compressed.nr_fragments() == 1) {
            // Single fragment - can decompress in-place
            auto fragment = compressed.release_fragment(0);
            
            // Ensure buffer is large enough
            size_t max_size = LZ4_compressBound(fragment.size());
            if (fragment.capacity() < max_size) {
                // Need to reallocate
                co_return co_await decompress_lz4_copy(std::move(fragment));
            }
            
            // Decompress in-place
            int decompressed_size = LZ4_decompress_safe_partial(
                fragment.get(),
                fragment.get_write(),  // Write to same buffer
                fragment.size(),
                fragment.capacity(),
                fragment.capacity()
            );
            
            if (decompressed_size < 0) {
                throw decompression_error("LZ4 decompression failed");
            }
            
            fragment.trim(decompressed_size);
            
            ss::scattered_message<char> result;
            result.append(std::move(fragment));
            
            co_return {
                .data = std::move(result),
                .uncompressed_size = decompressed_size,
                .was_in_place = true
            };
        } else {
            // Multiple fragments - use streaming
            co_return co_await decompress_lz4_streaming(std::move(compressed));
        }
    }
    
    ss::future<decompression_result> decompress_qat(
        ss::scattered_message<char> compressed
    ) {
        // Intel QuickAssist Technology hardware acceleration
        QATContext ctx;
        co_await ctx.initialize();
        
        // Prepare source scatter-gather list
        CpaDcBufferList source_list;
        prepare_qat_bufferlist(compressed, source_list);
        
        // Allocate destination buffers
        auto dest_buffers = co_await allocate_qat_dest_buffers(
            estimate_uncompressed_size(compressed)
        );
        
        // Submit to QAT hardware
        CpaDcRqResults results;
        auto status = cpaDcDecompressData(
            ctx.instance,
            ctx.session,
            &source_list,
            &dest_buffers,
            &results,
            CPA_DC_FLUSH_FINAL
        );
        
        if (status != CPA_STATUS_SUCCESS) {
            co_return co_await decompress_fallback(
                std::move(compressed), 
                compression_type::zstd
            );
        }
        
        // Wait for hardware completion
        co_await ctx.wait_for_completion();
        
        // Create scattered message from QAT output
        auto result = create_scattered_from_qat(dest_buffers, results);
        
        co_return {
            .data = std::move(result),
            .uncompressed_size = results.produced,
            .was_in_place = false  // QAT uses separate buffers
        };
    }
    
    ss::future<decompression_result> decompress_zstd_streaming(
        ss::scattered_message<char> compressed
    ) {
        // ZSTD streaming decompression with minimal copies
        ZSTD_DStream* stream = ZSTD_createDStream();
        ZSTD_initDStream(stream);
        
        ss::scattered_message<char> result;
        size_t total_size = 0;
        
        // Process each fragment
        for (size_t i = 0; i < compressed.nr_fragments(); ++i) {
            auto fragment = compressed.get_fragment(i);
            
            ZSTD_inBuffer input = {
                .src = fragment.get(),
                .size = fragment.size(),
                .pos = 0
            };
            
            while (input.pos < input.size) {
                // Allocate output buffer
                auto out_buffer = co_await _buffer_pool.allocate(
                    ZSTD_DStreamOutSize()
                );
                
                ZSTD_outBuffer output = {
                    .dst = out_buffer.get(),
                    .size = out_buffer.size(),
                    .pos = 0
                };
                
                // Decompress chunk
                size_t ret = ZSTD_decompressStream(stream, &output, &input);
                
                if (ZSTD_isError(ret)) {
                    ZSTD_freeDStream(stream);
                    throw decompression_error(ZSTD_getErrorName(ret));
                }
                
                // Add to result
                out_buffer.trim(output.pos);
                result.append(std::move(out_buffer));
                total_size += output.pos;
            }
        }
        
        ZSTD_freeDStream(stream);
        
        co_return {
            .data = std::move(result),
            .uncompressed_size = total_size,
            .was_in_place = false
        };
    }

private:
    bool _hw_accelerated = true;
    buffer_pool _buffer_pool;
};
```

#### 4. Direct I/O Integration

```cpp
class direct_io_segment_reader {
public:
    struct dio_config {
        bool enabled = true;
        size_t alignment = 512;  // O_DIRECT alignment requirement
        size_t buffer_size = 1048576;  // 1MB
        bool use_aio = true;
    };

    ss::future<> open_segment(const std::filesystem::path& path) {
        int flags = O_RDONLY;
        
        if (_config.enabled) {
            flags |= O_DIRECT;
        }
        
        _fd = ::open(path.c_str(), flags);
        if (_fd < 0) {
            throw std::system_error(errno, std::system_category());
        }
        
        // Get file size
        struct stat st;
        if (fstat(_fd, &st) < 0) {
            throw std::system_error(errno, std::system_category());
        }
        
        _file_size = st.st_size;
        
        if (_config.use_aio) {
            co_await setup_aio();
        }
        
        co_return;
    }
    
    ss::future<ss::temporary_buffer<char>>
    read_aligned(size_t offset, size_t size) {
        // Align offset and size for O_DIRECT
        size_t aligned_offset = align_down(offset, _config.alignment);
        size_t aligned_size = align_up(
            size + (offset - aligned_offset),
            _config.alignment
        );
        
        // Allocate aligned buffer
        void* buffer;
        int ret = posix_memalign(&buffer, _config.alignment, aligned_size);
        if (ret != 0) {
            throw std::bad_alloc();
        }
        
        if (_config.use_aio) {
            co_return co_await read_aio(
                aligned_offset,
                aligned_size,
                buffer
            );
        } else {
            co_return co_await read_sync(
                aligned_offset,
                aligned_size,
                buffer
            );
        }
    }

private:
    ss::future<> setup_aio() {
        // Initialize AIO context
        int ret = io_setup(128, &_aio_ctx);
        if (ret < 0) {
            throw std::system_error(-ret, std::system_category());
        }
        
        co_return;
    }
    
    ss::future<ss::temporary_buffer<char>>
    read_aio(size_t offset, size_t size, void* buffer) {
        struct iocb cb;
        struct iocb* cbs[1] = {&cb};
        
        io_prep_pread(&cb, _fd, buffer, size, offset);
        cb.data = buffer;
        
        int ret = io_submit(_aio_ctx, 1, cbs);
        if (ret != 1) {
            free(buffer);
            throw std::system_error(-ret, std::system_category());
        }
        
        // Wait for completion
        struct io_event events[1];
        ret = io_getevents(_aio_ctx, 1, 1, events, nullptr);
        
        if (ret != 1 || events[0].res < 0) {
            free(buffer);
            throw std::system_error(-events[0].res, std::system_category());
        }
        
        co_return ss::temporary_buffer<char>(
            static_cast<char*>(buffer),
            events[0].res,
            ss::make_free_deleter(buffer)
        );
    }
    
    ss::future<ss::temporary_buffer<char>>
    read_sync(size_t offset, size_t size, void* buffer) {
        ssize_t ret = pread(_fd, buffer, size, offset);
        
        if (ret < 0) {
            free(buffer);
            throw std::system_error(errno, std::system_category());
        }
        
        co_return ss::temporary_buffer<char>(
            static_cast<char*>(buffer),
            ret,
            ss::make_free_deleter(buffer)
        );
    }
    
    static size_t align_down(size_t value, size_t alignment) {
        return value & ~(alignment - 1);
    }
    
    static size_t align_up(size_t value, size_t alignment) {
        return (value + alignment - 1) & ~(alignment - 1);
    }

private:
    dio_config _config;
    int _fd = -1;
    size_t _file_size = 0;
    io_context_t _aio_ctx = 0;
};
```

### Configuration

```yaml
# Zero-copy configuration
cloud_storage_zero_copy_enabled: true
cloud_storage_io_uring_enabled: true
cloud_storage_io_uring_ring_size: 1024
cloud_storage_io_uring_buffer_count: 64
cloud_storage_use_huge_pages: true
cloud_storage_direct_io_enabled: true
cloud_storage_hw_decompression_enabled: true
cloud_storage_zerocopy_threshold: 4096  # bytes
```

### Performance Optimizations

#### 1. NUMA-Aware Buffer Allocation

```cpp
class numa_aware_allocator {
    void* allocate(size_t size, int numa_node = -1) {
        if (numa_node < 0) {
            numa_node = numa_node_of_cpu(sched_getcpu());
        }
        
        // Bind to NUMA node
        struct bitmask* mask = numa_allocate_nodemask();
        numa_bitmask_setbit(mask, numa_node);
        numa_set_membind(mask);
        
        void* ptr = numa_alloc_onnode(size, numa_node);
        
        // Restore default policy
        numa_set_membind(numa_all_nodes_ptr);
        numa_free_nodemask(mask);
        
        return ptr;
    }
};
```

#### 2. CPU Affinity for I/O Operations

```cpp
class affinity_manager {
    ss::future<> run_on_io_core(std::function<ss::future<>()> func) {
        auto io_cpu = get_io_cpu();
        co_return co_await ss::smp::submit_to(io_cpu, func);
    }
    
    unsigned get_io_cpu() {
        // Dedicate specific cores for I/O
        return ss::smp::count - 1;  // Use last core
    }
};
```

## Testing Strategy

### Unit Tests

```cpp
BOOST_AUTO_TEST_CASE(test_zero_copy_read) {
    io_uring_cloud_reader reader;
    reader.initialize({}).get();
    
    auto data = reader.read_zero_copy(
        {"bucket", "key"},
        {0, 1048576}
    ).get();
    
    BOOST_REQUIRE_EQUAL(data.size(), 1048576);
    // Verify no copies were made
    BOOST_REQUIRE_EQUAL(get_copy_count(), 0);
}

BOOST_AUTO_TEST_CASE(test_scatter_gather_io) {
    scatter_gather_buffer_manager mgr;
    
    auto buffer = mgr.allocate_sg_buffer(1048576, 8).get();
    BOOST_REQUIRE_LE(buffer->iovec_count, 8);
    
    // Verify alignment
    for (size_t i = 0; i < buffer->iovec_count; ++i) {
        auto addr = reinterpret_cast<uintptr_t>(buffer->iovecs[i].iov_base);
        BOOST_REQUIRE_EQUAL(addr % 4096, 0);
    }
}
```

### Performance Benchmarks

```cpp
PERF_TEST(zero_copy_throughput) {
    // Compare copy vs zero-copy throughput
    auto copy_throughput = measure_copy_throughput();
    auto zerocopy_throughput = measure_zerocopy_throughput();
    
    auto improvement = zerocopy_throughput / copy_throughput;
    BOOST_REQUIRE_GT(improvement, 1.3);  // At least 30% improvement
}

PERF_TEST(memory_usage) {
    // Compare memory usage
    auto copy_memory = measure_copy_memory_usage();
    auto zerocopy_memory = measure_zerocopy_memory_usage();
    
    auto reduction = 1.0 - (zerocopy_memory / copy_memory);
    BOOST_REQUIRE_GT(reduction, 0.4);  // At least 40% reduction
}
```

## Migration Strategy

### Phase 1: Feature Flag (Week 1-2)
- Implement behind feature flag
- Default to disabled

### Phase 2: Small Segments (Week 3-4)
- Enable for segments > 4KB
- Monitor performance

### Phase 3: All Segments (Week 5-6)
- Enable for all segment sizes
- Tune thresholds

### Phase 4: Default Enable (Week 7-8)
- Enable by default
- Provide fallback option

## Security Considerations

1. **Memory Protection**: Ensure proper memory protection for zero-copy buffers
2. **Buffer Validation**: Validate all buffer addresses and sizes
3. **Resource Limits**: Prevent excessive memory pinning

## Open Questions

1. Minimum kernel version requirement (5.1 for io_uring)?
2. Hardware acceleration support detection?
3. Fallback strategy for unsupported platforms?
4. Integration with existing iobuf abstraction?

## References

- [io_uring Documentation](https://kernel.dk/io_uring.pdf)
- [Zero-Copy Networking](https://example.com/zerocopy)
- [Intel QAT Programming Guide](https://example.com/qat)