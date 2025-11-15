# Zero-Copy Cloud Reads

This directory contains the implementation of zero-copy cloud reads for Redpanda, as described in RFC-003.

## Overview

The zero-copy cloud reads feature reduces CPU usage by 20-30% and memory allocations by 40-50% through:
- io_uring integration for kernel-level zero-copy operations
- Scatter-gather I/O for efficient buffer management
- In-place decompression where possible
- Direct I/O for aligned disk operations

## Components

### 1. io_uring Reader (`io_uring_reader.h/cc`)

Provides zero-copy reads using io_uring with registered buffer rings.

**Key Features:**
- Kernel-managed buffer rings for zero-copy
- Support for huge pages to reduce TLB misses
- Configurable ring size and buffer count
- Graceful fallback when io_uring is unavailable

**Example Usage:**
```cpp
#include "cloud_storage/zero_copy/io_uring_reader.h"

zero_copy_config config{
    .ring_size = 1024,
    .buffer_size = 1048576,  // 1MB
    .buffer_count = 64,
    .use_huge_pages = true
};

io_uring_cloud_reader reader;
co_await reader.initialize(config);

// Read data using zero-copy
offset_range range{.offset = 0, .size = 65536};
auto data = co_await reader.read_zero_copy(fd, range);

co_await reader.shutdown();
```

### 2. Scatter-Gather Buffer Manager (`scatter_gather_buffer.h/cc`)

Manages scatter-gather buffers for efficient I/O operations.

**Key Features:**
- Page-aligned buffer allocation
- Optimal fragment size calculation
- Support for MSG_ZEROCOPY send operations
- Reference counting for safe buffer sharing

**Example Usage:**
```cpp
#include "cloud_storage/zero_copy/scatter_gather_buffer.h"

scatter_gather_buffer_manager mgr;
co_await mgr.initialize(256);

// Allocate a 1MB buffer with up to 16 fragments
auto buffer = co_await mgr.allocate_sg_buffer(1048576, 16);

// Read into buffer using io_uring
auto bytes_read = co_await mgr.read_into_sg_buffer(fd, buffer.get(), 0);

// Send using zero-copy
auto bytes_sent = co_await mgr.write_from_sg_buffer(socket_fd, buffer.get());

co_await mgr.shutdown();
```

### 3. Zero-Copy Decompressor (`zero_copy_decompressor.h/cc`)

Performs decompression with minimal copies, supporting hardware acceleration.

**Key Features:**
- In-place decompression for compatible formats
- Streaming decompression for large data
- Intel QAT hardware acceleration support
- Fallback to software decompression

**Example Usage:**
```cpp
#include "cloud_storage/zero_copy/zero_copy_decompressor.h"

zero_copy_decompressor decomp;
decomp.set_hw_acceleration(true);

auto result = co_await decomp.decompress_in_place(
    std::move(compressed_data),
    compression::type::zstd
);

// Check if decompression was truly zero-copy
if (result.was_in_place) {
    // Decompression was done in-place without extra copies
}
```

### 4. Direct I/O Reader (`direct_io_reader.h/cc`)

Reads from files using Direct I/O (O_DIRECT) for cache bypass and DMA.

**Key Features:**
- O_DIRECT support for aligned reads
- Automatic alignment handling
- Fallback to regular I/O when needed
- Integration with Seastar file I/O

**Example Usage:**
```cpp
#include "cloud_storage/zero_copy/direct_io_reader.h"

dio_config config{
    .enabled = true,
    .alignment = 512,
    .buffer_size = 1048576
};

direct_io_segment_reader reader(config);
co_await reader.open_segment("/path/to/segment");

// Read with automatic alignment
auto data = co_await reader.read_aligned(offset, size);

co_await reader.close();
```

## Configuration

Configuration options are defined in `config.h/cc`:

```cpp
struct config {
    bool enabled = true;                    // Enable zero-copy
    bool io_uring_enabled = true;           // Enable io_uring
    size_t io_uring_ring_size = 1024;      // Ring size
    size_t io_uring_buffer_count = 64;     // Buffer count
    bool use_huge_pages = true;             // Use huge pages
    bool direct_io_enabled = true;          // Enable O_DIRECT
    bool hw_decompression_enabled = false;  // HW decompression
    size_t zerocopy_threshold = 4096;       // Min size for zero-copy
};
```

## Building

The zero-copy components are built as part of the cloud_storage library:

```bash
bazel build //src/v/cloud_storage/zero_copy:zero_copy
```

### Running Tests

```bash
bazel test //src/v/cloud_storage/tests/zero_copy:zero_copy_test
```

### Running Benchmarks

```bash
bazel run //src/v/cloud_storage/tests/zero_copy:zero_copy_benchmark
```

## Performance Considerations

### When to Use Zero-Copy

Zero-copy is most beneficial when:
- Reading large segments (> 4KB)
- High throughput is required
- Memory bandwidth is a bottleneck
- Multiple consumers read the same data

### When NOT to Use Zero-Copy

Zero-copy may have overhead for:
- Very small reads (< 4KB)
- Single small requests
- Systems without io_uring support (kernel < 5.1)

### Tuning

Key tuning parameters:

1. **Ring Size**: Larger rings support more concurrent operations but use more memory
   - Default: 1024
   - Range: 64 - 4096

2. **Buffer Count**: More buffers reduce contention but increase memory usage
   - Default: 64
   - Range: 8 - 256

3. **Zero-Copy Threshold**: Minimum size to use zero-copy path
   - Default: 4096 bytes
   - Tune based on workload characteristics

4. **Huge Pages**: Enable for large memory allocations to reduce TLB misses
   - Requires system configuration: `vm.nr_hugepages`

## System Requirements

### Kernel Requirements

- **io_uring**: Linux kernel 5.1 or later
- **Buffer rings**: Linux kernel 5.19 or later (recommended)
- **Huge pages**: Configured via `/proc/sys/vm/nr_hugepages`

### Hardware Requirements

- **Memory**: Additional memory for buffer rings
  - Default: ~64MB (64 buffers × 1MB)
- **CPU**: Modern x86-64 with DMA support
- **Optional**: Intel QAT for hardware decompression

## Troubleshooting

### io_uring not available

If io_uring is not available:
```
io_uring not available on this system, disabling io_uring features
```

**Solution**: Upgrade to Linux kernel 5.1+, or disable with `io_uring_enabled = false`

### Huge pages allocation failed

If huge pages fail to allocate:
```
Failed to allocate huge pages, falling back to regular pages
```

**Solution**: Configure huge pages:
```bash
echo 128 > /proc/sys/vm/nr_hugepages
```

### O_DIRECT alignment errors

If you see alignment errors:
```
Failed to open file with O_DIRECT, falling back to regular I/O
```

**Solution**: Ensure buffers and offsets are properly aligned (512 or 4096 bytes)

## Future Enhancements

- Integration with remote_segment for cloud reads
- Support for Azure Blob Storage zero-copy
- DPDK integration for network zero-copy
- Hardware compression offload (QAT, IAA)

## References

- [RFC-003: Zero-Copy Remote Segment Reads](../../../rfcs/20250115_zero_copy_cloud_reads.md)
- [io_uring Documentation](https://kernel.dk/io_uring.pdf)
- [Seastar Framework](http://seastar.io/)
