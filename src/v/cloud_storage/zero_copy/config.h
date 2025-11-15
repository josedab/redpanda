// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <cstddef>

namespace cloud_storage::zero_copy {

/// Configuration for zero-copy cloud reads feature
struct config {
    /// Enable zero-copy cloud reads
    bool enabled = true;

    /// Enable io_uring for cloud reads
    bool io_uring_enabled = true;

    /// io_uring ring size (submission/completion queue size)
    size_t io_uring_ring_size = 1024;

    /// Number of buffers in the io_uring buffer ring
    size_t io_uring_buffer_count = 64;

    /// Use huge pages for buffer allocation
    bool use_huge_pages = true;

    /// Enable Direct I/O (O_DIRECT) for local cache reads
    bool direct_io_enabled = true;

    /// Enable hardware-accelerated decompression (e.g., Intel QAT)
    bool hw_decompression_enabled = false;

    /// Minimum size threshold for using zero-copy (bytes)
    /// Reads smaller than this will use regular copy path
    size_t zerocopy_threshold = 4096; // 4KB

    /// Buffer size for Direct I/O operations
    size_t direct_io_buffer_size = 1048576; // 1MB

    /// Alignment for Direct I/O (512 or 4096 bytes)
    size_t direct_io_alignment = 512;

    /// Maximum number of scatter-gather fragments per buffer
    size_t max_sg_fragments = 16;

    /// Enable kernel polling (SQPOLL) for io_uring
    bool io_uring_sqpoll = false;

    /// Get default configuration
    static config get_default();

    /// Validate configuration and adjust invalid values
    void validate();
};

} // namespace cloud_storage::zero_copy
