// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/zero_copy/config.h"

#include "cloud_storage/zero_copy/io_uring_reader.h"
#include "vlog.h"

#include <algorithm>

namespace cloud_storage::zero_copy {

static ss::logger config_log("zero_copy_config");

config config::get_default() {
    config cfg;

    // Check if io_uring is available
    if (!io_uring_cloud_reader::is_zero_copy_available()) {
        vlog(
          config_log.warn,
          "io_uring not available, disabling zero-copy features");
        cfg.enabled = false;
        cfg.io_uring_enabled = false;
    }

    return cfg;
}

void config::validate() {
    // Ensure ring size is a power of 2
    if (io_uring_ring_size & (io_uring_ring_size - 1)) {
        size_t next_power = 1;
        while (next_power < io_uring_ring_size) {
            next_power <<= 1;
        }
        vlog(
          config_log.warn,
          "io_uring_ring_size ({}) is not a power of 2, rounding up to {}",
          io_uring_ring_size,
          next_power);
        io_uring_ring_size = next_power;
    }

    // Clamp ring size to reasonable bounds
    io_uring_ring_size = std::clamp(io_uring_ring_size, size_t(64), size_t(4096));

    // Clamp buffer count
    io_uring_buffer_count = std::clamp(
      io_uring_buffer_count, size_t(8), size_t(256));

    // Validate alignment (must be 512 or 4096)
    if (direct_io_alignment != 512 && direct_io_alignment != 4096) {
        vlog(
          config_log.warn,
          "Invalid direct_io_alignment ({}), using 512",
          direct_io_alignment);
        direct_io_alignment = 512;
    }

    // Validate threshold
    if (zerocopy_threshold < 512) {
        vlog(
          config_log.warn,
          "zerocopy_threshold ({}) is too small, using 512",
          zerocopy_threshold);
        zerocopy_threshold = 512;
    }

    // Validate max fragments
    max_sg_fragments = std::clamp(max_sg_fragments, size_t(1), size_t(128));

    // Disable features if zero-copy is disabled
    if (!enabled) {
        io_uring_enabled = false;
        direct_io_enabled = false;
    }

    // Check io_uring availability
    if (io_uring_enabled && !io_uring_cloud_reader::is_zero_copy_available()) {
        vlog(
          config_log.warn,
          "io_uring not available on this system, disabling io_uring features");
        io_uring_enabled = false;
    }
}

} // namespace cloud_storage::zero_copy
