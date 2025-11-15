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
#include "compression/compression.h"

#include <seastar/core/future.hh>

#include <cstddef>
#include <memory>

namespace cloud_storage::zero_copy {

/// Result of a decompression operation
struct decompression_result {
    iobuf data;
    size_t uncompressed_size;
    bool was_in_place;
};

/// Zero-copy decompressor with support for in-place decompression
class zero_copy_decompressor {
public:
    zero_copy_decompressor();
    ~zero_copy_decompressor();

    zero_copy_decompressor(const zero_copy_decompressor&) = delete;
    zero_copy_decompressor& operator=(const zero_copy_decompressor&) = delete;

    /// Decompress data, attempting in-place decompression when possible
    ss::future<decompression_result> decompress_in_place(
      iobuf compressed, compression::type type);

    /// Enable or disable hardware acceleration (e.g., Intel QAT)
    void set_hw_acceleration(bool enabled) { _hw_accelerated = enabled; }

    /// Check if hardware acceleration is available
    static bool is_hw_acceleration_available();

private:
    /// Decompress LZ4 in-place
    ss::future<decompression_result> decompress_lz4_in_place(iobuf compressed);

    /// Decompress LZ4 with copy
    ss::future<decompression_result> decompress_lz4_copy(iobuf compressed);

    /// Decompress ZSTD using streaming
    ss::future<decompression_result>
    decompress_zstd_streaming(iobuf compressed);

    /// Decompress using hardware acceleration (QAT)
    ss::future<decompression_result> decompress_qat(iobuf compressed);

    /// Fallback decompression
    ss::future<decompression_result>
    decompress_fallback(iobuf compressed, compression::type type);

    /// Estimate uncompressed size
    size_t estimate_uncompressed_size(const iobuf& compressed);

    /// Check if QAT is available
    bool has_qat_support() const;

private:
    bool _hw_accelerated = false;
};

} // namespace cloud_storage::zero_copy
