// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/zero_copy/zero_copy_decompressor.h"

#include "bytes/iobuf_parser.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

#include <lz4.h>
#include <zstd.h>

#include <stdexcept>

namespace cloud_storage::zero_copy {

static ss::logger decomp_log("zero_copy_decomp");

zero_copy_decompressor::zero_copy_decompressor() = default;

zero_copy_decompressor::~zero_copy_decompressor() = default;

bool zero_copy_decompressor::is_hw_acceleration_available() {
    // Check for Intel QAT availability
    // In production, this would check for /dev/qat_* devices
    return false;
}

ss::future<decompression_result> zero_copy_decompressor::decompress_in_place(
  iobuf compressed, compression::type type) {
    switch (type) {
    case compression::type::none:
        co_return decompression_result{
          .data = std::move(compressed),
          .uncompressed_size = compressed.size_bytes(),
          .was_in_place = true};

    case compression::type::lz4:
        co_return co_await decompress_lz4_in_place(std::move(compressed));

    case compression::type::zstd:
        if (_hw_accelerated && has_qat_support()) {
            co_return co_await decompress_qat(std::move(compressed));
        }
        co_return co_await decompress_zstd_streaming(std::move(compressed));

    default:
        co_return co_await decompress_fallback(std::move(compressed), type);
    }
}

ss::future<decompression_result>
zero_copy_decompressor::decompress_lz4_in_place(iobuf compressed) {
    // LZ4 supports in-place decompression in some cases
    // For simplicity, we'll use a copy-based approach here
    // In production, you would check if the buffer has enough capacity
    co_return co_await decompress_lz4_copy(std::move(compressed));
}

ss::future<decompression_result>
zero_copy_decompressor::decompress_lz4_copy(iobuf compressed) {
    // Get compressed data size
    size_t compressed_size = compressed.size_bytes();

    // Estimate uncompressed size
    // For LZ4, we need to read the frame header
    // Simplified implementation assumes max compression ratio
    size_t max_uncompressed = compressed_size * 10;

    // Allocate output buffer
    iobuf result;
    auto out_buffer = ss::temporary_buffer<char>(max_uncompressed);

    // Linearize input if needed
    auto in_buffer = iobuf_to_bytes(compressed);

    // Decompress
    int decompressed_size = LZ4_decompress_safe(
      reinterpret_cast<const char*>(in_buffer.data()),
      out_buffer.get_write(),
      in_buffer.size(),
      max_uncompressed);

    if (decompressed_size < 0) {
        throw std::runtime_error("LZ4 decompression failed");
    }

    // Trim buffer to actual size
    out_buffer.trim(decompressed_size);
    result.append(std::move(out_buffer));

    co_return decompression_result{
      .data = std::move(result),
      .uncompressed_size = static_cast<size_t>(decompressed_size),
      .was_in_place = false};
}

ss::future<decompression_result>
zero_copy_decompressor::decompress_zstd_streaming(iobuf compressed) {
    // ZSTD streaming decompression
    ZSTD_DStream* stream = ZSTD_createDStream();
    if (!stream) {
        throw std::runtime_error("Failed to create ZSTD stream");
    }

    size_t ret = ZSTD_initDStream(stream);
    if (ZSTD_isError(ret)) {
        ZSTD_freeDStream(stream);
        throw std::runtime_error(ZSTD_getErrorName(ret));
    }

    iobuf result;
    size_t total_size = 0;

    // Process input data
    iobuf_parser parser(std::move(compressed));
    while (parser.bytes_left() > 0) {
        // Get next chunk
        auto chunk_size = std::min(parser.bytes_left(), size_t(65536));
        auto chunk = parser.read_bytes(chunk_size);

        ZSTD_inBuffer input = {
          .src = chunk.data(), .size = chunk.size(), .pos = 0};

        while (input.pos < input.size) {
            // Allocate output buffer
            size_t out_size = ZSTD_DStreamOutSize();
            auto out_buffer = ss::temporary_buffer<char>(out_size);

            ZSTD_outBuffer output = {
              .dst = out_buffer.get_write(),
              .size = out_buffer.size(),
              .pos = 0};

            // Decompress chunk
            ret = ZSTD_decompressStream(stream, &output, &input);

            if (ZSTD_isError(ret)) {
                ZSTD_freeDStream(stream);
                throw std::runtime_error(ZSTD_getErrorName(ret));
            }

            // Add to result if we got data
            if (output.pos > 0) {
                out_buffer.trim(output.pos);
                result.append(std::move(out_buffer));
                total_size += output.pos;
            }

            // Yield to allow other tasks to run
            if (total_size % (1024 * 1024) == 0) {
                co_await ss::coroutine::maybe_yield();
            }
        }
    }

    ZSTD_freeDStream(stream);

    co_return decompression_result{
      .data = std::move(result),
      .uncompressed_size = total_size,
      .was_in_place = false};
}

ss::future<decompression_result>
zero_copy_decompressor::decompress_qat(iobuf compressed) {
    // Intel QuickAssist Technology hardware acceleration
    // This is a placeholder - production implementation would use QAT SDK
    vlog(
      decomp_log.warn,
      "QAT hardware acceleration not implemented, falling back to software");
    co_return co_await decompress_zstd_streaming(std::move(compressed));
}

ss::future<decompression_result> zero_copy_decompressor::decompress_fallback(
  iobuf compressed, compression::type type) {
    // Fall back to existing compression infrastructure
    vlog(
      decomp_log.debug,
      "Using fallback decompression for type {}",
      static_cast<int>(type));

    // Use the existing stream compressor
    auto compressor = compression::make_stream_compressor(type);

    auto result = co_await compressor->uncompress(std::move(compressed));

    co_return decompression_result{
      .data = std::move(result),
      .uncompressed_size = result.size_bytes(),
      .was_in_place = false};
}

size_t
zero_copy_decompressor::estimate_uncompressed_size(const iobuf& compressed) {
    // Conservative estimate: assume 10x compression ratio
    return compressed.size_bytes() * 10;
}

bool zero_copy_decompressor::has_qat_support() const {
    // Check for QAT device availability
    // In production, this would check for /dev/qat_* or similar
    return false;
}

} // namespace cloud_storage::zero_copy
