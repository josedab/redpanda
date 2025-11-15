// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "model/record.h"
#include "wasm/engine.h"

#include <seastar/core/future.hh>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace wasm {

/// Configuration for SIMD-enabled WASM runtime
struct simd_config {
    /// Enable SIMD instructions (128-bit vector operations)
    bool enable_simd = true;

    /// Enable bulk memory operations
    bool enable_bulk_memory = true;

    /// Enable multi-value returns
    bool enable_multi_value = true;

    /// Enable threads (experimental)
    bool enable_threads = false;

    /// SIMD vector width in bits (128 for SSE/NEON)
    size_t vector_width = 128;

    /// Batch processing chunk size for vectorized operations
    size_t vectorization_batch_size = 4;
};

/// SIMD vector type for aligned data processing
template<typename T>
class simd_vector {
public:
    simd_vector() = default;

    explicit simd_vector(size_t size, size_t alignment = 16)
      : _data(size)
      , _alignment(alignment) {
        // Ensure proper alignment for SIMD operations
        reserve(align_up(size, _alignment / sizeof(T)));
    }

    void reserve(size_t capacity) { _data.reserve(capacity); }

    void push_back(T value) { _data.push_back(value); }

    size_t size() const { return _data.size(); }

    bool empty() const { return _data.empty(); }

    T* data() { return _data.data(); }
    const T* data() const { return _data.data(); }

    T& operator[](size_t idx) { return _data[idx]; }
    const T& operator[](size_t idx) const { return _data[idx]; }

    auto begin() { return _data.begin(); }
    auto end() { return _data.end(); }
    auto begin() const { return _data.begin(); }
    auto end() const { return _data.end(); }

    void insert(
      typename std::vector<T>::iterator pos,
      typename std::vector<T>::const_iterator first,
      typename std::vector<T>::const_iterator last) {
        _data.insert(pos, first, last);
    }

private:
    static constexpr size_t align_up(size_t value, size_t alignment) {
        return (value + alignment - 1) & ~(alignment - 1);
    }

    std::vector<T> _data;
    size_t _alignment;
};

/// Vectorized batch representation for column-oriented processing
struct vectorized_batch {
    /// Column-oriented storage for SIMD processing
    std::vector<simd_vector<uint8_t>> keys;
    std::vector<simd_vector<uint8_t>> values;
    std::vector<int64_t> timestamps;
    std::vector<size_t> record_sizes;
    size_t original_record_count{0};
};

/// SIMD-enabled runtime that extends the base runtime with vectorization support
class simd_enabled_runtime : public runtime {
public:
    explicit simd_enabled_runtime(simd_config cfg);

    ~simd_enabled_runtime() override = default;

    /// Start the runtime with SIMD configuration
    ss::future<> start(runtime::config cfg) override;

    /// Stop the runtime
    ss::future<> stop() override;

    /// Create a factory for compiling WASM modules with SIMD support
    ss::future<ss::shared_ptr<factory>>
    make_factory(model::transform_metadata, iobuf, ss::logger*) override;

    /// Validate a WASM module for SIMD compatibility
    ss::future<> validate(model::wasm_binary_iobuf) override;

    /// Get SIMD configuration
    const simd_config& config() const { return _simd_config; }

    /// Check if SIMD is enabled and available
    bool is_simd_enabled() const { return _simd_config.enable_simd; }

protected:
    /// Setup SIMD-specific optimizations
    ss::future<> setup_simd_optimizations();

    /// Prepare vectorized input from record batch
    vectorized_batch prepare_vectorized_input(const model::record_batch& batch);

    /// Convert vectorized data to iobuf
    template<typename T>
    simd_vector<T> vectorize_bytes(const iobuf& buf);

    /// Pad data to SIMD vector width for aligned operations
    void pad_to_vector_width(vectorized_batch& vbatch);

private:
    simd_config _simd_config;
    ss::shared_ptr<runtime> _base_runtime;

    static constexpr size_t _simd_width_bytes = 16; // 128-bit SIMD
};

/// Create a SIMD-enabled runtime instance
ss::shared_ptr<runtime> create_simd_runtime(
  simd_config cfg = {}, std::unique_ptr<schema::registry> sr = nullptr);

} // namespace wasm
