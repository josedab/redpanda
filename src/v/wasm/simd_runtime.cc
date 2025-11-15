// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "wasm/simd_runtime.h"

#include "model/record.h"
#include "wasm/wasmtime.h"

#include <seastar/core/future.hh>

namespace wasm {

simd_enabled_runtime::simd_enabled_runtime(simd_config cfg)
  : _simd_config(std::move(cfg)) {}

ss::future<> simd_enabled_runtime::start(runtime::config cfg) {
    // Create the base Wasmtime runtime
    _base_runtime = create_runtime(std::move(cfg.sr));

    // Start the base runtime with SIMD-enhanced configuration
    co_await _base_runtime->start(std::move(cfg));

    // Setup SIMD-specific optimizations
    co_await setup_simd_optimizations();

    co_return;
}

ss::future<> simd_enabled_runtime::stop() {
    if (_base_runtime) {
        co_await _base_runtime->stop();
        _base_runtime.reset();
    }
    co_return;
}

ss::future<ss::shared_ptr<factory>> simd_enabled_runtime::make_factory(
  model::transform_metadata meta, iobuf buf, ss::logger* logger) {
    // Delegate to base runtime for factory creation
    // The Wasmtime engine will automatically use SIMD instructions
    // when the WASM module contains them and the CPU supports it
    co_return co_await _base_runtime->make_factory(
      std::move(meta), std::move(buf), logger);
}

ss::future<> simd_enabled_runtime::validate(model::wasm_binary_iobuf binary) {
    // Delegate to base runtime for validation
    co_return co_await _base_runtime->validate(std::move(binary));
}

ss::future<> simd_enabled_runtime::setup_simd_optimizations() {
    // SIMD optimizations are configured at the Wasmtime engine level
    // The actual SIMD instruction generation is handled by Cranelift
    // This method is reserved for future SIMD-specific optimizations
    co_return;
}

vectorized_batch simd_enabled_runtime::prepare_vectorized_input(
  const model::record_batch& batch) {
    vectorized_batch vbatch;
    vbatch.original_record_count = batch.record_count();

    // Reserve space for all records
    vbatch.keys.reserve(batch.record_count());
    vbatch.values.reserve(batch.record_count());
    vbatch.timestamps.reserve(batch.record_count());
    vbatch.record_sizes.reserve(batch.record_count());

    // Convert row-oriented to column-oriented representation
    batch.for_each_record([&](model::record record) {
        // Vectorize key
        if (record.key()) {
            vbatch.keys.push_back(vectorize_bytes<uint8_t>(record.key().value()));
        } else {
            vbatch.keys.push_back(simd_vector<uint8_t>());
        }

        // Vectorize value
        vbatch.values.push_back(vectorize_bytes<uint8_t>(record.value()));

        // Store timestamp
        vbatch.timestamps.push_back(record.timestamp().value());

        // Store record size for reconstruction
        vbatch.record_sizes.push_back(
          record.key().value_or(iobuf()).size_bytes()
          + record.value().size_bytes());
    });

    // Pad for SIMD alignment if configured
    if (_simd_config.enable_simd) {
        pad_to_vector_width(vbatch);
    }

    return vbatch;
}

template<typename T>
simd_vector<T> simd_enabled_runtime::vectorize_bytes(const iobuf& buf) {
    const size_t alignment = _simd_width_bytes / sizeof(T);
    const size_t aligned_size = (buf.size_bytes() + alignment - 1)
                                / alignment * alignment;

    simd_vector<T> vec(aligned_size, _simd_width_bytes);
    vec.reserve(aligned_size);

    // Copy buffer contents with SIMD alignment
    for (const auto& frag : buf) {
        const auto* data = reinterpret_cast<const T*>(frag.get());
        const size_t count = frag.size() / sizeof(T);
        for (size_t i = 0; i < count; ++i) {
            vec.push_back(data[i]);
        }
    }

    // Pad to SIMD width
    while (vec.size() % alignment != 0) {
        vec.push_back(T{});
    }

    return vec;
}

void simd_enabled_runtime::pad_to_vector_width(vectorized_batch& vbatch) {
    const size_t alignment = _simd_width_bytes;

    // Pad each key/value vector to alignment
    for (auto& key : vbatch.keys) {
        while (key.size() % alignment != 0) {
            key.push_back(0);
        }
    }

    for (auto& value : vbatch.values) {
        while (value.size() % alignment != 0) {
            value.push_back(0);
        }
    }
}

// Explicit template instantiations
template simd_vector<uint8_t>
simd_enabled_runtime::vectorize_bytes<uint8_t>(const iobuf& buf);

ss::shared_ptr<runtime> create_simd_runtime(
  simd_config cfg, std::unique_ptr<schema::registry> sr) {
    auto runtime = ss::make_shared<simd_enabled_runtime>(std::move(cfg));
    return runtime;
}

} // namespace wasm
