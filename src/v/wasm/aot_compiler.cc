// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "wasm/aot_compiler.h"

#include "hashing/xx.h"
#include "vlog.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>

#include <algorithm>
#include <filesystem>

namespace wasm {

static ss::logger aot_log("wasm_aot");

aot_compiler::aot_compiler(aot_config config)
  : _config(std::move(config)) {}

aot_compiler::~aot_compiler() = default;

ss::future<> aot_compiler::start() {
    vlog(aot_log.info, "Starting AOT compiler");

    // Initialize cache directory
    co_await initialize_cache_directory();

    // TODO: Load existing cache entries from disk
    _cache_initialized = true;

    vlog(
      aot_log.info,
      "AOT compiler started, cache directory: {}",
      _config.cache_directory.string());

    co_return;
}

ss::future<> aot_compiler::stop() {
    vlog(aot_log.info, "Stopping AOT compiler");

    // TODO: Flush in-memory cache to disk
    _module_cache.clear();
    _profile_cache.clear();

    vlog(aot_log.info, "AOT compiler stopped");
    co_return;
}

ss::future<compiled_module> aot_compiler::compile_to_native(
  model::wasm_binary_iobuf module,
  const compilation_options& opts) {
    // Convert wasm_binary_iobuf to regular iobuf
    iobuf module_buf = std::move(module).data;

    // Compute cache key
    auto key = compute_cache_key(module_buf, opts);

    // Check in-memory cache first
    if (auto it = _module_cache.find(key); it != _module_cache.end()) {
        vlog(aot_log.debug, "AOT cache hit for module hash: {}", key);
        _cache_stats.hits++;
        co_return it->second;
    }

    // Check disk cache
    if (auto cached = co_await load_from_cache(key)) {
        vlog(aot_log.debug, "AOT disk cache hit for module hash: {}", key);
        _cache_stats.hits++;
        _module_cache[key] = *cached;
        co_return *cached;
    }

    vlog(aot_log.debug, "AOT cache miss for module hash: {}", key);
    _cache_stats.misses++;

    // Compile the module
    auto compiled = co_await perform_aot_compilation(module_buf, opts);

    // Apply PGO if enabled and profile data exists
    if (opts.enable_pgo && _config.enable_pgo) {
        auto module_hash = compute_module_hash(module_buf);
        if (has_profile_data(module_hash)) {
            vlog(
              aot_log.debug,
              "Applying PGO to module hash: {}",
              module_hash);
            auto profile = co_await load_profile_data(module_hash);
            if (profile) {
                compiled = co_await apply_profile_guided_optimization(
                  module_buf, opts, *profile);
            }
        }
    }

    // Validate if configured
    if (_config.validate_modules) {
        if (!co_await validate_compiled_module(compiled)) {
            throw std::runtime_error("Compiled module validation failed");
        }
    }

    // Store in caches
    _module_cache[key] = compiled;
    co_await store_in_cache(key, compiled);

    // Check if we need to evict entries
    if (_module_cache.size() > 100) { // Arbitrary limit
        co_await evict_cache_entries();
    }

    co_return compiled;
}

ss::future<compiled_module> aot_compiler::perform_aot_compilation(
  const iobuf& module, const compilation_options& opts) {
    // Note: In a real implementation, this would use Wasmtime's C API
    // to compile the module to native code. For now, we create a stub.

    vlog(
      aot_log.debug,
      "Compiling module (size: {} bytes, opt_level: {})",
      module.size_bytes(),
      static_cast<int>(opts.opt_level));

    compiled_module result;

    // In a real implementation:
    // 1. Create wasmtime_engine with AOT config
    // 2. Call wasmtime_module_new() to compile
    // 3. Call wasmtime_module_serialize() to get native code
    // 4. Extract metadata

    // For now, copy the module as-is (stub implementation)
    result.native_code = module.copy();

    // Fill in metadata
    result.metadata.hash = compute_module_hash(module);
    result.metadata.native_code_size = module.size_bytes();
    result.metadata.compiled_at = std::chrono::system_clock::now();
    result.metadata.opt_level = opts.opt_level;
    result.validated = false;

    co_return result;
}

ss::future<std::optional<compiled_module>>
aot_compiler::load_from_cache(cache_key key) {
    // Check in-memory cache first
    if (auto it = _module_cache.find(key); it != _module_cache.end()) {
        co_return it->second;
    }

    // TODO: Load from disk cache
    // For now, return empty optional
    co_return std::nullopt;
}

ss::future<>
aot_compiler::store_in_cache(cache_key key, const compiled_module& module) {
    // Store in memory
    _module_cache[key] = module;
    _cache_stats.total_entries = _module_cache.size();

    // TODO: Store to disk
    co_return;
}

cache_key aot_compiler::compute_cache_key(
  const iobuf& module, const compilation_options& opts) {
    incremental_xxhash64 hasher;

    // Hash module content
    for (const auto& frag : module) {
        hasher.update(frag.get(), frag.size());
    }

    // Hash compilation options
    hasher.update(&opts.opt_level, sizeof(opts.opt_level));
    hasher.update(&opts.enable_simd, sizeof(opts.enable_simd));
    hasher.update(&opts.enable_bulk_memory, sizeof(opts.enable_bulk_memory));
    hasher.update(&opts.enable_multi_value, sizeof(opts.enable_multi_value));

    return hasher.digest();
}

uint64_t aot_compiler::compute_module_hash(const iobuf& module) {
    incremental_xxhash64 hasher;
    for (const auto& frag : module) {
        hasher.update(frag.get(), frag.size());
    }
    return hasher.digest();
}

ss::future<compiled_module>
aot_compiler::apply_profile_guided_optimization(
  const iobuf& module,
  const compilation_options& opts,
  const profile_data& profile) {
    vlog(
      aot_log.debug,
      "Applying PGO with {} hot functions",
      profile.hot_functions.size());

    // Create enhanced compilation options with profile data
    compilation_options pgo_opts = opts;
    pgo_opts.hot_functions = profile.hot_functions;
    pgo_opts.branch_hints = profile.branch_predictions;
    pgo_opts.inline_hints = profile.inline_candidates;

    // Recompile with profile information
    co_return co_await perform_aot_compilation(module, pgo_opts);
}

ss::future<std::optional<profile_data>>
aot_compiler::load_profile_data(uint64_t module_hash) {
    if (auto it = _profile_cache.find(module_hash);
        it != _profile_cache.end()) {
        co_return it->second;
    }

    // TODO: Load from disk
    co_return std::nullopt;
}

ss::future<> aot_compiler::store_profile_data(const profile_data& profile) {
    // Store in memory
    _profile_cache[profile.module_hash] = profile;

    // TODO: Store to disk
    co_return;
}

bool aot_compiler::has_profile_data(uint64_t module_hash) const {
    return _profile_cache.contains(module_hash);
}

module_metadata
aot_compiler::extract_module_metadata(const iobuf& native_code) {
    module_metadata meta;
    meta.native_code_size = native_code.size_bytes();
    meta.compiled_at = std::chrono::system_clock::now();

    // TODO: Extract exports/imports from native code
    return meta;
}

ss::future<bool>
aot_compiler::validate_compiled_module(const compiled_module& module) {
    // Basic validation
    if (module.native_code.empty()) {
        vlog(aot_log.warn, "Compiled module has no native code");
        co_return false;
    }

    if (module.metadata.native_code_size != module.native_code.size_bytes()) {
        vlog(aot_log.warn, "Metadata size mismatch");
        co_return false;
    }

    // TODO: More thorough validation
    co_return true;
}

ss::future<> aot_compiler::evict_cache_entries() {
    vlog(aot_log.debug, "Evicting old cache entries");

    // Simple LRU eviction - remove oldest entries
    // In a real implementation, this would consider:
    // - Entry age
    // - Access frequency
    // - Total cache size

    if (_module_cache.size() > 50) {
        auto to_remove = _module_cache.size() - 50;
        auto it = _module_cache.begin();
        for (size_t i = 0; i < to_remove && it != _module_cache.end(); ++i) {
            it = _module_cache.erase(it);
            _cache_stats.evictions++;
        }
    }

    co_return;
}

ss::future<> aot_compiler::initialize_cache_directory() {
    // Create cache directory if it doesn't exist
    try {
        if (!std::filesystem::exists(_config.cache_directory)) {
            vlog(
              aot_log.info,
              "Creating cache directory: {}",
              _config.cache_directory.string());
            std::filesystem::create_directories(_config.cache_directory);
        }
    } catch (const std::filesystem::filesystem_error& e) {
        vlog(
          aot_log.warn,
          "Failed to create cache directory: {}",
          e.what());
        // Continue without disk cache
    }

    co_return;
}

} // namespace wasm
