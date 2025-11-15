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
#include "container/fragmented_vector.h"
#include "hashing/xx.h"
#include "model/transform.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/flat_hash_map.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>

namespace wasm {

/// Optimization level for AOT compilation
enum class optimization_level {
    /// No optimization, fastest compilation
    none = 0,
    /// Basic optimizations, balanced speed/size
    baseline = 1,
    /// Aggressive optimizations, best runtime performance
    aggressive = 2,
};

/// Compilation options for AOT compiler
struct compilation_options {
    /// Optimization level to use
    optimization_level opt_level = optimization_level::aggressive;

    /// Enable SIMD instructions
    bool enable_simd = true;

    /// Enable bulk memory operations
    bool enable_bulk_memory = true;

    /// Enable multi-value returns
    bool enable_multi_value = true;

    /// Enable profile-guided optimization
    bool enable_pgo = false;

    /// Hot functions from profiling data (for PGO)
    std::vector<std::string> hot_functions;

    /// Branch prediction hints (for PGO)
    absl::flat_hash_map<std::string, double> branch_hints;

    /// Inline candidate functions (for PGO)
    std::vector<std::string> inline_hints;
};

/// Profile data collected during execution
struct profile_data {
    /// Hash of the module this profile is for
    uint64_t module_hash;

    /// Functions that are executed frequently
    std::vector<std::string> hot_functions;

    /// Branch prediction data (function -> taken probability)
    absl::flat_hash_map<std::string, double> branch_predictions;

    /// Functions that should be inlined
    std::vector<std::string> inline_candidates;

    /// Total execution count
    size_t execution_count{0};

    /// Last update timestamp
    std::chrono::steady_clock::time_point last_update;
};

/// Metadata extracted from a compiled module
struct module_metadata {
    /// Hash of the original WASM binary
    uint64_t hash;

    /// Size of the compiled native code
    size_t native_code_size;

    /// Exported function names
    std::vector<std::string> exports;

    /// Imported function names
    std::vector<std::string> imports;

    /// Compilation timestamp
    std::chrono::system_clock::time_point compiled_at;

    /// Optimization level used
    optimization_level opt_level;
};

/// Compiled module with native code
struct compiled_module {
    /// Serialized native code (AOT compiled)
    iobuf native_code;

    /// Module metadata
    module_metadata metadata;

    /// Whether this module has been validated
    bool validated{false};
};

/// Cache key for compiled modules
using cache_key = uint64_t;

/// Configuration for AOT compiler
struct aot_config {
    /// Directory for cached compiled modules
    std::filesystem::path cache_directory{"/var/lib/redpanda/wasm_cache"};

    /// Maximum cache size in bytes (default 1GB)
    size_t max_cache_size = 1073741824;

    /// Enable profile-guided optimization
    bool enable_pgo = true;

    /// Default optimization level
    optimization_level opt_level = optimization_level::aggressive;

    /// Enable module validation before caching
    bool validate_modules = true;

    /// Cache entry TTL (time-to-live)
    std::chrono::hours cache_ttl{24 * 7}; // 7 days
};

/// Ahead-of-Time compiler for WASM modules
class aot_compiler {
public:
    explicit aot_compiler(aot_config config);

    ~aot_compiler();

    /// Start the AOT compiler
    ss::future<> start();

    /// Stop the AOT compiler and flush caches
    ss::future<> stop();

    /// Compile a WASM module to native code
    ss::future<compiled_module> compile_to_native(
      model::wasm_binary_iobuf module,
      const compilation_options& opts = {});

    /// Load a pre-compiled module from cache
    ss::future<std::optional<compiled_module>>
      load_from_cache(cache_key key);

    /// Store a compiled module to cache
    ss::future<> store_in_cache(cache_key key, const compiled_module& module);

    /// Compute cache key for a module
    cache_key
      compute_cache_key(const iobuf& module, const compilation_options& opts);

    /// Apply profile-guided optimization to a module
    ss::future<compiled_module> apply_profile_guided_optimization(
      const iobuf& module,
      const compilation_options& opts,
      const profile_data& profile);

    /// Load profile data for a module
    ss::future<std::optional<profile_data>>
      load_profile_data(uint64_t module_hash);

    /// Store profile data for a module
    ss::future<> store_profile_data(const profile_data& profile);

    /// Check if profile data exists for a module
    bool has_profile_data(uint64_t module_hash) const;

    /// Evict old entries from cache to meet size limits
    ss::future<> evict_cache_entries();

    /// Get current cache statistics
    struct cache_stats {
        size_t total_entries;
        size_t total_size_bytes;
        size_t hits;
        size_t misses;
        size_t evictions;
    };
    cache_stats get_cache_stats() const { return _cache_stats; }

private:
    /// Perform actual AOT compilation
    ss::future<compiled_module> perform_aot_compilation(
      const iobuf& module, const compilation_options& opts);

    /// Extract metadata from a compiled module
    module_metadata extract_module_metadata(const iobuf& native_code);

    /// Validate a compiled module
    ss::future<bool> validate_compiled_module(const compiled_module& module);

    /// Initialize cache directory
    ss::future<> initialize_cache_directory();

    /// Compute module hash
    uint64_t compute_module_hash(const iobuf& module);

    aot_config _config;

    // In-memory cache of compiled modules
    absl::flat_hash_map<cache_key, compiled_module> _module_cache;

    // Profile data cache
    absl::flat_hash_map<uint64_t, profile_data> _profile_cache;

    // Cache statistics
    cache_stats _cache_stats{};

    // Cache directory initialized flag
    bool _cache_initialized{false};
};

} // namespace wasm
