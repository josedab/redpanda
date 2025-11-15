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
#include "hashing/xx.h"
#include "model/transform.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/flat_hash_map.h>

#include <chrono>
#include <cstddef>
#include <functional>
#include <vector>

namespace wasm {

/// Module hash type
using module_hash = uint64_t;

/// Module ID type
using module_id = uint64_t;

/// Cached module metadata
struct module_metadata {
    /// Hash of the WASM binary
    module_hash hash;

    /// Size of the module in bytes
    size_t size_bytes;

    /// Exported function names
    std::vector<std::string> exports;

    /// Number of active instances
    size_t instance_count{0};

    /// Last access time
    std::chrono::steady_clock::time_point last_access;

    /// Creation time
    std::chrono::steady_clock::time_point created_at;
};

/// Configuration for module cache
struct module_cache_config {
    /// Maximum number of cached modules
    size_t max_cached_modules = 100;

    /// Enable module preloading
    bool enable_preloading = true;

    /// Eviction policy: "lru" or "lfu"
    std::string eviction_policy = "lru";

    /// Check for stale modules interval
    std::chrono::minutes stale_check_interval{10};
};

/// Module cache manager
class module_cache_manager {
public:
    explicit module_cache_manager(module_cache_config config);

    ~module_cache_manager();

    /// Start the cache manager
    ss::future<> start();

    /// Stop the cache manager
    ss::future<> stop();

    /// Get or load a module by hash
    ss::future<module_metadata*>
      get_or_load_module(const iobuf& wasm_binary);

    /// Preload multiple modules
    ss::future<> preload_modules(const std::vector<iobuf>& modules);

    /// Increment instance count for a module
    void increment_instance_count(module_hash hash);

    /// Decrement instance count for a module
    void decrement_instance_count(module_hash hash);

    /// Evict least recently used module
    ss::future<> evict_least_recently_used();

    /// Check if module is cached
    bool is_cached(module_hash hash) const;

    /// Get cache statistics
    struct stats {
        size_t total_modules;
        size_t total_instances;
        size_t cache_hits;
        size_t cache_misses;
        size_t evictions;
    };
    stats get_stats() const { return _stats; }

private:
    /// Compute module hash
    module_hash compute_hash(const iobuf& binary);

    /// Load module from binary
    ss::future<module_metadata> load_module(const iobuf& binary);

    /// Extract metadata from module
    module_metadata extract_metadata(const iobuf& binary);

    module_cache_config _config;

    /// Cache of loaded modules
    absl::flat_hash_map<module_hash, module_metadata> _module_cache;

    /// Cache statistics
    stats _stats{};

    bool _started{false};
};

/// Instance handle with RAII cleanup
struct instance_handle {
    /// Opaque instance pointer (wasmtime_instance_t*)
    void* instance{nullptr};

    /// Opaque store pointer (wasmtime_store_t*)
    void* store{nullptr};

    /// Release callback
    std::function<void()> release;

    instance_handle() = default;

    instance_handle(void* inst, void* st, std::function<void()> rel)
      : instance(inst)
      , store(st)
      , release(std::move(rel)) {}

    ~instance_handle() {
        if (release) {
            release();
        }
    }

    // Non-copyable, moveable
    instance_handle(const instance_handle&) = delete;
    instance_handle& operator=(const instance_handle&) = delete;
    instance_handle(instance_handle&& other) noexcept
      : instance(other.instance)
      , store(other.store)
      , release(std::move(other.release)) {
        other.instance = nullptr;
        other.store = nullptr;
        other.release = nullptr;
    }
    instance_handle& operator=(instance_handle&& other) noexcept {
        if (this != &other) {
            if (release) {
                release();
            }
            instance = other.instance;
            store = other.store;
            release = std::move(other.release);
            other.instance = nullptr;
            other.store = nullptr;
            other.release = nullptr;
        }
        return *this;
    }
};

/// Pooled instance wrapper
struct pooled_instance {
    /// Instance handle
    void* instance{nullptr};

    /// Store handle
    void* store{nullptr};

    /// Whether instance is currently in use
    bool in_use{false};

    /// Last used timestamp
    std::chrono::steady_clock::time_point last_used;

    /// Creation timestamp
    std::chrono::steady_clock::time_point created_at;

    /// Number of times reused
    size_t reuse_count{0};
};

/// Configuration for instance pool
struct instance_pool_config {
    /// Maximum pool size per module
    size_t max_pool_size = 50;

    /// Idle timeout before instance cleanup
    std::chrono::minutes idle_timeout{5};

    /// Enable instance prewarming
    bool enable_prewarming = true;

    /// Minimum pool size to maintain
    size_t min_pool_size = 5;
};

/// Instance pool for WASM module instances
class instance_pool {
public:
    explicit instance_pool(instance_pool_config config);

    ~instance_pool();

    /// Start the instance pool
    ss::future<> start();

    /// Stop the instance pool
    ss::future<> stop();

    /// Acquire an instance from the pool
    ss::future<instance_handle> acquire_instance(module_id module);

    /// Create a new instance for a module
    ss::future<instance_handle> create_instance(module_id module);

    /// Prewarm instances for a module
    ss::future<> prewarm_instances(module_id module, size_t count);

    /// Cleanup idle instances
    ss::future<> cleanup_idle_instances();

    /// Get pool statistics
    struct stats {
        size_t total_instances;
        size_t in_use_instances;
        size_t idle_instances;
        size_t total_acquisitions;
        size_t cache_hits;
        size_t cache_misses;
    };
    stats get_stats() const { return _stats; }

private:
    /// Reset instance state
    ss::future<> reset_instance(pooled_instance& inst);

    /// Release instance back to pool
    void release_instance(module_id module, pooled_instance& inst);

    /// Check if pool needs cleanup
    bool should_cleanup_pool() const;

    instance_pool_config _config;

    /// Pools of instances per module
    absl::flat_hash_map<module_id, std::vector<pooled_instance>> _pools;

    /// Pool statistics
    stats _stats{};

    bool _started{false};
};

} // namespace wasm
