// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "wasm/instance_pool.h"

#include "hashing/xx.h"
#include "vlog.h"

#include <seastar/core/future.hh>

namespace wasm {

static ss::logger pool_log("wasm_instance_pool");

// Module cache manager implementation

module_cache_manager::module_cache_manager(module_cache_config config)
  : _config(std::move(config)) {}

module_cache_manager::~module_cache_manager() = default;

ss::future<> module_cache_manager::start() {
    vlog(
      pool_log.info,
      "Starting module cache manager (max modules: {})",
      _config.max_cached_modules);
    _started = true;
    co_return;
}

ss::future<> module_cache_manager::stop() {
    vlog(
      pool_log.info,
      "Stopping module cache manager ({} cached modules)",
      _module_cache.size());

    _started = false;
    _module_cache.clear();

    vlog(pool_log.info, "Module cache manager stopped");
    co_return;
}

ss::future<module_metadata*>
module_cache_manager::get_or_load_module(const iobuf& wasm_binary) {
    auto hash = compute_hash(wasm_binary);

    // Check cache
    if (auto it = _module_cache.find(hash); it != _module_cache.end()) {
        vlog(pool_log.debug, "Module cache hit for hash: {}", hash);
        it->second.last_access = std::chrono::steady_clock::now();
        it->second.instance_count++;
        _stats.cache_hits++;
        co_return &it->second;
    }

    vlog(pool_log.debug, "Module cache miss for hash: {}", hash);
    _stats.cache_misses++;

    // Load and compile module
    auto metadata = co_await load_module(wasm_binary);
    metadata.hash = hash;

    // Cache it
    _module_cache[hash] = std::move(metadata);
    _stats.total_modules = _module_cache.size();

    // Start background eviction if needed
    if (_module_cache.size() > _config.max_cached_modules) {
        co_await evict_least_recently_used();
    }

    co_return &_module_cache[hash];
}

ss::future<> module_cache_manager::preload_modules(
  const std::vector<iobuf>& modules) {
    if (!_config.enable_preloading) {
        co_return;
    }

    vlog(pool_log.info, "Preloading {} modules", modules.size());

    // Parallel preloading
    co_await ss::parallel_for_each(
      modules.begin(), modules.end(), [this](const auto& module) {
          return get_or_load_module(module).then([](auto*) {});
      });

    vlog(pool_log.info, "Preloading completed");
    co_return;
}

void module_cache_manager::increment_instance_count(module_hash hash) {
    if (auto it = _module_cache.find(hash); it != _module_cache.end()) {
        it->second.instance_count++;
        _stats.total_instances++;
    }
}

void module_cache_manager::decrement_instance_count(module_hash hash) {
    if (auto it = _module_cache.find(hash); it != _module_cache.end()) {
        if (it->second.instance_count > 0) {
            it->second.instance_count--;
            _stats.total_instances--;
        }
    }
}

ss::future<> module_cache_manager::evict_least_recently_used() {
    vlog(pool_log.debug, "Evicting LRU module");

    // Find LRU module with no active instances
    module_hash victim_hash = 0;
    std::chrono::steady_clock::time_point oldest_access
      = std::chrono::steady_clock::now();
    bool found_victim = false;

    for (const auto& [hash, meta] : _module_cache) {
        // Don't evict modules with active instances
        if (meta.instance_count > 0) {
            continue;
        }

        if (!found_victim || meta.last_access < oldest_access) {
            victim_hash = hash;
            oldest_access = meta.last_access;
            found_victim = true;
        }
    }

    if (found_victim) {
        vlog(pool_log.debug, "Evicting module with hash: {}", victim_hash);
        _module_cache.erase(victim_hash);
        _stats.evictions++;
        _stats.total_modules = _module_cache.size();
    }

    co_return;
}

bool module_cache_manager::is_cached(module_hash hash) const {
    return _module_cache.contains(hash);
}

module_hash module_cache_manager::compute_hash(const iobuf& binary) {
    incremental_xxhash64 hasher;
    for (const auto& frag : binary) {
        hasher.update(frag.get(), frag.size());
    }
    return hasher.digest();
}

ss::future<module_metadata>
module_cache_manager::load_module(const iobuf& binary) {
    vlog(pool_log.debug, "Loading module ({} bytes)", binary.size_bytes());

    // Extract metadata
    auto metadata = extract_metadata(binary);

    // In a real implementation, this would:
    // 1. Validate the WASM binary
    // 2. Compile it with Wasmtime
    // 3. Extract exports/imports

    co_return metadata;
}

module_metadata
module_cache_manager::extract_metadata(const iobuf& binary) {
    module_metadata meta;
    meta.size_bytes = binary.size_bytes();
    meta.last_access = std::chrono::steady_clock::now();
    meta.created_at = std::chrono::steady_clock::now();
    meta.instance_count = 0;

    // TODO: Parse WASM binary to extract exports
    return meta;
}

// Instance pool implementation

instance_pool::instance_pool(instance_pool_config config)
  : _config(std::move(config)) {}

instance_pool::~instance_pool() = default;

ss::future<> instance_pool::start() {
    vlog(
      pool_log.info,
      "Starting instance pool (max size: {}, min size: {})",
      _config.max_pool_size,
      _config.min_pool_size);
    _started = true;
    co_return;
}

ss::future<> instance_pool::stop() {
    vlog(
      pool_log.info,
      "Stopping instance pool ({} active instances)",
      _stats.total_instances);

    _started = false;

    // Cleanup all pools
    for (auto& [module_id, instances] : _pools) {
        for (auto& inst : instances) {
            // In a real implementation, we would destroy the instances
            // using Wasmtime API
        }
    }

    _pools.clear();
    _stats = {};

    vlog(pool_log.info, "Instance pool stopped");
    co_return;
}

ss::future<instance_handle>
instance_pool::acquire_instance(module_id module) {
    _stats.total_acquisitions++;

    // Try to get from pool
    if (auto it = _pools.find(module); it != _pools.end()) {
        for (auto& inst : it->second) {
            if (!inst.in_use) {
                vlog(
                  pool_log.debug,
                  "Reusing instance from pool for module {}",
                  module);

                inst.in_use = true;
                inst.last_used = std::chrono::steady_clock::now();
                inst.reuse_count++;

                _stats.cache_hits++;
                _stats.in_use_instances++;
                _stats.idle_instances--;

                // Reset instance state
                co_await reset_instance(inst);

                co_return instance_handle{
                  inst.instance,
                  inst.store,
                  [this, module, &inst]() { release_instance(module, inst); }};
            }
        }
    }

    vlog(pool_log.debug, "Creating new instance for module {}", module);
    _stats.cache_misses++;

    // Create new instance
    auto handle = co_await create_instance(module);

    co_return handle;
}

ss::future<instance_handle>
instance_pool::create_instance(module_id module) {
    // In a real implementation, this would:
    // 1. Get the compiled module
    // 2. Create a new Wasmtime instance
    // 3. Initialize it

    // For now, create a stub
    pooled_instance inst;
    inst.instance = nullptr; // Would be wasmtime_instance_t*
    inst.store = nullptr;    // Would be wasmtime_store_t*
    inst.in_use = true;
    inst.last_used = std::chrono::steady_clock::now();
    inst.created_at = std::chrono::steady_clock::now();

    // Add to pool
    _pools[module].push_back(inst);
    _stats.total_instances++;
    _stats.in_use_instances++;

    // Return handle with cleanup callback
    auto& added_inst = _pools[module].back();
    co_return instance_handle{
      added_inst.instance,
      added_inst.store,
      [this, module, &added_inst]() { release_instance(module, added_inst); }};
}

ss::future<>
instance_pool::prewarm_instances(module_id module, size_t count) {
    if (!_config.enable_prewarming) {
        co_return;
    }

    vlog(
      pool_log.info,
      "Prewarming {} instances for module {}",
      count,
      module);

    for (size_t i = 0; i < count; ++i) {
        auto handle = co_await create_instance(module);
        // Release immediately to put in pool
    }

    vlog(pool_log.info, "Prewarming completed for module {}", module);
    co_return;
}

ss::future<> instance_pool::cleanup_idle_instances() {
    vlog(pool_log.debug, "Cleaning up idle instances");

    auto now = std::chrono::steady_clock::now();
    size_t cleaned = 0;

    for (auto& [module_id, instances] : _pools) {
        auto new_end = std::remove_if(
          instances.begin(),
          instances.end(),
          [this, now, &cleaned](const auto& inst) {
              if (!inst.in_use
                  && (now - inst.last_used) > _config.idle_timeout) {
                  // Instance is idle and past timeout
                  cleaned++;
                  _stats.total_instances--;
                  _stats.idle_instances--;
                  return true;
              }
              return false;
          });

        instances.erase(new_end, instances.end());
    }

    vlog(pool_log.debug, "Cleaned up {} idle instances", cleaned);
    co_return;
}

ss::future<> instance_pool::reset_instance(pooled_instance& inst) {
    // In a real implementation, this would:
    // 1. Clear WASM linear memory
    // 2. Reset global variables
    // 3. Reset function tables

    vlog(pool_log.trace, "Resetting instance state");
    co_return;
}

void instance_pool::release_instance(module_id module, pooled_instance& inst) {
    vlog(pool_log.trace, "Releasing instance for module {}", module);

    inst.in_use = false;
    inst.last_used = std::chrono::steady_clock::now();

    _stats.in_use_instances--;
    _stats.idle_instances++;

    // Schedule cleanup if pool is too large
    if (should_cleanup_pool()) {
        // In a real implementation, trigger background cleanup
    }
}

bool instance_pool::should_cleanup_pool() const {
    return _stats.total_instances > _config.max_pool_size;
}

} // namespace wasm
