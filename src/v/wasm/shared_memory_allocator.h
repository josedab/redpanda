// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/flat_hash_map.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace wasm {

/// Memory protection flags
enum class protection_flags {
    /// Read-only memory
    read_only,
    /// Read-write memory
    read_write,
    /// Copy-on-write memory
    copy_on_write,
    /// Execute-only memory
    execute_only,
    /// Read and execute memory
    read_execute,
};

/// Shared memory region that can be used across multiple WASM instances
struct memory_region {
    /// Base address of the memory region
    void* base_address{nullptr};

    /// Size of the memory region in bytes
    size_t size{0};

    /// Protection flags for the memory region
    protection_flags protection{protection_flags::read_write};

    /// Reference count for the memory region
    std::atomic<size_t> ref_count{0};

    /// Name/identifier for the memory region
    std::string name;

    /// Whether the region is currently mapped
    bool is_mapped{false};

    memory_region() = default;

    memory_region(
      void* addr,
      size_t sz,
      protection_flags prot,
      std::string_view n = "")
      : base_address(addr)
      , size(sz)
      , protection(prot)
      , ref_count(0)
      , name(n)
      , is_mapped(true) {}

    ~memory_region() = default;

    // Non-copyable, moveable
    memory_region(const memory_region&) = delete;
    memory_region& operator=(const memory_region&) = delete;
    memory_region(memory_region&&) = default;
    memory_region& operator=(memory_region&&) = default;
};

/// Allocator for shared memory regions
class shared_memory_allocator {
public:
    shared_memory_allocator();
    ~shared_memory_allocator();

    /// Start the allocator
    ss::future<> start();

    /// Stop the allocator and free all regions
    ss::future<> stop();

    /// Allocate a shared memory region
    ss::future<ss::lw_shared_ptr<memory_region>>
    allocate_shared_region(size_t size, bool readonly = false);

    /// Create a copy-on-write mapping of an existing region
    ss::future<ss::lw_shared_ptr<memory_region>>
    create_cow_mapping(ss::lw_shared_ptr<memory_region> source);

    /// Deallocate a shared memory region
    ss::future<> deallocate_region(ss::lw_shared_ptr<memory_region> region);

    /// Share constant data across engine instances
    ss::future<ss::lw_shared_ptr<memory_region>>
    share_constant_data(const std::vector<uint8_t>& data, std::string_view name);

    /// Get a shared constant region by name
    ss::future<ss::lw_shared_ptr<memory_region>>
      get_shared_constant(std::string_view name);

    /// Check if constant data is already shared
    bool has_shared_constant(std::string_view name) const;

    /// Get total allocated memory
    size_t total_allocated() const { return _total_allocated; }

    /// Get number of active regions
    size_t active_regions() const { return _allocated_regions.size(); }

    /// Get statistics
    struct stats {
        size_t total_allocated_bytes;
        size_t total_regions;
        size_t shared_constants;
        size_t cow_mappings;
    };
    stats get_stats() const;

private:
    /// Align size up to page boundary
    static constexpr size_t align_to_page(size_t size) {
        return (size + _page_size - 1) & ~(_page_size - 1);
    }

    /// Check if a section name indicates executable code
    bool is_executable_section(std::string_view name) const;

    /// Allocate memory using mmap
    ss::future<void*>
    mmap_allocate(size_t size, protection_flags protection);

    /// Deallocate memory using munmap
    ss::future<> mmap_deallocate(void* addr, size_t size);

    /// Change memory protection
    ss::future<>
    change_protection(void* addr, size_t size, protection_flags protection);

    static constexpr size_t _page_size = 4096;

    size_t _total_allocated{0};
    size_t _total_cow_mappings{0};

    /// Map of allocated regions by base address
    absl::flat_hash_map<void*, ss::lw_shared_ptr<memory_region>>
      _allocated_regions;

    /// Map of shared constant regions by name
    absl::flat_hash_map<std::string, ss::lw_shared_ptr<memory_region>>
      _shared_constants;

    bool _started{false};
};

} // namespace wasm
