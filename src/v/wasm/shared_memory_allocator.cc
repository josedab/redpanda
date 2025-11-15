// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "wasm/shared_memory_allocator.h"

#include "vlog.h"

#include <seastar/core/seastar.hh>

#include <sys/mman.h>

#include <cstring>
#include <stdexcept>

namespace wasm {

static ss::logger shmem_log("wasm_shared_memory");

shared_memory_allocator::shared_memory_allocator() = default;

shared_memory_allocator::~shared_memory_allocator() {
    if (_started) {
        // Clean up any remaining regions
        for (auto& [addr, region] : _allocated_regions) {
            if (region->is_mapped) {
                ::munmap(region->base_address, region->size);
            }
        }
    }
}

ss::future<> shared_memory_allocator::start() {
    vlog(shmem_log.info, "Starting shared memory allocator");
    _started = true;
    co_return;
}

ss::future<> shared_memory_allocator::stop() {
    vlog(
      shmem_log.info,
      "Stopping shared memory allocator, {} active regions",
      _allocated_regions.size());

    // Deallocate all regions
    for (auto& [addr, region] : _allocated_regions) {
        if (region->is_mapped) {
            co_await mmap_deallocate(region->base_address, region->size);
            region->is_mapped = false;
        }
    }

    _allocated_regions.clear();
    _shared_constants.clear();
    _total_allocated = 0;
    _started = false;

    vlog(shmem_log.info, "Shared memory allocator stopped");
    co_return;
}

ss::future<ss::lw_shared_ptr<memory_region>>
shared_memory_allocator::allocate_shared_region(size_t size, bool readonly) {
    // Round up to page size
    size = align_to_page(size);

    vlog(
      shmem_log.debug,
      "Allocating shared region: {} bytes (readonly: {})",
      size,
      readonly);

    // Determine protection flags
    auto protection = readonly ? protection_flags::read_only
                               : protection_flags::read_write;

    // Allocate memory
    void* addr = co_await mmap_allocate(size, protection);

    // Create memory region
    auto region = ss::make_lw_shared<memory_region>(
      addr, size, protection, "shared_region");

    region->ref_count = 1;

    // Track allocation
    _allocated_regions[addr] = region;
    _total_allocated += size;

    vlog(
      shmem_log.debug,
      "Allocated shared region at {}, total allocated: {} bytes",
      addr,
      _total_allocated);

    co_return region;
}

ss::future<ss::lw_shared_ptr<memory_region>>
shared_memory_allocator::create_cow_mapping(
  ss::lw_shared_ptr<memory_region> source) {
    vlog(
      shmem_log.debug,
      "Creating COW mapping from region at {}",
      source->base_address);

    // Allocate new region
    void* addr = co_await mmap_allocate(
      source->size, protection_flags::copy_on_write);

    // Copy initial content from source
    std::memcpy(addr, source->base_address, source->size);

    // Create new region with COW protection
    auto cow_region = ss::make_lw_shared<memory_region>(
      addr, source->size, protection_flags::copy_on_write, "cow_mapping");

    cow_region->ref_count = 1;

    // Track allocation
    _allocated_regions[addr] = cow_region;
    _total_allocated += source->size;
    _total_cow_mappings++;

    vlog(
      shmem_log.debug,
      "Created COW mapping at {}, total COW mappings: {}",
      addr,
      _total_cow_mappings);

    co_return cow_region;
}

ss::future<> shared_memory_allocator::deallocate_region(
  ss::lw_shared_ptr<memory_region> region) {
    if (!region || !region->is_mapped) {
        co_return;
    }

    vlog(
      shmem_log.debug,
      "Deallocating region at {} ({} bytes)",
      region->base_address,
      region->size);

    // Decrement ref count
    auto prev_count = region->ref_count.fetch_sub(1);
    if (prev_count > 1) {
        // Other references still exist
        vlog(
          shmem_log.debug,
          "Region still has {} references",
          prev_count - 1);
        co_return;
    }

    // Last reference, actually deallocate
    co_await mmap_deallocate(region->base_address, region->size);

    _total_allocated -= region->size;
    _allocated_regions.erase(region->base_address);

    region->is_mapped = false;

    vlog(
      shmem_log.debug,
      "Deallocated region, total allocated: {} bytes",
      _total_allocated);

    co_return;
}

ss::future<ss::lw_shared_ptr<memory_region>>
shared_memory_allocator::share_constant_data(
  const std::vector<uint8_t>& data, std::string_view name) {
    // Check if already shared
    if (auto it = _shared_constants.find(std::string(name));
        it != _shared_constants.end()) {
        vlog(
          shmem_log.debug,
          "Using existing shared constant: {}",
          name);
        it->second->ref_count++;
        co_return it->second;
    }

    vlog(
      shmem_log.debug,
      "Sharing constant data: {} ({} bytes)",
      name,
      data.size());

    // Allocate readonly region
    auto region = co_await allocate_shared_region(data.size(), true);
    region->name = name;

    // Copy data to shared region (before making it readonly)
    std::memcpy(region->base_address, data.data(), data.size());

    // Make region executable if it's code
    if (is_executable_section(name)) {
        vlog(shmem_log.debug, "Making region executable: {}", name);
        co_await change_protection(
          region->base_address, region->size, protection_flags::read_execute);
        region->protection = protection_flags::read_execute;
    }

    // Register for sharing
    _shared_constants[std::string(name)] = region;

    co_return region;
}

ss::future<ss::lw_shared_ptr<memory_region>>
shared_memory_allocator::get_shared_constant(std::string_view name) {
    if (auto it = _shared_constants.find(std::string(name));
        it != _shared_constants.end()) {
        it->second->ref_count++;
        co_return it->second;
    }

    co_return nullptr;
}

bool shared_memory_allocator::has_shared_constant(std::string_view name)
  const {
    return _shared_constants.contains(std::string(name));
}

shared_memory_allocator::stats shared_memory_allocator::get_stats() const {
    return {
      .total_allocated_bytes = _total_allocated,
      .total_regions = _allocated_regions.size(),
      .shared_constants = _shared_constants.size(),
      .cow_mappings = _total_cow_mappings,
    };
}

bool shared_memory_allocator::is_executable_section(std::string_view name)
  const {
    // Check if name indicates code section
    return name.find("code") != std::string_view::npos
           || name.find("text") != std::string_view::npos
           || name.find(".so") != std::string_view::npos;
}

ss::future<void*> shared_memory_allocator::mmap_allocate(
  size_t size, protection_flags protection) {
    // Convert protection flags to mmap flags
    int prot = 0;
    int flags = MAP_PRIVATE | MAP_ANONYMOUS;

    switch (protection) {
    case protection_flags::read_only:
        prot = PROT_READ;
        flags = MAP_SHARED | MAP_ANONYMOUS;
        break;
    case protection_flags::read_write:
        prot = PROT_READ | PROT_WRITE;
        flags = MAP_PRIVATE | MAP_ANONYMOUS;
        break;
    case protection_flags::copy_on_write:
        prot = PROT_READ | PROT_WRITE;
        flags = MAP_PRIVATE | MAP_ANONYMOUS;
        break;
    case protection_flags::execute_only:
        prot = PROT_EXEC;
        break;
    case protection_flags::read_execute:
        prot = PROT_READ | PROT_EXEC;
        flags = MAP_SHARED | MAP_ANONYMOUS;
        break;
    }

    // Allocate memory
    void* addr = ::mmap(nullptr, size, prot, flags, -1, 0);

    if (addr == MAP_FAILED) {
        throw std::bad_alloc();
    }

    co_return addr;
}

ss::future<>
shared_memory_allocator::mmap_deallocate(void* addr, size_t size) {
    if (addr && size > 0) {
        ::munmap(addr, size);
    }
    co_return;
}

ss::future<> shared_memory_allocator::change_protection(
  void* addr, size_t size, protection_flags protection) {
    int prot = 0;

    switch (protection) {
    case protection_flags::read_only:
        prot = PROT_READ;
        break;
    case protection_flags::read_write:
        prot = PROT_READ | PROT_WRITE;
        break;
    case protection_flags::copy_on_write:
        prot = PROT_READ | PROT_WRITE;
        break;
    case protection_flags::execute_only:
        prot = PROT_EXEC;
        break;
    case protection_flags::read_execute:
        prot = PROT_READ | PROT_EXEC;
        break;
    }

    if (::mprotect(addr, size, prot) != 0) {
        throw std::runtime_error("Failed to change memory protection");
    }

    co_return;
}

} // namespace wasm
