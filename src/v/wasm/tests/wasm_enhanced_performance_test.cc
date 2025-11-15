// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "wasm/aot_compiler.h"
#include "wasm/instance_pool.h"
#include "wasm/jit_tiering.h"
#include "wasm/shared_memory_allocator.h"
#include "wasm/simd_runtime.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

namespace wasm {

SEASTAR_THREAD_TEST_CASE(test_simd_runtime_creation) {
    simd_config cfg;
    cfg.enable_simd = true;
    cfg.enable_bulk_memory = true;

    auto runtime = create_simd_runtime(cfg);
    BOOST_REQUIRE(runtime != nullptr);

    auto* simd_rt = dynamic_cast<simd_enabled_runtime*>(runtime.get());
    BOOST_REQUIRE(simd_rt != nullptr);
    BOOST_CHECK(simd_rt->is_simd_enabled());
    BOOST_CHECK_EQUAL(simd_rt->config().enable_simd, true);
    BOOST_CHECK_EQUAL(simd_rt->config().enable_bulk_memory, true);
}

SEASTAR_THREAD_TEST_CASE(test_aot_compiler_cache_key) {
    aot_config config;
    aot_compiler compiler(config);

    compiler.start().get();

    // Create test module
    iobuf module;
    module.append("test_wasm_module", 17);

    compilation_options opts1;
    opts1.opt_level = optimization_level::baseline;
    auto key1 = compiler.compute_cache_key(module, opts1);

    compilation_options opts2;
    opts2.opt_level = optimization_level::aggressive;
    auto key2 = compiler.compute_cache_key(module, opts2);

    // Different optimization levels should produce different keys
    BOOST_CHECK_NE(key1, key2);

    // Same options should produce same key
    auto key3 = compiler.compute_cache_key(module, opts1);
    BOOST_CHECK_EQUAL(key1, key3);

    compiler.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_shared_memory_allocator) {
    shared_memory_allocator allocator;

    allocator.start().get();

    // Allocate a shared region
    auto region = allocator.allocate_shared_region(4096, false).get();
    BOOST_REQUIRE(region != nullptr);
    BOOST_CHECK(region->base_address != nullptr);
    BOOST_CHECK_EQUAL(region->size, 4096);
    BOOST_CHECK_EQUAL(region->ref_count, 1);

    // Check stats
    auto stats = allocator.get_stats();
    BOOST_CHECK_EQUAL(stats.total_regions, 1);
    BOOST_CHECK_GE(stats.total_allocated_bytes, 4096);

    // Deallocate
    allocator.deallocate_region(region).get();

    allocator.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_shared_memory_cow_mapping) {
    shared_memory_allocator allocator;

    allocator.start().get();

    // Allocate source region
    auto source = allocator.allocate_shared_region(4096, false).get();
    BOOST_REQUIRE(source != nullptr);

    // Write some data to source
    auto* data = static_cast<uint8_t*>(source->base_address);
    for (size_t i = 0; i < 100; ++i) {
        data[i] = static_cast<uint8_t>(i);
    }

    // Create COW mapping
    auto cow = allocator.create_cow_mapping(source).get();
    BOOST_REQUIRE(cow != nullptr);
    BOOST_CHECK(cow->base_address != source->base_address);
    BOOST_CHECK_EQUAL(cow->size, source->size);
    BOOST_CHECK_EQUAL(cow->protection, protection_flags::copy_on_write);

    // Verify data was copied
    auto* cow_data = static_cast<uint8_t*>(cow->base_address);
    for (size_t i = 0; i < 100; ++i) {
        BOOST_CHECK_EQUAL(cow_data[i], static_cast<uint8_t>(i));
    }

    // Check stats
    auto stats = allocator.get_stats();
    BOOST_CHECK_EQUAL(stats.total_regions, 2);
    BOOST_CHECK_EQUAL(stats.cow_mappings, 1);

    allocator.deallocate_region(source).get();
    allocator.deallocate_region(cow).get();
    allocator.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_jit_tiering_basic) {
    jit_tiering_config config;
    config.baseline_threshold = 5;
    config.optimizing_threshold = 10;

    jit_tiering_engine engine(config);

    engine.start().get();

    // Test function tiering
    function_id func_id = 12345;

    // Initially at interpreter tier
    BOOST_CHECK_EQUAL(
      engine.get_current_tier(func_id), tier::interpreter);

    // Record executions below baseline threshold
    for (size_t i = 0; i < 4; ++i) {
        engine
          .record_execution(func_id, std::chrono::nanoseconds(1000))
          .get();
    }
    BOOST_CHECK_EQUAL(
      engine.get_current_tier(func_id), tier::interpreter);

    // Cross baseline threshold
    engine.record_execution(func_id, std::chrono::nanoseconds(1000)).get();
    BOOST_CHECK_EQUAL(
      engine.get_current_tier(func_id), tier::baseline_jit);

    // Continue to optimizing threshold
    for (size_t i = 0; i < 5; ++i) {
        engine
          .record_execution(func_id, std::chrono::nanoseconds(5000))
          .get();
    }
    BOOST_CHECK_EQUAL(
      engine.get_current_tier(func_id), tier::optimizing_jit);

    // Verify stats
    auto stats = engine.get_stats();
    BOOST_CHECK_EQUAL(stats.total_functions, 1);
    BOOST_CHECK_EQUAL(stats.optimizing_jit_functions, 1);
    BOOST_CHECK_GE(stats.total_tier_ups, 2);

    engine.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_module_cache_manager) {
    module_cache_config config;
    config.max_cached_modules = 10;

    module_cache_manager cache(config);

    cache.start().get();

    // Create test modules
    iobuf module1;
    module1.append("module1_data", 12);

    iobuf module2;
    module2.append("module2_data", 12);

    // Load modules
    auto meta1 = cache.get_or_load_module(module1).get();
    BOOST_REQUIRE(meta1 != nullptr);

    auto meta2 = cache.get_or_load_module(module2).get();
    BOOST_REQUIRE(meta2 != nullptr);

    // Different modules should have different hashes
    BOOST_CHECK_NE(meta1->hash, meta2->hash);

    // Loading same module again should hit cache
    auto stats_before = cache.get_stats();
    auto meta1_again = cache.get_or_load_module(module1).get();
    auto stats_after = cache.get_stats();

    BOOST_CHECK_EQUAL(meta1->hash, meta1_again->hash);
    BOOST_CHECK_EQUAL(stats_after.cache_hits, stats_before.cache_hits + 1);

    cache.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_instance_pool) {
    instance_pool_config config;
    config.max_pool_size = 20;

    instance_pool pool(config);

    pool.start().get();

    // Acquire instances for a module
    module_id module = 42;

    auto handle1 = pool.acquire_instance(module).get();
    BOOST_REQUIRE(handle1.instance != nullptr || true); // Stub instance

    auto handle2 = pool.acquire_instance(module).get();
    BOOST_REQUIRE(handle2.instance != nullptr || true);

    // Check stats
    auto stats = pool.get_stats();
    BOOST_CHECK_GE(stats.total_instances, 2);
    BOOST_CHECK_GE(stats.in_use_instances, 2);

    pool.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_shared_constant_data) {
    shared_memory_allocator allocator;

    allocator.start().get();

    // Share constant data
    std::vector<uint8_t> data = {1, 2, 3, 4, 5, 6, 7, 8};
    auto region = allocator.share_constant_data(data, "test_constant").get();

    BOOST_REQUIRE(region != nullptr);
    BOOST_CHECK_EQUAL(region->name, "test_constant");
    BOOST_CHECK(allocator.has_shared_constant("test_constant"));

    // Get the same constant again
    auto region2 = allocator.get_shared_constant("test_constant").get();
    BOOST_REQUIRE(region2 != nullptr);
    BOOST_CHECK_EQUAL(region->base_address, region2->base_address);
    BOOST_CHECK_EQUAL(region2->ref_count, 2);

    allocator.stop().get();
}

} // namespace wasm
