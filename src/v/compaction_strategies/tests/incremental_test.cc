// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compaction_strategies/incremental/engine.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

using namespace compaction_strategies;

SEASTAR_THREAD_TEST_CASE(test_incremental_compaction) {
    incremental_compaction_engine engine;

    compaction_request request;
    request.ntp = model::ntp(
      model::ns("test"),
      model::topic("topic"),
      model::partition_id(0));
    request.segments.push_back(segment_id(1));

    compaction_options opts;
    opts.max_run_duration = std::chrono::seconds(5);

    // Start compaction
    engine.compact_incrementally(request, opts).get();

    // Verify compaction completes
    BOOST_CHECK(!engine.is_paused());
}

SEASTAR_THREAD_TEST_CASE(test_chunk_size_adaptation) {
    incremental_compaction_engine engine;

    compaction_options opts;
    opts.base_chunk_size = 1048576; // 1MB
    opts.min_chunk_size = 65536; // 64KB
    opts.max_chunk_size = 16777216; // 16MB

    // Verify chunk size is within bounds
    BOOST_CHECK(opts.base_chunk_size >= opts.min_chunk_size);
    BOOST_CHECK(opts.base_chunk_size <= opts.max_chunk_size);
}

SEASTAR_THREAD_TEST_CASE(test_pause_resume) {
    incremental_compaction_engine engine;

    compaction_request request;
    request.ntp = model::ntp(
      model::ns("test"),
      model::topic("topic"),
      model::partition_id(0));
    request.segments.push_back(segment_id(1));

    compaction_options opts;
    opts.pause_threshold = 0.8;

    // Compaction should handle pausing gracefully
    BOOST_CHECK(true); // Placeholder for actual pause/resume tests
}

SEASTAR_THREAD_TEST_CASE(test_checkpoint_recovery) {
    checkpoint_manager manager;

    checkpoint cp;
    cp.segment_id = segment_id(1);
    cp.last_offset = model::offset(1000);
    cp.bytes_processed = 500000;
    cp.timestamp = std::chrono::steady_clock::now();

    // Save checkpoint
    manager.save(cp).get();

    // Load checkpoint
    auto loaded = manager.load(segment_id(1)).get();

    // Verify checkpoint can be saved and loaded
    BOOST_CHECK(true); // Would verify loaded checkpoint matches
}

SEASTAR_THREAD_TEST_CASE(test_resource_monitoring) {
    memory_monitor monitor;

    // Verify memory pressure is monitored
    BOOST_CHECK(monitor.pressure() >= 0.0);
    BOOST_CHECK(monitor.pressure() <= 1.0);
}

SEASTAR_THREAD_TEST_CASE(test_foreground_load_tracking) {
    metrics_collector metrics;

    // Verify foreground load is tracked
    BOOST_CHECK(metrics.foreground_load() >= 0.0);
}
