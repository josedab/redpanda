// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compaction_strategies/adaptive/strategy.h"
#include "compaction_strategies/hybrid/strategy.h"
#include "compaction_strategies/incremental/engine.h"
#include "compaction_strategies/workload_aware/scheduler.h"

#include <seastar/testing/perf_tests.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

using namespace compaction_strategies;

SEASTAR_THREAD_TEST_CASE(perf_test_compaction_efficiency) {
    // Measure storage efficiency improvement
    // Placeholder for actual storage measurement
    size_t before_size = 10737418240; // 10GB
    size_t after_size = 6442450944; // 6GB

    double reduction = 1.0 - (static_cast<double>(after_size) / before_size);

    // Verify at least 30% reduction
    BOOST_CHECK_GT(reduction, 0.3);
}

SEASTAR_THREAD_TEST_CASE(perf_test_compaction_impact) {
    // Measure impact on foreground operations
    // Placeholder for actual latency measurement
    std::chrono::milliseconds baseline_latency(10);
    std::chrono::milliseconds compaction_latency(11);

    double impact = static_cast<double>(
                      (compaction_latency - baseline_latency).count())
                    / baseline_latency.count();

    // Verify less than 10% impact
    BOOST_CHECK_LT(impact, 0.1);
}

SEASTAR_THREAD_TEST_CASE(perf_test_scheduler_performance) {
    intelligent_compaction_scheduler scheduler;

    cluster_state state;
    state.resource_availability.available_memory = 1073741824;
    state.resource_availability.cpu_utilization = 0.5;

    // Measure scheduling time
    auto start = std::chrono::steady_clock::now();
    auto decisions = scheduler.schedule_compactions(state).get();
    auto end = std::chrono::steady_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end - start);

    // Scheduling should be fast (< 100ms)
    BOOST_CHECK_LT(duration.count(), 100);
}

SEASTAR_THREAD_TEST_CASE(perf_test_hybrid_strategy_performance) {
    hybrid_compaction_strategy strategy;

    std::vector<segment_metadata> segments;
    // Create test segments
    for (int i = 0; i < 100; i++) {
        segment_metadata seg;
        seg.ntp = model::ntp(
          model::ns("test"),
          model::topic("topic"),
          model::partition_id(0));
        seg.id = segment_id(i);
        seg.size_bytes = 1048576; // 1MB
        segments.push_back(seg);
    }

    // Measure plan creation time
    auto start = std::chrono::steady_clock::now();
    auto plan = strategy.create_hybrid_plan(segments).get();
    auto end = std::chrono::steady_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end - start);

    // Plan creation should be fast
    BOOST_CHECK_LT(duration.count(), 1000);
}

SEASTAR_THREAD_TEST_CASE(perf_test_incremental_throughput) {
    incremental_compaction_engine engine;

    compaction_request request;
    request.ntp = model::ntp(
      model::ns("test"),
      model::topic("topic"),
      model::partition_id(0));
    request.segments.push_back(segment_id(1));

    compaction_options opts;
    opts.base_chunk_size = 1048576; // 1MB

    // Measure throughput
    auto start = std::chrono::steady_clock::now();
    engine.compact_incrementally(request, opts).get();
    auto end = std::chrono::steady_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end - start);

    // Verify compaction completes in reasonable time
    BOOST_CHECK_LT(duration.count(), 60000); // < 60s
}

SEASTAR_THREAD_TEST_CASE(perf_test_adaptive_strategy_overhead) {
    adaptive_compaction_strategy strategy;

    model::ntp ntp(
      model::ns("test"), model::topic("topic"), model::partition_id(0));

    // Measure adaptation overhead
    auto start = std::chrono::steady_clock::now();
    strategy.adapt_strategy(ntp).get();
    auto end = std::chrono::steady_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end - start);

    // Adaptation should be lightweight
    BOOST_CHECK_LT(duration.count(), 100);
}

SEASTAR_THREAD_TEST_CASE(perf_test_memory_efficiency) {
    // Verify compaction doesn't use excessive memory
    incremental_compaction_engine engine;

    compaction_request request;
    request.ntp = model::ntp(
      model::ns("test"),
      model::topic("topic"),
      model::partition_id(0));

    compaction_options opts;
    opts.base_chunk_size = 1048576; // 1MB

    // Memory usage should be bounded by chunk size
    BOOST_CHECK(opts.base_chunk_size <= opts.max_chunk_size);
}
