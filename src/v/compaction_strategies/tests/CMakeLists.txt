# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# Unit tests
rp_test(
  UNIT_TEST
  BINARY_NAME compaction_strategies_scheduler_test
  SOURCES scheduler_test.cc
  LIBRARIES
    v::compaction_strategies
    v::seastar_testing_main
  LABELS compaction_strategies
)

rp_test(
  UNIT_TEST
  BINARY_NAME compaction_strategies_incremental_test
  SOURCES incremental_test.cc
  LIBRARIES
    v::compaction_strategies
    v::seastar_testing_main
  LABELS compaction_strategies
)

# Performance benchmarks
rp_test(
  BENCHMARK_TEST
  BINARY_NAME compaction_strategies_benchmark
  SOURCES benchmark_test.cc
  LIBRARIES
    v::compaction_strategies
    v::seastar_testing_main
  LABELS compaction_strategies
)
