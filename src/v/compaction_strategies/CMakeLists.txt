# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# Advanced Compaction Strategies Library
v_cc_library(
  NAME compaction_strategies
  SRCS
    base/metrics.cc
    workload_aware/scheduler.cc
    incremental/engine.cc
    hybrid/strategy.cc
    cloud_native/compaction.cc
    adaptive/strategy.cc
  DEPS
    v::model
    v::storage
    v::compaction
    Seastar::seastar
)

# Tests
v_cc_library(
  NAME compaction_strategies_test_utils
  SRCS
  DEPS
    v::compaction_strategies
    Seastar::seastar
)

add_subdirectory(tests)
