// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "wasm/aot_compiler.h"

#include <seastar/core/future.hh>

#include <absl/container/flat_hash_map.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace wasm {

/// JIT compilation tiers
enum class tier {
    /// No compilation, interpretation only
    interpreter = 0,
    /// Quick baseline JIT compilation
    baseline_jit = 1,
    /// Full optimizing JIT compilation
    optimizing_jit = 2
};

/// Hot path information for profiling
struct hot_path_info {
    /// Function name
    std::string function_name;

    /// Number of times this path was executed
    size_t execution_count{0};

    /// Total time spent in this path
    std::chrono::nanoseconds total_time{0};

    /// Average time per execution
    std::chrono::nanoseconds avg_time{0};
};

/// Execution statistics for a function
struct execution_stats {
    /// Number of invocations
    size_t invocation_count{0};

    /// Total execution time
    std::chrono::nanoseconds total_execution_time{0};

    /// Last execution time
    std::chrono::nanoseconds last_execution_time{0};

    /// Hot paths within the function
    std::vector<hot_path_info> hot_paths;

    /// Current tier
    tier current_tier{tier::interpreter};

    /// Last tier-up timestamp
    std::chrono::steady_clock::time_point last_tier_up;
};

/// Configuration for JIT tiering
struct jit_tiering_config {
    /// Enable JIT tiering
    bool enabled = true;

    /// Invocation threshold for baseline JIT
    size_t baseline_threshold = 10;

    /// Invocation threshold for optimizing JIT
    size_t optimizing_threshold = 100;

    /// Minimum execution time to trigger optimization (microseconds)
    std::chrono::microseconds optimization_threshold_time{1000};

    /// CPU core to use for background compilation
    unsigned compilation_cpu = 0; // Will be set to last CPU

    /// Enable profiling for PGO
    bool enable_profiling = true;

    /// Maximum number of functions to track
    size_t max_tracked_functions = 1000;
};

/// Function identifier
using function_id = uint64_t;

/// Compiled function at a specific tier
struct compiled_function {
    /// Compiled module
    compiled_module module;

    /// Tier this was compiled at
    tier compilation_tier;

    /// Compilation timestamp
    std::chrono::steady_clock::time_point compiled_at;
};

/// JIT tiering engine that adaptively optimizes hot functions
class jit_tiering_engine {
public:
    explicit jit_tiering_engine(jit_tiering_config config);

    ~jit_tiering_engine();

    /// Start the tiering engine
    ss::future<> start();

    /// Stop the tiering engine
    ss::future<> stop();

    /// Record a function execution
    ss::future<> record_execution(
      function_id func_id,
      std::chrono::nanoseconds execution_time);

    /// Get current tier for a function
    tier get_current_tier(function_id func_id) const;

    /// Get execution statistics for a function
    const execution_stats* get_stats(function_id func_id) const;

    /// Check if a function should be tiered up
    bool should_tier_up(function_id func_id) const;

    /// Tier up a function to the next level
    ss::future<> tier_up_function(function_id func_id);

    /// Collect profiling data for a function
    ss::future<> collect_profile_data(
      function_id func_id, const std::vector<hot_path_info>& hot_paths);

    /// Get profiling data for PGO
    ss::future<std::optional<profile_data>>
      get_profile_data(function_id func_id);

    /// Get statistics
    struct stats {
        size_t total_functions;
        size_t interpreter_functions;
        size_t baseline_jit_functions;
        size_t optimizing_jit_functions;
        size_t total_tier_ups;
    };
    stats get_stats() const { return _stats; }

private:
    /// Determine the appropriate tier for a function
    tier determine_tier(const execution_stats& stats) const;

    /// Check if a function would benefit from optimization
    bool would_benefit_from_optimization(const execution_stats& stats) const;

    /// Compile a function at baseline JIT tier
    ss::future<> compile_baseline_jit(function_id func_id);

    /// Compile a function at optimizing JIT tier
    ss::future<> compile_optimizing_jit(function_id func_id);

    jit_tiering_config _config;
    aot_compiler _compiler;

    /// Function execution statistics
    absl::flat_hash_map<function_id, execution_stats> _function_stats;

    /// Compiled functions at different tiers
    absl::flat_hash_map<function_id, absl::flat_hash_map<tier, compiled_function>>
      _compiled_functions;

    /// Profile data for PGO
    absl::flat_hash_map<function_id, profile_data> _profile_data;

    /// Engine statistics
    stats _stats{};

    bool _started{false};
};

} // namespace wasm
