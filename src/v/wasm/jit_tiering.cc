// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "wasm/jit_tiering.h"

#include "vlog.h"

#include <seastar/core/smp.hh>

namespace wasm {

static ss::logger jit_log("wasm_jit_tiering");

jit_tiering_engine::jit_tiering_engine(jit_tiering_config config)
  : _config(std::move(config))
  , _compiler(aot_config{
      .enable_pgo = _config.enable_profiling,
      .opt_level = optimization_level::aggressive,
    }) {
    // Set compilation CPU to last core if not specified
    if (_config.compilation_cpu == 0) {
        _config.compilation_cpu = ss::smp::count > 1 ? ss::smp::count - 1 : 0;
    }
}

jit_tiering_engine::~jit_tiering_engine() = default;

ss::future<> jit_tiering_engine::start() {
    vlog(
      jit_log.info,
      "Starting JIT tiering engine (baseline threshold: {}, optimizing "
      "threshold: {})",
      _config.baseline_threshold,
      _config.optimizing_threshold);

    co_await _compiler.start();
    _started = true;

    vlog(jit_log.info, "JIT tiering engine started");
    co_return;
}

ss::future<> jit_tiering_engine::stop() {
    vlog(jit_log.info, "Stopping JIT tiering engine");

    _started = false;
    co_await _compiler.stop();

    _function_stats.clear();
    _compiled_functions.clear();
    _profile_data.clear();

    vlog(jit_log.info, "JIT tiering engine stopped");
    co_return;
}

ss::future<> jit_tiering_engine::record_execution(
  function_id func_id, std::chrono::nanoseconds execution_time) {
    auto& stats = _function_stats[func_id];
    stats.invocation_count++;
    stats.last_execution_time = execution_time;
    stats.total_execution_time += execution_time;

    // Update tier statistics
    _stats.total_functions = _function_stats.size();

    // Determine if we should tier up
    if (should_tier_up(func_id)) {
        vlog(
          jit_log.debug,
          "Function {} ready for tier-up (invocations: {})",
          func_id,
          stats.invocation_count);
        co_await tier_up_function(func_id);
    }

    co_return;
}

tier jit_tiering_engine::get_current_tier(function_id func_id) const {
    if (auto it = _function_stats.find(func_id); it != _function_stats.end()) {
        return it->second.current_tier;
    }
    return tier::interpreter;
}

const execution_stats* jit_tiering_engine::get_stats(function_id func_id)
  const {
    if (auto it = _function_stats.find(func_id); it != _function_stats.end()) {
        return &it->second;
    }
    return nullptr;
}

bool jit_tiering_engine::should_tier_up(function_id func_id) const {
    if (!_config.enabled) {
        return false;
    }

    auto it = _function_stats.find(func_id);
    if (it == _function_stats.end()) {
        return false;
    }

    const auto& stats = it->second;
    auto current_tier = stats.current_tier;

    switch (current_tier) {
    case tier::interpreter:
        return stats.invocation_count >= _config.baseline_threshold;

    case tier::baseline_jit:
        // Tier up if function is hot and would benefit
        return stats.invocation_count >= _config.optimizing_threshold
               && would_benefit_from_optimization(stats);

    case tier::optimizing_jit:
        return false; // Already at highest tier
    }

    return false;
}

ss::future<> jit_tiering_engine::tier_up_function(function_id func_id) {
    auto it = _function_stats.find(func_id);
    if (it == _function_stats.end()) {
        co_return;
    }

    auto& stats = it->second;
    auto from_tier = stats.current_tier;

    vlog(
      jit_log.info,
      "Tiering up function {} from tier {}",
      func_id,
      static_cast<int>(from_tier));

    switch (from_tier) {
    case tier::interpreter:
        co_await compile_baseline_jit(func_id);
        stats.current_tier = tier::baseline_jit;
        _stats.baseline_jit_functions++;
        break;

    case tier::baseline_jit:
        co_await compile_optimizing_jit(func_id);
        stats.current_tier = tier::optimizing_jit;
        _stats.optimizing_jit_functions++;
        _stats.baseline_jit_functions--;
        break;

    case tier::optimizing_jit:
        // Already at highest tier
        break;
    }

    stats.last_tier_up = std::chrono::steady_clock::now();
    _stats.total_tier_ups++;

    vlog(
      jit_log.info,
      "Function {} tiered up to tier {}",
      func_id,
      static_cast<int>(stats.current_tier));

    co_return;
}

ss::future<> jit_tiering_engine::collect_profile_data(
  function_id func_id, const std::vector<hot_path_info>& hot_paths) {
    if (!_config.enable_profiling) {
        co_return;
    }

    auto& stats = _function_stats[func_id];
    stats.hot_paths = hot_paths;

    // Build profile data for PGO
    profile_data profile;
    profile.module_hash = func_id; // Use function ID as module hash
    profile.execution_count = stats.invocation_count;
    profile.last_update = std::chrono::steady_clock::now();

    // Extract hot functions
    for (const auto& path : hot_paths) {
        if (path.execution_count > _config.baseline_threshold) {
            profile.hot_functions.push_back(path.function_name);
        }
    }

    _profile_data[func_id] = std::move(profile);

    co_return;
}

ss::future<std::optional<profile_data>>
jit_tiering_engine::get_profile_data(function_id func_id) {
    if (auto it = _profile_data.find(func_id); it != _profile_data.end()) {
        co_return it->second;
    }
    co_return std::nullopt;
}

tier jit_tiering_engine::determine_tier(const execution_stats& stats) const {
    // Start with interpreter for cold functions
    if (stats.invocation_count < _config.baseline_threshold) {
        return tier::interpreter;
    }

    // Move to baseline JIT for warm functions
    if (stats.invocation_count < _config.optimizing_threshold) {
        return tier::baseline_jit;
    }

    // Use optimizing JIT for hot functions
    return tier::optimizing_jit;
}

bool jit_tiering_engine::would_benefit_from_optimization(
  const execution_stats& stats) const {
    // Check if optimization would provide significant benefit
    if (stats.invocation_count == 0) {
        return false;
    }

    auto avg_time = stats.total_execution_time / stats.invocation_count;

    // Only optimize if function takes significant time
    return avg_time >= _config.optimization_threshold_time;
}

ss::future<> jit_tiering_engine::compile_baseline_jit(function_id func_id) {
    vlog(jit_log.debug, "Compiling function {} at baseline JIT tier", func_id);

    // Quick compilation with minimal optimization
    compilation_options opts;
    opts.opt_level = optimization_level::baseline;
    opts.enable_simd = false; // Skip for faster compilation

    // Note: In a real implementation, we would pass the actual WASM module
    // For now, we just create a placeholder compiled function
    compiled_function func;
    func.compilation_tier = tier::baseline_jit;
    func.compiled_at = std::chrono::steady_clock::now();

    _compiled_functions[func_id][tier::baseline_jit] = std::move(func);

    vlog(
      jit_log.debug,
      "Baseline JIT compilation completed for function {}",
      func_id);

    co_return;
}

ss::future<> jit_tiering_engine::compile_optimizing_jit(function_id func_id) {
    vlog(
      jit_log.debug,
      "Compiling function {} at optimizing JIT tier",
      func_id);

    // Full optimization with profile data
    compilation_options opts;
    opts.opt_level = optimization_level::aggressive;
    opts.enable_simd = true;
    opts.enable_pgo = _config.enable_profiling;

    // Add profile data if available
    if (auto it = _profile_data.find(func_id); it != _profile_data.end()) {
        opts.hot_functions = it->second.hot_functions;
        opts.branch_hints = it->second.branch_predictions;
        opts.inline_hints = it->second.inline_candidates;
    }

    // In a real implementation, this would be submitted to a background CPU
    // For now, we compile synchronously
    compiled_function func;
    func.compilation_tier = tier::optimizing_jit;
    func.compiled_at = std::chrono::steady_clock::now();

    _compiled_functions[func_id][tier::optimizing_jit] = std::move(func);

    vlog(
      jit_log.debug,
      "Optimizing JIT compilation completed for function {}",
      func_id);

    co_return;
}

} // namespace wasm
