# RFC-005: Advanced Compaction Strategies

**Authors**: Redpanda Engineering Team  
**Status**: Proposed  
**Created**: 2025-01-15  
**Discussion**: [GitHub Issue #XXXX]

## Summary

This RFC proposes advanced compaction strategies for Redpanda including workload-aware scheduling, incremental compaction, hybrid key-time compaction, and cloud-native compaction, improving storage efficiency by 30-50% while reducing impact on foreground operations.

## Motivation

### Current State

Current compaction is primarily time-based or size-based without considering access patterns, workload characteristics, or cloud storage optimization.

### Problems

1. **Inefficient Scheduling**: Compaction runs without considering workload patterns
2. **Impact on Performance**: Large compactions affect foreground operations
3. **Storage Waste**: Dead data retained longer than necessary
4. **Cloud Costs**: Inefficient compaction in cloud storage
5. **Single Strategy**: One-size-fits-all approach doesn't optimize for different workloads

### Use Cases

- Mixed workloads with different retention requirements
- Cloud deployments with tiered storage
- High-throughput systems requiring minimal latency impact
- Multi-tenant environments with varying SLAs
- Cost-sensitive deployments optimizing storage

## Detailed Design

### Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│              Advanced Compaction System                    │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              Workload Analyzer                        │ │
│  │  ┌─────────┐  ┌─────────┐  ┌────────────────────┐  │ │
│  │  │  Access  │  │  Dead   │  │    Retention       │  │ │
│  │  │  Pattern │  │  Ratio  │  │    Analysis        │  │ │
│  │  └─────────┘  └─────────┘  └────────────────────┘  │ │
│  └─────────────────────────────────────────────────────┘ │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │           Compaction Strategy Selector                │ │
│  │  ┌─────────┐  ┌─────────┐  ┌────────────────────┐  │ │
│  │  │   Time   │  │   Key   │  │     Hybrid         │  │ │
│  │  │  Based   │  │  Based  │  │   Strategy         │  │ │
│  │  └─────────┘  └─────────┘  └────────────────────┘  │ │
│  └─────────────────────────────────────────────────────┘ │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │             Execution Scheduler                       │ │
│  │  ┌─────────┐  ┌─────────┐  ┌────────────────────┐  │ │
│  │  │Priority  │  │Resource │  │   Incremental      │  │ │
│  │  │  Queue   │  │  Budget │  │   Processor        │  │ │
│  │  └─────────┘  └─────────┘  └────────────────────┘  │ │
│  └─────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

### Core Components

#### 1. Workload-Aware Compaction Scheduler

```cpp
class intelligent_compaction_scheduler {
public:
    struct segment_metadata {
        model::ntp ntp;
        segment_id id;
        size_t size_bytes;
        size_t live_bytes;
        size_t dead_bytes;
        std::chrono::system_clock::time_point creation_time;
        std::chrono::system_clock::time_point last_access_time;
        size_t access_frequency;
        double fragmentation_ratio;
    };
    
    struct compaction_decision {
        model::ntp ntp;
        std::vector<segment_id> segments;
        compaction_priority priority;
        estimated_benefit benefit;
        compaction_strategy strategy;
        resource_requirements resources;
    };
    
    struct workload_characteristics {
        access_pattern pattern;  // SEQUENTIAL, RANDOM, TEMPORAL
        double write_rate;
        double read_rate;
        retention_policy retention;
        double key_cardinality;
        bool has_deletes;
    };

    ss::future<std::vector<compaction_decision>> 
    schedule_compactions(const cluster_state& state) {
        std::vector<compaction_decision> decisions;
        
        // Analyze each partition
        for (const auto& partition : state.partitions) {
            auto metadata = co_await collect_segment_metadata(partition);
            auto workload = analyze_workload_characteristics(partition);
            
            // Score segments for compaction
            auto candidates = identify_compaction_candidates(
                metadata, 
                workload
            );
            
            if (!candidates.empty()) {
                auto decision = make_compaction_decision(
                    candidates,
                    workload,
                    state.resource_availability
                );
                
                decisions.push_back(decision);
            }
        }
        
        // Prioritize and schedule
        co_return prioritize_decisions(decisions, state.constraints);
    }

private:
    std::vector<segment_metadata> identify_compaction_candidates(
        const std::vector<segment_metadata>& segments,
        const workload_characteristics& workload
    ) {
        std::vector<segment_metadata> candidates;
        
        for (const auto& segment : segments) {
            auto score = calculate_compaction_score(segment, workload);
            
            if (should_compact(score)) {
                candidates.push_back(segment);
            }
        }
        
        // Group adjacent segments for efficiency
        return group_adjacent_segments(candidates);
    }
    
    double calculate_compaction_score(
        const segment_metadata& segment,
        const workload_characteristics& workload
    ) {
        // Dead data ratio (primary factor)
        double dead_ratio = static_cast<double>(segment.dead_bytes) / 
                          segment.size_bytes;
        
        // Access frequency (inverse relationship)
        double access_factor = 1.0 / (1.0 + segment.access_frequency);
        
        // Age factor (older segments score higher)
        auto age = clock::now() - segment.creation_time;
        double age_factor = std::min(1.0, age.count() / _max_age.count());
        
        // Fragmentation penalty
        double fragmentation_penalty = segment.fragmentation_ratio;
        
        // Workload-specific adjustments
        double workload_multiplier = 1.0;
        
        if (workload.pattern == access_pattern::temporal) {
            // Prioritize time-based compaction
            workload_multiplier = age_factor * 2.0;
        } else if (workload.has_deletes) {
            // Prioritize segments with tombstones
            workload_multiplier = dead_ratio * 2.0;
        }
        
        return (dead_ratio * _dead_ratio_weight +
                access_factor * _access_weight +
                age_factor * _age_weight +
                fragmentation_penalty * _fragmentation_weight) *
               workload_multiplier;
    }
    
    compaction_decision make_compaction_decision(
        const std::vector<segment_metadata>& candidates,
        const workload_characteristics& workload,
        const resource_availability& resources
    ) {
        // Choose strategy based on workload
        auto strategy = select_compaction_strategy(workload);
        
        // Estimate benefit
        auto benefit = estimate_compaction_benefit(candidates);
        
        // Calculate priority
        auto priority = calculate_priority(benefit, resources);
        
        // Determine resource requirements
        auto requirements = calculate_resource_requirements(
            candidates,
            strategy
        );
        
        return {
            .ntp = candidates[0].ntp,
            .segments = extract_segment_ids(candidates),
            .priority = priority,
            .benefit = benefit,
            .strategy = strategy,
            .resources = requirements
        };
    }
    
    compaction_strategy select_compaction_strategy(
        const workload_characteristics& workload
    ) {
        if (workload.has_deletes && workload.key_cardinality < 10000) {
            return compaction_strategy::key_based;
        }
        
        if (workload.pattern == access_pattern::temporal) {
            return compaction_strategy::time_window;
        }
        
        if (workload.retention.type == retention_type::size_based) {
            return compaction_strategy::size_tiered;
        }
        
        // Default to hybrid for mixed workloads
        return compaction_strategy::hybrid;
    }

private:
    static constexpr duration _max_age = 7 * 24h;
    static constexpr double _dead_ratio_weight = 0.4;
    static constexpr double _access_weight = 0.2;
    static constexpr double _age_weight = 0.2;
    static constexpr double _fragmentation_weight = 0.2;
};
```

#### 2. Incremental Compaction Engine

```cpp
class incremental_compaction_engine {
public:
    struct compaction_state {
        segment_id source_segment;
        size_t bytes_processed;
        size_t bytes_total;
        model::offset last_offset;
        bool is_paused;
        std::chrono::steady_clock::time_point started_at;
        std::chrono::steady_clock::time_point last_progress;
    };

    ss::future<> compact_incrementally(
        const compaction_request& request,
        compaction_options opts = {}
    ) {
        // Initialize state
        auto state = initialize_compaction_state(request);
        
        // Process in chunks
        while (!state.is_complete()) {
            // Check if we should pause
            if (should_pause(state, opts)) {
                co_await pause_compaction(state);
                continue;
            }
            
            // Process next chunk
            auto chunk_size = calculate_chunk_size(state, opts);
            co_await process_chunk(state, chunk_size);
            
            // Update progress
            update_progress(state);
            
            // Yield to foreground operations
            co_await ss::yield();
            
            // Check resource availability
            if (!has_sufficient_resources(opts)) {
                co_await wait_for_resources(opts.min_resources);
            }
        }
        
        // Finalize compaction
        co_await finalize_compaction(state);
    }

private:
    ss::future<> process_chunk(
        compaction_state& state,
        size_t chunk_size
    ) {
        // Read chunk from source
        auto reader = co_await make_segment_reader(
            state.source_segment,
            state.last_offset,
            chunk_size
        );
        
        // Filter dead records
        auto filtered = co_await filter_records(reader);
        
        // Write to new segment
        auto writer = get_or_create_writer(state);
        co_await writer.write(filtered);
        
        // Update state
        state.bytes_processed += chunk_size;
        state.last_offset = filtered.last_offset();
        state.last_progress = clock::now();
    }
    
    bool should_pause(
        const compaction_state& state,
        const compaction_options& opts
    ) {
        // Pause if running too long
        if (clock::now() - state.started_at > opts.max_run_duration) {
            return true;
        }
        
        // Pause if foreground load is high
        if (_metrics.foreground_load() > opts.pause_threshold) {
            return true;
        }
        
        // Pause if memory pressure
        if (_memory_monitor.pressure() > 0.8) {
            return true;
        }
        
        return false;
    }
    
    ss::future<> pause_compaction(compaction_state& state) {
        state.is_paused = true;
        
        // Save state for resumption
        co_await save_checkpoint(state);
        
        // Calculate backoff
        auto backoff = calculate_backoff(state);
        
        // Wait before resuming
        co_await ss::sleep(backoff);
        
        state.is_paused = false;
    }
    
    size_t calculate_chunk_size(
        const compaction_state& state,
        const compaction_options& opts
    ) {
        // Base chunk size
        size_t base_size = opts.base_chunk_size;
        
        // Adjust based on progress rate
        auto progress_rate = calculate_progress_rate(state);
        if (progress_rate < opts.min_progress_rate) {
            // Reduce chunk size if too slow
            base_size /= 2;
        } else if (progress_rate > opts.max_progress_rate) {
            // Increase chunk size if too fast
            base_size *= 2;
        }
        
        // Apply limits
        return std::clamp(
            base_size,
            opts.min_chunk_size,
            opts.max_chunk_size
        );
    }
    
    ss::future<> save_checkpoint(const compaction_state& state) {
        // Persist state for crash recovery
        checkpoint cp{
            .segment_id = state.source_segment,
            .last_offset = state.last_offset,
            .bytes_processed = state.bytes_processed,
            .timestamp = clock::now()
        };
        
        co_await _checkpoint_manager.save(cp);
    }
    
    ss::future<> resume_from_checkpoint(segment_id segment) {
        auto checkpoint = co_await _checkpoint_manager.load(segment);
        
        if (checkpoint) {
            compaction_state state;
            state.source_segment = checkpoint->segment_id;
            state.last_offset = checkpoint->last_offset;
            state.bytes_processed = checkpoint->bytes_processed;
            
            // Resume compaction
            co_await compact_incrementally(state);
        }
    }

private:
    checkpoint_manager _checkpoint_manager;
    memory_monitor _memory_monitor;
    metrics_collector _metrics;
};
```

#### 3. Hybrid Compaction Strategy

```cpp
class hybrid_compaction_strategy {
public:
    struct hybrid_config {
        bool enable_key_compaction = true;
        bool enable_time_compaction = true;
        duration time_window = 1h;
        double key_uniqueness_threshold = 0.8;
        size_t min_segment_size = 1048576;
    };

    ss::future<compaction_plan> create_hybrid_plan(
        const std::vector<segment_metadata>& segments,
        const hybrid_config& config
    ) {
        compaction_plan plan;
        
        // Analyze key distribution
        auto key_stats = co_await analyze_key_distribution(segments);
        
        // Analyze temporal distribution
        auto time_stats = analyze_temporal_distribution(segments);
        
        // Decide compaction mode
        if (should_use_key_compaction(key_stats, config)) {
            plan = co_await create_key_based_plan(segments, key_stats);
        } else if (should_use_time_compaction(time_stats, config)) {
            plan = create_time_based_plan(segments, time_stats);
        } else {
            // Use hybrid approach
            plan = co_await create_hybrid_plan_impl(
                segments,
                key_stats,
                time_stats,
                config
            );
        }
        
        co_return plan;
    }

private:
    struct key_statistics {
        size_t total_keys;
        size_t unique_keys;
        double uniqueness_ratio;
        absl::flat_hash_map<model::record_key, size_t> key_frequency;
        std::vector<model::record_key> hot_keys;
    };
    
    struct time_statistics {
        model::timestamp min_timestamp;
        model::timestamp max_timestamp;
        duration span;
        std::vector<time_bucket> buckets;
        double temporal_locality;
    };

    ss::future<compaction_plan> create_hybrid_plan_impl(
        const std::vector<segment_metadata>& segments,
        const key_statistics& key_stats,
        const time_statistics& time_stats,
        const hybrid_config& config
    ) {
        compaction_plan plan;
        
        // Partition segments by strategy
        auto [key_segments, time_segments] = partition_segments(
            segments,
            key_stats,
            time_stats
        );
        
        // Create key-based compaction for segments with high key overlap
        if (!key_segments.empty()) {
            auto key_plan = co_await create_key_compaction_tasks(
                key_segments,
                key_stats
            );
            plan.tasks.insert(
                plan.tasks.end(),
                key_plan.begin(),
                key_plan.end()
            );
        }
        
        // Create time-based compaction for temporal segments
        if (!time_segments.empty()) {
            auto time_plan = create_time_compaction_tasks(
                time_segments,
                time_stats,
                config.time_window
            );
            plan.tasks.insert(
                plan.tasks.end(),
                time_plan.begin(),
                time_plan.end()
            );
        }
        
        // Optimize execution order
        plan = optimize_execution_order(plan);
        
        co_return plan;
    }
    
    std::pair<segment_list, segment_list> partition_segments(
        const std::vector<segment_metadata>& segments,
        const key_statistics& key_stats,
        const time_statistics& time_stats
    ) {
        segment_list key_candidates;
        segment_list time_candidates;
        
        for (const auto& segment : segments) {
            // Check key overlap with other segments
            auto key_overlap = calculate_key_overlap(
                segment,
                segments,
                key_stats
            );
            
            // Check temporal clustering
            auto time_clustering = calculate_temporal_clustering(
                segment,
                time_stats
            );
            
            // Assign to appropriate strategy
            if (key_overlap > 0.5) {
                key_candidates.push_back(segment);
            } else if (time_clustering > 0.7) {
                time_candidates.push_back(segment);
            }
            // Segments not fitting either criteria are skipped
        }
        
        return {key_candidates, time_candidates};
    }
    
    ss::future<std::vector<compaction_task>>
    create_key_compaction_tasks(
        const segment_list& segments,
        const key_statistics& stats
    ) {
        std::vector<compaction_task> tasks;
        
        // Group segments with overlapping keys
        auto groups = group_by_key_overlap(segments, stats);
        
        for (const auto& group : groups) {
            compaction_task task;
            task.type = compaction_type::key_based;
            task.segments = group;
            task.priority = calculate_key_compaction_priority(group, stats);
            
            // Create key filter
            task.key_filter = create_deduplication_filter(group, stats);
            
            tasks.push_back(task);
        }
        
        co_return tasks;
    }
    
    std::vector<compaction_task> create_time_compaction_tasks(
        const segment_list& segments,
        const time_statistics& stats,
        duration window
    ) {
        std::vector<compaction_task> tasks;
        
        // Group segments by time window
        auto windows = group_by_time_window(segments, window);
        
        for (const auto& [window_start, window_segments] : windows) {
            compaction_task task;
            task.type = compaction_type::time_window;
            task.segments = window_segments;
            task.priority = calculate_time_compaction_priority(
                window_segments,
                stats
            );
            
            // Set time bounds
            task.time_range = {
                window_start,
                window_start + window
            };
            
            tasks.push_back(task);
        }
        
        return tasks;
    }
};
```

#### 4. Cloud-Native Compaction

```cpp
class cloud_native_compaction {
public:
    struct cloud_config {
        size_t min_segment_size = 134217728;  // 128MB
        size_t max_segment_size = 1073741824;  // 1GB
        bool compact_in_cloud = true;
        bool use_lambda_functions = false;
        size_t parallel_downloads = 4;
        size_t parallel_uploads = 2;
    };

    ss::future<> compact_cloud_segments(
        const cloud_storage::ntp& ntp,
        const std::vector<cloud_segment_metadata>& segments,
        cloud_config config = {}
    ) {
        if (config.use_lambda_functions) {
            co_return co_await compact_using_lambda(ntp, segments, config);
        } else {
            co_return co_await compact_locally(ntp, segments, config);
        }
    }

private:
    ss::future<> compact_using_lambda(
        const cloud_storage::ntp& ntp,
        const std::vector<cloud_segment_metadata>& segments,
        const cloud_config& config
    ) {
        // Prepare lambda invocation
        lambda_request request;
        request.function_name = "redpanda-compaction";
        request.payload = {
            {"ntp", ntp.path()},
            {"segments", segments_to_json(segments)},
            {"config", config_to_json(config)}
        };
        
        // Invoke lambda function
        auto result = co_await _lambda_client.invoke_async(request);
        
        if (!result.success) {
            throw compaction_error(result.error_message);
        }
        
        // Wait for completion
        co_await wait_for_lambda_completion(result.request_id);
        
        // Update metadata
        co_await update_cloud_manifest(ntp, result.compacted_segments);
    }
    
    ss::future<> compact_locally(
        const cloud_storage::ntp& ntp,
        const std::vector<cloud_segment_metadata>& segments,
        const cloud_config& config
    ) {
        // Download segments in parallel
        auto downloaded = co_await download_segments_parallel(
            segments,
            config.parallel_downloads
        );
        
        // Perform compaction
        auto compacted = co_await perform_local_compaction(downloaded);
        
        // Upload compacted segments
        co_await upload_segments_parallel(
            compacted,
            config.parallel_uploads
        );
        
        // Clean up old segments
        co_await delete_old_segments(segments);
        
        // Update manifest
        co_await update_cloud_manifest(ntp, compacted);
    }
    
    ss::future<std::vector<local_segment>>
    download_segments_parallel(
        const std::vector<cloud_segment_metadata>& segments,
        size_t parallelism
    ) {
        ss::semaphore limit(parallelism);
        std::vector<ss::future<local_segment>> futures;
        
        for (const auto& segment : segments) {
            futures.push_back(
                ss::with_semaphore(
                    limit,
                    1,
                    [this, segment]() {
                        return download_segment(segment);
                    }
                )
            );
        }
        
        co_return co_await ss::when_all_succeed(
            futures.begin(),
            futures.end()
        );
    }
    
    ss::future<local_segment> download_segment(
        const cloud_segment_metadata& metadata
    ) {
        // Stream download to minimize memory usage
        auto reader = co_await _cloud_storage.make_segment_reader(
            metadata.path
        );
        
        // Write to local temporary file
        auto temp_path = generate_temp_path(metadata.id);
        auto writer = co_await make_file_writer(temp_path);
        
        co_await ss::copy(reader, writer);
        
        co_return local_segment{
            .metadata = metadata,
            .path = temp_path
        };
    }
    
    ss::future<> upload_segments_parallel(
        const std::vector<local_segment>& segments,
        size_t parallelism
    ) {
        ss::semaphore limit(parallelism);
        std::vector<ss::future<>> futures;
        
        for (const auto& segment : segments) {
            futures.push_back(
                ss::with_semaphore(
                    limit,
                    1,
                    [this, segment]() {
                        return upload_segment(segment);
                    }
                )
            );
        }
        
        co_await ss::when_all_succeed(
            futures.begin(),
            futures.end()
        );
    }
    
    // Optimize segment size for cloud storage
    std::vector<segment_group> optimize_segment_sizes(
        const std::vector<cloud_segment_metadata>& segments,
        const cloud_config& config
    ) {
        std::vector<segment_group> groups;
        segment_group current_group;
        size_t current_size = 0;
        
        for (const auto& segment : segments) {
            if (current_size + segment.size > config.max_segment_size) {
                // Start new group
                if (!current_group.empty()) {
                    groups.push_back(std::move(current_group));
                }
                current_group.clear();
                current_size = 0;
            }
            
            current_group.push_back(segment);
            current_size += segment.size;
        }
        
        // Add last group if it meets minimum size
        if (current_size >= config.min_segment_size) {
            groups.push_back(std::move(current_group));
        }
        
        return groups;
    }

private:
    cloud_storage::remote& _cloud_storage;
    lambda_client _lambda_client;
};
```

#### 5. Adaptive Compaction Strategy

```cpp
class adaptive_compaction_strategy {
public:
    struct performance_metrics {
        double write_amplification;
        double space_amplification;
        double read_amplification;
        duration average_latency;
        double throughput;
    };

    ss::future<> adapt_strategy(const model::ntp& ntp) {
        // Collect historical metrics
        auto history = co_await collect_performance_history(ntp);
        
        // Analyze effectiveness of current strategy
        auto effectiveness = analyze_strategy_effectiveness(history);
        
        // Determine if strategy change is needed
        if (should_change_strategy(effectiveness)) {
            auto new_strategy = select_optimal_strategy(history);
            co_await apply_new_strategy(ntp, new_strategy);
        } else {
            // Tune current strategy parameters
            auto tuned_params = tune_strategy_parameters(history);
            co_await apply_tuned_parameters(ntp, tuned_params);
        }
    }

private:
    strategy_effectiveness analyze_strategy_effectiveness(
        const performance_history& history
    ) {
        return {
            .write_amp_trend = calculate_trend(history.write_amplification),
            .space_efficiency = calculate_space_efficiency(history),
            .read_performance = calculate_read_performance(history),
            .overall_score = calculate_overall_score(history)
        };
    }
    
    compaction_strategy select_optimal_strategy(
        const performance_history& history
    ) {
        // Score each strategy based on workload
        std::vector<std::pair<compaction_strategy, double>> scores;
        
        for (auto strategy : all_strategies()) {
            auto score = simulate_strategy_performance(strategy, history);
            scores.emplace_back(strategy, score);
        }
        
        // Select best performing strategy
        auto best = std::max_element(
            scores.begin(),
            scores.end(),
            [](const auto& a, const auto& b) {
                return a.second < b.second;
            }
        );
        
        return best->first;
    }
    
    strategy_parameters tune_strategy_parameters(
        const performance_history& history
    ) {
        strategy_parameters params;
        
        // Tune based on observed patterns
        if (history.write_amplification > _target_write_amp) {
            // Increase segment size to reduce write amp
            params.min_segment_size *= 1.5;
            params.max_segment_size *= 1.5;
        }
        
        if (history.space_amplification > _target_space_amp) {
            // More aggressive compaction
            params.dead_ratio_threshold *= 0.8;
            params.compaction_frequency *= 1.2;
        }
        
        if (history.average_latency > _target_latency) {
            // Reduce compaction chunk size
            params.chunk_size *= 0.8;
            params.max_concurrent_compactions--;
        }
        
        return params;
    }

private:
    static constexpr double _target_write_amp = 5.0;
    static constexpr double _target_space_amp = 1.5;
    static constexpr duration _target_latency = 10ms;
};
```

### Configuration

```yaml
# Advanced compaction configuration
compaction_workload_aware_enabled: true
compaction_incremental_enabled: true
compaction_incremental_chunk_size: 1048576  # 1MB
compaction_incremental_max_runtime: 60s
compaction_hybrid_strategy_enabled: true
compaction_cloud_native_enabled: true
compaction_cloud_min_segment_size: 134217728  # 128MB
compaction_adaptive_tuning_enabled: true
compaction_dead_ratio_threshold: 0.5
compaction_min_segments_for_compaction: 3
compaction_max_concurrent_compactions: 2
```

### Performance Optimizations

#### 1. Parallel Segment Processing

```cpp
class parallel_compaction_processor {
    ss::future<> compact_segments_parallel(
        const std::vector<segment_pair>& pairs,
        size_t parallelism = 4
    ) {
        ss::semaphore limit(parallelism);
        
        co_await ss::parallel_for_each(
            pairs.begin(),
            pairs.end(),
            [&limit, this](const auto& pair) {
                return ss::with_semaphore(
                    limit,
                    1,
                    [this, pair]() {
                        return compact_segment_pair(pair);
                    }
                );
            }
        );
    }
};
```

#### 2. Memory-Efficient Streaming

```cpp
class streaming_compactor {
    ss::future<> compact_with_streaming(
        segment_reader& source,
        segment_writer& target
    ) {
        model::offset last_key;
        
        while (co_await source.has_next()) {
            auto batch = co_await source.read_batch();
            
            // Filter tombstones and duplicates
            auto filtered = filter_batch(batch, last_key);
            
            if (!filtered.empty()) {
                co_await target.write(filtered);
                last_key = filtered.last_offset();
            }
            
            // Yield periodically
            co_await ss::maybe_yield();
        }
    }
};
```

## Testing Strategy

### Unit Tests

```cpp
BOOST_AUTO_TEST_CASE(test_workload_aware_scheduling) {
    intelligent_compaction_scheduler scheduler;
    
    // Create test segments with different characteristics
    auto segments = create_test_segments_with_patterns();
    
    auto decisions = scheduler.schedule_compactions(segments).get();
    
    // Verify high-dead-ratio segments are prioritized
    BOOST_REQUIRE(decisions[0].priority == compaction_priority::high);
    
    // Verify appropriate strategy selection
    BOOST_REQUIRE(decisions[0].strategy == compaction_strategy::key_based);
}

BOOST_AUTO_TEST_CASE(test_incremental_compaction) {
    incremental_compaction_engine engine;
    
    auto request = create_large_compaction_request();
    
    // Start compaction
    auto future = engine.compact_incrementally(request);
    
    // Simulate pause condition
    inject_high_foreground_load();
    
    // Verify compaction pauses
    ss::sleep(100ms).get();
    BOOST_REQUIRE(engine.is_paused());
    
    // Remove load and verify resumption
    clear_foreground_load();
    future.get();
    
    // Verify completion
    BOOST_REQUIRE(verify_compaction_result());
}
```

### Performance Benchmarks

```cpp
PERF_TEST(compaction_efficiency) {
    // Measure storage efficiency improvement
    auto before_size = measure_storage_size();
    
    run_advanced_compaction();
    
    auto after_size = measure_storage_size();
    
    auto reduction = 1.0 - (after_size / before_size);
    BOOST_REQUIRE_GT(reduction, 0.3);  // At least 30% reduction
}

PERF_TEST(compaction_impact) {
    // Measure impact on foreground operations
    auto baseline_latency = measure_baseline_latency();
    
    start_incremental_compaction();
    auto compaction_latency = measure_latency_during_compaction();
    
    auto impact = (compaction_latency - baseline_latency) / baseline_latency;
    BOOST_REQUIRE_LT(impact, 0.1);  // Less than 10% impact
}
```

## Migration Strategy

### Phase 1: Workload Analysis (Week 1-2)
- Deploy workload analyzer
- Collect access patterns
- Identify optimization opportunities

### Phase 2: Incremental Rollout (Week 3-4)
- Enable incremental compaction
- Monitor impact metrics
- Tune chunk sizes

### Phase 3: Strategy Selection (Week 5-6)
- Enable adaptive strategy selection
- Deploy hybrid compaction
- Monitor effectiveness

### Phase 4: Cloud Optimization (Week 7-8)
- Enable cloud-native compaction
- Optimize segment sizes
- Full production deployment

## Metrics and Observability

### New Metrics

```cpp
namespace compaction::metrics {
    // Scheduling metrics
    histogram compaction_score_distribution;
    counter compaction_decisions_made;
    gauge pending_compactions;
    
    // Execution metrics
    histogram incremental_chunk_size;
    counter compaction_pauses;
    histogram compaction_duration;
    
    // Effectiveness metrics
    gauge dead_data_ratio;
    gauge space_amplification;
    gauge write_amplification;
    
    // Strategy metrics
    counter strategy_changes;
    histogram strategy_effectiveness_score;
}
```

## Security Considerations

1. **Resource Limits**: Prevent compaction from consuming excessive resources
2. **Access Control**: Ensure compaction respects data access permissions
3. **Encryption**: Maintain encryption during compaction
4. **Audit Logging**: Log compaction decisions for compliance

## Open Questions

1. Should compaction be tenant-aware in multi-tenant deployments?
2. How to handle compaction during cluster rebalancing?
3. Integration with backup and restore operations?
4. Optimal default thresholds for different workloads?

## References

- [Log Structured Merge Trees](https://example.com/lsm)
- [RocksDB Compaction](https://example.com/rocksdb-compaction)
- [Cloud Storage Best Practices](https://example.com/cloud-storage)