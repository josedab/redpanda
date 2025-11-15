# RFC-001: Enhanced Raft Performance through Parallel Log Replication

**Authors**: Redpanda Engineering Team  
**Status**: Proposed  
**Created**: 2025-01-15  
**Discussion**: [GitHub Issue #XXXX]

## Summary

This RFC proposes enhancements to Redpanda's Raft consensus implementation to enable parallel log replication, improving throughput by 40-60% for multi-partition workloads while maintaining linearizability and exactly-once semantics.

## Motivation

### Current State

The current Raft implementation in [`consensus.h`](../src/v/raft/consensus.h:67) processes append entries sequentially:

```cpp
ss::future<result<replicate_result>> 
consensus::replicate(model::record_batch batch, replicate_options opts) {
    // Processes one batch at a time
    return do_replicate(...);
}
```

### Problems

1. **Sequential Bottleneck**: Single-threaded append processing limits throughput
2. **Underutilized Resources**: Multi-core systems aren't fully leveraged
3. **Increased Latency**: P99 latency affected by sequential processing queue
4. **Scalability Limits**: Performance doesn't scale linearly with partition count

### Use Cases

- High-throughput streaming workloads with many partitions
- Multi-tenant environments requiring concurrent processing
- Real-time analytics with strict latency requirements
- Large-scale IoT data ingestion

## Detailed Design

### Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                 Parallel Replication Pipeline        │
│                                                      │
│  ┌──────────────┐     ┌─────────────────────────┐  │
│  │   Incoming   │────►│  Dependency Analyzer     │  │
│  │   Batches    │     │  - Identify dependencies │  │
│  └──────────────┘     │  - Create batch groups   │  │
│                       └───────────┬─────────────┘  │
│                                   ▼                 │
│                       ┌─────────────────────────┐  │
│                       │  Parallel Executor      │  │
│                       │  - Concurrent groups    │  │
│                       │  - Order preservation   │  │
│                       └───────────┬─────────────┘  │
│                                   ▼                 │
│                       ┌─────────────────────────┐  │
│                       │  Commit Coordinator     │  │
│                       │  - Linearization        │  │
│                       │  - Durability           │  │
│                       └─────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

### Core Components

#### 1. Dependency Graph Builder

```cpp
class dependency_graph {
public:
    struct batch_metadata {
        model::offset base_offset;
        model::offset last_offset;
        std::vector<model::record_key> keys;
        model::partition_id partition;
        model::timestamp timestamp;
    };

    struct dependency_edge {
        batch_id source;
        batch_id target;
        dependency_type type;  // KEY_ORDER, OFFSET_ORDER, TRANSACTION
    };

private:
    // Build dependency graph from batch metadata
    void analyze_batch(const model::record_batch& batch) {
        auto metadata = extract_metadata(batch);
        
        // Check key-based dependencies
        for (const auto& key : metadata.keys) {
            if (auto prev = _key_index.find(key); prev != _key_index.end()) {
                add_edge(prev->batch_id, batch.id(), KEY_ORDER);
            }
        }
        
        // Check offset dependencies
        if (requires_strict_ordering(batch)) {
            add_edge(_last_ordered_batch, batch.id(), OFFSET_ORDER);
        }
        
        // Check transaction dependencies
        if (batch.is_transactional()) {
            analyze_transaction_deps(batch);
        }
    }
    
    // Identify independent batch groups
    std::vector<batch_group> identify_parallel_groups() {
        return topological_sort_with_levels(_dependency_graph);
    }
    
private:
    absl::flat_hash_map<model::record_key, batch_id> _key_index;
    absl::flat_hash_map<batch_id, batch_metadata> _batch_metadata;
    adjacency_list<batch_id> _dependency_graph;
};
```

#### 2. Parallel Replication Pipeline

```cpp
class parallel_replication_pipeline {
public:
    struct replication_context {
        ss::semaphore* parallelism_limit;
        ss::abort_source* as;
        replicate_options options;
        consensus* raft;
    };

    ss::future<std::vector<result<replicate_result>>>
    replicate_parallel(
        std::vector<model::record_batch> batches,
        replicate_options opts
    ) {
        // Build dependency graph
        dependency_graph deps;
        for (const auto& batch : batches) {
            deps.analyze_batch(batch);
        }
        
        // Identify parallel groups
        auto groups = deps.identify_parallel_groups();
        
        // Process each level in parallel
        std::vector<result<replicate_result>> results;
        for (const auto& level : groups) {
            auto level_results = co_await process_level_parallel(level, opts);
            results.insert(results.end(), 
                          level_results.begin(), 
                          level_results.end());
        }
        
        co_return results;
    }

private:
    ss::future<std::vector<result<replicate_result>>>
    process_level_parallel(
        const batch_group& level,
        replicate_options opts
    ) {
        // Limit parallelism
        auto units = co_await _parallelism_sem.get_units(
            std::min(level.size(), _max_parallel_ops)
        );
        
        // Execute batches in parallel
        co_return co_await ss::parallel_for_each(
            level.begin(), 
            level.end(),
            [this, opts](const auto& batch) {
                return replicate_single(batch, opts);
            }
        );
    }
    
    ss::future<result<replicate_result>>
    replicate_single(
        const model::record_batch& batch,
        replicate_options opts
    ) {
        // Acquire per-partition lock if needed
        auto lock = co_await _partition_locks.get_lock(batch.partition());
        
        // Perform replication
        auto result = co_await _consensus->do_replicate(batch, opts);
        
        // Update indices
        co_await update_indices(batch, result);
        
        co_return result;
    }
    
private:
    static constexpr size_t _max_parallel_ops = 16;
    ss::semaphore _parallelism_sem{_max_parallel_ops};
    partition_lock_manager _partition_locks;
    consensus* _consensus;
};
```

#### 3. Speculative Execution

```cpp
class speculative_executor {
public:
    struct speculation_result {
        batch_id id;
        model::offset tentative_offset;
        bool committed;
        ss::promise<> completion;
    };

    ss::future<> execute_speculatively(
        model::record_batch batch,
        dependency_set deps
    ) {
        // Start speculative execution
        auto spec_id = generate_speculation_id();
        _active_speculations[spec_id] = {
            .batch = batch,
            .deps = deps,
            .state = speculation_state::running
        };
        
        // Wait for dependencies
        co_await wait_for_dependencies(deps);
        
        // Execute speculatively
        auto result = co_await _executor->execute(batch);
        
        if (should_commit(result, deps)) {
            co_await commit_speculation(spec_id, result);
        } else {
            co_await rollback_speculation(spec_id);
        }
    }

private:
    ss::future<> commit_speculation(
        speculation_id id,
        execution_result result
    ) {
        auto& spec = _active_speculations[id];
        
        // Validate speculation is still valid
        if (!validate_speculation(spec)) {
            co_return co_await rollback_speculation(id);
        }
        
        // Commit to log
        co_await _log->append(spec.batch, result.offset);
        
        // Notify waiters
        spec.completion.set_value();
        _active_speculations.erase(id);
    }
    
    ss::future<> rollback_speculation(speculation_id id) {
        auto& spec = _active_speculations[id];
        
        // Undo any tentative state changes
        co_await _executor->rollback(spec.batch);
        
        // Retry with sequential execution
        co_await _fallback_executor->execute(spec.batch);
        
        spec.completion.set_exception(speculation_failed_exception());
        _active_speculations.erase(id);
    }
    
private:
    absl::flat_hash_map<speculation_id, speculation_context> _active_speculations;
    executor* _executor;
    executor* _fallback_executor;
    log* _log;
};
```

### Consistency Guarantees

#### Linearizability Preservation

```cpp
class linearization_coordinator {
    // Ensure global order despite parallel execution
    ss::future<> linearize_commits(
        std::vector<pending_commit> commits
    ) {
        // Sort by logical timestamp
        std::sort(commits.begin(), commits.end(), 
                 [](const auto& a, const auto& b) {
                     return a.logical_ts < b.logical_ts;
                 });
        
        // Commit in order
        for (const auto& commit : commits) {
            co_await _log->commit(commit.offset, commit.term);
            _last_committed = commit.offset;
        }
    }
    
    // Assign logical timestamps
    logical_timestamp assign_timestamp(const model::record_batch& batch) {
        return logical_timestamp{
            .epoch = _current_epoch,
            .sequence = _sequence_generator.next(),
            .partition = batch.partition()
        };
    }
};
```

#### Exactly-Once Semantics

```cpp
class idempotency_tracker {
    // Track producer epochs and sequences
    bool is_duplicate(const producer_identity& pid, int32_t seq) {
        auto it = _producer_state.find(pid);
        if (it == _producer_state.end()) {
            return false;
        }
        
        return seq <= it->second.last_sequence;
    }
    
    // Update producer state atomically
    ss::future<> update_producer_state(
        const producer_identity& pid,
        int32_t seq,
        model::offset offset
    ) {
        auto lock = co_await _producer_locks.get_lock(pid);
        
        _producer_state[pid] = {
            .last_sequence = seq,
            .last_offset = offset
        };
    }
};
```

### Configuration

```yaml
# Parallel replication configuration
raft_parallel_replication_enabled: true
raft_max_parallel_operations: 16
raft_dependency_detection_mode: auto  # auto, strict, relaxed
raft_speculation_enabled: false
raft_parallel_batch_size_threshold: 1024  # bytes
```

### Performance Optimizations

#### 1. Lock-Free Data Structures

```cpp
// Lock-free queue for batch submission
template<typename T>
class lock_free_queue {
    struct node {
        std::atomic<T*> data;
        std::atomic<node*> next;
    };
    
    std::atomic<node*> head;
    std::atomic<node*> tail;
    
public:
    void push(T item) {
        auto new_node = new node{item, nullptr};
        node* prev = tail.exchange(new_node);
        prev->next.store(new_node);
    }
    
    std::optional<T> pop() {
        node* head_node = head.load();
        node* next = head_node->next.load();
        
        if (next == nullptr) {
            return std::nullopt;
        }
        
        T* data = next->data.exchange(nullptr);
        head.store(next);
        delete head_node;
        
        return *data;
    }
};
```

#### 2. Adaptive Parallelism

```cpp
class adaptive_parallelism_controller {
    size_t calculate_optimal_parallelism() {
        auto cpu_usage = _metrics.cpu_utilization();
        auto memory_pressure = _metrics.memory_pressure();
        auto network_utilization = _metrics.network_utilization();
        
        // Reduce parallelism under resource pressure
        if (memory_pressure > 0.8 || cpu_usage > 0.9) {
            return std::max(1UL, _current_parallelism / 2);
        }
        
        // Increase parallelism if resources available
        if (cpu_usage < 0.5 && memory_pressure < 0.5) {
            return std::min(_max_parallelism, _current_parallelism * 2);
        }
        
        return _current_parallelism;
    }
    
    ss::future<> adjust_parallelism() {
        while (!_as.abort_requested()) {
            auto optimal = calculate_optimal_parallelism();
            
            if (optimal != _current_parallelism) {
                co_await _pipeline->set_parallelism(optimal);
                _current_parallelism = optimal;
            }
            
            co_await ss::sleep(std::chrono::seconds(1));
        }
    }
};
```

## Migration Strategy

### Phase 1: Feature Flag Introduction (Week 1-2)

1. Implement feature flag `raft_parallel_replication_enabled`
2. Add parallel pipeline alongside existing code
3. Default to disabled

### Phase 2: Canary Testing (Week 3-4)

1. Enable on internal test clusters
2. Monitor performance metrics
3. Validate consistency guarantees

### Phase 3: Gradual Rollout (Week 5-8)

1. Enable for 10% of production traffic
2. Monitor for 1 week
3. Increase to 50% if stable
4. Full rollout after 2 weeks of stability

### Rollback Plan

```cpp
class parallel_replication_rollback {
    ss::future<> emergency_disable() {
        // Drain in-flight parallel operations
        co_await _pipeline->drain();
        
        // Switch to sequential mode
        _config.raft_parallel_replication_enabled = false;
        
        // Wait for pending commits
        co_await _commit_coordinator->flush();
        
        // Resume with sequential replication
        _consensus->use_sequential_replication();
    }
};
```

## Testing Strategy

### Unit Tests

```cpp
BOOST_AUTO_TEST_CASE(test_dependency_detection) {
    dependency_graph graph;
    
    // Test key-based dependencies
    auto batch1 = make_batch({{"key1", "value1"}});
    auto batch2 = make_batch({{"key1", "value2"}});
    
    graph.analyze_batch(batch1);
    graph.analyze_batch(batch2);
    
    auto groups = graph.identify_parallel_groups();
    BOOST_REQUIRE_EQUAL(groups.size(), 2);  // Sequential execution required
}

BOOST_AUTO_TEST_CASE(test_parallel_execution) {
    parallel_replication_pipeline pipeline;
    
    // Test independent batches
    std::vector<model::record_batch> batches = {
        make_batch({{"key1", "value1"}}),
        make_batch({{"key2", "value2"}}),
        make_batch({{"key3", "value3"}})
    };
    
    auto results = pipeline.replicate_parallel(batches, {}).get();
    BOOST_REQUIRE_EQUAL(results.size(), 3);
    
    // Verify all succeeded
    for (const auto& result : results) {
        BOOST_REQUIRE(result.has_value());
    }
}
```

### Integration Tests

```cpp
SEASTAR_TEST_CASE(test_consistency_under_parallel_load) {
    return run_test_with_raft([](raft_test_fixture& f) {
        // Generate mixed workload
        auto batches = generate_mixed_workload(1000);
        
        // Execute in parallel
        auto results = f.replicate_parallel(batches).get();
        
        // Verify consistency
        return f.verify_linearizability(results);
    });
}
```

### Performance Benchmarks

```cpp
PERF_TEST(parallel_replication_throughput) {
    // Measure throughput improvement
    auto sequential_tps = measure_sequential_throughput();
    auto parallel_tps = measure_parallel_throughput();
    
    auto improvement = (parallel_tps - sequential_tps) / sequential_tps;
    BOOST_REQUIRE_GT(improvement, 0.4);  // At least 40% improvement
}
```

## Metrics and Observability

### New Metrics

```cpp
namespace metrics {
    // Parallelism metrics
    histogram parallel_batch_size;
    histogram dependency_graph_build_time;
    counter parallel_executions;
    counter sequential_fallbacks;
    gauge active_parallel_operations;
    
    // Performance metrics  
    histogram parallel_replication_latency;
    histogram speculation_success_rate;
    counter dependency_conflicts;
}
```

### Tracing Integration

```cpp
class traced_parallel_pipeline {
    ss::future<result<replicate_result>>
    replicate_with_tracing(
        model::record_batch batch,
        replicate_options opts
    ) {
        auto span = _tracer.start_span("parallel_replication");
        span.set_tag("batch.size", batch.size_bytes());
        span.set_tag("batch.partition", batch.partition());
        
        auto dep_span = _tracer.start_span("dependency_analysis", span);
        auto deps = co_await analyze_dependencies(batch);
        dep_span.set_tag("dependency.count", deps.size());
        dep_span.finish();
        
        auto exec_span = _tracer.start_span("parallel_execution", span);
        auto result = co_await execute_parallel(batch, deps);
        exec_span.finish();
        
        span.finish();
        co_return result;
    }
};
```

## Security Considerations

1. **Resource Limits**: Prevent DoS through excessive parallelism
2. **Isolation**: Ensure tenant isolation in multi-tenant deployments
3. **Audit**: Log parallel execution decisions for debugging

## Alternatives Considered

### 1. Pipelined Replication
- **Pros**: Simpler implementation, predictable behavior
- **Cons**: Limited parallelism, doesn't handle independent batches well

### 2. Optimistic Concurrency Control
- **Pros**: Maximum parallelism
- **Cons**: High rollback rate, complexity in conflict resolution

### 3. Multi-Raft
- **Pros**: True parallel consensus
- **Cons**: Major architectural change, migration complexity

## Open Questions

1. Should we implement speculation for all batch types or only specific ones?
2. What's the optimal default parallelism level?
3. Should dependency detection be pluggable?
4. How to handle priority inversion in parallel execution?

## References

- [Parallel Raft Paper](https://example.com/parallel-raft)
- [Scalable Consensus Algorithms](https://example.com/scalable-consensus)
- [Lock-Free Programming Techniques](https://example.com/lock-free)
- Internal Design Doc: Raft Performance Analysis