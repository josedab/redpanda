# RFC-002: Intelligent Tiered Storage Prefetching

**Authors**: Redpanda Engineering Team  
**Status**: Proposed  
**Created**: 2025-01-15  
**Discussion**: [GitHub Issue #XXXX]

## Summary

This RFC proposes an ML-based predictive prefetching system for Redpanda's tiered storage, reducing cold read latency by 50-70% through intelligent access pattern detection and proactive segment fetching.

## Motivation

### Current State

The current cloud storage implementation in [`remote_partition.h`](../src/v/cloud_storage/remote_partition.h:49) uses reactive fetching:

```cpp
class remote_partition {
    // Fetches segments on-demand
    ss::future<model::record_batch_reader>
    make_reader(storage::local_log_reader_config cfg);
};
```

### Problems

1. **Cold Read Latency**: First access to cloud segments incurs full fetch latency
2. **Cache Misses**: No prediction of future access patterns
3. **Inefficient Bandwidth**: Multiple small fetches instead of batched prefetching
4. **User Experience**: Unpredictable performance for historical data access

### Use Cases

- Time-series analytics with predictable access patterns
- Batch processing jobs reading sequential segments
- Replay scenarios for event sourcing
- ML training pipelines accessing historical data

## Detailed Design

### Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│                 Intelligent Prefetch System               │
│                                                           │
│  ┌─────────────────┐        ┌────────────────────────┐  │
│  │  Access Pattern │◄───────│   Read Request         │  │
│  │   Collector     │        │    Interceptor         │  │
│  └────────┬────────┘        └────────────────────────┘  │
│           │                                              │
│           ▼                                              │
│  ┌─────────────────┐        ┌────────────────────────┐  │
│  │  Pattern        │───────►│   Prediction           │  │
│  │   Analyzer      │        │     Engine             │  │
│  └─────────────────┘        └───────────┬────────────┘  │
│                                          │               │
│                                          ▼               │
│  ┌─────────────────┐        ┌────────────────────────┐  │
│  │  Prefetch       │◄───────│   Prefetch             │  │
│  │   Executor      │        │    Scheduler           │  │
│  └────────┬────────┘        └────────────────────────┘  │
│           │                                              │
│           ▼                                              │
│  ┌─────────────────────────────────────────────────┐   │
│  │              Cache Manager                       │   │
│  │  (Memory Budget, Eviction, Prioritization)       │   │
│  └─────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────┘
```

### Core Components

#### 1. Access Pattern Collector

```cpp
class access_pattern_collector {
public:
    struct access_event {
        model::ntp ntp;
        model::offset offset;
        size_t size_bytes;
        std::chrono::steady_clock::time_point timestamp;
        read_direction direction;
        client_id client;
        access_type type;  // SEQUENTIAL, RANDOM, REPLAY
    };
    
    struct pattern_features {
        double inter_arrival_mean;
        double inter_arrival_stddev;
        double stride_mean;
        double stride_stddev;
        double temporal_locality;
        double spatial_locality;
        size_t sequence_length;
        read_direction dominant_direction;
    };

    void record_access(const access_event& event) {
        // Update circular buffer
        _access_history.push_back(event);
        
        // Update online statistics
        update_statistics(event);
        
        // Trigger pattern detection if threshold met
        if (_access_history.size() >= _min_pattern_size) {
            _pattern_detector.analyze(_access_history);
        }
    }
    
    pattern_features extract_features() const {
        return {
            .inter_arrival_mean = calculate_inter_arrival_mean(),
            .inter_arrival_stddev = calculate_inter_arrival_stddev(),
            .stride_mean = calculate_stride_mean(),
            .stride_stddev = calculate_stride_stddev(),
            .temporal_locality = calculate_temporal_locality(),
            .spatial_locality = calculate_spatial_locality(),
            .sequence_length = _access_history.size(),
            .dominant_direction = determine_dominant_direction()
        };
    }

private:
    void update_statistics(const access_event& event) {
        if (!_last_access) {
            _last_access = event;
            return;
        }
        
        // Update inter-arrival time
        auto delta_time = event.timestamp - _last_access->timestamp;
        _inter_arrival_stats.update(delta_time.count());
        
        // Update stride
        auto stride = event.offset - _last_access->offset;
        _stride_stats.update(stride());
        
        // Update locality metrics
        update_locality_metrics(event);
        
        _last_access = event;
    }
    
    double calculate_temporal_locality() const {
        // Measure how clustered accesses are in time
        return _temporal_clustering_coefficient;
    }
    
    double calculate_spatial_locality() const {
        // Measure how clustered accesses are in offset space
        return _spatial_clustering_coefficient;
    }

private:
    static constexpr size_t _min_pattern_size = 10;
    static constexpr size_t _max_history_size = 1000;
    
    circular_buffer<access_event> _access_history{_max_history_size};
    std::optional<access_event> _last_access;
    online_statistics _inter_arrival_stats;
    online_statistics _stride_stats;
    double _temporal_clustering_coefficient = 0.0;
    double _spatial_clustering_coefficient = 0.0;
    pattern_detector _pattern_detector;
};
```

#### 2. Pattern Recognition Engine

```cpp
class pattern_recognition_engine {
public:
    enum class pattern_type {
        sequential_forward,
        sequential_backward,
        strided,
        random,
        temporal_periodic,
        replay_pattern,
        unknown
    };
    
    struct recognized_pattern {
        pattern_type type;
        double confidence;
        pattern_parameters params;
        duration estimated_duration;
    };

    recognized_pattern recognize(const pattern_features& features) {
        // Use decision tree for initial classification
        auto initial_class = _decision_tree.classify(features);
        
        // Refine with pattern-specific models
        switch (initial_class) {
        case pattern_type::sequential_forward:
            return refine_sequential_pattern(features);
        case pattern_type::strided:
            return refine_strided_pattern(features);
        case pattern_type::temporal_periodic:
            return refine_periodic_pattern(features);
        default:
            return {.type = initial_class, .confidence = 0.5};
        }
    }

private:
    recognized_pattern refine_sequential_pattern(
        const pattern_features& features
    ) {
        // Check for pure sequential access
        if (features.stride_stddev < 0.1 && 
            features.stride_mean > 0 &&
            features.spatial_locality > 0.9) {
            
            return {
                .type = pattern_type::sequential_forward,
                .confidence = 0.95,
                .params = {
                    .stride = static_cast<size_t>(features.stride_mean),
                    .lookahead = calculate_optimal_lookahead(features)
                }
            };
        }
        
        return {.type = pattern_type::unknown, .confidence = 0.3};
    }
    
    recognized_pattern refine_strided_pattern(
        const pattern_features& features
    ) {
        // Detect fixed-stride patterns
        if (features.stride_stddev < features.stride_mean * 0.1) {
            auto stride = static_cast<size_t>(features.stride_mean);
            
            // Validate stride consistency
            if (validate_stride_pattern(stride)) {
                return {
                    .type = pattern_type::strided,
                    .confidence = 0.85,
                    .params = {
                        .stride = stride,
                        .lookahead = stride * _prefetch_depth
                    }
                };
            }
        }
        
        return {.type = pattern_type::unknown, .confidence = 0.4};
    }
    
    recognized_pattern refine_periodic_pattern(
        const pattern_features& features
    ) {
        // Use FFT to detect periodicity
        auto periods = detect_periods_fft(features);
        
        if (!periods.empty()) {
            return {
                .type = pattern_type::temporal_periodic,
                .confidence = periods[0].strength,
                .params = {
                    .period = periods[0].period,
                    .phase = periods[0].phase
                }
            };
        }
        
        return {.type = pattern_type::unknown, .confidence = 0.2};
    }

private:
    decision_tree _decision_tree;
    static constexpr size_t _prefetch_depth = 4;
};
```

#### 3. Markov Chain Predictor

```cpp
class markov_predictor {
public:
    struct state {
        model::offset offset;
        size_t segment_id;
    };
    
    struct prediction {
        std::vector<state> next_states;
        std::vector<double> probabilities;
        double confidence;
    };

    void update(const state& from, const state& to) {
        _transition_counts[from][to]++;
        _state_counts[from]++;
        
        // Decay old transitions
        if (++_update_count % _decay_interval == 0) {
            apply_decay();
        }
    }
    
    prediction predict(const state& current, size_t k = 3) {
        auto it = _transition_counts.find(current);
        if (it == _transition_counts.end()) {
            return {.confidence = 0.0};
        }
        
        // Calculate transition probabilities
        std::vector<std::pair<state, double>> transitions;
        double total_count = _state_counts[current];
        
        for (const auto& [next_state, count] : it->second) {
            double probability = count / total_count;
            transitions.emplace_back(next_state, probability);
        }
        
        // Sort by probability and take top-k
        std::partial_sort(
            transitions.begin(),
            transitions.begin() + std::min(k, transitions.size()),
            transitions.end(),
            [](const auto& a, const auto& b) {
                return a.second > b.second;
            }
        );
        
        // Build prediction
        prediction pred;
        for (size_t i = 0; i < std::min(k, transitions.size()); ++i) {
            pred.next_states.push_back(transitions[i].first);
            pred.probabilities.push_back(transitions[i].second);
        }
        
        // Calculate confidence based on entropy
        pred.confidence = calculate_confidence(pred.probabilities);
        
        return pred;
    }
    
    // Multi-step prediction using power method
    prediction predict_multi_step(const state& current, size_t steps) {
        auto matrix = build_transition_matrix();
        auto initial = state_to_vector(current);
        
        // Power iteration
        auto result = initial;
        for (size_t i = 0; i < steps; ++i) {
            result = matrix * result;
        }
        
        return vector_to_prediction(result);
    }

private:
    void apply_decay() {
        for (auto& [from, transitions] : _transition_counts) {
            for (auto& [to, count] : transitions) {
                count *= _decay_factor;
                if (count < _min_count_threshold) {
                    transitions.erase(to);
                }
            }
        }
    }
    
    double calculate_confidence(const std::vector<double>& probs) {
        // Use normalized entropy as confidence measure
        double entropy = 0.0;
        for (double p : probs) {
            if (p > 0) {
                entropy -= p * std::log2(p);
            }
        }
        
        double max_entropy = std::log2(probs.size());
        return 1.0 - (entropy / max_entropy);
    }

private:
    absl::flat_hash_map<state, absl::flat_hash_map<state, double>> 
        _transition_counts;
    absl::flat_hash_map<state, double> _state_counts;
    
    size_t _update_count = 0;
    static constexpr size_t _decay_interval = 1000;
    static constexpr double _decay_factor = 0.95;
    static constexpr double _min_count_threshold = 0.01;
};
```

#### 4. Adaptive Prefetch Scheduler

```cpp
class adaptive_prefetch_scheduler {
public:
    struct prefetch_request {
        model::ntp ntp;
        segment_id segment;
        priority priority;
        deadline deadline;
        size_t size_estimate;
        confidence confidence;
    };
    
    struct prefetch_stats {
        size_t hits;
        size_t misses;
        size_t evictions_before_use;
        double accuracy;
        size_t bytes_prefetched;
        size_t bytes_used;
    };

    ss::future<> schedule_prefetch(
        const prediction& pred,
        const resource_constraints& constraints
    ) {
        // Convert prediction to prefetch requests
        auto requests = build_prefetch_requests(pred);
        
        // Apply admission control
        requests = apply_admission_control(requests, constraints);
        
        // Schedule based on priority and deadline
        for (const auto& req : requests) {
            co_await schedule_single_prefetch(req);
        }
        
        // Update statistics
        _stats.bytes_prefetched += calculate_total_size(requests);
    }

private:
    std::vector<prefetch_request> build_prefetch_requests(
        const prediction& pred
    ) {
        std::vector<prefetch_request> requests;
        
        for (size_t i = 0; i < pred.next_states.size(); ++i) {
            requests.push_back({
                .segment = state_to_segment(pred.next_states[i]),
                .priority = calculate_priority(pred.probabilities[i]),
                .deadline = calculate_deadline(i),
                .confidence = pred.confidence * pred.probabilities[i]
            });
        }
        
        return requests;
    }
    
    std::vector<prefetch_request> apply_admission_control(
        std::vector<prefetch_request> requests,
        const resource_constraints& constraints
    ) {
        // Sort by utility (priority * confidence / size)
        std::sort(requests.begin(), requests.end(),
                 [](const auto& a, const auto& b) {
                     auto utility_a = a.priority * a.confidence / a.size_estimate;
                     auto utility_b = b.priority * b.confidence / b.size_estimate;
                     return utility_a > utility_b;
                 });
        
        // Apply constraints
        size_t total_size = 0;
        auto it = requests.begin();
        
        while (it != requests.end()) {
            if (total_size + it->size_estimate > constraints.memory_budget ||
                _active_prefetches.size() >= constraints.max_concurrent) {
                break;
            }
            
            total_size += it->size_estimate;
            ++it;
        }
        
        requests.erase(it, requests.end());
        return requests;
    }
    
    ss::future<> schedule_single_prefetch(const prefetch_request& req) {
        // Check if already in cache
        if (_cache_manager.contains(req.segment)) {
            _stats.hits++;
            co_return;
        }
        
        // Check if already being prefetched
        if (_active_prefetches.contains(req.segment)) {
            co_return;
        }
        
        // Schedule prefetch
        _active_prefetches.insert(req.segment);
        
        co_await _prefetch_queue.submit(
            req,
            [this](const prefetch_request& r) {
                return execute_prefetch(r);
            }
        );
    }
    
    ss::future<> execute_prefetch(const prefetch_request& req) {
        try {
            // Fetch from cloud storage
            auto data = co_await _cloud_storage.fetch_segment(req.segment);
            
            // Add to cache
            co_await _cache_manager.insert(
                req.segment,
                std::move(data),
                req.priority
            );
            
            _active_prefetches.erase(req.segment);
        } catch (...) {
            _stats.misses++;
            _active_prefetches.erase(req.segment);
        }
    }

private:
    prefetch_stats _stats;
    absl::flat_hash_set<segment_id> _active_prefetches;
    priority_queue<prefetch_request> _prefetch_queue;
    cache_manager& _cache_manager;
    cloud_storage& _cloud_storage;
};
```

#### 5. Cache Management with Prefetch Awareness

```cpp
class prefetch_aware_cache_manager {
public:
    struct cache_entry {
        segment_id id;
        ss::lw_shared_ptr<const segment_data> data;
        std::chrono::steady_clock::time_point last_access;
        std::chrono::steady_clock::time_point prefetch_time;
        bool was_prefetched;
        bool was_used;
        priority priority;
        size_t access_count;
    };

    ss::future<> insert(
        segment_id id,
        ss::lw_shared_ptr<const segment_data> data,
        priority prio,
        bool is_prefetch = true
    ) {
        // Check memory budget
        while (_current_size + data->size() > _max_size) {
            co_await evict_least_valuable();
        }
        
        _entries[id] = {
            .id = id,
            .data = data,
            .last_access = clock::now(),
            .prefetch_time = is_prefetch ? clock::now() : time_point{},
            .was_prefetched = is_prefetch,
            .was_used = false,
            .priority = prio,
            .access_count = 0
        };
        
        _current_size += data->size();
        
        if (is_prefetch) {
            _prefetch_metrics.total_prefetched++;
        }
    }
    
    std::optional<ss::lw_shared_ptr<const segment_data>> 
    get(segment_id id) {
        auto it = _entries.find(id);
        if (it == _entries.end()) {
            _metrics.misses++;
            return std::nullopt;
        }
        
        // Update access statistics
        it->second.last_access = clock::now();
        it->second.access_count++;
        
        if (it->second.was_prefetched && !it->second.was_used) {
            it->second.was_used = true;
            _prefetch_metrics.useful_prefetches++;
        }
        
        _metrics.hits++;
        return it->second.data;
    }

private:
    ss::future<> evict_least_valuable() {
        // Calculate value score for each entry
        auto calculate_value = [](const cache_entry& entry) {
            double recency_score = calculate_recency_score(entry);
            double frequency_score = entry.access_count;
            double prefetch_score = entry.was_prefetched ? 0.5 : 1.0;
            double usage_score = entry.was_used ? 1.0 : 0.2;
            
            return recency_score * frequency_score * 
                   prefetch_score * usage_score * entry.priority;
        };
        
        // Find entry with minimum value
        auto victim = std::min_element(
            _entries.begin(),
            _entries.end(),
            [&](const auto& a, const auto& b) {
                return calculate_value(a.second) < calculate_value(b.second);
            }
        );
        
        if (victim != _entries.end()) {
            _current_size -= victim->second.data->size();
            
            if (victim->second.was_prefetched && !victim->second.was_used) {
                _prefetch_metrics.wasted_prefetches++;
            }
            
            _entries.erase(victim);
        }
        
        co_return;
    }
    
    static double calculate_recency_score(const cache_entry& entry) {
        auto age = clock::now() - entry.last_access;
        return std::exp(-age.count() / 1e9);  // Exponential decay
    }

private:
    size_t _max_size;
    size_t _current_size = 0;
    absl::flat_hash_map<segment_id, cache_entry> _entries;
    
    struct {
        size_t hits = 0;
        size_t misses = 0;
    } _metrics;
    
    struct {
        size_t total_prefetched = 0;
        size_t useful_prefetches = 0;
        size_t wasted_prefetches = 0;
    } _prefetch_metrics;
};
```

### Configuration

```yaml
# Prefetching configuration
cloud_storage_prefetch_enabled: true
cloud_storage_prefetch_mode: adaptive  # off, sequential, adaptive
cloud_storage_prefetch_depth: 4
cloud_storage_prefetch_memory_budget: 1073741824  # 1GB
cloud_storage_prefetch_max_concurrent: 8
cloud_storage_pattern_detection_min_samples: 10
cloud_storage_markov_chain_order: 2
cloud_storage_confidence_threshold: 0.7
```

### Implementation Strategies

#### 1. Lightweight ML Models

```cpp
// Use simple, interpretable models that can run inline
class lightweight_predictor {
    // Linear regression for stride prediction
    struct linear_model {
        double slope;
        double intercept;
        double r_squared;
        
        double predict(double x) const {
            return slope * x + intercept;
        }
    };
    
    // Online learning with stochastic gradient descent
    void update_model(double x, double y) {
        double prediction = _model.predict(x);
        double error = y - prediction;
        
        // SGD update
        _model.slope += _learning_rate * error * x;
        _model.intercept += _learning_rate * error;
    }
    
private:
    linear_model _model;
    static constexpr double _learning_rate = 0.01;
};
```

#### 2. Fallback Mechanisms

```cpp
class prefetch_fallback_strategy {
    ss::future<> prefetch_with_fallback(
        const prediction& pred,
        const resource_constraints& constraints
    ) {
        if (pred.confidence < _confidence_threshold) {
            // Fall back to simple sequential prefetch
            co_return co_await sequential_prefetch(_current_offset);
        }
        
        if (constraints.memory_pressure > 0.8) {
            // Reduce prefetch depth under memory pressure
            co_return co_await limited_prefetch(pred, 1);
        }
        
        // Normal adaptive prefetch
        co_return co_await adaptive_prefetch(pred);
    }
    
private:
    static constexpr double _confidence_threshold = 0.5;
    model::offset _current_offset;
};
```

## Performance Analysis

### Expected Improvements

| Metric | Current | Proposed | Improvement |
|--------|---------|----------|-------------|
| Cold read latency (p50) | 100ms | 30ms | 70% |
| Cold read latency (p99) | 500ms | 250ms | 50% |
| Cache hit rate | 40% | 70% | 75% |
| Cloud egress (bytes) | 100GB/hour | 70GB/hour | 30% |
| CPU overhead | N/A | <2% | Acceptable |

### Cost-Benefit Analysis

```
Monthly savings (1PB stored, 10% active):
- Reduced egress: $500 (30% reduction)
- Improved efficiency: $200 (fewer redundant fetches)
- Total: $700/month per PB

Implementation cost:
- Development: 4-6 months
- ROI: 6-8 months
```

## Testing Strategy

### Unit Tests

```cpp
BOOST_AUTO_TEST_CASE(test_pattern_detection) {
    access_pattern_collector collector;
    
    // Generate sequential pattern
    for (size_t i = 0; i < 100; ++i) {
        collector.record_access({
            .offset = model::offset(i * 1024),
            .timestamp = clock::now()
        });
    }
    
    auto features = collector.extract_features();
    pattern_recognition_engine engine;
    auto pattern = engine.recognize(features);
    
    BOOST_REQUIRE_EQUAL(pattern.type, pattern_type::sequential_forward);
    BOOST_REQUIRE_GT(pattern.confidence, 0.9);
}

BOOST_AUTO_TEST_CASE(test_markov_prediction) {
    markov_predictor predictor;
    
    // Train with pattern
    for (size_t i = 0; i < 100; ++i) {
        predictor.update({i}, {i + 1});
    }
    
    auto pred = predictor.predict({50});
    BOOST_REQUIRE_EQUAL(pred.next_states[0].segment_id, 51);
    BOOST_REQUIRE_GT(pred.confidence, 0.95);
}
```

### Integration Tests

```cpp
SEASTAR_TEST_CASE(test_prefetch_effectiveness) {
    return run_test_with_cloud_storage([](cloud_storage_fixture& f) {
        // Create predictable workload
        auto reader = f.make_sequential_reader();
        
        // Enable prefetching
        f.enable_prefetch(prefetch_mode::adaptive);
        
        // Measure cold vs warm reads
        auto cold_latency = f.measure_read_latency(reader);
        auto warm_latency = f.measure_read_latency(reader);
        
        // Verify improvement
        BOOST_REQUIRE_LT(warm_latency, cold_latency * 0.5);
    });
}
```

## Metrics and Observability

### New Metrics

```cpp
namespace cloud_storage::prefetch_metrics {
    // Pattern detection metrics
    histogram pattern_detection_latency;
    counter patterns_detected;
    gauge active_patterns;
    
    // Prediction metrics
    histogram prediction_confidence;
    counter predictions_made;
    gauge prediction_accuracy;
    
    // Prefetch metrics
    counter prefetch_hits;
    counter prefetch_misses;
    counter prefetch_evictions;
    gauge prefetch_queue_size;
    histogram prefetch_latency;
    
    // Resource metrics
    gauge prefetch_memory_usage;
    gauge prefetch_bandwidth_usage;
}
```

## Migration Strategy

### Phase 1: Monitoring Only (Week 1-2)
- Deploy pattern detection without prefetching
- Collect access patterns and validate detection

### Phase 2: Conservative Prefetch (Week 3-4)
- Enable prefetching with high confidence threshold (0.9)
- Limit to sequential patterns only

### Phase 3: Adaptive Prefetch (Week 5-6)
- Lower confidence threshold to 0.7
- Enable all pattern types
- Monitor cache efficiency

### Phase 4: Full Rollout (Week 7-8)
- Enable by default for all deployments
- Tune parameters based on workload

## Open Questions

1. Should prefetching be partition-specific or global?
2. How to handle multi-tenant workloads with different patterns?
3. Should we persist learned patterns across restarts?
4. Integration with tiering policies?

## References

- [Adaptive Prefetching Strategies](https://example.com/prefetch)
- [Time Series Prediction Models](https://example.com/prediction)
- [Cache Replacement Algorithms](https://example.com/cache)