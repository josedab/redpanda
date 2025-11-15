# Advanced Compaction Strategies

This module implements advanced compaction strategies for Redpanda as specified in RFC-005.

## Overview

The advanced compaction strategies improve storage efficiency by 30-50% while reducing impact on foreground operations through intelligent scheduling, incremental processing, and adaptive tuning.

## Architecture

```
compaction_strategies/
├── base/                  # Base types and interfaces
│   ├── types.h           # Core data structures
│   ├── config.h          # Configuration options
│   ├── metrics.h/.cc     # Metrics and observability
├── workload_aware/        # Workload-aware scheduling
│   ├── scheduler.h/.cc   # Intelligent compaction scheduler
├── incremental/           # Incremental compaction
│   ├── engine.h/.cc      # Incremental processing engine
├── hybrid/                # Hybrid compaction strategy
│   ├── strategy.h/.cc    # Combined time/key-based compaction
├── cloud_native/          # Cloud-native optimizations
│   ├── compaction.h/.cc  # Cloud storage compaction
├── adaptive/              # Adaptive strategy selection
│   ├── strategy.h/.cc    # ML-based parameter tuning
├── integration/           # Integration with existing code
└── tests/                 # Unit tests and benchmarks
    ├── scheduler_test.cc
    ├── incremental_test.cc
    └── benchmark_test.cc
```

## Components

### 1. Workload-Aware Scheduler

Analyzes access patterns, dead data ratios, and workload characteristics to intelligently schedule compactions.

**Key Features:**
- Access pattern analysis
- Dead data ratio tracking
- Fragmentation monitoring
- Resource-aware scheduling
- Priority-based execution

**Usage:**
```cpp
#include "compaction_strategies/workload_aware/scheduler.h"

intelligent_compaction_scheduler scheduler;
cluster_state state;
auto decisions = scheduler.schedule_compactions(state).get();
```

### 2. Incremental Compaction Engine

Processes compaction in chunks with pause/resume support to minimize impact on foreground operations.

**Key Features:**
- Chunk-based processing
- Automatic pause/resume
- Checkpoint-based recovery
- Resource monitoring
- Adaptive chunk sizing

**Usage:**
```cpp
#include "compaction_strategies/incremental/engine.h"

incremental_compaction_engine engine;
compaction_request request;
compaction_options opts;
engine.compact_incrementally(request, opts).get();
```

### 3. Hybrid Compaction Strategy

Combines time-based and key-based compaction for optimal efficiency.

**Key Features:**
- Key distribution analysis
- Temporal pattern detection
- Strategy selection per segment
- Optimized execution order

**Usage:**
```cpp
#include "compaction_strategies/hybrid/strategy.h"

hybrid_compaction_strategy strategy;
std::vector<segment_metadata> segments;
auto plan = strategy.create_hybrid_plan(segments).get();
```

### 4. Cloud-Native Compaction

Optimized compaction for tiered storage and cloud deployments.

**Key Features:**
- Parallel download/upload
- Segment size optimization
- Lambda function support
- Manifest management

**Usage:**
```cpp
#include "compaction_strategies/cloud_native/compaction.h"

cloud_native_compaction compactor;
cloud_ntp ntp;
std::vector<cloud_segment_metadata> segments;
compactor.compact_cloud_segments(ntp, segments).get();
```

### 5. Adaptive Strategy

Automatically tunes compaction parameters based on performance metrics.

**Key Features:**
- Performance monitoring
- Strategy effectiveness analysis
- Automatic parameter tuning
- Performance-based strategy selection

**Usage:**
```cpp
#include "compaction_strategies/adaptive/strategy.h"

adaptive_compaction_strategy strategy;
model::ntp ntp;
strategy.adapt_strategy(ntp).get();
```

## Configuration

Configuration options are defined in `base/config.h`:

```cpp
advanced_compaction_config config;
config.workload_aware_enabled = true;
config.incremental_enabled = true;
config.incremental_chunk_size = 1048576;  // 1MB
config.hybrid_strategy_enabled = true;
config.cloud_native_enabled = true;
config.adaptive_tuning_enabled = true;
config.dead_ratio_threshold = 0.5;
config.max_concurrent_compactions = 2;
```

## Metrics

The module exposes comprehensive metrics through `base/metrics.h`:

### Scheduling Metrics
- `compaction_score_distribution` - Distribution of compaction scores
- `compaction_decisions_made` - Total decisions made
- `pending_compactions` - Number of pending compactions

### Execution Metrics
- `incremental_chunk_size` - Size of processing chunks
- `compaction_pauses` - Number of pauses
- `compaction_duration` - Duration of compactions

### Effectiveness Metrics
- `dead_data_ratio` - Ratio of dead data
- `space_amplification` - Space amplification factor
- `write_amplification` - Write amplification factor

### Strategy Metrics
- `strategy_changes` - Number of strategy changes
- `strategy_effectiveness_score` - Effectiveness scores

## Testing

### Unit Tests
```bash
# Run scheduler tests
ninja compaction_strategies_scheduler_test
./build/release/src/v/compaction_strategies/tests/compaction_strategies_scheduler_test

# Run incremental tests
ninja compaction_strategies_incremental_test
./build/release/src/v/compaction_strategies/tests/compaction_strategies_incremental_test
```

### Performance Benchmarks
```bash
# Run benchmarks
ninja compaction_strategies_benchmark
./build/release/src/v/compaction_strategies/tests/compaction_strategies_benchmark
```

## Integration

The advanced compaction strategies integrate with the existing Redpanda storage layer:

1. **Storage Layer Integration** - Via `storage/disk_log_impl.h`
2. **Configuration** - Via `storage/log_config`
3. **Metrics Collection** - Via `storage/log_manager.h`
4. **PID Control** - Via `storage/compaction_controller.h`

## Performance Characteristics

Based on the RFC goals:

- **Storage Efficiency**: 30-50% improvement
- **Latency Impact**: < 10% increase during compaction
- **Write Amplification**: Target < 5.0
- **Space Amplification**: Target < 1.5
- **Throughput**: Minimal degradation during compaction

## Future Enhancements

1. Machine learning-based workload prediction
2. Multi-tenant aware scheduling
3. Cost-based optimization for cloud deployments
4. Integration with backup and restore
5. Cross-partition compaction coordination

## References

- RFC-005: Advanced Compaction Strategies (`/rfcs/20250115_advanced_compaction_strategies.md`)
- Existing compaction: `src/v/compaction/`
- Storage layer: `src/v/storage/`
