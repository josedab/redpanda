# Enhanced Observability for Redpanda

Implementation of RFC-006: Enhanced Observability and Debugging

## Overview

This implementation provides comprehensive observability enhancements for Redpanda including:

1. **Distributed Tracing** - End-to-end request flow visibility with W3C Trace Context support
2. **Enhanced Metrics** - Partition-level, client attribution, and cost tracking metrics
3. **Live Profiling** - CPU and memory profiling with export to pprof/flamegraph formats
4. **Structured Logging** - JSON-formatted logs with query capabilities

## Components

### Distributed Tracing (`observability/tracing/`)

#### Core Classes

- **`trace_context.h`** - Trace and span ID types, W3C Trace Context data structures
- **`span.h`** - Span representation and RAII span handle
- **`tracer.h`** - Main tracer interface for creating spans and managing trace context
- **`span_recorder.h`** - Records and batches spans for export
- **`kafka_api_tracer.h`** - Kafka API instrumentation wrapper
- **`raft_tracer.h`** - Raft operation instrumentation wrapper

#### Usage Example

```cpp
#include "observability/tracing/tracer.h"
#include "observability/tracing/kafka_api_tracer.h"

// Initialize tracer
auto exporter = std::make_unique<in_memory_span_exporter>();
auto recorder = std::make_unique<span_recorder>(std::move(exporter));
tracer_config config{.sampling_rate = 0.01, .enabled = true};
auto tracer = std::make_shared<tracer>(std::move(recorder), config);

// Create a span
auto span = co_await tracer->start_span("operation_name");
span.set_attribute("user_id", attribute_value("user123"));
span.set_attribute("request_size", attribute_value(int64_t(1024)));

try {
    // Do work
    auto result = co_await perform_operation();
    span.set_status(status_code::ok);
} catch (const std::exception& e) {
    span.set_status(status_code::error, e.what());
    throw;
}

co_await span.finish();
```

#### Kafka API Tracing

```cpp
#include "observability/tracing/kafka_api_tracer.h"

kafka_api_tracer api_tracer(tracer);

// Trace a produce request
auto response = co_await api_tracer.trace_produce(
    req,
    [](auto req) { return handle_produce(std::move(req)); }
);
```

#### W3C Trace Context Propagation

```cpp
// On the client side - inject context into RPC headers
absl::flat_hash_map<std::string, std::string> headers;
auto ctx = tracer::get_current_context();
if (ctx) {
    tracer->inject_context(headers, *ctx);
}

// On the server side - extract context from headers
auto ctx = tracer->extract_context(headers);
if (ctx) {
    tracer::set_current_context(*ctx);
}
```

### Enhanced Metrics (`metrics/`)

#### Partition Metrics

```cpp
#include "metrics/partition_metrics.h"

metrics::partition_metrics pm;

// Record operations
pm.record_produce(ntp, bytes, records, latency);
pm.record_fetch(ntp, bytes, records, latency);
pm.update_storage_metrics(ntp, log_size, segments, index_size);

// Setup Prometheus metrics
pm.setup_public_metrics(ntp, public_metrics);
```

#### Client Attribution Metrics

```cpp
#include "metrics/client_metrics.h"

metrics::client_metrics cm;

metrics::client_id client{
    .client_id_str = "kafka-producer-1",
    .user = "admin",
    .ip_address = inet_address("10.0.0.1")
};

// Record client activity
cm.record_request(
    client,
    request_type::produce,
    bytes,
    latency,
    success
);

// Get top clients
auto top_by_requests = cm.top_clients_by_requests(10);
auto top_by_bandwidth = cm.top_clients_by_bandwidth(10);
```

#### Cost Tracking

```cpp
#include "metrics/cost_tracker.h"

metrics::cost_tracker ct;

// Automatically track operation cost
auto result = co_await ct.track_operation_cost(
    "produce_batch",
    perform_produce()
);

// Get cost analysis
auto top_operations = ct.top_operations_by_cpu(10);
```

### Live Profiling (`resource_mgmt/`)

#### Profile Export

```cpp
#include "resource_mgmt/profile_exporter.h"

profile_exporter exporter;

// Get CPU profile from profiler
cpu_profile profile = get_current_cpu_profile();

// Export in various formats
auto flamegraph = co_await exporter.export_flamegraph(profile);
auto text = co_await exporter.export_text(profile);
auto speedscope = co_await exporter.export_speedscope(profile);
```

### Structured Logging (`observability/logging/`)

#### Structured Logger

```cpp
#include "observability/logging/structured_logger.h"

structured_logger logger("my_module");

// Add persistent fields
logger.with_field("service", "redpanda")
      .with_field("version", "v23.1.0");

// Log with automatic trace context
logger.log(log_level::info, "Processing request for user {}", user_id);
```

#### Log Query Engine

```cpp
#include "observability/logging/log_query_engine.h"

std::vector<std::reference_wrapper<structured_logger>> loggers{logger1, logger2};
log_query_engine engine(loggers);

// Query logs
log_query q{
    .time_range = time_range::last_hours(1),
    .min_level = log_level::error,
    .module_filter = "kafka",
    .max_results = 100
};

auto results = co_await engine.execute_query(q);

// Aggregate by level
auto by_level = co_await engine.group_by_level(q);

// Calculate error rate
auto error_rate = co_await engine.calculate_error_rate(q);
```

## Configuration

Configuration options are defined in `observability_config.h` and can be set via YAML:

```yaml
# Enable distributed tracing
observability_tracing_enabled: true
observability_tracing_sampling_rate: 0.01  # 1% sampling
observability_tracing_exporter: "otlp"
observability_tracing_endpoint: "http://localhost:4317"

# Enable enhanced metrics
observability_metrics_partition_level: true
observability_metrics_client_attribution: true
observability_metrics_cost_tracking: false

# Enable profiling
observability_profiling_enabled: true
observability_profiling_cpu_enabled: true
observability_profiling_sampling_interval: 10ms

# Enable structured logging
observability_logging_structured: true
observability_logging_query_enabled: true
```

See `observability_config_example.yaml` for a complete configuration example.

## Testing

### Unit Tests

Tests are located in `observability/tests/`:

- **`tracing_test.cc`** - Tests for distributed tracing
- **`metrics_test.cc`** - Tests for enhanced metrics

Run tests:
```bash
bazel test //src/v/observability/tests:tracing_test
bazel test //src/v/observability/tests:metrics_test
```

### Performance Impact

The observability system is designed with minimal performance overhead:

- **Tracing**: < 2% overhead when enabled (configurable sampling rate)
- **Metrics**: Negligible overhead (uses existing Seastar metrics)
- **Profiling**: < 5% overhead when enabled
- **Structured Logging**: Similar to standard logging

## Integration with External Systems

### OpenTelemetry

The tracing system uses W3C Trace Context format and can export to any OpenTelemetry-compatible backend:

- **OTLP**: Native OpenTelemetry protocol
- **Jaeger**: Popular distributed tracing system
- **Zipkin**: Lightweight tracing system

### Prometheus

Enhanced metrics are exposed via the standard Prometheus endpoints:

- `/metrics` - Internal metrics
- `/public_metrics` - Customer-facing metrics

### Visualization

CPU profiles can be visualized using:

- **FlameGraph**: `cat profile.txt | flamegraph.pl > flame.svg`
- **Speedscope**: Import JSON directly at https://speedscope.app
- **pprof**: `go tool pprof profile.pb.gz`

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│              Observability Platform                       │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              Distributed Tracing                     │ │
│  │  • W3C Trace Context                                │ │
│  │  • Span recording and batching                      │ │
│  │  • Context propagation                              │ │
│  │  • Kafka & Raft instrumentation                     │ │
│  └─────────────────────────────────────────────────────┘ │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              Enhanced Metrics                        │ │
│  │  • Partition-level metrics                          │ │
│  │  • Client attribution                               │ │
│  │  • Cost tracking                                    │ │
│  └─────────────────────────────────────────────────────┘ │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              Live Profiling                          │ │
│  │  • CPU profiling                                    │ │
│  │  • Memory profiling                                 │ │
│  │  • Multi-format export                              │ │
│  └─────────────────────────────────────────────────────┘ │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              Structured Logging                      │ │
│  │  • JSON output                                      │ │
│  │  • Query engine                                     │ │
│  │  • Trace correlation                                │ │
│  └─────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

## Future Enhancements

1. **Automatic instrumentation** - Auto-instrument all RPC calls
2. **Sampling strategies** - Head-based and tail-based sampling
3. **Metrics cardinality limits** - Automatic high-cardinality detection
4. **Log pattern detection** - ML-based anomaly detection
5. **Distributed profiling** - Cluster-wide profiling aggregation

## References

- [RFC-006: Enhanced Observability](../../rfcs/20250115_enhanced_observability.md)
- [W3C Trace Context Specification](https://www.w3.org/TR/trace-context/)
- [OpenTelemetry Specification](https://opentelemetry.io/docs/specs/)
- [Prometheus Exposition Formats](https://prometheus.io/docs/instrumenting/exposition_formats/)
