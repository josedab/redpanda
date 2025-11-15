# RFC-006: Enhanced Observability and Debugging

**Authors**: Redpanda Engineering Team  
**Status**: Proposed  
**Created**: 2025-01-15  
**Discussion**: [GitHub Issue #XXXX]

## Summary

This RFC proposes comprehensive observability and debugging enhancements for Redpanda including distributed tracing, enhanced metrics, live profiling, and a query language for logs, reducing MTTR by 50-70% and improving visibility into system behavior.

## Motivation

### Current State

Production debugging and performance analysis currently require manual correlation of logs, metrics, and system state with limited visibility into request flow and performance bottlenecks.

### Problems

1. **Limited Request Tracing**: No end-to-end visibility of request flow
2. **Metric Granularity**: Insufficient per-partition and client-level metrics
3. **Debugging Difficulty**: Hard to diagnose production issues without invasive actions
4. **Performance Analysis**: Limited profiling capabilities in production
5. **Log Analysis**: Unstructured logs difficult to query and correlate

### Use Cases

- Production incident diagnosis and root cause analysis
- Performance bottleneck identification
- Client-side issue debugging
- Capacity planning and optimization
- Compliance and audit requirements

## Detailed Design

### Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│              Observability Platform                        │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              Distributed Tracing                      │ │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────────────────┐ │ │
│  │  │  Trace   │  │  Span   │  │    Context         │ │ │
│  │  │  Context │  │  Store  │  │    Propagation     │ │ │
│  │  └─────────┘  └─────────┘  └─────────────────────┘ │ │
│  └─────────────────────────────────────────────────────┘ │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              Enhanced Metrics                         │ │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────────────────┐ │ │
│  │  │Partition│  │  Client │  │     Cost           │ │ │
│  │  │ Metrics │  │Attribution│ │    Tracking        │ │ │
│  │  └─────────┘  └─────────┘  └─────────────────────┘ │ │
│  └─────────────────────────────────────────────────────┘ │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              Live Profiling                           │ │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────────────────┐ │ │
│  │  │   CPU   │  │  Memory │  │      I/O           │ │ │
│  │  │ Profiler│  │ Profiler│  │    Profiler        │ │ │
│  │  └─────────┘  └─────────┘  └─────────────────────┘ │ │
│  └─────────────────────────────────────────────────────┘ │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              Structured Logging                       │ │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────────────────┐ │ │
│  │  │  Query  │  │  Index  │  │   Aggregation      │ │ │
│  │  │  Engine │  │  Builder│  │     Engine         │ │ │
│  │  └─────────┘  └─────────┘  └─────────────────────┘ │ │
│  └─────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

### Core Components

#### 1. Distributed Tracing System

```cpp
class distributed_tracing_system {
public:
    struct trace_context {
        trace_id id;
        span_id parent_span;
        span_id current_span;
        trace_flags flags;
        trace_state state;
        baggage baggage_items;
    };
    
    struct span {
        span_id id;
        trace_id trace;
        span_id parent;
        std::string name;
        span_kind kind;
        std::chrono::system_clock::time_point start_time;
        std::chrono::system_clock::time_point end_time;
        attributes attrs;
        std::vector<event> events;
        std::vector<link> links;
        status_code status;
    };

    class tracer {
    public:
        ss::future<span_handle> start_span(
            std::string_view name,
            const span_options& opts = {}
        ) {
            auto span = create_span(name, opts);
            
            // Set parent from context
            if (auto ctx = get_current_context()) {
                span.parent = ctx->current_span;
                span.trace = ctx->id;
            } else {
                // Root span
                span.trace = generate_trace_id();
            }
            
            span.id = generate_span_id();
            span.start_time = clock::now();
            
            // Store in context
            set_current_context({
                .id = span.trace,
                .parent_span = span.parent,
                .current_span = span.id
            });
            
            // Record span start
            co_await _recorder.record_start(span);
            
            co_return span_handle{std::move(span), this};
        }
        
        void inject_context(rpc::header& header, const trace_context& ctx) {
            // W3C Trace Context format
            header.set("traceparent", format_traceparent(ctx));
            
            if (!ctx.state.empty()) {
                header.set("tracestate", format_tracestate(ctx.state));
            }
            
            if (!ctx.baggage_items.empty()) {
                header.set("baggage", format_baggage(ctx.baggage_items));
            }
        }
        
        trace_context extract_context(const rpc::header& header) {
            trace_context ctx;
            
            if (auto traceparent = header.get("traceparent")) {
                parse_traceparent(*traceparent, ctx);
            }
            
            if (auto tracestate = header.get("tracestate")) {
                ctx.state = parse_tracestate(*tracestate);
            }
            
            if (auto baggage = header.get("baggage")) {
                ctx.baggage_items = parse_baggage(*baggage);
            }
            
            return ctx;
        }
        
    private:
        std::string format_traceparent(const trace_context& ctx) {
            // version-trace_id-parent_id-trace_flags
            return fmt::format(
                "00-{:032x}-{:016x}-{:02x}",
                ctx.id.value,
                ctx.current_span.value,
                ctx.flags.value
            );
        }
        
        void parse_traceparent(std::string_view tp, trace_context& ctx) {
            // Parse W3C traceparent format
            auto parts = split(tp, '-');
            if (parts.size() == 4) {
                ctx.id = trace_id{parse_hex(parts[1])};
                ctx.parent_span = span_id{parse_hex(parts[2])};
                ctx.flags = trace_flags{parse_hex(parts[3])};
            }
        }
        
    private:
        span_recorder _recorder;
        static thread_local std::optional<trace_context> _current_context;
    };

    class span_handle {
    public:
        ~span_handle() {
            if (_span) {
                finish();
            }
        }
        
        void set_attribute(std::string_view key, attribute_value value) {
            _span->attrs[std::string(key)] = std::move(value);
        }
        
        void add_event(std::string_view name, const attributes& attrs = {}) {
            _span->events.push_back({
                .name = std::string(name),
                .timestamp = clock::now(),
                .attributes = attrs
            });
        }
        
        void set_status(status_code code, std::string_view description = {}) {
            _span->status = code;
            if (!description.empty()) {
                set_attribute("status.description", std::string(description));
            }
        }
        
        ss::future<> finish() {
            _span->end_time = clock::now();
            co_await _tracer->_recorder.record_end(*_span);
            _span.reset();
        }
        
    private:
        std::optional<span> _span;
        tracer* _tracer;
    };

    // Instrumentation for Kafka API
    class kafka_api_tracer {
    public:
        ss::future<produce_response> trace_produce(
            produce_request req,
            produce_handler handler
        ) {
            auto span = co_await _tracer.start_span(
                "kafka.produce",
                {.kind = span_kind::server}
            );
            
            span.set_attribute("kafka.topic", req.topic);
            span.set_attribute("kafka.partition", req.partition);
            span.set_attribute("kafka.message_count", req.records.size());
            span.set_attribute("kafka.acks", req.acks);
            
            try {
                auto response = co_await handler(std::move(req));
                
                span.set_attribute("kafka.offset", response.base_offset);
                span.set_status(status_code::ok);
                
                co_return response;
            } catch (const std::exception& e) {
                span.set_status(status_code::error, e.what());
                throw;
            }
        }
        
        ss::future<fetch_response> trace_fetch(
            fetch_request req,
            fetch_handler handler
        ) {
            auto span = co_await _tracer.start_span(
                "kafka.fetch",
                {.kind = span_kind::server}
            );
            
            span.set_attribute("kafka.topic", req.topic);
            span.set_attribute("kafka.partition", req.partition);
            span.set_attribute("kafka.offset", req.fetch_offset);
            span.set_attribute("kafka.max_bytes", req.max_bytes);
            
            try {
                auto response = co_await handler(std::move(req));
                
                span.set_attribute("kafka.records_fetched", response.records.size());
                span.set_attribute("kafka.bytes_fetched", response.size_bytes());
                span.set_status(status_code::ok);
                
                co_return response;
            } catch (const std::exception& e) {
                span.set_status(status_code::error, e.what());
                throw;
            }
        }
        
    private:
        tracer _tracer;
    };

    // Instrumentation for Raft
    class raft_tracer {
    public:
        ss::future<> trace_append_entries(
            const append_entries_request& req,
            append_entries_handler handler
        ) {
            auto span = co_await _tracer.start_span(
                "raft.append_entries",
                {.kind = span_kind::internal}
            );
            
            span.set_attribute("raft.term", req.term);
            span.set_attribute("raft.leader_id", req.leader_id);
            span.set_attribute("raft.prev_log_index", req.prev_log_index);
            span.set_attribute("raft.entries_count", req.entries.size());
            
            co_await handler(req);
            
            span.set_status(status_code::ok);
        }
        
    private:
        tracer _tracer;
    };

private:
    tracer _tracer;
    kafka_api_tracer _kafka_tracer;
    raft_tracer _raft_tracer;
};\n```

#### 2. Enhanced Metrics System

```cpp
class enhanced_metrics_system {
public:
    // Per-partition metrics
    class partition_metrics {
    public:
        struct metrics {
            // Throughput metrics
            counter bytes_produced;
            counter bytes_consumed;
            counter records_produced;
            counter records_consumed;
            
            // Latency metrics
            histogram produce_latency;
            histogram fetch_latency;
            histogram commit_latency;
            
            // Storage metrics
            gauge log_size_bytes;
            gauge segment_count;
            gauge index_size_bytes;
            
            // Error metrics
            counter produce_errors;
            counter fetch_errors;
            counter out_of_order_records;
        };
        
        void record_produce(
            const model::ntp& ntp,
            size_t bytes,
            size_t records,
            duration latency
        ) {
            auto& m = get_or_create(ntp);
            m.bytes_produced += bytes;
            m.records_produced += records;
            m.produce_latency.observe(latency);
        }
        
        void record_fetch(
            const model::ntp& ntp,
            size_t bytes,
            size_t records,
            duration latency
        ) {
            auto& m = get_or_create(ntp);
            m.bytes_consumed += bytes;
            m.records_consumed += records;
            m.fetch_latency.observe(latency);
        }
        
    private:
        metrics& get_or_create(const model::ntp& ntp) {
            return _metrics[ntp];
        }
        
        absl::flat_hash_map<model::ntp, metrics> _metrics;
    };

    // Client attribution
    class client_metrics {
    public:
        struct client_id {
            std::string client_id;
            std::string user;
            std::string ip_address;
        };
        
        struct metrics {
            // Request metrics
            counter requests;
            counter errors;
            histogram request_latency;
            
            // Bandwidth metrics
            counter bytes_sent;
            counter bytes_received;
            
            // Resource usage
            gauge connection_count;
            counter cpu_time_us;
            counter memory_bytes;
        };
        
        void record_request(
            const client_id& client,
            request_type type,
            size_t bytes,
            duration latency,
            bool success
        ) {
            auto& m = get_or_create(client);
            m.requests++;
            
            if (!success) {
                m.errors++;
            }
            
            m.request_latency.observe(latency);
            
            if (is_produce(type)) {
                m.bytes_received += bytes;
            } else {
                m.bytes_sent += bytes;
            }
        }
        
        void record_resource_usage(
            const client_id& client,
            duration cpu_time,
            size_t memory_bytes
        ) {
            auto& m = get_or_create(client);
            m.cpu_time_us += cpu_time.count();
            m.memory_bytes += memory_bytes;
        }
        
        // Top clients by various metrics
        std::vector<std::pair<client_id, double>>
        top_clients_by_requests(size_t n = 10) {
            return top_clients_by_metric(
                n,
                [](const metrics& m) { return m.requests.value(); }
            );
        }
        
        std::vector<std::pair<client_id, double>>
        top_clients_by_bandwidth(size_t n = 10) {
            return top_clients_by_metric(
                n,
                [](const metrics& m) {
                    return m.bytes_sent.value() + m.bytes_received.value();
                }
            );
        }
        
    private:
        template<typename F>
        std::vector<std::pair<client_id, double>>
        top_clients_by_metric(size_t n, F metric_fn) {
            std::vector<std::pair<client_id, double>> clients;
            
            for (const auto& [id, metrics] : _metrics) {
                clients.emplace_back(id, metric_fn(metrics));
            }
            
            std::partial_sort(
                clients.begin(),
                clients.begin() + std::min(n, clients.size()),
                clients.end(),
                [](const auto& a, const auto& b) {
                    return a.second > b.second;
                }
            );
            
            clients.resize(std::min(n, clients.size()));
            return clients;
        }
        
        metrics& get_or_create(const client_id& client) {
            return _metrics[client];
        }
        
        absl::flat_hash_map<client_id, metrics> _metrics;
    };

    // Cost tracking
    class cost_tracker {
    public:
        struct operation_cost {
            duration cpu_time;
            size_t memory_bytes;
            size_t disk_io_bytes;
            size_t network_io_bytes;
        };
        
        ss::future<T> track_operation_cost(
            std::string_view operation,
            ss::future<T> fut
        ) {
            auto start_cpu = get_thread_cpu_time();
            auto start_mem = get_current_memory();
            
            auto result = co_await std::move(fut);
            
            auto end_cpu = get_thread_cpu_time();
            auto end_mem = get_current_memory();
            
            operation_cost cost{
                .cpu_time = end_cpu - start_cpu,
                .memory_bytes = end_mem - start_mem,
                .disk_io_bytes = get_disk_io_delta(),
                .network_io_bytes = get_network_io_delta()
            };
            
            record_cost(operation, cost);
            
            co_return result;
        }
        
        void record_cost(std::string_view operation, const operation_cost& cost) {
            _costs[std::string(operation)] += cost;
            
            // Update metrics
            _metrics.cpu_time_by_operation[std::string(operation)]
                .observe(cost.cpu_time);
            _metrics.memory_by_operation[std::string(operation)]
                .observe(cost.memory_bytes);
        }
        
        // Cost analysis
        std::vector<std::pair<std::string, operation_cost>>
        top_operations_by_cpu(size_t n = 10) {
            return top_operations_by_metric(
                n,
                [](const operation_cost& c) { return c.cpu_time.count(); }
            );
        }
        
    private:
        absl::flat_hash_map<std::string, operation_cost> _costs;
        
        struct {
            std::map<std::string, histogram> cpu_time_by_operation;
            std::map<std::string, histogram> memory_by_operation;
        } _metrics;
    };

private:
    partition_metrics _partition_metrics;
    client_metrics _client_metrics;
    cost_tracker _cost_tracker;
};\n```

#### 3. Live Profiling System

```cpp
class live_profiling_system {
public:
    struct profile_config {
        bool enable_cpu_profiling = true;
        bool enable_memory_profiling = true;
        bool enable_io_profiling = true;
        duration sampling_interval = 10ms;
        size_t max_stack_depth = 64;
        size_t ring_buffer_size = 1048576;  // 1MB
    };

    class cpu_profiler {
    public:
        ss::future<> start_profiling(profile_config config) {
            _config = config;
            _profiling = true;
            
            // Set up sampling timer
            struct sigevent sev;
            sev.sigev_notify = SIGEV_SIGNAL;
            sev.sigev_signo = SIGPROF;
            
            timer_create(CLOCK_THREAD_CPUTIME_ID, &sev, &_timer);
            
            // Install signal handler
            struct sigaction sa;
            sa.sa_sigaction = &cpu_profiler::sample_handler;
            sa.sa_flags = SA_SIGINFO | SA_RESTART;
            sigemptyset(&sa.sa_mask);
            sigaction(SIGPROF, &sa, nullptr);
            
            // Start timer
            struct itimerspec its;
            its.it_interval = to_timespec(config.sampling_interval);
            its.it_value = its.it_interval;
            timer_settime(_timer, 0, &its, nullptr);
            
            co_return;
        }
        
        ss::future<> stop_profiling() {
            _profiling = false;
            
            // Stop timer
            struct itimerspec its = {};
            timer_settime(_timer, 0, &its, nullptr);
            timer_delete(_timer);
            
            co_return;
        }
        
        ss::future<cpu_profile> get_profile() {
            cpu_profile profile;
            
            // Aggregate samples
            for (const auto& [stack, count] : _samples) {
                profile.add_sample(stack, count);
            }
            
            // Symbolize addresses
            co_await symbolize_profile(profile);
            
            co_return profile;
        }
        
    private:
        static void sample_handler(int sig, siginfo_t* info, void* context) {
            if (!_profiling) return;
            
            // Capture stack trace
            void* stack[64];
            int depth = backtrace(stack, 64);
            
            // Store in ring buffer
            stack_trace trace;
            trace.depth = depth;
            std::copy(stack, stack + depth, trace.addresses);
            
            _ring_buffer.push(trace);
            
            // Periodically aggregate
            if (++_sample_count % 1000 == 0) {
                aggregate_samples();
            }
        }
        
        static void aggregate_samples() {
            while (!_ring_buffer.empty()) {
                auto trace = _ring_buffer.pop();
                _samples[trace]++;
            }
        }
        
        ss::future<> symbolize_profile(cpu_profile& profile) {
            // Use addr2line or similar for symbolization
            for (auto& sample : profile.samples) {
                for (auto& frame : sample.stack_trace) {
                    frame.symbol = co_await symbolize_address(frame.address);
                }
            }
        }
        
    private:
        profile_config _config;
        static thread_local bool _profiling;
        timer_t _timer;
        static lock_free_ring_buffer<stack_trace> _ring_buffer;
        static absl::flat_hash_map<stack_trace, size_t> _samples;
        static std::atomic<size_t> _sample_count;
    };

    class memory_profiler {
    public:
        ss::future<> enable_allocation_tracking() {
            // Hook into allocator
            _original_malloc = malloc;
            _original_free = free;
            
            malloc = tracked_malloc;
            free = tracked_free;
            
            _tracking_enabled = true;
            
            co_return;
        }
        
        ss::future<memory_profile> get_allocation_profile() {
            memory_profile profile;
            
            // Aggregate allocation sites
            for (const auto& [stack, info] : _allocations) {
                profile.add_allocation_site(
                    stack,
                    info.count,
                    info.total_bytes,
                    info.current_bytes
                );
            }
            
            co_return profile;
        }
        
    private:
        static void* tracked_malloc(size_t size) {
            void* ptr = _original_malloc(size);
            
            if (_tracking_enabled && ptr) {
                // Capture allocation stack
                void* stack[32];
                int depth = backtrace(stack, 32);
                
                stack_trace trace;
                trace.depth = depth;
                std::copy(stack, stack + depth, trace.addresses);
                
                // Record allocation
                auto& info = _allocations[trace];
                info.count++;
                info.total_bytes += size;
                info.current_bytes += size;
                
                _ptr_to_size[ptr] = size;
                _ptr_to_stack[ptr] = trace;
            }
            
            return ptr;
        }
        
        static void tracked_free(void* ptr) {
            if (_tracking_enabled && ptr) {
                // Update allocation info
                if (auto it = _ptr_to_size.find(ptr); it != _ptr_to_size.end()) {
                    auto size = it->second;
                    auto stack = _ptr_to_stack[ptr];
                    
                    _allocations[stack].current_bytes -= size;
                    
                    _ptr_to_size.erase(it);
                    _ptr_to_stack.erase(ptr);
                }
            }
            
            _original_free(ptr);
        }
        
    private:
        static bool _tracking_enabled;
        static void* (*_original_malloc)(size_t);
        static void (*_original_free)(void*);
        
        struct allocation_info {
            size_t count = 0;
            size_t total_bytes = 0;
            size_t current_bytes = 0;
        };
        
        static absl::flat_hash_map<stack_trace, allocation_info> _allocations;
        static absl::flat_hash_map<void*, size_t> _ptr_to_size;
        static absl::flat_hash_map<void*, stack_trace> _ptr_to_stack;
    };

    // Export profiles in standard formats
    class profile_exporter {
    public:
        ss::future<std::string> export_pprof(const cpu_profile& profile) {
            // Export in pprof format
            pprof::Profile pprof_profile;
            
            for (const auto& sample : profile.samples) {
                auto* pprof_sample = pprof_profile.add_sample();
                
                for (const auto& frame : sample.stack_trace) {
                    auto location_id = get_or_create_location(frame);
                    pprof_sample->add_location_id(location_id);
                }
                
                pprof_sample->add_value(sample.count);
            }
            
            std::string output;
            pprof_profile.SerializeToString(&output);
            
            co_return output;
        }
        
        ss::future<std::string> export_flamegraph(const cpu_profile& profile) {
            // Export in FlameGraph format
            std::stringstream ss;
            
            for (const auto& sample : profile.samples) {
                // Build stack string
                std::string stack;
                for (auto it = sample.stack_trace.rbegin();
                     it != sample.stack_trace.rend();
                     ++it) {
                    if (!stack.empty()) stack += ";";
                    stack += it->symbol;
                }
                
                ss << stack << " " << sample.count << "\n";
            }
            
            co_return ss.str();
        }
    };

private:
    cpu_profiler _cpu_profiler;
    memory_profiler _memory_profiler;
    profile_exporter _exporter;
};\n```

#### 4. Structured Logging and Query System

```cpp
class structured_logging_system {
public:
    struct log_entry {
        std::chrono::system_clock::time_point timestamp;
        log_level level;
        std::string module;
        std::string message;
        json::object fields;
        std::optional<trace_id> trace_id;
        std::optional<span_id> span_id;
    };

    class structured_logger {
    public:
        template<typename... Args>
        void log(
            log_level level,
            std::string_view module,
            std::string_view format,
            Args&&... args
        ) {
            log_entry entry;
            entry.timestamp = clock::now();
            entry.level = level;
            entry.module = std::string(module);
            entry.message = fmt::format(format, std::forward<Args>(args)...);
            
            // Add trace context if available
            if (auto ctx = get_current_trace_context()) {
                entry.trace_id = ctx->id;
                entry.span_id = ctx->current_span;
            }
            
            // Extract structured fields
            extract_fields(entry.fields, args...);
            
            // Write to log
            write_log(entry);
        }
        
        structured_logger& with_field(std::string_view key, auto value) {
            _fields[std::string(key)] = json::value(value);
            return *this;
        }
        
    private:
        void write_log(const log_entry& entry) {
            // Write to ring buffer for querying
            _ring_buffer.push(entry);
            
            // Also write to file/stdout
            write_to_output(format_log_entry(entry));
        }
        
        std::string format_log_entry(const log_entry& entry) {
            json::object obj;
            obj["timestamp"] = format_timestamp(entry.timestamp);
            obj["level"] = to_string(entry.level);
            obj["module"] = entry.module;
            obj["message"] = entry.message;
            
            if (entry.trace_id) {
                obj["trace_id"] = format_trace_id(*entry.trace_id);
            }
            
            if (entry.span_id) {
                obj["span_id"] = format_span_id(*entry.span_id);
            }
            
            // Add custom fields
            for (const auto& [key, value] : entry.fields) {
                obj[key] = value;
            }
            
            return json::stringify(obj);
        }
        
    private:
        json::object _fields;
        static ring_buffer<log_entry> _ring_buffer;
    };

    class log_query_engine {
    public:
        struct query {
            std::optional<time_range> time_range;
            std::optional<log_level> min_level;
            std::optional<std::string> module_filter;
            std::optional<std::string> message_pattern;
            std::map<std::string, json::value> field_filters;
            std::optional<trace_id> trace_filter;
        };
        
        ss::future<std::vector<log_entry>> execute_query(const query& q) {
            std::vector<log_entry> results;
            
            // Scan log entries
            co_await _index.scan(
                q.time_range.value_or(time_range::all()),
                [&](const log_entry& entry) {
                    if (matches_query(entry, q)) {
                        results.push_back(entry);
                    }
                    return results.size() < _max_results;
                }
            );
            
            co_return results;
        }
        
        // Aggregation queries
        ss::future<json::object> aggregate(
            const query& q,
            aggregation_type agg_type,
            std::string_view field
        ) {
            json::object result;
            
            switch (agg_type) {
            case aggregation_type::count:
                result["count"] = co_await count_matching(q);
                break;
                
            case aggregation_type::group_by:
                result["groups"] = co_await group_by(q, field);
                break;
                
            case aggregation_type::histogram:
                result["histogram"] = co_await histogram(q, field);
                break;
            }
            
            co_return result;
        }
        
        // Pattern detection
        ss::future<std::vector<log_pattern>> detect_patterns(
            const time_range& range,
            double min_frequency = 0.01
        ) {
            // Collect log messages
            std::vector<std::string> messages;
            co_await _index.scan(
                range,
                [&messages](const log_entry& entry) {
                    messages.push_back(entry.message);
                    return messages.size() < _pattern_sample_size;
                }
            );
            
            // Extract patterns using template mining
            auto patterns = extract_patterns(messages, min_frequency);
            
            co_return patterns;
        }
        
    private:
        bool matches_query(const log_entry& entry, const query& q) {
            if (q.min_level && entry.level < *q.min_level) {
                return false;
            }
            
            if (q.module_filter && entry.module != *q.module_filter) {
                return false;
            }
            
            if (q.message_pattern) {
                std::regex pattern(*q.message_pattern);
                if (!std::regex_search(entry.message, pattern)) {
                    return false;
                }
            }
            
            for (const auto& [key, value] : q.field_filters) {
                if (auto it = entry.fields.find(key); it != entry.fields.end()) {
                    if (it->second != value) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            
            if (q.trace_filter && entry.trace_id != q.trace_filter) {
                return false;
            }
            
            return true;
        }
        
        std::vector<log_pattern> extract_patterns(
            const std::vector<std::string>& messages,
            double min_frequency
        ) {
            // Simple pattern extraction using token analysis
            std::map<std::string, size_t> template_counts;
            
            for (const auto& msg : messages) {
                auto template_str = extract_template(msg);
                template_counts[template_str]++;
            }
            
            std::vector<log_pattern> patterns;
            size_t total = messages.size();
            
            for (const auto& [tmpl, count] : template_counts) {
                double frequency = static_cast<double>(count) / total;
                if (frequency >= min_frequency) {
                    patterns.push_back({
                        .template_string = tmpl,
                        .frequency = frequency,
                        .count = count
                    });
                }
            }
            
            return patterns;
        }
        
    private:
        log_index _index;
        static constexpr size_t _max_results = 10000;
        static constexpr size_t _pattern_sample_size = 100000;
    };

    // Real-time log aggregation
    class log_aggregator {
    public:
        ss::future<> start_aggregation() {
            while (!_as.abort_requested()) {
                co_await aggregate_window();
                co_await ss::sleep(_aggregation_interval);
            }
        }
        
        ss::future<> aggregate_window() {
            auto now = clock::now();
            auto window_start = now - _window_size;
            
            // Aggregate by level
            absl::flat_hash_map<log_level, size_t> level_counts;
            
            // Aggregate by module
            absl::flat_hash_map<std::string, size_t> module_counts;
            
            // Error rate calculation
            size_t total = 0;
            size_t errors = 0;
            
            co_await _index.scan(
                {window_start, now},
                [&](const log_entry& entry) {
                    level_counts[entry.level]++;
                    module_counts[entry.module]++;
                    total++;
                    
                    if (entry.level >= log_level::error) {
                        errors++;
                    }
                    
                    return true;
                }
            );
            
            // Update metrics
            for (const auto& [level, count] : level_counts) {
                _metrics.logs_by_level[level] = count;
            }
            
            _metrics.error_rate = static_cast<double>(errors) / total;
        }
        
    private:
        duration _window_size = 1min;
        duration _aggregation_interval = 10s;
        log_index& _index;
        ss::abort_source _as;
        
        struct {
            std::map<log_level, gauge> logs_by_level;
            gauge error_rate;
        } _metrics;
    };

private:
    structured_logger _logger;
    log_query_engine _query_engine;
    log_aggregator _aggregator;
};\n```

### Configuration

```yaml
# Observability configuration
observability_tracing_enabled: true
observability_tracing_sampling_rate: 0.01  # 1% sampling
observability_tracing_exporter: "otlp"
observability_tracing_endpoint: "http://localhost:4317"

observability_metrics_partition_level: true
observability_metrics_client_attribution: true
observability_metrics_cost_tracking: true
observability_metrics_export_interval: 10s

observability_profiling_enabled: true
observability_profiling_cpu_enabled: true
observability_profiling_memory_enabled: true
observability_profiling_sampling_interval: 10ms

observability_logging_structured: true
observability_logging_query_enabled: true
observability_logging_retention_days: 7
observability_logging_index_size_mb: 1024
```

### Integration Examples

#### 1. Request Flow Visualization

```cpp
class request_flow_visualizer {
    ss::future<flow_graph> visualize_request(trace_id trace) {
        // Collect all spans for trace
        auto spans = co_await collect_spans(trace);
        
        // Build dependency graph
        flow_graph graph;
        
        for (const auto& span : spans) {
            graph.add_node(span.id, {
                .name = span.name,
                .start = span.start_time,
                .end = span.end_time,
                .status = span.status
            });
            
            if (span.parent) {
                graph.add_edge(span.parent, span.id);
            }
        }
        
        co_return graph;
    }
};
```

#### 2. Performance Bottleneck Detection

```cpp
class bottleneck_detector {
    ss::future<std::vector<bottleneck>> detect_bottlenecks() {
        std::vector<bottleneck> bottlenecks;
        
        // Analyze CPU profile
        auto cpu_profile = co_await get_cpu_profile();
        auto hot_functions = find_hot_functions(cpu_profile);
        
        for (const auto& func : hot_functions) {
            if (func.cpu_percentage > 20.0) {
                bottlenecks.push_back({
                    .type = bottleneck_type::cpu,
                    .location = func.name,
                    .impact = func.cpu_percentage
                });
            }
        }
        
        // Analyze memory allocations
        auto memory_profile = co_await get_memory_profile();
        auto hot_allocations = find_hot_allocations(memory_profile);
        
        for (const auto& alloc : hot_allocations) {
            if (alloc.bytes_percentage > 30.0) {
                bottlenecks.push_back({
                    .type = bottleneck_type::memory,
                    .location = alloc.stack_trace,
                    .impact = alloc.bytes_percentage
                });
            }
        }
        
        co_return bottlenecks;
    }
};
```

## Testing Strategy

### Unit Tests

```cpp
BOOST_AUTO_TEST_CASE(test_distributed_tracing) {
    distributed_tracing_system tracing;
    
    auto span = tracing.start_span("test_operation").get();
    span.set_attribute("test.key", "value");
    
    // Verify context propagation
    rpc::header header;
    tracing.inject_context(header, get_current_context());
    
    auto extracted = tracing.extract_context(header);
    BOOST_REQUIRE_EQUAL(extracted.trace_id, get_current_context().trace_id);
}

BOOST_AUTO_TEST_CASE(test_structured_logging_query) {
    structured_logging_system logging;
    
    // Write test logs
    logging.log(log_level::info, "test", "Message 1", "key1", "value1");
    logging.log(log_level::error, "test", "Error message", "key2", "value2");
    
    // Query logs
    log_query_engine::query q;
    q.min_level = log_level::error;
    
    auto results = logging.execute_query(q).get();
    BOOST_REQUIRE_EQUAL(results.size(), 1);
    BOOST_REQUIRE_EQUAL(results[0].message, "Error message");
}
```

### Performance Impact Tests

```cpp
PERF_TEST(tracing_overhead) {
    // Measure overhead with tracing disabled
    auto baseline = measure_request_latency(tracing_disabled);
    
    // Measure overhead with tracing enabled
    auto with_tracing = measure_request_latency(tracing_enabled);
    
    auto overhead = (with_tracing - baseline) / baseline;
    BOOST_REQUIRE_LT(overhead, 0.02);  // Less than 2% overhead
}
```

## Migration Strategy

### Phase 1: Tracing Infrastructure (Week 1-2)
- Deploy tracing system
- Instrument critical paths
- Set up trace collection

### Phase 2: Enhanced Metrics (Week 3-4)
- Enable partition-level metrics
- Deploy client attribution
- Set up dashboards

### Phase 3: Profiling (Week 5-6)
- Enable CPU profiling
- Deploy memory tracking
- Create profile analysis tools

### Phase 4: Structured Logging (Week 7-8)
- Migrate to structured logs
- Deploy query engine
- Set up log aggregation

## Open Questions

1. Which tracing backend to use (Jaeger, Zipkin, OTLP)?
2. Sampling strategy for high-volume deployments?
3. How long to retain trace and profile data?
4. Integration with existing monitoring tools?

## References

- [OpenTelemetry Specification](https://opentelemetry.io/docs/)
- [W3C Trace Context](https://www.w3.org/TR/trace-context/)
- [Linux Perf Tools](https://perf.wiki.kernel.org/)