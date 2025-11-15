# RFC 011: Query Engine for Streaming Analytics

## Summary

This RFC proposes implementing a native SQL query engine within Redpanda for real-time analytics on streaming data. This eliminates the need for external stream processing frameworks like Flink or ksqlDB, providing a fully integrated solution for streaming analytics with SQL semantics, materialized views, and windowing functions.

## Motivation

### Current State

Currently, Redpanda users must deploy separate stream processing systems for analytics:
- **Apache Flink**: Complex deployment and operational overhead
- **ksqlDB**: Requires Kafka Connect and additional infrastructure
- **Spark Streaming**: Heavy resource requirements
- **Custom Applications**: High development and maintenance costs

### Problems

1. **Complexity**: Managing separate systems for storage and analytics
2. **Latency**: Data movement between systems adds delays
3. **Cost**: Additional infrastructure and operational overhead
4. **Consistency**: Maintaining data consistency across systems
5. **Learning Curve**: Different APIs and operational models

### Benefits

Implementing a native query engine provides:
- **Simplicity**: Single system for streaming and analytics
- **Performance**: Co-located computation and storage
- **Cost Efficiency**: Reduced infrastructure requirements
- **Developer Experience**: Familiar SQL interface
- **Real-time Insights**: Sub-second query latency

## Detailed Design

### 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                     Query Engine Architecture                 │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                    SQL Parser Layer                   │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────────┐ │   │
│  │  │   Lexer    │→ │   Parser   │→ │  AST Builder   │ │   │
│  │  └────────────┘  └────────────┘  └────────────────┘ │   │
│  └──────────────────────────────────────────────────────┘   │
│                              ↓                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                  Query Planning Layer                 │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────────┐ │   │
│  │  │  Analyzer  │→ │ Optimizer  │→ │ Plan Builder   │ │   │
│  │  └────────────┘  └────────────┘  └────────────────┘ │   │
│  └──────────────────────────────────────────────────────┘   │
│                              ↓                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                  Execution Engine Layer               │   │
│  │  ┌────────────────────────────────────────────────┐  │   │
│  │  │          Physical Operator Pipeline            │  │   │
│  │  │  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────────┐ │  │   │
│  │  │  │ Scan │→ │Filter│→ │ Join │→ │ Aggregate│ │  │   │
│  │  │  └──────┘  └──────┘  └──────┘  └──────────┘ │  │   │
│  │  └────────────────────────────────────────────────┘  │   │
│  └──────────────────────────────────────────────────────┘   │
│                              ↓                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                   Storage Integration                  │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────────┐ │   │
│  │  │   Kafka    │  │ Materialize│  │     Index      │ │   │
│  │  │  Reader    │  │    View    │  │    Manager     │ │   │
│  │  └────────────┘  └────────────┘  └────────────────┘ │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

### 2. SQL Parser Implementation

```cpp
namespace redpanda::sql {

// Abstract Syntax Tree nodes
struct ast_node {
    enum class type {
        select,
        from,
        where,
        group_by,
        having,
        order_by,
        window,
        join,
        expression,
        literal,
        identifier,
        function_call
    };
    
    type node_type;
    std::vector<std::unique_ptr<ast_node>> children;
    std::any value;
};

// SQL Parser using ANTLR4 or custom parser
class sql_parser {
public:
    struct parse_result {
        std::unique_ptr<ast_node> ast;
        std::vector<parse_error> errors;
        parse_metadata metadata;
    };
    
    ss::future<parse_result> parse(std::string_view sql) {
        co_return co_await _worker.submit([sql = std::string(sql)]() {
            // Tokenize SQL
            auto tokens = tokenize(sql);
            
            // Build AST
            parser_state state;
            auto ast = parse_statement(tokens, state);
            
            // Validate syntax
            auto errors = validate_syntax(ast);
            
            return parse_result{
                .ast = std::move(ast),
                .errors = std::move(errors),
                .metadata = extract_metadata(ast)
            };
        });
    }
    
private:
    // Recursive descent parser for SQL grammar
    std::unique_ptr<ast_node> parse_statement(
        token_stream& tokens,
        parser_state& state
    );
    
    std::unique_ptr<ast_node> parse_select(
        token_stream& tokens,
        parser_state& state
    );
    
    std::unique_ptr<ast_node> parse_expression(
        token_stream& tokens,
        parser_state& state,
        int precedence = 0
    );
    
    ss::smp_submit_to_options _worker{.smp_group = ss::smp_service_group(1)};
};

} // namespace redpanda::sql
```

### 3. Query Planning and Optimization

```cpp
namespace redpanda::sql {

// Logical plan nodes
struct logical_plan {
    enum class op_type {
        table_scan,
        stream_scan,
        filter,
        project,
        join,
        aggregate,
        window,
        sort,
        limit,
        union_all,
        materialize
    };
    
    op_type operation;
    std::vector<std::unique_ptr<logical_plan>> inputs;
    std::any properties;
    cost_estimate cost;
};

// Query planner and optimizer
class query_planner {
public:
    struct optimization_rules {
        bool predicate_pushdown = true;
        bool join_reordering = true;
        bool expression_simplification = true;
        bool common_subexpression_elimination = true;
        bool partition_pruning = true;
    };
    
    ss::future<std::unique_ptr<logical_plan>> 
    create_logical_plan(const ast_node& ast) {
        // Convert AST to logical plan
        auto plan = build_initial_plan(ast);
        
        // Apply optimization rules
        plan = co_await optimize(std::move(plan));
        
        co_return plan;
    }
    
    ss::future<std::unique_ptr<physical_plan>>
    create_physical_plan(const logical_plan& logical) {
        // Select physical operators
        auto physical = select_operators(logical);
        
        // Add exchange operators for distribution
        physical = add_exchanges(physical);
        
        // Optimize physical plan
        physical = optimize_physical(physical);
        
        co_return physical;
    }
    
private:
    // Cost-based optimizer
    class cost_optimizer {
        struct statistics {
            size_t row_count;
            size_t avg_row_size;
            histogram<double> value_distribution;
            double selectivity;
        };
        
        cost_estimate estimate_cost(const logical_plan& plan) {
            switch (plan.operation) {
            case logical_plan::op_type::table_scan:
                return estimate_scan_cost(plan);
            case logical_plan::op_type::join:
                return estimate_join_cost(plan);
            case logical_plan::op_type::aggregate:
                return estimate_aggregate_cost(plan);
            default:
                return cost_estimate{};
            }
        }
        
        // Dynamic programming for join order selection
        std::unique_ptr<logical_plan> optimize_join_order(
            std::vector<std::unique_ptr<logical_plan>> relations,
            std::vector<join_predicate> predicates
        );
    };
    
    // Rule-based optimizer
    std::unique_ptr<logical_plan> apply_rules(
        std::unique_ptr<logical_plan> plan,
        const optimization_rules& rules
    );
    
    // Predicate pushdown
    std::unique_ptr<logical_plan> push_predicates(
        std::unique_ptr<logical_plan> plan
    );
    
    cost_optimizer _cost_optimizer;
    optimization_rules _rules;
};

} // namespace redpanda::sql
```

### 4. Execution Engine

```cpp
namespace redpanda::sql {

// Physical execution operators
class physical_operator {
public:
    virtual ~physical_operator() = default;
    
    struct row_batch {
        std::vector<column_vector> columns;
        size_t row_count;
        std::optional<selection_vector> selection;
    };
    
    virtual ss::future<> open() = 0;
    virtual ss::future<std::optional<row_batch>> next() = 0;
    virtual ss::future<> close() = 0;
    
    virtual operator_stats get_stats() const = 0;
};

// Vectorized execution for SIMD optimization
class column_vector {
public:
    enum class type {
        int32,
        int64,
        float32,
        float64,
        string,
        bytes,
        timestamp,
        boolean
    };
    
    template<typename T>
    span<T> get_data() {
        return span<T>(
            reinterpret_cast<T*>(_data.data()),
            _count
        );
    }
    
    // Vectorized operations
    column_vector apply_filter(const selection_vector& sel) const;
    column_vector apply_function(const vectorized_function& func) const;
    
private:
    type _type;
    std::vector<uint8_t> _data;
    size_t _count;
    std::optional<bitmap> _nulls;
};

// Stream scan operator
class stream_scan_operator : public physical_operator {
public:
    stream_scan_operator(
        model::ntp ntp,
        scan_config config,
        std::optional<expression> predicate
    ) : _ntp(ntp)
      , _config(config)
      , _predicate(predicate) {}
    
    ss::future<> open() override {
        _reader = co_await make_kafka_reader(_ntp, _config);
        co_return;
    }
    
    ss::future<std::optional<row_batch>> next() override {
        auto batch = co_await _reader.read_batch();
        if (!batch) {
            co_return std::nullopt;
        }
        
        // Convert to columnar format
        auto row_batch = convert_to_columnar(*batch);
        
        // Apply predicate if present
        if (_predicate) {
            row_batch = apply_predicate(row_batch, *_predicate);
        }
        
        _stats.rows_scanned += row_batch.row_count;
        co_return row_batch;
    }
    
private:
    model::ntp _ntp;
    scan_config _config;
    std::optional<expression> _predicate;
    kafka_reader _reader;
    operator_stats _stats;
};

// Hash join operator
class hash_join_operator : public physical_operator {
public:
    hash_join_operator(
        std::unique_ptr<physical_operator> left,
        std::unique_ptr<physical_operator> right,
        join_predicate predicate,
        join_type type
    ) : _left(std::move(left))
      , _right(std::move(right))
      , _predicate(predicate)
      , _type(type) {}
    
    ss::future<> open() override {
        co_await _left->open();
        co_await _right->open();
        
        // Build hash table from right side
        co_await build_hash_table();
    }
    
    ss::future<std::optional<row_batch>> next() override {
        while (true) {
            // Get next batch from left side
            auto left_batch = co_await _left->next();
            if (!left_batch) {
                co_return std::nullopt;
            }
            
            // Probe hash table
            auto result = probe_hash_table(*left_batch);
            if (result.row_count > 0) {
                co_return result;
            }
        }
    }
    
private:
    ss::future<> build_hash_table() {
        while (auto batch = co_await _right->next()) {
            for (size_t i = 0; i < batch->row_count; ++i) {
                auto key = extract_key(*batch, i);
                _hash_table[key].push_back(extract_row(*batch, i));
            }
        }
    }
    
    row_batch probe_hash_table(const row_batch& left);
    
    std::unique_ptr<physical_operator> _left;
    std::unique_ptr<physical_operator> _right;
    join_predicate _predicate;
    join_type _type;
    absl::flat_hash_map<row_key, std::vector<row>> _hash_table;
};

// Window aggregate operator
class window_operator : public physical_operator {
public:
    window_operator(
        std::unique_ptr<physical_operator> input,
        window_definition window,
        std::vector<aggregate_function> aggregates
    ) : _input(std::move(input))
      , _window(window)
      , _aggregates(aggregates) {}
    
    ss::future<std::optional<row_batch>> next() override {
        // Handle different window types
        switch (_window.type) {
        case window_type::tumbling:
            co_return co_await process_tumbling_window();
        case window_type::sliding:
            co_return co_await process_sliding_window();
        case window_type::session:
            co_return co_await process_session_window();
        case window_type::hopping:
            co_return co_await process_hopping_window();
        }
    }
    
private:
    ss::future<row_batch> process_tumbling_window() {
        auto window_end = calculate_window_end(_window.size);
        row_batch result;
        
        while (auto batch = co_await _input->next()) {
            for (size_t i = 0; i < batch->row_count; ++i) {
                auto timestamp = extract_timestamp(*batch, i);
                if (timestamp >= window_end) {
                    // Emit current window
                    result = finalize_aggregates();
                    reset_aggregates();
                    window_end = calculate_window_end(_window.size);
                }
                
                // Update aggregates
                update_aggregates(*batch, i);
            }
        }
        
        co_return result;
    }
    
    std::unique_ptr<physical_operator> _input;
    window_definition _window;
    std::vector<aggregate_function> _aggregates;
    std::vector<aggregate_state> _states;
};

} // namespace redpanda::sql
```

### 5. Materialized Views

```cpp
namespace redpanda::sql {

// Materialized view management
class materialized_view {
public:
    struct definition {
        std::string name;
        std::string query;
        refresh_policy policy;
        storage_options storage;
        std::optional<partition_spec> partitioning;
    };
    
    enum class refresh_policy {
        continuous,    // Update on every write
        periodic,      // Update at intervals
        on_demand     // Manual refresh
    };
    
    ss::future<> create(const definition& def) {
        // Parse and validate query
        auto plan = co_await _planner.plan(def.query);
        
        // Create storage backend
        _storage = co_await create_storage(def.storage);
        
        // Initialize view state
        _state = view_state{
            .definition = def,
            .plan = std::move(plan),
            .last_refresh = model::timestamp::now()
        };
        
        // Start continuous refresh if needed
        if (def.policy == refresh_policy::continuous) {
            _refresh_fiber = refresh_continuously();
        }
    }
    
    ss::future<query_result> query(const query_request& req) {
        // Check if view needs refresh
        if (needs_refresh()) {
            co_await refresh();
        }
        
        // Execute query against materialized data
        co_return co_await _storage->query(req);
    }
    
private:
    ss::future<> refresh() {
        // Execute view query
        auto executor = execution_engine(_state.plan);
        
        // Write results to storage
        while (auto batch = co_await executor.next()) {
            co_await _storage->write(batch);
        }
        
        _state.last_refresh = model::timestamp::now();
    }
    
    ss::future<> refresh_continuously() {
        // Subscribe to source changes
        auto subscription = co_await subscribe_to_sources();
        
        while (!_as.abort_requested()) {
            auto change = co_await subscription.next();
            
            // Incrementally update view
            co_await incremental_update(change);
        }
    }
    
    // Incremental view maintenance
    ss::future<> incremental_update(const change_event& event) {
        switch (_state.plan->operation) {
        case logical_plan::op_type::aggregate:
            co_await update_aggregate(event);
            break;
        case logical_plan::op_type::join:
            co_await update_join(event);
            break;
        default:
            // Fall back to full refresh
            co_await refresh();
        }
    }
    
    view_state _state;
    std::unique_ptr<view_storage> _storage;
    query_planner _planner;
    ss::abort_source _as;
    ss::future<> _refresh_fiber;
};

// View storage backend
class view_storage {
public:
    virtual ~view_storage() = default;
    
    // Write batch to storage
    virtual ss::future<> write(const row_batch& batch) = 0;
    
    // Query stored data
    virtual ss::future<query_result> query(const query_request& req) = 0;
    
    // Compact storage
    virtual ss::future<> compact() = 0;
};

// In-memory storage for small views
class memory_view_storage : public view_storage {
public:
    ss::future<> write(const row_batch& batch) override {
        _data.push_back(batch);
        _row_count += batch.row_count;
        co_return;
    }
    
    ss::future<query_result> query(const query_request& req) override {
        // Apply filters and projections
        query_result result;
        for (const auto& batch : _data) {
            auto filtered = apply_filters(batch, req.filters);
            auto projected = apply_projection(filtered, req.projection);
            result.add_batch(projected);
        }
        co_return result;
    }
    
private:
    std::vector<row_batch> _data;
    size_t _row_count{0};
};

// Disk-based storage for large views
class disk_view_storage : public view_storage {
public:
    disk_view_storage(storage_config config) : _config(config) {}
    
    ss::future<> write(const row_batch& batch) override {
        // Write to segment files
        auto segment = co_await get_current_segment();
        co_await segment->append(batch);
        
        // Update indexes
        co_await update_indexes(batch);
    }
    
    ss::future<query_result> query(const query_request& req) override {
        // Use indexes to find relevant segments
        auto segments = co_await find_segments(req);
        
        // Read and filter data
        query_result result;
        for (auto& seg : segments) {
            auto reader = co_await seg->make_reader();
            while (auto batch = co_await reader.next()) {
                result.add_batch(batch);
            }
        }
        
        co_return result;
    }
    
private:
    storage_config _config;
    std::vector<std::unique_ptr<segment>> _segments;
    std::unique_ptr<index_manager> _indexes;
};

} // namespace redpanda::sql
```

### 6. SQL Interface Examples

```sql
-- Create a stream from Kafka topic
CREATE STREAM user_events (
    user_id BIGINT,
    event_type VARCHAR,
    timestamp TIMESTAMP,
    properties JSON
) WITH (
    kafka_topic = 'user-events',
    value_format = 'JSON',
    timestamp_field = 'timestamp'
);

-- Create a table from compacted topic
CREATE TABLE users (
    user_id BIGINT PRIMARY KEY,
    username VARCHAR,
    email VARCHAR,
    created_at TIMESTAMP
) WITH (
    kafka_topic = 'users',
    value_format = 'AVRO',
    key_field = 'user_id'
);

-- Simple streaming query
SELECT 
    event_type,
    COUNT(*) as event_count
FROM user_events
WHERE timestamp > NOW() - INTERVAL '1 HOUR'
GROUP BY event_type;

-- Tumbling window aggregation
SELECT 
    window_start,
    window_end,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(*) as total_events
FROM TABLE(
    TUMBLE(TABLE user_events, DESCRIPTOR(timestamp), INTERVAL '5' MINUTES)
)
GROUP BY window_start, window_end;

-- Sliding window with average
SELECT 
    user_id,
    AVG(cast(properties->>'duration' as DOUBLE)) OVER (
        PARTITION BY user_id
        ORDER BY timestamp
        RANGE BETWEEN INTERVAL '10' MINUTES PRECEDING AND CURRENT ROW
    ) as avg_duration
FROM user_events
WHERE event_type = 'video_played';

-- Stream-table join
SELECT 
    e.user_id,
    u.username,
    e.event_type,
    e.timestamp
FROM user_events e
JOIN users u ON e.user_id = u.user_id
WHERE e.timestamp > NOW() - INTERVAL '1' HOUR;

-- Create materialized view
CREATE MATERIALIZED VIEW hourly_stats AS
SELECT 
    DATE_TRUNC('hour', timestamp) as hour,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users
FROM user_events
GROUP BY DATE_TRUNC('hour', timestamp), event_type
WITH (
    refresh_policy = 'continuous',
    storage = 'disk',
    partition_by = 'hour'
);

-- Query materialized view
SELECT * FROM hourly_stats
WHERE hour >= NOW() - INTERVAL '24' HOURS
ORDER BY hour DESC, event_count DESC;

-- Complex analytical query
WITH event_sessions AS (
    SELECT 
        user_id,
        timestamp,
        event_type,
        LAG(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp) as prev_timestamp,
        CASE 
            WHEN timestamp - LAG(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp) > INTERVAL '30' MINUTES
            THEN 1 
            ELSE 0 
        END as new_session
    FROM user_events
),
session_ids AS (
    SELECT 
        user_id,
        timestamp,
        event_type,
        SUM(new_session) OVER (PARTITION BY user_id ORDER BY timestamp) as session_id
    FROM event_sessions
)
SELECT 
    user_id,
    session_id,
    MIN(timestamp) as session_start,
    MAX(timestamp) as session_end,
    COUNT(*) as events_in_session,
    ARRAY_AGG(event_type ORDER BY timestamp) as event_sequence
FROM session_ids
GROUP BY user_id, session_id
HAVING COUNT(*) > 5;

-- User-defined functions
CREATE FUNCTION parse_user_agent(agent VARCHAR) 
RETURNS TABLE (browser VARCHAR, os VARCHAR, device VARCHAR)
LANGUAGE WASM
AS 'user_agent_parser.wasm';

-- Use UDF in query
SELECT 
    parse_user_agent(properties->>'user_agent') as ua,
    COUNT(*) as count
FROM user_events
WHERE event_type = 'page_view'
GROUP BY parse_user_agent(properties->>'user_agent');
```

### 7. Performance Optimizations

```cpp
namespace redpanda::sql {

// Query compilation to native code
class query_compiler {
public:
    using compiled_query = std::function<ss::future<query_result>(runtime_context&)>;
    
    ss::future<compiled_query> compile(const physical_plan& plan) {
        // Generate LLVM IR
        auto module = generate_llvm_ir(plan);
        
        // Optimize IR
        optimize_module(module);
        
        // JIT compile to native code
        auto native_code = jit_compile(module);
        
        co_return [native_code](runtime_context& ctx) -> ss::future<query_result> {
            co_return co_await native_code->execute(ctx);
        };
    }
    
private:
    llvm::Module* generate_llvm_ir(const physical_plan& plan) {
        ir_generator gen;
        
        // Generate code for each operator
        for (const auto& op : plan.operators) {
            gen.visit(op);
        }
        
        return gen.get_module();
    }
    
    void optimize_module(llvm::Module* module) {
        llvm::PassManagerBuilder builder;
        builder.OptLevel = 3;
        builder.SizeLevel = 0;
        builder.Inliner = llvm::createFunctionInliningPass(275);
        
        llvm::legacy::FunctionPassManager fpm(module);
        builder.populateFunctionPassManager(fpm);
        fpm.run(*module);
    }
};

// Adaptive query execution
class adaptive_executor {
public:
    ss::future<query_result> execute(physical_plan plan) {
        // Start with initial plan
        auto executor = create_executor(plan);
        
        // Monitor execution statistics
        auto stats_collector = start_stats_collection();
        
        // Execute with adaptation
        query_result result;
        while (auto batch = co_await executor->next()) {
            result.add_batch(batch);
            
            // Check if re-optimization needed
            if (should_reoptimize(stats_collector.get_stats())) {
                plan = reoptimize_plan(plan, stats_collector.get_stats());
                executor = create_executor(plan);
            }
        }
        
        co_return result;
    }
    
private:
    bool should_reoptimize(const execution_stats& stats) {
        // Check for cardinality misestimation
        if (stats.actual_rows / stats.estimated_rows > 10 ||
            stats.estimated_rows / stats.actual_rows > 10) {
            return true;
        }
        
        // Check for skew in data distribution
        if (stats.data_skew > 0.8) {
            return true;
        }
        
        return false;
    }
    
    physical_plan reoptimize_plan(
        const physical_plan& original,
        const execution_stats& stats
    ) {
        // Update cardinality estimates
        auto updated = update_estimates(original, stats);
        
        // Re-run optimizer with actual statistics
        return _optimizer.optimize(updated, stats);
    }
    
    query_optimizer _optimizer;
};

// Parallel query execution
class parallel_executor {
public:
    ss::future<query_result> execute(const physical_plan& plan) {
        // Identify parallelizable operators
        auto parallel_segments = identify_parallel_segments(plan);
        
        // Create execution tasks
        std::vector<ss::future<query_result>> tasks;
        for (const auto& segment : parallel_segments) {
            tasks.push_back(execute_segment(segment));
        }
        
        // Execute in parallel
        auto results = co_await ss::when_all(tasks.begin(), tasks.end());
        
        // Merge results
        co_return merge_results(results);
    }
    
private:
    std::vector<plan_segment> identify_parallel_segments(const physical_plan& plan) {
        std::vector<plan_segment> segments;
        
        // Find operators that can run in parallel
        for (const auto& op : plan.operators) {
            if (is_parallelizable(op)) {
                segments.push_back(create_segment(op));
            }
        }
        
        return segments;
    }
    
    ss::future<query_result> execute_segment(const plan_segment& segment) {
        // Pin to specific core for cache locality
        co_return co_await ss::smp::submit_to(
            segment.target_core,
            [segment]() -> ss::future<query_result> {
                segment_executor exec(segment);
                co_return co_await exec.run();
            }
        );
    }
};

} // namespace redpanda::sql
```

### 8. Integration with Redpanda

```cpp
namespace redpanda {

// SQL subsystem integration
class sql_service : public ss::sharded_service<sql_service> {
public:
    ss::future<> start(sql_config config) {
        _config = config;
        
        // Initialize query engine components
        co_await _parser.start();
        co_await _planner.start();
        co_await _executor.start();
        co_await _view_manager.start();
        
        // Register Kafka API extensions
        co_await register_sql_api();
        
        // Start background tasks
        _maintenance_fiber = maintenance_loop();
    }
    
    ss::future<> stop() {
        _as.request_abort();
        co_await _maintenance_fiber;
        
        co_await _view_manager.stop();
        co_await _executor.stop();
        co_await _planner.stop();
        co_await _parser.stop();
    }
    
    // Execute SQL query
    ss::future<query_response> execute_query(query_request req) {
        // Parse SQL
        auto parse_result = co_await _parser.parse(req.sql);
        if (!parse_result.errors.empty()) {
            co_return make_error_response(parse_result.errors);
        }
        
        // Create execution plan
        auto logical_plan = co_await _planner.create_logical_plan(parse_result.ast);
        auto physical_plan = co_await _planner.create_physical_plan(logical_plan);
        
        // Execute query
        auto result = co_await _executor.execute(physical_plan);
        
        // Format response
        co_return format_response(result, req.format);
    }
    
    // Manage materialized views
    ss::future<> create_materialized_view(view_definition def) {
        co_return co_await _view_manager.create_view(def);
    }
    
    ss::future<> drop_materialized_view(ss::sstring name) {
        co_return co_await _view_manager.drop_view(name);
    }
    
private:
    ss::future<> maintenance_loop() {
        while (!_as.abort_requested()) {
            // Refresh materialized views
            co_await _view_manager.refresh_views();
            
            // Clean up expired query results
            co_await cleanup_expired_results();
            
            // Update statistics
            co_await update_statistics();
            
            co_await ss::sleep_abortable(
                std::chrono::seconds(60),
                _as
            );
        }
    }
    
    sql_config _config;
    ss::sharded<sql::sql_parser> _parser;
    ss::sharded<sql::query_planner> _planner;
    ss::sharded<sql::query_executor> _executor;
    ss::sharded<sql::view_manager> _view_manager;
    ss::abort_source _as;
    ss::future<> _maintenance_fiber;
};

} // namespace redpanda
```

### 9. Configuration

```yaml
# SQL engine configuration
sql:
  # Enable SQL query engine
  enabled: true
  
  # Parser settings
  parser:
    max_query_length: 1048576  # 1MB
    timeout_ms: 5000
    
  # Planner settings
  planner:
    enable_cost_based_optimization: true
    enable_rule_based_optimization: true
    statistics_sample_rate: 0.01
    join_reorder_threshold: 12
    
  # Executor settings
  executor:
    max_memory_per_query: 1073741824  # 1GB
    parallelism: auto  # or specific number
    enable_compilation: true
    compilation_threshold: 100  # queries before compiling
    spill_to_disk: true
    spill_directory: /var/lib/redpanda/sql/spill
    
  # Materialized views
  materialized_views:
    enabled: true
    storage_directory: /var/lib/redpanda/sql/views
    max_views: 1000
    refresh_interval_ms: 5000
    incremental_maintenance: true
    
  # Performance tuning
  performance:
    enable_simd: true
    enable_adaptive_execution: true
    cache_size: 268435456  # 256MB
    buffer_pool_size: 536870912  # 512MB
    
  # Security
  security:
    enable_row_level_security: true
    enable_column_level_security: true
    audit_queries: true
```

## Testing Strategy

### 1. Unit Tests

```cpp
BOOST_AUTO_TEST_CASE(test_sql_parser) {
    sql::sql_parser parser;
    
    auto result = parser.parse(
        "SELECT COUNT(*) FROM events WHERE timestamp > '2024-01-01'"
    ).get();
    
    BOOST_CHECK(result.errors.empty());
    BOOST_CHECK_EQUAL(result.ast->node_type, sql::ast_node::type::select);
}

BOOST_AUTO_TEST_CASE(test_window_operator) {
    auto input = make_test_operator({
        {1, "event1", 1000},
        {2, "event2", 2000},
        {3, "event3", 6000}
    });
    
    sql::window_operator window(
        std::move(input),
        sql::window_definition{
            .type = sql::window_type::tumbling,
            .size = std::chrono::seconds(5)
        },
        {sql::aggregate_function::count()}
    );
    
    auto result = window.next().get();
    BOOST_CHECK_EQUAL(result->row_count, 1);
    BOOST_CHECK_EQUAL(get_value<int64_t>(result, 0, 0), 2);  // Count = 2
}
```

### 2. Integration Tests

```python
import pytest
from redpanda.client import RedpandaClient

@pytest.mark.integration
def test_streaming_query():
    client = RedpandaClient()
    
    # Create stream
    client.execute_sql("""
        CREATE STREAM test_events (
            id BIGINT,
            value DOUBLE,
            timestamp TIMESTAMP
        ) WITH (kafka_topic = 'test-events', value_format = 'JSON')
    """)
    
    # Insert test data
    for i in range(100):
        client.produce('test-events', {
            'id': i,
            'value': i * 1.5,
            'timestamp': time.time()
        })
    
    # Execute aggregation query
    result = client.execute_sql("""
        SELECT 
            COUNT(*) as count,
            AVG(value) as avg_value
        FROM test_events
    """)
    
    assert result[0]['count'] == 100
    assert abs(result[0]['avg_value'] - 74.25) < 0.01

@pytest.mark.integration  
def test_materialized_view():
    client = RedpandaClient()
    
    # Create materialized view
    client.execute_sql("""
        CREATE MATERIALIZED VIEW test_view AS
        SELECT 
            DATE_TRUNC('minute', timestamp) as minute,
            COUNT(*) as event_count
        FROM test_events
        GROUP BY DATE_TRUNC('minute', timestamp)
    """)
    
    # Query view
    result = client.execute_sql("SELECT * FROM test_view")
    assert len(result) > 0
```

### 3. Performance Benchmarks

```cpp
BENCHMARK(BM_WindowedAggregation) {
    // Setup: Create 1M events
    auto events = generate_events(1'000'000);
    
    // Benchmark: 5-minute tumbling window
    auto start = std::chrono::high_resolution_clock::now();
    
    auto result = execute_query(R"(
        SELECT 
            window_start,
            COUNT(*) as count,
            AVG(value) as avg
        FROM TABLE(
            TUMBLE(TABLE events, DESCRIPTOR(timestamp), INTERVAL '5' MINUTES)
        )
        GROUP BY window_start
    )");
    
    auto end = std::chrono::high_resolution_clock::now();
    
    return std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
}

// Expected performance targets:
// - Simple aggregations: < 100ms for 1M records
// - Windowed aggregations: < 500ms for 1M records  
// - Joins: < 1s for 100K x 100K records
// - Materialized view refresh: < 200ms incremental update
```

## Migration Path

### Phase 1: Core SQL Engine (3-6 months)
- SQL parser and AST
- Basic query planner
- Simple operators (scan, filter, project)
- In-memory execution

### Phase 2: Advanced Features (3-6 months)
- Window functions
- Joins (hash, merge, nested loop)
- Aggregations
- Basic optimization

### Phase 3: Materialized Views (3-4 months)
- View creation and management
- Continuous refresh
- Incremental maintenance
- View querying

### Phase 4: Performance (3-4 months)
- Query compilation
- Vectorized execution
- Parallel execution
- Adaptive optimization

### Phase 5: Enterprise Features (3-4 months)
- User-defined functions
- Row/column-level security
- Query federation
- Advanced analytics

## Success Metrics

### Performance KPIs
- Query latency p50: < 50ms
- Query latency p99: < 500ms
- Throughput: > 100K queries/second
- Memory efficiency: < 100MB per concurrent query

### Adoption Metrics
- Number of materialized views created
- Queries executed per day
- Data volume processed
- User satisfaction scores

## Risks and Mitigations

### Risk: Complexity
**Mitigation**: Start with MVP focusing on core features, iterate based on feedback

### Risk: Performance
**Mitigation**: Extensive benchmarking, optimization, and compilation to native code

### Risk: SQL Compatibility
**Mitigation**: Follow ANSI SQL standards, provide compatibility mode for popular dialects

### Risk: Resource Usage
**Mitigation**: Query resource limits, admission control, spilling to disk

## Alternatives Considered

1. **Embed Existing Engine**: License and integration complexity
2. **ksqlDB Fork**: Java-based, different architecture
3. **Flink Integration**: Heavy dependency, operational complexity
4. **Custom DSL**: Learning curve, limited ecosystem

## Open Questions

1. Should we support multiple SQL dialects (PostgreSQL, MySQL)?
2. What level of transaction support is needed?
3. Should we integrate with external metastores (Hive, Glue)?
4. How to handle schema evolution in materialized views?
5. Should we support federated queries across multiple Redpanda clusters?

## Conclusion

Implementing a native SQL query engine positions Redpanda as a complete streaming data platform, eliminating the need for external processing systems. This provides significant value through simplified operations, improved performance, and enhanced developer experience. The phased approach allows iterative development while delivering value at each milestone.