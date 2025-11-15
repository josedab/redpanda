# Redpanda SQL Query Engine

This directory contains the implementation of the Redpanda SQL query engine for streaming analytics, as described in RFC 011.

## Overview

The SQL query engine provides native SQL capabilities for real-time analytics on streaming data, eliminating the need for external stream processing frameworks.

## Architecture

The query engine consists of several key components:

### 1. Parser (`parser/`)
- **Tokenizer**: Lexical analysis of SQL statements
- **AST Builder**: Abstract Syntax Tree construction
- **SQL Parser**: Recursive descent parser supporting standard SQL syntax

### 2. Query Planner (`planner/`)
- **Logical Plan**: High-level query representation
- **Physical Plan**: Executable operator pipeline
- **Query Optimizer**: Cost-based and rule-based optimization

### 3. Execution Engine (`executor/`)
- **Physical Operators**: Stream scan, filter, project, join, aggregate, window, sort, limit
- **Columnar Execution**: Vectorized processing for performance
- **Operator Pipeline**: Volcano-style iterator model

### 4. Materialized Views (`materialized_view/`)
- **View Manager**: Creation, refresh, and query of materialized views
- **Storage Backends**: In-memory and disk-based storage
- **Refresh Policies**: Continuous, periodic, and on-demand refresh

### 5. SQL Service (`sql_service.h/cc`)
- **Service Integration**: Integration with Redpanda core
- **Query Execution**: End-to-end query processing
- **Statistics**: Query metrics and monitoring

## Features

### Supported SQL Operations
- SELECT with projections
- WHERE clause filtering
- GROUP BY aggregation (COUNT, SUM, AVG, MIN, MAX)
- ORDER BY sorting
- LIMIT/OFFSET
- JOINs (INNER, LEFT, RIGHT, FULL)
- Window functions (tumbling, sliding, session, hopping)
- Materialized views

### Example Queries

```sql
-- Simple aggregation
SELECT
    event_type,
    COUNT(*) as event_count
FROM user_events
WHERE timestamp > NOW() - INTERVAL '1 HOUR'
GROUP BY event_type;

-- Tumbling window
SELECT
    window_start,
    window_end,
    COUNT(DISTINCT user_id) as unique_users
FROM TABLE(
    TUMBLE(TABLE user_events, DESCRIPTOR(timestamp), INTERVAL '5' MINUTES)
)
GROUP BY window_start, window_end;

-- Stream-table join
SELECT
    e.user_id,
    u.username,
    e.event_type
FROM user_events e
JOIN users u ON e.user_id = u.user_id;

-- Materialized view
CREATE MATERIALIZED VIEW hourly_stats AS
SELECT
    DATE_TRUNC('hour', timestamp) as hour,
    event_type,
    COUNT(*) as event_count
FROM user_events
GROUP BY DATE_TRUNC('hour', timestamp), event_type
WITH (
    refresh_policy = 'continuous',
    storage = 'disk'
);
```

## Configuration

See `sql_config_schema.yaml` for detailed configuration options.

Example configuration:
```yaml
sql:
  enabled: true
  parser:
    max_query_length: 1048576
    timeout_ms: 5000
  executor:
    max_memory_per_query: 1073741824
    parallelism: auto
  materialized_views:
    enabled: true
    refresh_interval_ms: 5000
```

## Building

The SQL engine is built as part of the Redpanda build system using Bazel:

```bash
bazel build //src/v/sql:sql
```

## Testing

### Unit Tests
```bash
bazel test //src/v/sql/tests:sql_parser_test
bazel test //src/v/sql/tests:sql_planner_test
bazel test //src/v/sql/tests:sql_executor_test
```

### Integration Tests
```bash
bazel test //src/v/sql/tests:sql_integration_test
```

## Performance

Expected performance targets:
- Simple aggregations: < 100ms for 1M records
- Windowed aggregations: < 500ms for 1M records
- Joins: < 1s for 100K x 100K records
- Materialized view refresh: < 200ms incremental update

## Development Roadmap

### Phase 1: Core SQL Engine (Completed)
- ✅ SQL parser and AST
- ✅ Basic query planner
- ✅ Simple operators (scan, filter, project)
- ✅ In-memory execution

### Phase 2: Advanced Features (In Progress)
- ⏳ Window functions
- ⏳ Joins (hash, merge, nested loop)
- ⏳ Aggregations
- ⏳ Basic optimization

### Phase 3: Materialized Views (In Progress)
- ✅ View creation and management
- ⏳ Continuous refresh
- ⏳ Incremental maintenance
- ⏳ View querying

### Phase 4: Performance (Future)
- ⏳ Query compilation
- ⏳ Vectorized execution
- ⏳ Parallel execution
- ⏳ Adaptive optimization

### Phase 5: Enterprise Features (Future)
- ⏳ User-defined functions
- ⏳ Row/column-level security
- ⏳ Query federation
- ⏳ Advanced analytics

## Contributing

See the main Redpanda contributing guidelines.

## License

See the main Redpanda license file (BSL 1.1).
