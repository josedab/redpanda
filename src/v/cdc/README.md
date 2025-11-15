# Redpanda Integrated CDC

This module implements native Change Data Capture (CDC) capabilities for Redpanda, enabling seamless streaming of database changes without external connectors.

## Overview

The integrated CDC feature provides:

- **Native CDC Sources**: PostgreSQL and MySQL connectors built directly into Redpanda
- **Minimal Latency**: Direct integration reduces network hops and serialization overhead
- **Exactly-Once Semantics**: Native support for exactly-once delivery guarantees
- **Schema Evolution**: Automatic handling of schema changes with compatibility checking
- **Transform Pipeline**: Built-in data transformation and filtering capabilities
- **State Management**: Automatic checkpointing and failure recovery

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    Database Sources                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                │
│  │PostgreSQL│  │  MySQL   │  │ MongoDB  │  (Future)      │
│  └─────┬────┘  └─────┬────┘  └─────┬────┘                │
└────────┼──────────────┼──────────────┼────────────────────┘
         │              │              │
         ▼              ▼              ▼
┌──────────────────────────────────────────────────────────┐
│               CDC Connector Framework                      │
│  ┌─────────────────────────────────────────────────────┐ │
│  │  Source Connectors | Schema Mgr | Transform Engine │ │
│  └─────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│                   Redpanda Core                           │
│              (Topics, Partitions, Replication)            │
└──────────────────────────────────────────────────────────┘
```

## Components

### Core Components

1. **CDC Source Interface** (`cdc_source.h`)
   - Abstract base class for all CDC sources
   - Defines lifecycle methods: start, stop, pause, resume
   - Provides change stream interface
   - Supports checkpointing and recovery

2. **PostgreSQL CDC Source** (`postgres_cdc_source.h/cc`)
   - Uses PostgreSQL logical replication
   - Supports pgoutput and wal2json plugins
   - Handles snapshots and incremental changes
   - Manages replication slots

3. **MySQL CDC Source** (`mysql_cdc_source.h/cc`)
   - Uses MySQL binlog replication
   - Registers as binlog slave
   - Decodes row-based binlog events
   - Supports GTID-based replication

4. **Schema Manager** (`schema_manager.h/cc`)
   - Tracks schema changes
   - Handles schema evolution
   - Validates compatibility
   - Caches schema metadata

5. **Transform Engine** (`transform_engine.h/cc`)
   - Applies data transformations
   - Supports filtering
   - Provides Debezium compatibility mode
   - Extensible via WASM (future)

6. **State Manager** (`state_manager.h/cc`)
   - Manages connector state
   - Handles checkpointing
   - Implements recovery strategies
   - Persists state to internal topic

7. **Admin API** (`admin_api.h/cc`)
   - REST API for connector management
   - Create, list, get, delete connectors
   - Pause, resume connectors
   - Monitor metrics and status

## Usage

### Creating a CDC Connector

```bash
# Create a PostgreSQL CDC connector
curl -X POST http://localhost:9644/v1/cdc/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-cdc",
    "source_type": "postgresql",
    "config": {
      "connection_string": "postgresql://user:pass@localhost:5432/mydb",
      "tables": ["users", "orders"],
      "mode": "snapshot_and_incremental"
    }
  }'
```

### Listing Connectors

```bash
curl http://localhost:9644/v1/cdc/connectors
```

### Getting Connector Status

```bash
curl http://localhost:9644/v1/cdc/connectors/postgres-cdc
```

### Pausing a Connector

```bash
curl -X POST http://localhost:9644/v1/cdc/connectors/postgres-cdc/pause
```

### Deleting a Connector

```bash
curl -X DELETE http://localhost:9644/v1/cdc/connectors/postgres-cdc
```

## Configuration

CDC can be configured in `redpanda.yaml`:

```yaml
cdc:
  # Enable CDC feature
  enabled: true

  # Maximum number of concurrent connectors
  max_connectors: 100

  # Snapshot settings
  snapshot_fetch_size: 10000
  snapshot_parallel_tables: 4

  # Performance tuning
  binlog_buffer_size_mb: 64
  checkpoint_interval_ms: 60000

  # State topic settings
  state_topic_replication_factor: 3

  # PostgreSQL specific
  postgres:
    plugin: "pgoutput"  # or "wal2json"
    publication_autocreate: true
    slot_drop_on_stop: false

  # MySQL specific
  mysql:
    server_id: 123456
    gtid_mode: true
    include_schema_changes: true
```

## Event Format

CDC events are produced to Redpanda topics with the following structure:

```json
{
  "op": "insert",  // insert, update, delete, truncate, schema_change
  "database": "mydb",
  "schema": "public",
  "table": "users",
  "before": null,  // Previous row values (for updates/deletes)
  "after": {       // New row values
    "id": 123,
    "name": "John Doe",
    "email": "john@example.com"
  },
  "key": {
    "id": 123
  },
  "timestamp": "2025-01-15T10:30:00Z",
  "transaction_id": "1234567890",
  "sequence_number": 42,
  "metadata": {
    "snapshot": "false"
  }
}
```

## Topic Naming

CDC events are routed to topics based on the pattern:

```
{connector_name}_{database}.{table}
```

For example, a connector named `postgres-cdc` capturing changes from table `users` in database `mydb` will produce events to topic:

```
postgres-cdc_mydb.users
```

## Transforms

The transform engine supports:

- **Flatten**: Flatten nested JSON structures
- **Rename Fields**: Rename fields in events
- **Add Metadata**: Add custom metadata fields
- **Route**: Route events to specific topics
- **Filter**: Filter events based on expressions
- **Mask Sensitive**: Mask sensitive data (PII)
- **Extract Key**: Extract primary key from payload
- **Debezium Format**: Convert to Debezium-compatible format

## Database Setup

### PostgreSQL

Enable logical replication:

```sql
-- postgresql.conf
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10

-- Create publication
CREATE PUBLICATION redpanda_cdc FOR ALL TABLES;
```

### MySQL

Enable binlog with row format:

```ini
# my.cnf
server-id = 1
log-bin = mysql-bin
binlog-format = ROW
binlog-row-image = FULL
gtid-mode = ON
enforce-gtid-consistency = ON
```

## Testing

### Unit Tests

```bash
bazel test //src/v/cdc:cdc_test
```

### Integration Tests

```bash
cd tests
./ducktape rptest/tests/cdc_test/
```

## Performance Considerations

- **Batch Processing**: Events are batched before producing to topics
- **Compression**: Use compression for CDC topics to reduce storage
- **Parallel Processing**: Multiple tables can be processed in parallel
- **Connection Pooling**: Database connections are reused
- **Schema Caching**: Schema metadata is cached to reduce queries

## Limitations (Current Implementation)

1. This is a framework implementation with placeholders for actual database connectivity
2. PostgreSQL logical replication client needs full implementation
3. MySQL binlog parsing needs complete implementation
4. MongoDB and Oracle connectors are not yet implemented
5. WASM-based custom transforms are not yet supported
6. Schema registry integration needs completion
7. Metrics collection and reporting needs implementation

## Future Enhancements

1. **Additional Connectors**:
   - MongoDB (change streams)
   - Oracle (LogMiner)
   - SQL Server (CDC feature)
   - Cassandra

2. **Advanced Features**:
   - Multi-table transactions
   - Cross-database joins
   - Data validation
   - Dead letter queues
   - Metrics and monitoring

3. **Performance Optimizations**:
   - Connection multiplexing
   - Adaptive batching
   - Compression tuning
   - Parallel snapshot processing

4. **Operations**:
   - Connector monitoring dashboard
   - Alerting integration
   - Backup and restore
   - Migration tools

## References

- RFC: `rfcs/20250115_integrated_cdc.md`
- PostgreSQL Logical Replication: https://www.postgresql.org/docs/current/logical-replication.html
- MySQL Binlog: https://dev.mysql.com/doc/internals/en/binary-log.html
- Debezium: https://debezium.io/

## License

Copyright 2025 Redpanda Data, Inc.

Use of this software is governed by the Business Source License included in the file licenses/BSL.md
