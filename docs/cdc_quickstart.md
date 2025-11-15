# CDC Quick Start Guide

This guide walks you through setting up and using Redpanda's integrated CDC (Change Data Capture) feature.

## Prerequisites

- Redpanda cluster running version X.X.X or later
- PostgreSQL 10+ or MySQL 5.7+ database
- Database with logical replication/binlog enabled

## Step 1: Enable CDC in Redpanda

Edit your `redpanda.yaml` configuration:

```yaml
cdc:
  enabled: true
  max_connectors: 100
```

Restart Redpanda for changes to take effect.

## Step 2: Prepare Your Database

### For PostgreSQL

```sql
-- Enable logical replication in postgresql.conf
-- wal_level = logical
-- max_replication_slots = 10

-- Create a publication for tables you want to capture
CREATE PUBLICATION redpanda_cdc FOR TABLE users, orders;

-- Grant replication privileges
ALTER USER myuser WITH REPLICATION;
```

### For MySQL

```sql
-- Enable binlog in my.cnf
-- log-bin = mysql-bin
-- binlog-format = ROW
-- server-id = 1

-- Grant replication privileges
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'myuser'@'%';
FLUSH PRIVILEGES;
```

## Step 3: Create a CDC Connector

Use the Redpanda Admin API to create a connector:

```bash
curl -X POST http://localhost:9644/v1/cdc/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-postgres-connector",
    "source_type": "postgresql",
    "config": {
      "connection_string": "postgresql://user:password@localhost:5432/mydb",
      "tables": ["users", "orders"],
      "mode": "snapshot_and_incremental",
      "include_schema_changes": true
    }
  }'
```

## Step 4: Verify Connector Status

Check that your connector is running:

```bash
curl http://localhost:9644/v1/cdc/connectors/my-postgres-connector
```

Expected response:

```json
{
  "name": "my-postgres-connector",
  "type": "postgresql",
  "state": "running",
  "metrics": {
    "events_total": 1250,
    "events_per_second": 42.5
  }
}
```

## Step 5: Consume CDC Events

Use any Kafka-compatible consumer to read CDC events:

```bash
# Using rpk
rpk topic consume my-postgres-connector_mydb.users

# Using kafka-console-consumer
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic my-postgres-connector_mydb.users \
  --from-beginning
```

## Step 6: Make Changes to Your Database

```sql
-- Insert a new user
INSERT INTO users (id, name, email) VALUES (123, 'Jane Doe', 'jane@example.com');

-- Update a user
UPDATE users SET email = 'jane.doe@example.com' WHERE id = 123;

-- Delete a user
DELETE FROM users WHERE id = 123;
```

You should see corresponding CDC events in the Redpanda topic.

## Example CDC Event

```json
{
  "op": "insert",
  "database": "mydb",
  "schema": "public",
  "table": "users",
  "after": {
    "id": 123,
    "name": "Jane Doe",
    "email": "jane@example.com",
    "created_at": "2025-01-15T10:30:00Z"
  },
  "key": {
    "id": 123
  },
  "timestamp": "2025-01-15T10:30:00.123Z",
  "transaction_id": "1234567890",
  "sequence_number": 42
}
```

## Advanced: Adding Transforms

Create a connector with data transformations:

```bash
curl -X POST http://localhost:9644/v1/cdc/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-connector-with-transforms",
    "source_type": "postgresql",
    "config": {
      "connection_string": "postgresql://user:password@localhost:5432/mydb",
      "tables": ["users"]
    },
    "transforms": [
      {
        "name": "add_metadata",
        "type": "add_metadata",
        "parameters": {
          "fields": ["timestamp", "source"]
        }
      },
      {
        "name": "mask_email",
        "type": "mask_sensitive",
        "parameters": {
          "fields": ["email"],
          "mask_char": "*"
        }
      }
    ]
  }'
```

## Monitoring

List all connectors:

```bash
curl http://localhost:9644/v1/cdc/connectors
```

Get detailed metrics:

```bash
curl http://localhost:9644/v1/cdc/connectors/my-connector/metrics
```

## Troubleshooting

### Connector fails to start

- Verify database credentials
- Check database has logical replication/binlog enabled
- Ensure network connectivity between Redpanda and database

### No events appearing

- Verify connector is in "running" state
- Check database has activity
- Review connector logs

### High latency

- Increase `checkpoint_interval_ms` to reduce overhead
- Use `incremental` mode instead of `snapshot_and_incremental`
- Ensure database server has sufficient resources

## Next Steps

- Read the full [CDC documentation](../src/v/cdc/README.md)
- Review the [CDC RFC](../rfcs/20250115_integrated_cdc.md)
- Explore transform options
- Set up monitoring and alerting

## Support

For issues and questions, please file an issue on GitHub or contact Redpanda support.
