# Disaggregated Storage and Compute Architecture

This document describes the implementation of Redpanda's disaggregated storage and compute architecture as specified in RFC-007.

## Overview

The disaggregated architecture separates Redpanda into three main layers:

1. **Storage Service Layer** - Handles data persistence to cloud object storage
2. **Metadata Service Layer** - Manages partition and consumer group state
3. **Compute Layer** - Stateless compute nodes that handle Kafka API requests

## Components

### Storage Service (`src/v/storage_service/`)

The storage service layer provides an abstraction for storing and retrieving data:

- `storage_service.h` - Main storage service interface
- `cloud_storage_service.h` - Implementation using cloud object storage (S3/GCS/Azure)

Key features:
- Append batches to partitions
- Create readers for fetching data
- Manage partition metadata
- List segments

### Metadata Service (`src/v/metadata_service/`)

The metadata service manages all cluster metadata:

- `metadata_service.h` - Main metadata service interface
- `consensus_metadata_service.h` - Implementation using distributed consensus

Key features:
- Partition metadata management
- Consumer group coordination
- Offset management
- Watch for metadata changes

### Compute Layer (`src/v/compute/`)

Stateless compute nodes handle Kafka protocol requests:

- `compute_node.h` - Main compute node implementation
- `compute_cache.h` - Intelligent caching layer for compute nodes

Key features:
- Kafka API request handling
- Local caching for performance
- Metadata refresh
- Connection to storage and metadata services

### Service Mesh (`src/v/service_mesh/`)

Service discovery and load balancing:

- `service_registry.h` - Service registration and discovery
- `load_balancer.h` - Client-side load balancing

Key features:
- Service registration with heartbeats
- Health monitoring
- Multiple load balancing strategies (round-robin, least connections, etc.)

## Configuration

Configuration options are defined in `src/v/config/disaggregated_config.h`:

```yaml
# Enable disaggregated mode
disaggregated_enabled: false

# Storage service settings
storage_service_endpoint: "storage.redpanda.internal:9092"
storage_segment_size_mb: 128
storage_replication_factor: 3
storage_cloud_provider: "s3"
storage_cloud_bucket: "redpanda-segments"

# Metadata service settings
metadata_service_endpoint: "metadata.redpanda.internal:9093"
metadata_consensus_backend: "raft"
metadata_replication_factor: 5
metadata_snapshot_interval_ms: 60000

# Compute node settings
compute_cache_size_mb: 10240
compute_max_connections: 10000
compute_metadata_refresh_ms: 10000

# Service mesh settings
service_mesh_load_balancing: "round_robin"
service_mesh_heartbeat_interval_ms: 5000
```

## Testing

### Unit Tests

Unit tests for individual components:

- `src/v/storage_service/tests/storage_service_test.cc` - Storage service tests

### Integration Tests

End-to-end integration tests:

- `src/v/compute/tests/disaggregated_integration_test.cc` - Full stack tests

Run tests with:
```bash
# Unit tests
./build/release/tests/unit_test_name

# Integration tests
./build/release/tests/disaggregated_integration_test
```

## Migration Strategy

The implementation supports hybrid mode where both monolithic and disaggregated modes can coexist:

### Phase 1: Storage Service Development (Current)
- Implement storage service interfaces
- Add cloud storage backend
- Support dual-mode operation

### Phase 2: Metadata Service Deployment
- Deploy metadata service alongside existing cluster
- Gradually migrate metadata operations
- Implement dual-write for consistency

### Phase 3: Compute Layer Rollout
- Deploy stateless compute nodes
- Route read traffic to compute layer
- Gradually migrate write traffic

### Phase 4: Full Migration
- Complete migration of all workloads
- Decommission monolithic nodes
- Performance optimization

## Architecture Benefits

1. **Independent Scaling** - Scale compute and storage based on actual needs
2. **True Elasticity** - Add/remove compute nodes in seconds
3. **Cost Optimization** - Pay only for active compute time
4. **Higher Availability** - Storage layer provides 99.99%+ durability
5. **Simplified Operations** - Stateless compute nodes are easier to manage

## Performance Optimizations

- **Request Batching** - Aggregate multiple small requests
- **Prefetching** - Predictive segment prefetching
- **Caching** - Multi-level caching strategy
- **Compression** - Automatic segment compression

## Security Considerations

- **mTLS Authentication** - Service-to-service authentication
- **Encryption at Rest** - AES-256-GCM encryption
- **Authorization** - Role-based access control

## Monitoring

Key metrics to monitor:

- `storage_append_latency_ms` - Storage write latency
- `storage_read_latency_ms` - Storage read latency
- `metadata_update_latency_ms` - Metadata update latency
- `compute_cache_hit_rate` - Cache effectiveness
- `compute_active_connections` - Active client connections

## References

- RFC-007: Disaggregated Storage and Compute Architecture
- [Amazon S3 Express One Zone](https://aws.amazon.com/s3/storage-classes/express-one-zone/)
- [Snowflake Architecture](https://docs.snowflake.com/en/user-guide/intro-key-concepts)
- [Apache Kafka Tiered Storage KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405)

## Next Steps

1. Implement segment writer and reader classes
2. Add consensus store integration (Raft/etcd)
3. Implement Kafka request handlers in compute layer
4. Add comprehensive test coverage
5. Performance benchmarking
6. Documentation and examples
