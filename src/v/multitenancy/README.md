# Redpanda Multi-Tenancy

This module implements native multi-tenancy support for Redpanda with strong resource isolation, quota enforcement, and per-tenant encryption.

## Overview

The multi-tenancy implementation provides:

- **Resource Isolation**: CPU, memory, disk, and network isolation per tenant
- **Fair Scheduling**: Guaranteed minimum resources with burst capability
- **Security**: Per-tenant encryption and key management
- **Observability**: Detailed per-tenant metrics and resource tracking
- **Quota Enforcement**: Hard and soft limits with backpressure

## Architecture

### Core Components

1. **Tenant** (`tenant.h`/`tenant.cc`)
   - Core tenant abstraction with resource limits and usage statistics
   - Manages tenant lifecycle and quota validation

2. **Resource Domain** (`resource_domain.h`/`resource_domain.cc`)
   - Provides isolated execution environment for tenant workloads
   - Coordinates CPU, memory, I/O, and network resources

3. **CPU Scheduler** (`cpu_scheduler.h`/`cpu_scheduler.cc`)
   - CFS-inspired fair scheduling with virtual runtime
   - Token bucket rate limiting
   - Concurrent request throttling

4. **Memory Manager** (`memory_manager.h`/`memory_manager.cc`)
   - Per-tenant memory accounting
   - Soft and hard limit enforcement
   - Memory pressure backpressure

5. **I/O Scheduler** (`io_scheduler.h`/`io_scheduler.cc`)
   - Bandwidth and IOPS rate limiting
   - Separate read/write quotas
   - Integration with kernel I/O schedulers

6. **Network Shaper** (`network_shaper.h`/`network_shaper.cc`)
   - Ingress/egress traffic shaping
   - Connection limiting
   - Network bandwidth quotas

7. **Encryption Manager** (`encryption_manager.h`/`encryption_manager.cc`)
   - Per-tenant encryption keys
   - Key rotation support
   - Data-at-rest encryption

8. **Tenant Manager** (`tenant_manager.h`/`tenant_manager.cc`)
   - Central tenant registry
   - Tenant CRUD operations
   - Encryption key management

## Usage

### Creating a Tenant

```cpp
#include "multitenancy/tenant_manager.h"
#include "multitenancy/types.h"

// Create tenant manager
encryption_config enc_config;
tenant_manager mgr(enc_config);

// Define tenant
tenant_id tid{"customer-123", "acme-corp"};

// Set resource limits
resource_limits limits;
limits.cpu.quota_per_second = std::chrono::microseconds(500000);  // 0.5 CPU
limits.memory.hard_limit_bytes = 2ULL * 1024 * 1024 * 1024;      // 2 GB
limits.disk.read_bytes_per_second = 50 * 1024 * 1024;            // 50 MB/s
limits.network.max_connections = 100;

// Create tenant
auto tenant = co_await mgr.create_tenant(tid, limits);
```

### Executing Work in Tenant Context

```cpp
execution_context ctx{
    .estimated_cpu_time = std::chrono::microseconds(1000),
    .estimated_memory_bytes = 1024 * 1024,
};

auto result = co_await tenant->domain().execute(
    [&]() -> seastar::future<int> {
        // Your work here
        co_return 42;
    },
    ctx
);
```

## Configuration

Configuration options are defined in `config.h`:

- `multitenancy_enabled`: Enable/disable multi-tenancy
- `default_cpu_quota_us`: Default CPU quota in microseconds
- `default_memory_limit_mb`: Default memory limit in MB
- `default_disk_quota_mb`: Default disk quota in MB
- `encryption_enabled`: Enable per-tenant encryption
- `key_rotation_days`: Key rotation period

## Testing

Unit tests are located in `tests/`:

- `tenant_test.cc`: Core tenant functionality
- `tenant_manager_test.cc`: Tenant management operations
- `token_bucket_test.cc`: Rate limiting primitives

Run tests:
```bash
bazel test //src/v/multitenancy:all
```

## Implementation Status

This is the initial implementation based on RFC-008. The following features are included:

- [x] Core tenant abstraction
- [x] Resource domain isolation
- [x] CPU scheduling
- [x] Memory management
- [x] I/O scheduling
- [x] Network shaping
- [x] Encryption framework
- [x] Tenant manager
- [x] Basic unit tests

Future enhancements (see RFC for details):

- [ ] cgroup v2 integration
- [ ] eBPF-based traffic shaping
- [ ] io_uring integration
- [ ] Hardware security module (HSM) integration
- [ ] Per-tenant metrics export
- [ ] Integration tests
- [ ] Performance benchmarks

## References

- RFC: `/rfcs/20250115_native_multi_tenancy.md`
- Linux cgroups v2: https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html
- Seastar scheduling: https://seastar.io/
