# RFC-008: Native Multi-Tenancy with Strong Isolation

**Authors**: Redpanda Engineering Team  
**Status**: Proposed  
**Created**: 2025-01-15  
**Discussion**: [GitHub Issue #XXXX]

## Summary

This RFC proposes comprehensive multi-tenancy support for Redpanda with strong resource isolation, quota enforcement, performance guarantees, and tenant-level encryption, enabling true SaaS deployments with per-tenant SLAs.

## Motivation

### Current Limitations

The current Redpanda architecture provides limited multi-tenancy through:
- Topic-level ACLs for access control
- Basic quota mechanisms
- No resource isolation between tenants
- No performance guarantees
- Shared encryption keys

### Problems

1. **Noisy Neighbor**: One tenant can impact others' performance
2. **Resource Starvation**: No guaranteed minimum resources per tenant
3. **Security Concerns**: Shared encryption and potential data leakage
4. **Billing Complexity**: Difficult to track per-tenant resource usage
5. **SLA Challenges**: Cannot guarantee tenant-specific SLAs

### Requirements

- **Hard Isolation**: CPU, memory, disk, and network isolation
- **Fair Scheduling**: Guaranteed minimum resources with burst capability
- **Security**: Per-tenant encryption and key management
- **Observability**: Detailed per-tenant metrics and billing data
- **Elasticity**: Dynamic resource allocation based on demand

## Detailed Design

### Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│                   Multi-Tenant Redpanda                    │
│                                                            │
│  ┌──────────────────────────────────────────────────────┐ │
│  │              Tenant Admission Controller              │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  │ │
│  │  │  Auth &   │  │  Quota   │  │    Routing       │  │ │
│  │  │  AuthZ    │  │  Check   │  │    Engine        │  │ │
│  │  └──────────┘  └──────────┘  └──────────────────┘  │ │
│  └──────────────────────────────────────────────────────┘ │
│                                                            │
│  ┌──────────────────────────────────────────────────────┐ │
│  │           Resource Domain Management                  │ │
│  │  ┌──────────────────────────────────────────────┐   │ │
│  │  │              Tenant Domain A                  │   │ │
│  │  │  ┌─────────┐ ┌─────────┐ ┌─────────────┐   │   │ │
│  │  │  │   CPU   │ │ Memory  │ │    Disk     │   │   │ │
│  │  │  │  Quota  │ │  Pool   │ │   Quota     │   │   │ │
│  │  │  └─────────┘ └─────────┘ └─────────────┘   │   │ │
│  │  └──────────────────────────────────────────────┘   │ │
│  │  ┌──────────────────────────────────────────────┐   │ │
│  │  │              Tenant Domain B                  │   │ │
│  │  │  ┌─────────┐ ┌─────────┐ ┌─────────────┐   │   │ │
│  │  │  │   CPU   │ │ Memory  │ │    Disk     │   │   │ │
│  │  │  │  Quota  │ │  Pool   │ │   Quota     │   │   │ │
│  │  │  └─────────┘ └─────────┘ └─────────────┘   │   │ │
│  │  └──────────────────────────────────────────────┘   │ │
│  └──────────────────────────────────────────────────────┘ │
│                                                            │
│  ┌──────────────────────────────────────────────────────┐ │
│  │            Scheduling & Isolation Layer               │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  │ │
│  │  │   CPU    │  │  Memory  │  │      I/O         │  │ │
│  │  │ Scheduler│  │  Manager │  │    Scheduler     │  │ │
│  │  └──────────┘  └──────────┘  └──────────────────┘  │ │
│  └──────────────────────────────────────────────────────┘ │
│                                                            │
│  ┌──────────────────────────────────────────────────────┐ │
│  │            Tenant Security & Encryption               │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  │ │
│  │  │   Key    │  │Encryption│  │    Audit         │  │ │
│  │  │  Manager │  │  Engine  │  │    Logger        │  │ │
│  │  └──────────┘  └──────────┘  └──────────────────┘  │ │
│  └──────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

### Core Components

#### 1. Tenant Resource Domains

```cpp
namespace redpanda::multitenancy {

// Core tenant abstraction
class tenant {
public:
    struct tenant_id {
        std::string id;
        std::string organization;
        
        bool operator==(const tenant_id& other) const {
            return id == other.id && organization == other.organization;
        }
    };
    
    struct resource_limits {
        // CPU limits
        struct cpu_quota {
            std::chrono::microseconds quota_per_second;
            std::chrono::microseconds burst_quota;
            size_t max_concurrent_requests;
            double cpu_shares;  // For proportional share scheduling
        };
        
        // Memory limits
        struct memory_quota {
            size_t soft_limit_bytes;  // Can be exceeded temporarily
            size_t hard_limit_bytes;  // Never exceeded
            size_t cache_size_bytes;
            size_t buffer_pool_bytes;
        };
        
        // Disk I/O limits
        struct disk_quota {
            size_t read_bytes_per_second;
            size_t write_bytes_per_second;
            size_t read_iops;
            size_t write_iops;
            size_t max_storage_bytes;
        };
        
        // Network limits
        struct network_quota {
            size_t ingress_bytes_per_second;
            size_t egress_bytes_per_second;
            size_t max_connections;
            size_t max_partitions;
        };
        
        cpu_quota cpu;
        memory_quota memory;
        disk_quota disk;
        network_quota network;
        
        // Request rate limits
        rate_limit produce_rate;
        rate_limit fetch_rate;
        rate_limit metadata_rate;
    };
    
    struct usage_statistics {
        // Current usage
        std::atomic<size_t> cpu_time_us{0};
        std::atomic<size_t> memory_bytes{0};
        std::atomic<size_t> disk_bytes{0};
        std::atomic<size_t> network_bytes_in{0};
        std::atomic<size_t> network_bytes_out{0};
        
        // Historical metrics
        sliding_window<size_t> cpu_usage_history;
        sliding_window<size_t> memory_usage_history;
        
        // Request counts
        std::atomic<size_t> produce_requests{0};
        std::atomic<size_t> fetch_requests{0};
        std::atomic<size_t> errors{0};
    };
    
public:
    tenant(tenant_id id, resource_limits limits)
        : _id(std::move(id))
        , _limits(limits)
        , _domain(create_resource_domain(limits)) {
    }
    
    const tenant_id& id() const { return _id; }
    const resource_limits& limits() const { return _limits; }
    const usage_statistics& usage() const { return _usage; }
    resource_domain& domain() { return _domain; }
    
    ss::future<> update_limits(resource_limits new_limits) {
        // Validate limits
        validate_resource_limits(new_limits);
        
        // Update resource domain
        co_await _domain.reconfigure(new_limits);
        
        _limits = new_limits;
    }
    
private:
    tenant_id _id;
    resource_limits _limits;
    usage_statistics _usage;
    resource_domain _domain;
};

// Resource domain provides isolation
class resource_domain {
public:
    resource_domain(tenant::resource_limits limits)
        : _cpu_scheduler(limits.cpu)
        , _memory_manager(limits.memory)
        , _io_scheduler(limits.disk)
        , _network_shaper(limits.network) {
    }
    
    // Execute work within tenant domain
    template<typename Func>
    ss::future<std::invoke_result_t<Func>>
    execute(Func&& func, execution_context ctx) {
        // Account for CPU
        co_await _cpu_scheduler.acquire_cpu_time(ctx.estimated_cpu_time);
        
        // Set memory context
        auto mem_guard = _memory_manager.enter_context();
        
        // Set I/O context
        auto io_guard = _io_scheduler.enter_context();
        
        // Execute with accounting
        auto start = clock::now();
        
        try {
            auto result = co_await func();
            
            // Update usage statistics
            auto duration = clock::now() - start;
            update_usage_stats(duration, ctx);
            
            co_return result;
            
        } catch (...) {
            // Still account for resources on error
            auto duration = clock::now() - start;
            update_usage_stats(duration, ctx);
            throw;
        }
    }
    
private:
    cpu_scheduler _cpu_scheduler;
    memory_manager _memory_manager;
    io_scheduler _io_scheduler;
    network_shaper _network_shaper;
};

} // namespace redpanda::multitenancy
```

#### 2. CPU Scheduling and Isolation

```cpp
namespace redpanda::multitenancy {

// Hierarchical CPU scheduler with CFS-like behavior
class cpu_scheduler {
public:
    cpu_scheduler(tenant::resource_limits::cpu_quota quota)
        : _quota(quota)
        , _token_bucket(quota.quota_per_second, quota.burst_quota) {
    }
    
    // Fair queuing with priority
    ss::future<> acquire_cpu_time(duration estimated_time) {
        // Check if we have tokens
        while (!_token_bucket.try_consume(estimated_time)) {
            // Add to wait queue with virtual runtime
            auto vruntime = calculate_vruntime(estimated_time);
            
            _wait_queue.emplace(vruntime, ss::promise<>());
            auto& entry = _wait_queue.back();
            
            // Wait for our turn
            co_await entry.promise.get_future();
        }
        
        // Track active requests
        _active_requests++;
        
        if (_active_requests > _quota.max_concurrent_requests) {
            // Apply backpressure
            co_await apply_backpressure();
        }
    }
    
    void release_cpu_time(duration actual_time) {
        // Return unused tokens
        auto unused = _estimated_time - actual_time;
        if (unused > 0ns) {
            _token_bucket.return_tokens(unused);
        }
        
        _active_requests--;
        
        // Wake up waiters
        process_wait_queue();
    }
    
private:
    // CFS-inspired virtual runtime calculation
    std::chrono::nanoseconds calculate_vruntime(duration requested_time) {
        // vruntime = actual_runtime * (nice_0_weight / weight)
        auto weight = _quota.cpu_shares;
        auto nice_0_weight = 1024.0;  // Default nice value weight
        
        auto vruntime = requested_time * (nice_0_weight / weight);
        return std::chrono::duration_cast<std::chrono::nanoseconds>(vruntime);
    }
    
    void process_wait_queue() {
        while (!_wait_queue.empty() && _token_bucket.available() > 0) {
            auto& entry = _wait_queue.top();
            entry.promise.set_value();
            _wait_queue.pop();
        }
    }
    
    ss::future<> apply_backpressure() {
        auto delay = calculate_backpressure_delay();
        co_await ss::sleep(delay);
    }
    
private:
    tenant::resource_limits::cpu_quota _quota;
    token_bucket _token_bucket;
    
    struct wait_entry {
        std::chrono::nanoseconds vruntime;
        ss::promise<> promise;
        
        bool operator<(const wait_entry& other) const {
            return vruntime > other.vruntime;  // Min heap
        }
    };
    
    std::priority_queue<wait_entry> _wait_queue;
    std::atomic<size_t> _active_requests{0};
};

// CPU accounting with cgroups v2 integration
class cpu_accounting {
public:
    ss::future<> initialize_cgroup(const tenant_id& tid) {
        auto cgroup_path = fmt::format("/sys/fs/cgroup/redpanda/{}", tid.id);
        
        // Create cgroup
        co_await create_directory(cgroup_path);
        
        // Set CPU limits
        co_await write_file(
            fmt::format("{}/cpu.max", cgroup_path),
            fmt::format("{} 100000", _quota.quota_per_second.count())
        );
        
        // Set CPU shares for proportional scheduling
        co_await write_file(
            fmt::format("{}/cpu.weight", cgroup_path),
            fmt::format("{}", static_cast<int>(_quota.cpu_shares))
        );
    }
    
    ss::future<cpu_stats> get_cpu_stats(const tenant_id& tid) {
        auto cgroup_path = fmt::format("/sys/fs/cgroup/redpanda/{}", tid.id);
        
        // Read CPU statistics
        auto stat_content = co_await read_file(
            fmt::format("{}/cpu.stat", cgroup_path)
        );
        
        co_return parse_cpu_stats(stat_content);
    }
    
private:
    struct cpu_stats {
        std::chrono::microseconds usage_usec;
        std::chrono::microseconds user_usec;
        std::chrono::microseconds system_usec;
        size_t nr_periods;
        size_t nr_throttled;
        std::chrono::microseconds throttled_usec;
    };
    
    cpu_stats parse_cpu_stats(std::string_view content) {
        cpu_stats stats;
        // Parse cgroup v2 cpu.stat format
        // usage_usec 123456789
        // user_usec 123456789
        // system_usec 123456789
        // nr_periods 1234
        // nr_throttled 12
        // throttled_usec 123456
        
        std::istringstream iss(std::string(content));
        std::string key;
        size_t value;
        
        while (iss >> key >> value) {
            if (key == "usage_usec") {
                stats.usage_usec = std::chrono::microseconds(value);
            } else if (key == "user_usec") {
                stats.user_usec = std::chrono::microseconds(value);
            } else if (key == "system_usec") {
                stats.system_usec = std::chrono::microseconds(value);
            } else if (key == "nr_periods") {
                stats.nr_periods = value;
            } else if (key == "nr_throttled") {
                stats.nr_throttled = value;
            } else if (key == "throttled_usec") {
                stats.throttled_usec = std::chrono::microseconds(value);
            }
        }
        
        return stats;
    }
};

} // namespace redpanda::multitenancy
```

#### 3. Memory Isolation and Management

```cpp
namespace redpanda::multitenancy {

// Per-tenant memory management
class memory_manager {
public:
    memory_manager(tenant::resource_limits::memory_quota quota)
        : _quota(quota)
        , _allocator(create_tenant_allocator(quota)) {
        
        // Pre-allocate memory pools
        _buffer_pool.reserve(quota.buffer_pool_bytes);
        _cache.set_max_size(quota.cache_size_bytes);
    }
    
    // Custom allocator for tenant
    class tenant_allocator {
    public:
        void* allocate(size_t size) {
            // Check against hard limit
            if (_current_usage + size > _quota.hard_limit_bytes) {
                throw std::bad_alloc();
            }
            
            // Check against soft limit
            if (_current_usage + size > _quota.soft_limit_bytes) {
                // Try to reclaim memory
                reclaim_memory(size);
                
                // Still over soft limit? Apply backpressure
                if (_current_usage + size > _quota.soft_limit_bytes) {
                    apply_memory_backpressure();
                }
            }
            
            // Allocate from tenant's memory region
            void* ptr = _memory_region.allocate(size);
            if (ptr) {
                _current_usage += size;
                track_allocation(ptr, size);
            }
            
            return ptr;
        }
        
        void deallocate(void* ptr) {
            if (auto size = get_allocation_size(ptr)) {
                _memory_region.deallocate(ptr);
                _current_usage -= *size;
                untrack_allocation(ptr);
            }
        }
        
    private:
        void reclaim_memory(size_t needed) {
            // Evict from cache
            size_t evicted = _cache.evict_lru(needed);
            
            // Shrink buffers if needed
            if (evicted < needed) {
                _buffer_pool.shrink(needed - evicted);
            }
        }
        
        void apply_memory_backpressure() {
            // Slow down allocations
            std::this_thread::sleep_for(
                calculate_backpressure_delay(_current_usage, _quota)
            );
        }
        
    private:
        tenant::resource_limits::memory_quota _quota;
        memory_region _memory_region;
        std::atomic<size_t> _current_usage{0};
        absl::flat_hash_map<void*, size_t> _allocation_tracker;
    };
    
    // Scoped memory context for requests
    class memory_context {
    public:
        memory_context(memory_manager& mgr)
            : _mgr(mgr)
            , _old_allocator(ss::memory::get_current_allocator()) {
            
            // Switch to tenant allocator
            ss::memory::set_current_allocator(&_mgr._allocator);
        }
        
        ~memory_context() {
            // Restore previous allocator
            ss::memory::set_current_allocator(_old_allocator);
            
            // Update usage statistics
            _mgr.update_usage_stats();
        }
        
    private:
        memory_manager& _mgr;
        ss::memory::allocator* _old_allocator;
    };
    
    memory_context enter_context() {
        return memory_context(*this);
    }
    
    // Per-tenant cache
    class tenant_cache {
    public:
        void set_max_size(size_t max_bytes) {
            _max_size = max_bytes;
        }
        
        std::optional<ss::temporary_buffer<char>>
        get(const cache_key& key) {
            if (auto it = _entries.find(key); it != _entries.end()) {
                // Move to front (LRU)
                _lru.erase(it->second.lru_it);
                _lru.push_front(key);
                it->second.lru_it = _lru.begin();
                
                // Update stats
                it->second.hits++;
                it->second.last_access = clock::now();
                
                return it->second.data.share();
            }
            
            return std::nullopt;
        }
        
        void put(const cache_key& key, ss::temporary_buffer<char> data) {
            auto size = data.size();
            
            // Evict if necessary
            while (_current_size + size > _max_size && !_lru.empty()) {
                evict_one();
            }
            
            // Add new entry
            _lru.push_front(key);
            _entries[key] = {
                .data = std::move(data),
                .size = size,
                .lru_it = _lru.begin(),
                .last_access = clock::now()
            };
            
            _current_size += size;
        }
        
        size_t evict_lru(size_t target_bytes) {
            size_t evicted = 0;
            
            while (evicted < target_bytes && !_lru.empty()) {
                evicted += evict_one();
            }
            
            return evicted;
        }
        
    private:
        size_t evict_one() {
            if (_lru.empty()) return 0;
            
            auto key = _lru.back();
            _lru.pop_back();
            
            auto it = _entries.find(key);
            if (it != _entries.end()) {
                auto size = it->second.size;
                _entries.erase(it);
                _current_size -= size;
                return size;
            }
            
            return 0;
        }
        
    private:
        struct cache_entry {
            ss::temporary_buffer<char> data;
            size_t size;
            std::list<cache_key>::iterator lru_it;
            std::chrono::steady_clock::time_point last_access;
            size_t hits = 0;
        };
        
        absl::flat_hash_map<cache_key, cache_entry> _entries;
        std::list<cache_key> _lru;
        size_t _max_size;
        size_t _current_size = 0;
    };
    
private:
    tenant::resource_limits::memory_quota _quota;
    tenant_allocator _allocator;
    tenant_cache _cache;
    buffer_pool _buffer_pool;
};

} // namespace redpanda::multitenancy
```

#### 4. I/O Scheduling and Prioritization

```cpp
namespace redpanda::multitenancy {

// Hierarchical I/O scheduler with tenant priorities
class io_scheduler {
public:
    io_scheduler(tenant::resource_limits::disk_quota quota)
        : _quota(quota)
        , _read_limiter(quota.read_bytes_per_second, quota.read_iops)
        , _write_limiter(quota.write_bytes_per_second, quota.write_iops) {
    }
    
    // Schedule I/O request with tenant priority
    template<typename Op>
    ss::future<size_t> schedule_io(io_direction direction, size_t size, Op&& op) {
        io_limiter* limiter = (direction == io_direction::read) 
            ? &_read_limiter : &_write_limiter;
        
        // Wait for tokens
        co_await limiter->acquire(size);
        
        // Tag I/O with tenant ID for kernel scheduling
        auto io_context = create_io_context();
        
        try {
            // Execute I/O operation
            auto bytes_transferred = co_await op();
            
            // Update statistics
            update_io_stats(direction, bytes_transferred);
            
            co_return bytes_transferred;
            
        } catch (...) {
            // Return unused tokens on error
            limiter->release(size);
            throw;
        }
    }
    
private:
    class io_limiter {
    public:
        io_limiter(size_t bytes_per_sec, size_t iops)
            : _bandwidth_bucket(bytes_per_sec, bytes_per_sec / 10)  // 100ms burst
            , _iops_bucket(iops, iops / 10) {
        }
        
        ss::future<> acquire(size_t bytes) {
            // Wait for bandwidth tokens
            co_await _bandwidth_bucket.acquire(bytes);
            
            // Wait for IOPS token
            co_await _iops_bucket.acquire(1);
        }
        
        void release(size_t bytes) {
            _bandwidth_bucket.release(bytes);
            _iops_bucket.release(1);
        }
        
    private:
        token_bucket _bandwidth_bucket;
        token_bucket _iops_bucket;
    };
    
    // Integration with Linux cgroup v2 I/O controller
    class cgroup_io_controller {
    public:
        ss::future<> configure_tenant_io(
            const tenant_id& tid,
            const tenant::resource_limits::disk_quota& quota
        ) {
            auto cgroup_path = fmt::format("/sys/fs/cgroup/redpanda/{}", tid.id);
            
            // Set I/O limits (using io.max for cgroup v2)
            // Format: "MAJ:MIN rbps=X wbps=Y riops=Z wiops=W"
            for (const auto& device : get_block_devices()) {
                auto io_max = fmt::format(
                    "{}:{} rbps={} wbps={} riops={} wiops={}",
                    device.major, device.minor,
                    quota.read_bytes_per_second,
                    quota.write_bytes_per_second,
                    quota.read_iops,
                    quota.write_iops
                );
                
                co_await write_file(
                    fmt::format("{}/io.max", cgroup_path),
                    io_max
                );
            }
            
            // Set I/O weight for proportional scheduling
            co_await write_file(
                fmt::format("{}/io.weight", cgroup_path),
                fmt::format("{}", calculate_io_weight(quota))
            );
        }
        
        ss::future<io_stats> get_io_stats(const tenant_id& tid) {
            auto cgroup_path = fmt::format("/sys/fs/cgroup/redpanda/{}", tid.id);
            
            auto stat_content = co_await read_file(
                fmt::format("{}/io.stat", cgroup_path)
            );
            
            co_return parse_io_stats(stat_content);
        }
        
    private:
        struct io_stats {
            size_t read_bytes;
            size_t write_bytes;
            size_t read_ios;
            size_t write_ios;
            std::chrono::microseconds read_wait_time;
            std::chrono::microseconds write_wait_time;
        };
    };
    
    // io_uring integration for efficient I/O
    class uring_io_context {
    public:
        ss::future<size_t> submit_read(
            int fd,
            void* buf,
            size_t len,
            off_t offset,
            tenant_id tid
        ) {
            io_uring_sqe* sqe = io_uring_get_sqe(&_ring);
            
            io_uring_prep_read(sqe, fd, buf, len, offset);
            
            // Set user data to track tenant
            sqe->user_data = reinterpret_cast<uint64_t>(&tid);
            
            // Set I/O priority based on tenant
            sqe->ioprio = get_tenant_io_priority(tid);
            
            io_uring_submit(&_ring);
            
            // Wait for completion
            io_uring_cqe* cqe;
            io_uring_wait_cqe(&_ring, &cqe);
            
            auto result = cqe->res;
            io_uring_cqe_seen(&_ring, cqe);
            
            co_return result;
        }
        
    private:
        io_uring _ring;
    };
    
private:
    tenant::resource_limits::disk_quota _quota;
    io_limiter _read_limiter;
    io_limiter _write_limiter;
    cgroup_io_controller _cgroup_controller;
    uring_io_context _uring_context;
};

} // namespace redpanda::multitenancy
```

#### 5. Network Traffic Shaping

```cpp
namespace redpanda::multitenancy {

// Network bandwidth and connection management
class network_shaper {
public:
    network_shaper(tenant::resource_limits::network_quota quota)
        : _quota(quota)
        , _ingress_limiter(quota.ingress_bytes_per_second)
        , _egress_limiter(quota.egress_bytes_per_second)
        , _connection_limiter(quota.max_connections) {
    }
    
    // Shape incoming traffic
    ss::future<> shape_ingress(size_t bytes) {
        co_await _ingress_limiter.acquire(bytes);
        _stats.ingress_bytes += bytes;
    }
    
    // Shape outgoing traffic
    ss::future<> shape_egress(size_t bytes) {
        co_await _egress_limiter.acquire(bytes);
        _stats.egress_bytes += bytes;
    }
    
    // Connection management
    ss::future<connection_permit> acquire_connection() {
        if (_active_connections >= _quota.max_connections) {
            // Wait for a connection to be released
            co_await _connection_available.wait([this] {
                return _active_connections < _quota.max_connections;
            });
        }
        
        _active_connections++;
        co_return connection_permit(*this);
    }
    
    class connection_permit {
    public:
        connection_permit(network_shaper& shaper)
            : _shaper(shaper) {}
        
        ~connection_permit() {
            _shaper.release_connection();
        }
        
        connection_permit(connection_permit&&) = default;
        connection_permit& operator=(connection_permit&&) = default;
        
    private:
        network_shaper& _shaper;
    };
    
private:
    void release_connection() {
        _active_connections--;
        _connection_available.signal();
    }
    
    // TC (Traffic Control) integration for kernel-level shaping
    class tc_shaper {
    public:
        ss::future<> configure_tenant_tc(
            const tenant_id& tid,
            const tenant::resource_limits::network_quota& quota
        ) {
            // Create TC class for tenant
            auto cmd = fmt::format(
                "tc class add dev {} parent 1: classid 1:{} htb "
                "rate {}kbit ceil {}kbit burst 64kb",
                _interface,
                get_tenant_class_id(tid),
                quota.egress_bytes_per_second * 8 / 1024,  // Convert to kbit
                quota.egress_bytes_per_second * 8 / 1024 * 1.1  // Allow 10% burst
            );
            
            co_await execute_command(cmd);
            
            // Add filter to classify tenant traffic
            cmd = fmt::format(
                "tc filter add dev {} parent 1: protocol ip prio 1 "
                "handle {} fw flowid 1:{}",
                _interface,
                get_tenant_mark(tid),
                get_tenant_class_id(tid)
            );
            
            co_await execute_command(cmd);
        }
        
    private:
        std::string _interface = "eth0";
    };
    
    // eBPF-based traffic shaping (modern approach)
    class ebpf_shaper {
    public:
        ss::future<> load_tenant_program(const tenant_id& tid) {
            // BPF program for per-tenant rate limiting
            const char* bpf_program = R"(
                #include <linux/bpf.h>
                #include <linux/pkt_cls.h>
                
                struct tenant_limits {
                    __u64 rate_bps;
                    __u64 last_time;
                    __u64 tokens;
                };
                
                BPF_HASH(tenant_map, __u32, struct tenant_limits);
                
                int tc_egress(struct __sk_buff *skb) {
                    __u32 tenant_id = get_tenant_id(skb);
                    struct tenant_limits *limits = tenant_map.lookup(&tenant_id);
                    
                    if (!limits) {
                        return TC_ACT_OK;
                    }
                    
                    __u64 now = bpf_ktime_get_ns();
                    __u64 elapsed = now - limits->last_time;
                    
                    // Token bucket algorithm
                    __u64 new_tokens = elapsed * limits->rate_bps / 1000000000;
                    limits->tokens = min(limits->tokens + new_tokens, limits->rate_bps);
                    
                    if (skb->len > limits->tokens) {
                        // Drop or queue packet
                        return TC_ACT_SHOT;
                    }
                    
                    limits->tokens -= skb->len;
                    limits->last_time = now;
                    
                    return TC_ACT_OK;
                }
            )";
            
            // Load and attach BPF program
            auto prog = co_await compile_bpf_program(bpf_program);
            co_await attach_tc_program(prog, "tc_egress");
            
            // Update tenant limits map
            co_await update_bpf_map("tenant_map", tid.id, {
                .rate_bps = _quota.egress_bytes_per_second,
                .last_time = 0,
                .tokens = _quota.egress_bytes_per_second
            });
        }
    };
    
private:
    tenant::resource_limits::network_quota _quota;
    token_bucket _ingress_limiter;
    token_bucket _egress_limiter;
    connection_limiter _connection_limiter;
    std::atomic<size_t> _active_connections{0};
    ss::condition_variable _connection_available;
    
    struct {
        std::atomic<size_t> ingress_bytes{0};
        std::atomic<size_t> egress_bytes{0};
    } _stats;
};

} // namespace redpanda::multitenancy
```

#### 6. Tenant-Level Encryption

```cpp
namespace redpanda::multitenancy {

// Per-tenant encryption and key management
class tenant_encryption_manager {
public:
    struct encryption_config {
        encryption_algorithm algorithm = encryption_algorithm::aes_256_gcm;
        key_derivation_function kdf = key_derivation_function::pbkdf2;
        size_t kdf_iterations = 100000;
        bool encrypt_at_rest = true;
        bool encrypt_in_transit = true;
        key_rotation_policy rotation_policy;
    };
    
    class tenant_key_manager {
    public:
        ss::future<> initialize_tenant(
            const tenant_id& tid,
            const encryption_config& config
        ) {
            // Generate master key for tenant
            auto master_key = generate_master_key();
            
            // Store in secure key store (HSM/KMS integration)
            co_await _key_store.store_key(
                make_key_id(tid, key_type::master),
                master_key,
                {.hsm_backed = true}
            );
            
            // Derive data encryption key
            auto dek = derive_data_key(master_key, tid);
            
            // Cache DEK in memory (encrypted)
            _key_cache[tid] = encrypt_key_for_cache(dek);
            
            // Set up key rotation schedule
            _rotation_scheduler.schedule(tid, config.rotation_policy);
        }
        
        ss::future<encryption_key> get_data_key(const tenant_id& tid) {
            // Check cache first
            if (auto it = _key_cache.find(tid); it != _key_cache.end()) {
                co_return decrypt_key_from_cache(it->second);
            }
            
            // Fetch from key store
            auto master_key = co_await _key_store.get_key(
                make_key_id(tid, key_type::master)
            );
            
            auto dek = derive_data_key(master_key, tid);
            
            // Update cache
            _key_cache[tid] = encrypt_key_for_cache(dek);
            
            co_return dek;
        }
        
        ss::future<> rotate_keys(const tenant_id& tid) {
            // Generate new master key
            auto new_master = generate_master_key();
            
            // Get old master key
            auto old_master = co_await _key_store.get_key(
                make_key_id(tid, key_type::master)
            );
            
            // Store new key
            co_await _key_store.store_key(
                make_key_id(tid, key_type::master),
                new_master,
                {.hsm_backed = true}
            );
            
            // Mark old key for re-encryption
            _reencryption_queue.push({
                .tenant = tid,
                .old_key = old_master,
                .new_key = new_master,
                .timestamp = clock::now()
            });
            
            // Clear cache to force new key usage
            _key_cache.erase(tid);
            
            // Start background re-encryption
            trigger_reencryption(tid);
        }
        
    private:
        encryption_key derive_data_key(
            const encryption_key& master,
            const tenant_id& tid
        ) {
            // Use HKDF to derive data key
            std::array<uint8_t, 32> derived;
            
            HKDF(
                derived.data(), derived.size(),
                EVP_sha256(),
                master.data(), master.size(),
                nullptr, 0,  // No salt
                reinterpret_cast<const uint8_t*>(tid.id.data()),
                tid.id.size()
            );
            
            return encryption_key(derived);
        }
        
    private:
        secure_key_store _key_store;
        absl::flat_hash_map<tenant_id, encrypted_cache_key> _key_cache;
        key_rotation_scheduler _rotation_scheduler;
        reencryption_queue _reencryption_queue;
    };
    
    // Transparent encryption for tenant data
    class data_encryptor {
    public:
        ss::future<encrypted_batch> encrypt_batch(
            const tenant_id& tid,
            model::record_batch batch
        ) {
            // Get tenant's encryption key
            auto key = co_await _key_manager.get_data_key(tid);
            
            // Generate IV for this batch
            auto iv = generate_iv();
            
            // Serialize batch
            iobuf serialized = batch.to_iobuf();
            
            // Encrypt data
            iobuf encrypted;
            
            EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
            EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, key.data(), iv.data());
            
            for (const auto& frag : serialized) {
                std::array<uint8_t, 4096 + EVP_CIPHER_block_size(EVP_aes_256_gcm())> out;
                int out_len;
                
                EVP_EncryptUpdate(
                    ctx,
                    out.data(), &out_len,
                    frag.get(), frag.size()
                );
                
                encrypted.append(out.data(), out_len);
            }
            
            // Finalize and get auth tag
            std::array<uint8_t, 128> final_out;
            int final_len;
            EVP_EncryptFinal_ex(ctx, final_out.data(), &final_len);
            encrypted.append(final_out.data(), final_len);
            
            std::array<uint8_t, 16> auth_tag;
            EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, 16, auth_tag.data());
            
            EVP_CIPHER_CTX_free(ctx);
            
            co_return encrypted_batch{
                .data = std::move(encrypted),
                .iv = iv,
                .auth_tag = auth_tag,
                .algorithm = encryption_algorithm::aes_256_gcm,
                .key_id = make_key_id(tid, key_type::data)
            };
        }
        
        ss::future<model::record_batch> decrypt_batch(
            const tenant_id& tid,
            encrypted_batch encrypted
        ) {
            // Get tenant's encryption key
            auto key = co_await _key_manager.get_data_key(tid);
            
            // Decrypt data
            iobuf decrypted;
            
            EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
            EVP_DecryptInit_ex(
                ctx,
                EVP_aes_256_gcm(),
                nullptr,
                key.data(),
                encrypted.iv.data()
            );
            
            // Set auth tag
            EVP_CIPHER_CTX_ctrl(
                ctx,
                EVP_CTRL_GCM_SET_TAG,
                encrypted.auth_tag.size(),
                const_cast<uint8_t*>(encrypted.auth_tag.data())
            );
            
            for (const auto& frag : encrypted.data) {
                std::array<uint8_t, 4096 + EVP_CIPHER_block_size(EVP_aes_256_gcm())> out;
                int out_len;
                
                EVP_DecryptUpdate(
                    ctx,
                    out.data(), &out_len,
                    frag.get(), frag.size()
                );
                
                decrypted.append(out.data(), out_len);
            }
            
            // Verify auth tag
            std::array<uint8_t, 128> final_out;
            int final_len;
            
            if (EVP_DecryptFinal_ex(ctx, final_out.data(), &final_len) != 1) {
                EVP_CIPHER_CTX_free(ctx);
                throw encryption_exception("Authentication tag verification failed");
            }
            
            decrypted.append(final_out.data(), final_len);
            EVP_CIPHER_CTX_free(ctx);
            
            // Deserialize batch
            co_return model::record_batch::from_iobuf(std::move(decrypted));
        }
        
    private:
        std::array<uint8_t, 12> generate_iv() {
            std::array<uint8_t, 12> iv;
            RAND_bytes(iv.data(), iv.size());
            return iv;
        }
        
    private:
        tenant_key_manager& _key_manager;
    };
    
    // Background re-encryption after key rotation
    class reencryption_service {
    public:
        ss::future<> reencrypt_tenant_data(
            const tenant_id& tid,
            const encryption_key& old_key,
            const encryption_key& new_key
        ) {
            // Get all partitions for tenant
            auto partitions = co_await get_tenant_partitions(tid);
            
            for (const auto& partition : partitions) {
                co_await reencrypt_partition(partition, old_key, new_key);
            }
        }
        
    private:
        ss::future<> reencrypt_partition(
            const model::ntp& ntp,
            const encryption_key& old_key,
            const encryption_key& new_key
        ) {
            // Read segments
            auto segments = co_await list_partition_segments(ntp);
            
            for (const auto& segment : segments) {
                // Read with old key
                auto data = co_await read_and_decrypt_segment(segment, old_key);
                
                // Encrypt with new key
                auto encrypted = co_await encrypt_segment(data, new_key);
                
                // Write back
                co_await write_encrypted_segment(segment, encrypted);
            }
        }
    };
    
private:
    tenant_key_manager _key_manager;
    data_encryptor _encryptor;
    reencryption_service _reencryption_service;
};

} // namespace redpanda::multitenancy
```

### Request Flow with Multi-Tenancy

```cpp
class multi_tenant_request_handler {
    ss::future<produce_response> handle_produce(
        produce_request request,
        request_context ctx
    ) {
        // 1. Identify tenant
        auto tenant_id = extract_tenant_id(ctx);
        auto tenant = co_await _tenant_manager.get_tenant(tenant_id);
        
        // 2. Check quotas
        if (!tenant->check_produce_quota(request)) {
            co_return make_quota_exceeded_response();
        }
        
        // 3. Execute within tenant domain
        co_return co_await tenant->domain().execute(
            [this, request = std::move(request)]() -> ss::future<produce_response> {
                // Normal produce handling, but with resource accounting
                co_return co_await _kafka_handler.handle_produce(std::move(request));
            },
            {.estimated_cpu_time = estimate_produce_cpu_time(request)}
        );
    }
};
```

### Monitoring and Metrics

```yaml
# Per-tenant metrics
tenant_cpu_usage_seconds: counter
tenant_cpu_throttled_seconds: counter
tenant_memory_usage_bytes: gauge
tenant_memory_limit_hits: counter
tenant_disk_read_bytes: counter
tenant_disk_write_bytes: counter
tenant_network_rx_bytes: counter
tenant_network_tx_bytes: counter
tenant_produce_requests: counter
tenant_fetch_requests: counter
tenant_errors: counter
tenant_quota_violations: counter
```

## Testing Strategy

### Unit Tests

```cpp
BOOST_AUTO_TEST_CASE(test_cpu_scheduling) {
    tenant::resource_limits limits;
    limits.cpu.quota_per_second = 100ms;
    limits.cpu.max_concurrent_requests = 10;
    
    cpu_scheduler scheduler(limits.cpu);
    
    // Test token bucket behavior
    auto start = clock::now();
    
    std::vector<ss::future<>> futures;
    for (int i = 0; i < 20; ++i) {
        futures.push_back(scheduler.acquire_cpu_time(10ms));
    }
    
    ss::when_all(futures.begin(), futures.end()).get();
    
    auto elapsed = clock::now() - start;
    
    // Should take ~200ms due to quota
    BOOST_REQUIRE_GE(elapsed, 180ms);
    BOOST_REQUIRE_LE(elapsed, 220ms);
}

BOOST_AUTO_TEST_CASE(test_memory_isolation) {
    tenant::resource_limits limits;
    limits.memory.soft_limit_bytes = 100_MiB;
    limits.memory.hard_limit_bytes = 150_MiB;
    
    memory_manager mgr(limits.memory);
    
    // Test allocation within limits
    {
        auto ctx = mgr.enter_context();
        auto buf = ss::allocate_aligned_buffer<char>(50_MiB, 4096);
        BOOST_REQUIRE(buf);
    }
    
    // Test hard limit enforcement
    {
        auto ctx = mgr.enter_context();
        BOOST_CHECK_THROW(
            ss::allocate_aligned_buffer<char>(200_MiB, 4096),
            std::bad_alloc
        );
    }
}
```

### Integration Tests

```cpp
class multi_tenancy_integration_test {
    ss::future<> test_tenant_isolation() {
        // Create two tenants with different quotas
        auto tenant_a = co_await create_tenant("tenant-a", {
            .cpu = {.quota_per_second = 100ms},
            .memory = {.hard_limit_bytes = 1_GiB}
        });
        
        auto tenant_b = co_await create_tenant("tenant-b", {
            .cpu = {.quota_per_second = 200ms},
            .memory = {.hard_limit_bytes = 2_GiB}
        });
        
        // Run workloads concurrently
        auto workload_a = run_tenant_workload(tenant_a, high_cpu_workload);
        auto workload_b = run_tenant_workload(tenant_b, high_memory_workload);
        
        auto [result_a, result_b] = co_await ss::when_all(
            std::move(workload_a),
            std::move(workload_b)
        );
        
        // Verify isolation
        BOOST_REQUIRE_LE(result_a.cpu_usage, 100ms);
        BOOST_REQUIRE_LE(result_b.memory_usage, 2_GiB);
        
        // Verify no cross-tenant impact
        BOOST_REQUIRE_LE(result_a.p99_latency, 10ms);
        BOOST_REQUIRE_LE(result_b.p99_latency, 10ms);
    }
};
```

## Migration Strategy

### Phase 1: Infrastructure (Months 1-3)
- Deploy resource domain infrastructure
- Implement CPU and memory isolation
- Add tenant management APIs

### Phase 2: Resource Control (Months 3-6)
- Enable I/O scheduling
- Deploy network shaping
- Implement quota enforcement

### Phase 3: Security (Months 6-9)
- Deploy per-tenant encryption
- Implement key management
- Enable audit logging

### Phase 4: Production Rollout (Months 9-12)
- Gradual migration of existing workloads
- Performance tuning
- SLA validation

## Configuration

```yaml
# Multi-tenancy configuration
multitenancy_enabled: false
multitenancy_default_cpu_quota_ms: 1000
multitenancy_default_memory_limit_mb: 1024
multitenancy_default_disk_quota_mb: 10240
multitenancy_default_network_bandwidth_mbps: 100

# Encryption configuration
multitenancy_encryption_enabled: true
multitenancy_encryption_algorithm: "aes-256-gcm"
multitenancy_key_rotation_days: 90
multitenancy_kms_endpoint: "https://kms.example.com"

# Resource control
multitenancy_cgroup_enabled: true
multitenancy_ebpf_enabled: false
multitenancy_io_scheduler: "mq-deadline"
```

## Open Questions

1. Should we support nested tenants (organizations with sub-tenants)?
2. How to handle tenant migration between clusters?
3. What billing granularity is needed (per-request, hourly, daily)?
4. Should encryption be mandatory or optional per tenant?
5. How to handle oversubscription of resources?

## References

- [Linux cgroups v2](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html)
- [io_uring](https://kernel.dk/io_uring.pdf)
- [eBPF for networking](https://docs.cilium.io/en/stable/bpf/)
- [AWS Nitro Enclaves](https://aws.amazon.com/ec2/nitro/nitro-enclaves/)