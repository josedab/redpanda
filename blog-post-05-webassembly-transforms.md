# WebAssembly Transforms: User-Defined Processing at Streaming Speed

**Part 5 of the Redpanda Deep Dive Technical Series**

*An in-depth exploration of how Redpanda brings user-defined functions to streaming data using WebAssembly*

---

## Introduction

Stream processing has traditionally required separate frameworks like Kafka Streams, Flink, or Spark Streaming. These add operational complexity, increase latency, and create data pipeline fragmentation. Redpanda takes a different approach: bring computation to the data through WebAssembly transforms.

WebAssembly (WASM) transforms enable users to write custom data processing logic in any language (Rust, Go, C++, JavaScript) and deploy it directly inside Redpanda brokers. The WASM sandbox provides security and isolation while delivering near-native performance. This architecture enables filtering, enrichment, transformation, and aggregation without external infrastructure.

In this post, we'll explore Redpanda's WASM transform engine, examining the runtime architecture, foreign function interface (FFI), performance optimizations, and real-world applications.

## Why WebAssembly?

WebAssembly offers unique advantages for in-broker processing:

**Security & Isolation**:
- Sandboxed execution prevents malicious code
- Memory isolation between transforms
- CPU time limits prevent runaway processes

**Performance**:
- Near-native execution speed
- Ahead-of-time (AOT) compilation
- SIMD support for vectorized operations

**Portability**:
- Write once, run anywhere
- Language agnostic (Rust, Go, C++, JS, etc.)
- Standard binary format

**Resource Control**:
- Deterministic memory usage
- CPU quota enforcement
- Explicit I/O permissions

## Architecture Overview

Redpanda's WASM subsystem follows a three-tier architecture:

```mermaid
graph TD
    subgraph "Process Level"
        RT[Runtime<br/>Singleton]
    end
    
    subgraph "Transform Level"
        F1[Factory 1<br/>Transform A]
        F2[Factory 2<br/>Transform B]
    end
    
    subgraph "Core Level"
        E1[Engine 1.1<br/>Core 0]
        E2[Engine 1.2<br/>Core 1]
        E3[Engine 2.1<br/>Core 0]
        E4[Engine 2.2<br/>Core 1]
    end
    
    RT -->|Compile Module| F1
    RT -->|Compile Module| F2
    F1 -->|Create Instance| E1
    F1 -->|Create Instance| E2
    F2 -->|Create Instance| E3
    F2 -->|Create Instance| E4
    
    style RT fill:#9f9
    style F1 fill:#ff9
    style F2 fill:#ff9
    style E1 fill:#f99
    style E2 fill:#f99
    style E3 fill:#f99
    style E4 fill:#f99
```

### Core Abstractions

Let's examine the key interfaces from [`engine.h`](src/v/wasm/engine.h:46):

```cpp
namespace wasm {

/**
 * A runtime is capable of creating factories.
 * There should only be a single runtime for a given process.
 */
class runtime {
public:
    struct config {
        struct heap_memory {
            size_t per_core_pool_size_bytes;
            size_t per_engine_memory_limit;
        };
        heap_memory heap_memory;
        
        struct stack_memory {
            bool debug_host_stack_usage;
        };
        stack_memory stack_memory;
        
        struct cpu {
            std::chrono::milliseconds per_invocation_timeout;
        };
        cpu cpu;
    };
    
    virtual ss::future<> start(config) = 0;
    virtual ss::future<> stop() = 0;
    
    // Compile WASM module into factory
    virtual ss::future<ss::shared_ptr<factory>>
        make_factory(
            model::transform_metadata metadata,
            model::wasm_binary_iobuf wasm_binary
        ) = 0;
    
    // Validate WASM module
    virtual ss::future<> validate(model::wasm_binary_iobuf) = 0;
};

/**
 * A factory creates engines from a compiled module.
 * Thread-safe, can be used across cores.
 */
class factory {
public:
    virtual ss::future<ss::shared_ptr<engine>>
        make_engine(std::unique_ptr<wasm::logger>) = 0;
};

/**
 * An engine is a running VM instance.
 * Core-local, not thread-safe.
 */
class engine {
public:
    virtual ss::future<> transform(
        model::record_batch batch,
        transform_probe* probe,
        transform_callback callback
    ) = 0;
    
    virtual ss::future<> start() = 0;
    virtual ss::future<> stop() = 0;
};

} // namespace wasm
```

## Wasmtime Integration

Redpanda uses [Wasmtime](https://wasmtime.dev/) as its WebAssembly runtime. Wasmtime provides high-performance JIT compilation and a robust C API.

### Wasmtime Engine Wrapper

```cpp
class wasmtime_engine : public engine {
    wasmtime_store_t* _store;
    wasmtime_instance_t _instance;
    wasmtime_memory_t _memory;
    
    // Host function state
    std::optional<ss::promise<>> _pending_host_function;
    
public:
    ss::future<> transform(
        model::record_batch batch,
        transform_probe* probe,
        transform_callback callback
    ) override {
        // Write batch to WASM memory
        co_await write_batch_to_wasm(batch);
        
        // Resume WASM execution
        co_await resume_wasm();
        
        // WASM will call back to read records and write outputs
        // via host functions (see FFI section)
        
        co_return;
    }
    
private:
    ss::future<> resume_wasm() {
        // Wasmtime async execution
        wasmtime_call_future_t* future;
        
        auto error = wasmtime_func_call_async(
            _store,
            &_transform_func,
            nullptr,  // No args
            0,
            nullptr,  // No results
            0,
            &future,
            &_wasm_trap
        );
        
        if (error) {
            throw wasm_exception(error);
        }
        
        // Wait for WASM to suspend or complete
        co_await wait_for_wasm_suspension(future);
    }
    
    ss::future<> wait_for_wasm_suspension(wasmtime_call_future_t* future) {
        // Create promise for host function completion
        ss::promise<> pr;
        _pending_host_function = pr.get_future();
        
        // Poll wasmtime in background
        ssx::background = poll_wasmtime_future(future);
        
        // Wait for host function to suspend WASM
        co_await std::move(*_pending_host_function);
        _pending_host_function.reset();
    }
};
```

### Stack Switching

Wasmtime's async support uses explicit stacks:

```cpp
// Wasmtime allocates separate stack for WASM execution
wasmtime_config_async_stack_size_set(config, 2_MiB);

// WASM runs on its stack, switches back to host for I/O
//
// Host Stack        WASM Stack
//     │                 │
//     │   call func     │
//     ├────────────────>│
//     │                 │ executing...
//     │                 │
//     │  host_fn call   │
//     │<────────────────┤
//     │                 │ (suspended)
//     │ I/O operation   │
//     │                 │
//     │ return result   │
//     ├────────────────>│
//     │                 │ resume...
//     │                 │
```

This enables host functions to return futures without blocking:

```cpp
ss::future<> invoke_async_host_fn(host_function fn) {
    // Switch from WASM stack to host stack
    wasmtime_async_continue_t* continuation;
    
    // Execute host function (may await)
    auto result = co_await fn();
    
    // Resume WASM with result
    wasmtime_async_continue(continuation, result);
}
```

## Foreign Function Interface (FFI)

The FFI enables WASM code to call into Redpanda. Let's examine the key modules.

### Transform Module

The transform module is the core interface for record processing:

```cpp
// Transform module host functions
namespace transform_module {

// Read next batch header
// Returns number of records in batch (0 = no more batches)
int32_t read_batch_header();

// Read next record from current batch
// Returns record size (0 = no more records in batch)
int32_t read_record();

// Write output record
// Returns 0 on success, error code otherwise
int32_t write_record(const uint8_t* key_ptr, int32_t key_len,
                     const uint8_t* val_ptr, int32_t val_len,
                     const uint8_t* headers_ptr, int32_t headers_len);

// Write to named output topic
int32_t write_record_to_topic(const char* topic_name,
                               const uint8_t* key_ptr, int32_t key_len,
                               const uint8_t* val_ptr, int32_t val_len,
                               const uint8_t* headers_ptr, int32_t headers_len);

} // namespace transform_module
```

### Implementation

```cpp
class transform_module_impl {
    wasmtime_engine* _engine;
    model::record_batch_reader _input_reader;
    std::optional<model::record_batch> _current_batch;
    size_t _current_record_idx{0};
    transform_callback _output_callback;
    
public:
    // Host function: read_batch_header
    int32_t read_batch_header() {
        if (!_current_batch) {
            // Read next batch from input
            auto batch = read_next_batch_from_reader();
            
            if (!batch) {
                return 0;  // No more batches
            }
            
            _current_batch = std::move(*batch);
            _current_record_idx = 0;
        }
        
        return _current_batch->record_count();
    }
    
    // Host function: read_record
    int32_t read_record() {
        if (!_current_batch) {
            return 0;  // No batch loaded
        }
        
        if (_current_record_idx >= _current_batch->record_count()) {
            // Batch exhausted
            _current_batch.reset();
            return 0;
        }
        
        // Get record
        auto& record = _current_batch->records()[_current_record_idx++];
        
        // Copy to WASM memory
        copy_record_to_wasm(record);
        
        return record.size_bytes();
    }
    
    // Host function: write_record
    int32_t write_record(
        const uint8_t* key_ptr, int32_t key_len,
        const uint8_t* val_ptr, int32_t val_len,
        const uint8_t* headers_ptr, int32_t headers_len
    ) {
        // Read from WASM memory
        auto key = read_from_wasm_memory(key_ptr, key_len);
        auto val = read_from_wasm_memory(val_ptr, val_len);
        auto headers = parse_headers(headers_ptr, headers_len);
        
        // Build output record
        model::transformed_data data{
            .key = std::move(key),
            .val = std::move(val),
            .headers = std::move(headers)
        };
        
        // Invoke callback (suspends WASM)
        suspend_and_invoke([this, data = std::move(data)]() mutable {
            return _output_callback(std::nullopt, std::move(data));
        });
        
        return 0;  // Success
    }
    
private:
    void suspend_and_invoke(ss::noncopyable_function<ss::future<>()> fn) {
        // Signal WASM to suspend
        _engine->signal_suspension();
        
        // Execute async operation
        ssx::background = fn().then([this] {
            // Resume WASM after operation completes
            _engine->resume_wasm();
        });
    }
    
    void copy_record_to_wasm(const model::record& record) {
        // Get WASM memory
        auto mem_data = wasmtime_memory_data(_engine->store(), &_memory);
        auto mem_size = wasmtime_memory_data_size(_engine->store(), &_memory);
        
        // Write key
        auto key_offset = allocate_in_wasm(record.key().size());
        std::memcpy(mem_data + key_offset, record.key().data(), record.key().size());
        
        // Write value
        auto val_offset = allocate_in_wasm(record.value().size());
        std::memcpy(mem_data + val_offset, record.value().data(), record.value().size());
        
        // Store pointers for WASM to read
        write_record_pointers(key_offset, record.key().size(),
                            val_offset, record.value().size());
    }
};
```

### WASM Guest Code

Here's how a transform looks from the WASM side (example in Rust):

```rust
// Redpanda Transform SDK (Rust)
use redpanda_transform_sdk::*;

// Transform function
pub fn transform(record: Record) -> Result<Vec<Record>, TransformError> {
    // Parse JSON value
    let json: serde_json::Value = serde_json::from_slice(&record.value)?;
    
    // Filter: only process "user_signup" events
    if json["event_type"] != "user_signup" {
        return Ok(vec![]);  // Drop record
    }
    
    // Enrich: add timestamp
    let mut enriched = json.clone();
    enriched["processed_at"] = json!(chrono::Utc::now().to_rfc3339());
    
    // Create output record
    let output = Record {
        key: record.key,
        value: serde_json::to_vec(&enriched)?,
        headers: record.headers,
    };
    
    Ok(vec![output])
}

// SDK handles the event loop
#[no_mangle]
pub extern "C" fn main() {
    // Check ABI version
    check_abi_version();
    
    // Main transform loop
    loop {
        // Read batch header
        let record_count = unsafe { read_batch_header() };
        
        if record_count == 0 {
            break;  // No more batches
        }
        
        // Process each record
        for _ in 0..record_count {
            let record = read_record();
            
            // User's transform function
            match transform(record) {
                Ok(outputs) => {
                    for output in outputs {
                        write_record(output);
                    }
                }
                Err(e) => {
                    log_error(&e.to_string());
                }
            }
        }
    }
}
```

## Memory Management

WASM transforms have strict memory constraints to prevent resource exhaustion.

### Custom Allocators

Redpanda provides custom allocators for Wasmtime integration:

```cpp
class seastar_aligned_allocator {
    // Seastar requires specific alignment
    static constexpr size_t alignment = 4096;
    
    size_t _allocated{0};
    size_t _limit;
    
public:
    void* allocate(size_t size) {
        if (_allocated + size > _limit) {
            return nullptr;  // Out of memory
        }
        
        void* ptr = ss::allocate_aligned_buffer<char>(
            align_up(size, alignment),
            alignment
        );
        
        _allocated += size;
        return ptr;
    }
    
    void deallocate(void* ptr, size_t size) {
        ss::free_aligned_buffer(ptr, align_up(size, alignment));
        _allocated -= size;
    }
};

// Register with Wasmtime
void register_allocator(wasmtime_config_t* config) {
    static seastar_aligned_allocator allocator;
    
    wasmtime_config_host_memory_creator_set(
        config,
        &allocator,
        [](void* ctx, size_t size) {
            return static_cast<seastar_aligned_allocator*>(ctx)->allocate(size);
        },
        [](void* ctx, void* ptr, size_t size) {
            static_cast<seastar_aligned_allocator*>(ctx)->deallocate(ptr, size);
        }
    );
}
```

### Memory Pools

Per-core memory pools prevent one transform from exhausting memory:

```cpp
class wasm_memory_pool {
    size_t _total_allocated{0};
    size_t _per_core_limit;
    ss::semaphore _allocation_sem;
    
public:
    ss::future<allocation_guard> allocate(size_t size) {
        // Wait for available memory
        auto units = co_await _allocation_sem.get_units(size);
        
        _total_allocated += size;
        
        co_return allocation_guard{
            .units = std::move(units),
            .size = size,
            .release_fn = [this, size] {
                _total_allocated -= size;
            }
        };
    }
    
    size_t available() const {
        return _per_core_limit - _total_allocated;
    }
};
```

### Linear Memory Management

WASM linear memory is the heap available to guest code:

```cpp
class wasm_linear_memory {
    wasmtime_memory_t _memory;
    size_t _current_size;
    size_t _max_size;
    
public:
    // Grow memory
    ss::future<> grow(uint32_t pages) {
        auto new_size = _current_size + (pages * 64_KiB);
        
        if (new_size > _max_size) {
            throw std::runtime_error("Memory limit exceeded");
        }
        
        auto prev_size = wasmtime_memory_grow(
            _store,
            &_memory,
            pages
        );
        
        if (prev_size == size_t(-1)) {
            throw std::runtime_error("Memory grow failed");
        }
        
        _current_size = new_size;
        co_return;
    }
    
    // Access memory
    std::span<uint8_t> data() {
        auto ptr = wasmtime_memory_data(_store, &_memory);
        auto size = wasmtime_memory_data_size(_store, &_memory);
        return {ptr, size};
    }
};
```

## Schema Registry Integration

Transforms can access schema registry for serialization:

```cpp
// Schema registry module
namespace schema_registry_module {

// Get schema by ID
int32_t get_schema(int32_t schema_id, uint8_t* output_ptr, int32_t output_len);

// Get latest schema for subject
int32_t get_latest_schema(const char* subject, uint8_t* output_ptr, int32_t output_len);

} // namespace schema_registry_module

class schema_registry_module_impl {
    ss::shared_ptr<pandaproxy::schema_registry::schema_getter> _registry;
    
public:
    // Host function implementation
    int32_t get_schema(int32_t schema_id, uint8_t* output_ptr, int32_t output_len) {
        // This is async, so we suspend WASM
        suspend_and_invoke([this, schema_id, output_ptr, output_len] {
            return fetch_and_write_schema(schema_id, output_ptr, output_len);
        });
        
        return 0;  // Will be populated async
    }
    
private:
    ss::future<> fetch_and_write_schema(
        int32_t schema_id,
        uint8_t* output_ptr,
        int32_t output_len
    ) {
        // Fetch from registry
        auto schema = co_await _registry->get_schema(
            pandaproxy::schema_registry::schema_id(schema_id)
        );
        
        if (!schema) {
            throw schema_not_found_exception(schema_id);
        }
        
        // Serialize schema
        auto serialized = schema->to_json();
        
        if (serialized.size() > output_len) {
            throw buffer_too_small_exception();
        }
        
        // Copy to WASM memory
        std::memcpy(output_ptr, serialized.data(), serialized.size());
    }
};
```

## CPU Time Limiting

Redpand uses Wasmtime's "fuel" mechanism to limit CPU usage:

```cpp
class fuel_manager {
    // Fuel = approximate instruction count
    static constexpr uint64_t microsecond_to_fuel(std::chrono::microseconds us) {
        // Calibrated: ~1000 instructions per microsecond
        return us.count() * 1000;
    }
    
public:
    void set_timeout(wasmtime_store_t* store, std::chrono::milliseconds timeout) {
        auto fuel = microsecond_to_fuel(
            std::chrono::duration_cast<std::chrono::microseconds>(timeout)
        );
        
        wasmtime_store_add_fuel(store, fuel);
    }
    
    bool check_timeout(wasmtime_store_t* store) {
        uint64_t remaining;
        wasmtime_store_fuel_remaining(store, &remaining);
        
        return remaining == 0;
    }
    
    void consume_fuel(wasmtime_store_t* store, uint64_t amount) {
        uint64_t consumed;
        wasmtime_store_fuel_consume(store, amount, &consumed);
    }
};

// Timeout handling
ss::future<> wasmtime_engine::execute_with_timeout(
    std::chrono::milliseconds timeout
) {
    // Set fuel for timeout
    _fuel_manager.set_timeout(_store, timeout);
    
    // Execute
    co_await resume_wasm();
    
    // Check if timed out
    if (_fuel_manager.check_timeout(_store)) {
        throw timeout_exception("Transform exceeded CPU time limit");
    }
}
```

## Performance Optimizations

### Engine Pooling

Reuse engines to avoid compilation overhead:

```cpp
class engine_pool {
    struct pooled_engine {
        ss::shared_ptr<engine> engine;
        clock_type::time_point last_used;
        bool in_use{false};
    };
    
    std::vector<pooled_engine> _engines;
    ss::shared_ptr<factory> _factory;
    
public:
    ss::future<ss::shared_ptr<engine>> acquire() {
        // Find idle engine
        for (auto& pe : _engines) {
            if (!pe.in_use) {
                pe.in_use = true;
                pe.last_used = clock_type::now();
                co_return pe.engine;
            }
        }
        
        // Create new engine if pool not full
        if (_engines.size() < config::max_engines_per_core()) {
            auto engine = co_await _factory->make_engine(
                std::make_unique<default_logger>()
            );
            
            _engines.push_back({
                .engine = engine,
                .last_used = clock_type::now(),
                .in_use = true
            });
            
            co_return engine;
        }
        
        // Wait for engine to become available
        co_await wait_for_available_engine();
        co_return co_await acquire();
    }
    
    void release(ss::shared_ptr<engine> engine) {
        for (auto& pe : _engines) {
            if (pe.engine == engine) {
                pe.in_use = false;
                return;
            }
        }
    }
    
    // Evict idle engines
    ss::future<> evict_idle() {
        auto now = clock_type::now();
        auto idle_threshold = config::engine_idle_timeout();
        
        std::vector<size_t> to_remove;
        
        for (size_t i = 0; i < _engines.size(); ++i) {
            auto& pe = _engines[i];
            
            if (!pe.in_use && (now - pe.last_used) > idle_threshold) {
                to_remove.push_back(i);
            }
        }
        
        // Remove from back to front to preserve indices
        for (auto it = to_remove.rbegin(); it != to_remove.rend(); ++it) {
            co_await _engines[*it].engine->stop();
            _engines.erase(_engines.begin() + *it);
        }
    }
};
```

### Compilation Caching

Factory compilation is expensive, so cache compiled modules:

```cpp
class factory_cache {
    struct cache_entry {
        ss::shared_ptr<factory> factory;
        model::transform_id transform_id;
        size_t use_count{0};
    };
    
    absl::flat_hash_map<model::transform_id, cache_entry> _cache;
    
public:
    ss::future<ss::shared_ptr<factory>> get_or_create(
        model::transform_id id,
        model::transform_metadata meta,
        model::wasm_binary_iobuf binary
    ) {
        // Check cache
        auto it = _cache.find(id);
        if (it != _cache.end()) {
            it->second.use_count++;
            co_return it->second.factory;
        }
        
        // Compile new factory
        auto factory = co_await _runtime->make_factory(
            std::move(meta),
            std::move(binary)
        );
        
        // Cache it
        _cache[id] = cache_entry{
            .factory = factory,
            .transform_id = id,
            .use_count = 1
        };
        
        co_return factory;
    }
    
    void evict_unused() {
        std::vector<model::transform_id> to_evict;
        
        for (auto& [id, entry] : _cache) {
            if (entry.use_count == 0) {
                to_evict.push_back(id);
            }
        }
        
        for (auto& id : to_evict) {
            _cache.erase(id);
        }
    }
};
```

```mermaid
sequenceDiagram
    participant Source as Source Partition
    participant TP as Transform Processor
    participant Engine as WASM Engine
    participant Output as Output Partition
    
    loop Transform Loop
        TP->>Source: Read batches
        Source-->>TP: Record batches
        
        loop For each batch
            TP->>Engine: Transform batch
            
            loop For each record
                Engine->>Engine: Execute WASM
                Note over Engine: User function runs
                
                alt Record passes filter
                    Engine->>TP: write_record()
                    TP->>Output: Replicate
                else Record filtered
                    Engine->>Engine: Drop
                end
            end
            
            Engine-->>TP: Transform complete
        end
        
        TP->>TP: Commit offset
    end
```

## Integration with Partitions

Transforms are integrated into the partition read/write path:

```cpp
class transform_processor {
    ss::shared_ptr<partition> _source_partition;
    ss::shared_ptr<partition> _output_partition;
    ss::shared_ptr<engine> _engine;
    
public:
    ss::future<> run() {
        while (!_as.abort_requested()) {
            // Read from source partition
            auto reader = co_await _source_partition->make_reader(
                storage::log_reader_config{
                    .start_offset = _last_processed_offset + model::offset(1),
                    .max_bytes = 1_MiB
                }
            );
            
            // Read batches
            auto batches = co_await reader.read_some();
            
            if (batches.empty()) {
                // No new data, sleep
                co_await ss::sleep(100ms);
                continue;
            }
            
            // Transform each batch
            for (auto& batch : batches) {
                co_await _engine->transform(
                    std::move(batch),
                    _probe.get(),
                    [this](std::optional<model::topic_view> topic, 
                           model::transformed_data data) {
                        return write_output(topic, std::move(data));
                    }
                );
                
                _last_processed_offset = batch.last_offset();
            }
            
            // Commit progress
            co_await commit_offset(_last_processed_offset);
        }
    }
    
private:
    ss::future<write_success> write_output(
        std::optional<model::topic_view> topic,
        model::transformed_data data
    ) {
        // Build record batch
        model::record_batch batch = build_output_batch(std::move(data));
        
        // Write to output partition
        auto result = co_await _output_partition->replicate(
            std::move(batch),
            raft::replicate_options{
                .consistency = raft::consistency_level::quorum_ack
            }
        );
        
        co_return write_success(result.has_value());
    }
};
```

## WASI Support

WASI (WebAssembly System Interface) provides POSIX-like APIs:

```cpp
class wasi_module {
public:
    // Environment variables
    static int32_t environ_get(
        wasm_exec_env_t exec_env,
        uint32_t environ_ptr,
        uint32_t environ_buf_ptr
    ) {
        // Provide limited environment
        static const std::vector<std::string> env = {
            "REDPANDA_TRANSFORM=true",
            "REDPANDA_VERSION=v24.1.0"
        };
        
        // Copy to WASM memory
        auto mem = get_wasm_memory(exec_env);
        copy_string_array_to_wasm(mem, environ_ptr, environ_buf_ptr, env);
        
        return 0;  // Success
    }
    
    // Clock access
    static int32_t clock_time_get(
        wasm_exec_env_t exec_env,
        uint32_t clock_id,
        uint64_t precision,
        uint32_t time_ptr
    ) {
        auto now = std::chrono::system_clock::now();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()
        ).count();
        
        // Write to WASM memory
        auto mem = get_wasm_memory(exec_env);
        write_u64(mem, time_ptr, nanos);
        
        return 0;
    }
    
    // File operations (stubbed out)
    static int32_t fd_read(/* ... */) {
        return WASI_ERRNO_BADF;  // No file access
    }
    
    static int32_t fd_write(
        wasm_exec_env_t exec_env,
        uint32_t fd,
        uint32_t iovs_ptr,
        uint32_t iovs_len,
        uint32_t nwritten_ptr
    ) {
        // Only allow stdout/stderr
        if (fd != 1 && fd != 2) {
            return WASI_ERRNO_BADF;
        }
        
        // Read from WASM memory and log
        auto mem = get_wasm_memory(exec_env);
        auto message = read_iovs(mem, iovs_ptr, iovs_len);
        
        vlog(_logger.info, "WASM log: {}", message);
        
        write_u32(mem, nwritten_ptr, message.size());
        return 0;
    }
};
```

## Monitoring and Observability

Transforms are instrumented for monitoring:

```cpp
class transform_probe {
    metrics::internal_metric_groups _metrics;
    
    // Latency histogram
    log_hist_internal _transform_latency;
    
    // Counters
    uint64_t _records_processed{0};
    uint64_t _records_produced{0};
    uint64_t _records_filtered{0};
    uint64_t _errors{0};
    
public:
    void record_transform_latency(std::chrono::microseconds latency) {
        _transform_latency.record(latency.count());
    }
    
    void record_processed(size_t count) {
        _records_processed += count;
    }
    
    void record_produced(size_t count) {
        _records_produced += count;
    }
    
    void record_error() {
        _errors++;
    }
    
    // Metrics
    double average_latency_ms() const {
        return _transform_latency.mean() / 1000.0;
    }
    
    double throughput_records_per_sec() const {
        auto runtime = clock_type::now() - _start_time;
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(runtime).count();
        return static_cast<double>(_records_processed) / seconds;
    }
    
    double filter_ratio() const {
        return static_cast<double>(_records_filtered) / _records_processed;
    }
};
```

## Error Handling

Robust error handling is critical for production deployments:

```cpp
class transform_error_handler {
public:
    enum class error_policy {
        fail_fast,        // Stop on first error
        skip_record,      // Skip erroring records
        dead_letter_queue // Send errors to DLQ
    };
    
    ss::future<> handle_error(
        const transform_error& error,
        const model::record& input_record,
        error_policy policy
    ) {
        switch (policy) {
        case error_policy::fail_fast:
            throw error;
            
        case error_policy::skip_record:
            vlog(
                _logger.warn,
                "Transform error on record at offset {}: {}. Skipping.",
                input_record.offset(),
                error.what()
            );
            _probe->record_error();
            co_return;
            
        case error_policy::dead_letter_queue:
            co_await write_to_dlq(input_record, error);
            _probe->record_error();
            co_return;
        }
    }
    
private:
    ss::future<> write_to_dlq(
        const model::record& record,
        const transform_error& error
    ) {
        // Build DLQ record with error metadata
        model::record dlq_record{
            .key = record.key(),
            .value = record.value(),
            .headers = record.headers()
        };
        
        // Add error metadata to headers
        dlq_record.headers.push_back({
            .key = "error_message",
            .value = error.what()
        });
        dlq_record.headers.push_back({
            .key = "original_offset",
            .value = fmt::format("{}", record.offset())
        });
        
        // Write to DLQ topic
        co_await _dlq_partition->replicate(
            make_batch(dlq_record),
            raft::replicate_options{}
        );
    }
};
```

## Real-World Examples

### Example 1: PII Redaction

```rust
// Redact personally identifiable information
pub fn transform(record: Record) -> Result<Vec<Record>, TransformError> {
    let mut json: serde_json::Value = serde_json::from_slice(&record.value)?;
    
    // Redact sensitive fields
    if let Some(obj) = json.as_object_mut() {
        for sensitive_field in ["ssn", "credit_card", "email"] {
            if obj.contains_key(sensitive_field) {
                obj[sensitive_field] = json!("***REDACTED***");
            }
        }
    }
    
    Ok(vec![Record {
        key: record.key,
        value: serde_json::to_vec(&json)?,
        headers: record.headers,
    }])
}
```

### Example 2: Event Routing

```rust
// Route events to different topics based on type
pub fn transform(record: Record) -> Result<Vec<Record>, TransformError> {
    let json: serde_json::Value = serde_json::from_slice(&record.value)?;
    
    let event_type = json["event_type"].as_str().ok_or(TransformError::MissingField)?;
    
    // Route to appropriate topic
    let target_topic = match event_type {
        "user_signup" => "events.users.signups",
        "purchase" => "events.orders.purchases",
        "click" => "events.analytics.clicks",
        _ => "events.other"
    };
    
    Ok(vec![Record {
        key: record.key,
        value: record.value,
        headers: record.headers,
        output_topic: Some(target_topic.to_string()),
    }])
}
```

### Example 3: Aggregation

```rust
// Aggregate events into time windows (stateful transform)
static mut WINDOW: HashMap<String, Counter> = HashMap::new();

pub fn transform(record: Record) -> Result<Vec<Record>, TransformError> {
    let json: serde_json::Value = serde_json::from_slice(&record.value)?;
    
    let user_id = json["user_id"].as_str().ok_or(TransformError::MissingField)?;
    
    unsafe {
        // Update counter
        let counter = WINDOW.entry(user_id.to_string()).or_insert(Counter::new());
        counter.increment();
        
        // Flush window every 1000 records
        if counter.count % 1000 == 0 {
            let output = json!({
                "user_id": user_id,
                "count": counter.count,
                "window_end": chrono::Utc::now().to_rfc3339()
            });
            
            return Ok(vec![Record {
                key: user_id.as_bytes().to_vec(),
                value: serde_json::to_vec(&output)?,
                headers: vec![],
            }]);
        }
    }
    
    // Don't output anything until window completes
    Ok(vec![])
}
```

## Performance Benchmarks

### Throughput

**Simple transforms** (filter, map):
- Rust: 500K-1M records/sec per core
- Go: 300K-500K records/sec per core
- JavaScript: 100K-200K records/sec per core

**Complex transforms** (JSON parsing, schema validation):
- Rust: 100K-300K records/sec per core
- Go: 80K-150K records/sec per core

**Factors affecting performance**:
- Serialization overhead (JSON vs binary)
- Memory allocations
- Host function call frequency
- Transform complexity

### Latency

**Per-record latency**:
- Rust (simple): 1-5μs
- Rust (complex): 10-50μs
- Go (simple): 5-10μs
- Go (complex): 20-100μs

**Startup latency**:
- JIT compilation: 50-200ms
- AOT compilation: 5-20ms
- Engine reuse: <1ms

## Performance Characteristics

### Transform Throughput by Language

| Language | Simple Transform | Complex Transform | Memory Overhead | Compilation Time |
|----------|------------------|-------------------|-----------------|------------------|
| **Rust** | 500K-1M rec/sec | 100K-300K rec/sec | ~2MB | 50-200ms (JIT) |
| **Go** | 300K-500K rec/sec | 80K-150K rec/sec | ~5MB | 100-300ms (JIT) |
| **C++** | 600K-1.2M rec/sec | 150K-400K rec/sec | ~1MB | 30-150ms (JIT) |
| **JavaScript** | 100K-200K rec/sec | 20K-50K rec/sec | ~10MB | 200-500ms (JIT) |
| **AssemblyScript** | 200K-400K rec/sec | 50K-100K rec/sec | ~3MB | 100-250ms (JIT) |

*Note: Simple = filter/map; Complex = JSON parse + validation + enrichment*

### Latency Breakdown

| Operation | Latency | Notes |
|-----------|---------|-------|
| **Host → WASM call** | ~50ns | Function call overhead |
| **WASM → Host call** | ~200ns | Stack switch required |
| **Memory copy (1KB)** | ~500ns | Host ↔ WASM boundary |
| **Simple transform** | 1-5μs | Filter, map, basic logic |
| **JSON parse (1KB)** | 5-20μs | Depends on complexity |
| **Schema validation** | 10-50μs | Depends on schema size |
| **Engine acquisition** | <1μs | From pool (warm) |
| **Engine creation** | 5-20ms | New instance (cold) |

### Resource Limits

| Resource | Default Limit | Configurable Range | Enforcement |
|----------|---------------|-------------------|-------------|
| **Heap Memory** | 10MB | 1MB - 100MB | Per-engine limit |
| **Stack Memory** | 2MB | 512KB - 8MB | Wasmtime config |
| **CPU Time** | 100ms | 10ms - 1000ms | Fuel mechanism |
| **Execution Time** | 5s | 1s - 30s | Timeout |

### Comparison: Transform vs External Stream Processing

| Aspect | Redpanda Transforms | Kafka Streams | Flink |
|--------|---------------------|---------------|-------|
| **Deployment** | In-broker | Separate app | Separate cluster |
| **Latency** | 1-5μs | 50-500ms | 100-1000ms |
| **Ops Complexity** | None | Medium | High |
| **Scalability** | Per-core | Per-instance | Per-task |
| **State Management** | In-memory | RocksDB | Distributed |
| **Language Support** | Any (WASM) | Java/Kotlin | Java/Scala/Python |

## Troubleshooting Transform Issues

### Issue 1: Transform Timeout

**Symptoms**:
- "Transform exceeded CPU time limit" errors
- Records being dropped
- High transform latency

**Diagnostic Steps**:
```bash
# Check transform metrics
curl localhost:9644/metrics | grep wasm_transform_

# View timeout errors
journalctl -u redpanda | grep "transform.*timeout"

# Check fuel consumption
curl localhost:9644/metrics | grep wasm_fuel_consumed
```

**Solutions**:

1. **Increase timeout**
   ```yaml
   # /etc/redpanda/redpanda.yaml
   redpanda:
     wasm_per_invocation_timeout_ms: 200  # Increase from 100ms
   ```

2. **Optimize transform code**
   ```rust
   // BAD: Inefficient JSON parsing
   pub fn transform(record: Record) -> Result<Vec<Record>, TransformError> {
       let json: Value = serde_json::from_slice(&record.value)?;  // Slow
       // ... processing
   }
   
   // GOOD: Use streaming parser
   pub fn transform(record: Record) -> Result<Vec<Record>, TransformError> {
       let mut deserializer = serde_json::Deserializer::from_slice(&record.value);
       // ... streaming processing
   }
   ```

3. **Profile transform**
   ```bash
   # Add timing logs in transform code
   let start = std::time::Instant::now();
   // ... transform logic
   println!("Transform took: {:?}", start.elapsed());
   ```

### Issue 2: Memory Exhaustion

**Symptoms**:
- "Memory limit exceeded" errors
- Transform crashes
- Broker memory pressure

**Diagnostic Steps**:
```bash
# Check WASM memory usage
curl localhost:9644/metrics | grep wasm_memory

# View per-engine allocation
curl localhost:9644/metrics | grep wasm_engine_memory_bytes

# Monitor total WASM memory
pmap $(pgrep redpanda) | grep -i wasm
```

**Solutions**:

1. **Increase memory limit**
   ```yaml
   redpanda:
     wasm_per_engine_memory_limit: 20971520  # 20MB per engine
     wasm_per_core_pool_size_bytes: 104857600  # 100MB per core
   ```

2. **Fix memory leaks in transform**
   ```rust
   // BAD: Accumulating state
   static mut CACHE: Vec<String> = Vec::new();
   
   pub fn transform(record: Record) -> Result<Vec<Record>, TransformError> {
       unsafe { CACHE.push(parse_record(&record)); }  // Leak!
       // ...
   }
   
   // GOOD: Bounded cache
   use lru::LruCache;
   static mut CACHE: LruCache<String, Value> = LruCache::new(1000);
   ```

3. **Reduce allocation**
   ```rust
   // Reuse buffers
   thread_local! {
       static BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(4096));
   }
   ```

### Issue 3: Transform Not Processing Records

**Symptoms**:
- No output records produced
- Transform appears stuck
- Source topic has data, output topic empty

**Diagnostic Steps**:
```bash
# Check transform status
curl localhost:9644/v1/transforms/<transform-name>

# View transform lag
curl localhost:9644/metrics | grep transform_lag_records

# Check for errors
journalctl -u redpanda | grep "<transform-name>"
```

**Solutions**:

1. **Verify transform is deployed**
   ```bash
   # List transforms
   rpk transform list
   
   # Deploy if missing
   rpk transform deploy --file transform.wasm --name my-transform \
     --input-topic source --output-topic output
   ```

2. **Check transform state**
   ```bash
   # View transform details
   curl localhost:9644/v1/transforms/<name> | jq
   
   # Restart transform
   rpk transform restart <name>
   ```

3. **Debug transform logic**
   ```rust
   // Add debug logging
   pub fn transform(record: Record) -> Result<Vec<Record>, TransformError> {
       eprintln!("Processing record at offset: {:?}", record.offset);
       
       // Your logic
       let result = process(record)?;
       
       eprintln!("Produced {} output records", result.len());
       Ok(result)
   }
   ```

### Issue 4: High Transform Latency

**Symptoms**:
- p99 transform latency > 100ms
- Consumer lag on output topics
- Slow end-to-end processing

**Diagnostic Steps**:
```bash
# Check transform latency
curl localhost:9644/metrics | grep transform_latency

# View throughput
curl localhost:9644/metrics | grep transform_records_processed

# Profile transform
journalctl -u redpanda | grep "transform.*duration"
```

**Solutions**:

1. **Use faster serialization**
   ```rust
   // BAD: JSON for everything
   let json: Value = serde_json::from_slice(&record.value)?;
   
   // GOOD: Use binary formats when possible
   let proto: MyProto = prost::Message::decode(&record.value)?;
   ```

2. **Batch processing**
   ```rust
   // Process multiple records together
   static mut BATCH: Vec<Record> = Vec::new();
   
   pub fn transform(record: Record) -> Result<Vec<Record>, TransformError> {
       unsafe {
           BATCH.push(record);
           
           if BATCH.len() >= 100 {
               let results = process_batch(&BATCH)?;
               BATCH.clear();
               return Ok(results);
           }
       }
       
       Ok(vec![])  // Wait for more records
   }
   ```

3. **Reduce allocations**
   ```rust
   // Avoid unnecessary clones
   pub fn transform(mut record: Record) -> Result<Vec<Record>, TransformError> {
       // Modify in place instead of cloning
       record.headers.push(Header {
           key: "processed".into(),
           value: "true".into(),
       });
       
       Ok(vec![record])
   }
   ```

### Issue 5: Transform Compilation Failures

**Symptoms**:
- "Invalid WASM module" errors
- Transform deployment fails
- Validation errors

**Diagnostic Steps**:
```bash
# Validate WASM module
wasm-validate transform.wasm

# Check module imports
wasm-objdump -x transform.wasm | grep import

# View compilation errors
journalctl -u redpanda | grep "compilation.*error"
```

**Solutions**:

1. **Verify WASM format**
   ```bash
   # Check module is valid
   wasm-validate transform.wasm
   
   # Optimize module
   wasm-opt -O3 transform.wasm -o transform.optimized.wasm
   ```

2. **Check imports compatibility**
   ```rust
   // Only use allowed imports
   extern "C" {
       fn read_batch_header() -> i32;
       fn read_record() -> i32;
       fn write_record(
           key_ptr: *const u8, key_len: i32,
           val_ptr: *const u8, val_len: i32,
           headers_ptr: *const u8, headers_len: i32
       ) -> i32;
   }
   ```

3. **Build for correct target**
   ```bash
   # Rust
   cargo build --target wasm32-wasi --release
   
   # Go
   GOOS=wasip1 GOARCH=wasm go build -o transform.wasm
   ```

### Debugging Tools

**Transform Inspection**:
```bash
# List all transforms
curl localhost:9644/v1/transforms

# Get transform details
curl localhost:9644/v1/transforms/<name> | jq

# View transform metrics
curl localhost:9644/metrics | grep "transform_<name>"
```

**WASM Module Analysis**:
```bash
# Inspect module structure
wasm-objdump -x transform.wasm

# View exported functions
wasm-objdump -x transform.wasm | grep export

# Check memory usage
wasm-objdump -h transform.wasm | grep memory
```

**Metrics to Monitor**:
```bash
# Key transform metrics
curl localhost:9644/metrics | grep -E \
  "(wasm_transform_latency|wasm_records_processed|wasm_errors|wasm_memory_bytes)"

# Set up alerts for:
# - Transform latency p99 > 100ms
# - Error rate > 1%
# - Memory usage > 80% of limit
# - Lag > 1000 records
```

### Performance Tuning Checklist

- [ ] **Choose Right Language**: Rust/C++ for performance, Go/JS for ease
- [ ] **Minimize Memory**: Use bounded caches, avoid leaks
- [ ] **Optimize Serialization**: Prefer binary formats over JSON
- [ ] **Batch When Possible**: Process multiple records together
- [ ] **Limit CPU Time**: Set appropriate timeout for workload
- [ ] **Profile Regularly**: Identify bottlenecks early
- [ ] **Use Engine Pool**: Configure pool size for concurrency
- [ ] **Monitor Metrics**: Track latency, throughput, errors
- [ ] **Test Thoroughly**: Verify with production-like data
- [ ] **Handle Errors**: Use appropriate error policy (skip/DLQ/fail)

## Conclusion

Redpanda's WebAssembly transform engine demonstrates how to bring user-defined processing to streaming data without sacrificing performance or safety:

**Key Innovations**:
1. **WASM Sandbox**: Security and isolation for untrusted code
2. **Async Host Functions**: Non-blocking I/O via stack switching
3. **Resource Limits**: CPU time, memory, and I/O controls
4. **Engine Pooling**: Amortize compilation costs
5. **Schema Registry Integration**: First-class schema support

**Performance Summary**:
- 500K-1M records/sec per core (simple transforms)
- 1-5μs per-record latency (Rust)
- <1ms engine reuse overhead
- Zero external infrastructure required

In the next post, we'll explore Redpanda's serialization framework and RPC layer—the nervous system that connects all these components.

---

## Further Reading

- [Source: WASM Engine](src/v/wasm/engine.h)
- [Source: Transform Module](src/v/wasm/transform_module.h)
- [Source: Wasmtime Integration](src/v/wasm/wasmtime.h)
- [Wasmtime Documentation](https://docs.wasmtime.dev/)
- [WebAssembly Specification](https://webassembly.github.io/spec/)

*Next: Part 6 - Serialization and RPC*