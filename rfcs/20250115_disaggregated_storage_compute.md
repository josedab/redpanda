# RFC-007: Disaggregated Storage and Compute Architecture

**Authors**: Redpanda Engineering Team  
**Status**: Proposed  
**Created**: 2025-01-15  
**Discussion**: [GitHub Issue #XXXX]

## Summary

This RFC proposes a fundamental architectural transformation of Redpanda to fully separate storage and compute layers, enabling independent scaling, true serverless operation, and achieving 99.99%+ availability with multi-region storage.

## Motivation

### Current Architecture Limitations

The current monolithic architecture couples compute and storage within each broker, leading to:

1. **Scaling Inefficiency**: Must scale compute and storage together even when only one is needed
2. **Resource Waste**: Over-provisioning for peak loads that occur infrequently
3. **Operational Complexity**: Complex rebalancing when adding/removing nodes
4. **Cost Inefficiency**: Paying for idle compute during low-traffic periods
5. **Limited Elasticity**: Slow to respond to traffic spikes

### Industry Trends

- Cloud-native architectures favor separation of concerns
- Serverless computing models reduce operational overhead
- Object storage provides unlimited scalability at low cost
- Microservices enable independent component evolution

### Benefits of Disaggregation

- **Independent Scaling**: Scale compute and storage based on actual needs
- **True Elasticity**: Add/remove compute nodes in seconds
- **Cost Optimization**: Pay only for active compute time
- **Higher Availability**: Storage layer can provide 99.99%+ durability
- **Simplified Operations**: Stateless compute nodes are easier to manage

## Detailed Design

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     Client Applications                      │
└─────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                      Load Balancer                           │
└─────────────────────────────────────────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
┌─────────────────────────┐    ┌─────────────────────────┐
│    Compute Node 1       │    │    Compute Node N       │
│  ┌─────────────────┐    │    │  ┌─────────────────┐    │
│  │   Kafka API     │    │    │  │   Kafka API     │    │
│  └─────────────────┘    │    │  └─────────────────┘    │
│  ┌─────────────────┐    │    │  ┌─────────────────┐    │
│  │  Request Router │    │    │  │  Request Router │    │
│  └─────────────────┘    │    │  └─────────────────┘    │
│  ┌─────────────────┐    │    │  ┌─────────────────┐    │
│  │  Cache Manager  │    │    │  │  Cache Manager  │    │
│  └─────────────────┘    │    │  └─────────────────┘    │
└─────────────────────────┘    └─────────────────────────┘
            │                               │
            └───────────┬───────────────────┘
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                    Service Mesh (gRPC)                       │
└─────────────────────────────────────────────────────────────┘
                        │
         ┌──────────────┼──────────────┐
         ▼              ▼              ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│   Storage    │ │   Metadata   │ │    Index     │
│   Service    │ │   Service    │ │   Service    │
└──────────────┘ └──────────────┘ └──────────────┘
         │              │              │
         └──────────────┼──────────────┘
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Distributed Storage Layer                       │
│  ┌─────────────────────────────────────────────────────┐   │
│  │               Object Storage (S3/GCS/Azure)         │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │            Metadata Store (FoundationDB/etcd)       │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Core Components

#### 1. Storage Service Layer

```cpp
namespace redpanda::storage_service {

// Main storage service interface
class storage_service {
public:
    struct segment_metadata {
        model::ntp ntp;
        model::offset base_offset;
        model::offset committed_offset;
        model::timestamp base_timestamp;
        model::timestamp max_timestamp;
        size_t size_bytes;
        size_t record_count;
        std::optional<model::compression> compression;
        cloud_storage::segment_name name;
        std::vector<index_entry> index;
    };

    // Append operations
    virtual ss::future<append_result> append_batch(
        model::ntp ntp,
        model::record_batch batch,
        append_options opts
    ) = 0;
    
    virtual ss::future<append_result> append_batches(
        model::ntp ntp,
        std::vector<model::record_batch> batches,
        append_options opts
    ) = 0;
    
    // Read operations
    virtual ss::future<ss::input_stream<model::record_batch>>
    create_reader(
        model::ntp ntp,
        model::offset start_offset,
        model::offset end_offset,
        reader_options opts
    ) = 0;
    
    // Metadata operations
    virtual ss::future<partition_metadata> get_partition_metadata(
        model::ntp ntp
    ) = 0;
    
    virtual ss::future<std::vector<segment_metadata>>
    list_segments(
        model::ntp ntp,
        std::optional<model::offset> start_offset = std::nullopt
    ) = 0;
    
    // Transaction support
    virtual ss::future<tx::tx_id> begin_transaction(
        tx::transaction_options opts
    ) = 0;
    
    virtual ss::future<> commit_transaction(tx::tx_id id) = 0;
    virtual ss::future<> abort_transaction(tx::tx_id id) = 0;
};

// Implementation using object storage
class cloud_storage_service : public storage_service {
private:
    class segment_writer {
    public:
        ss::future<> append(model::record_batch batch) {
            // Buffer batches until segment size threshold
            _buffer.push_back(std::move(batch));
            _current_size += batch.size_bytes();
            
            if (_current_size >= _segment_size_threshold) {
                co_await flush_segment();
            }
        }
        
        ss::future<> flush_segment() {
            if (_buffer.empty()) {
                co_return;
            }
            
            // Create segment
            auto segment = create_segment(_buffer);
            
            // Compress if beneficial
            if (should_compress(segment)) {
                segment = co_await compress_segment(segment);
            }
            
            // Upload to cloud storage
            auto segment_name = generate_segment_name(_ntp, _base_offset);
            co_await _cloud_client.put_object(
                segment_name,
                segment.to_iobuf()
            );
            
            // Update index
            co_await update_segment_index({
                .name = segment_name,
                .base_offset = _base_offset,
                .last_offset = _last_offset,
                .size_bytes = segment.size_bytes(),
                .index_entries = build_index_entries(segment)
            });
            
            // Clear buffer
            _buffer.clear();
            _current_size = 0;
            _base_offset = _last_offset + model::offset(1);
        }
        
    private:
        model::ntp _ntp;
        std::vector<model::record_batch> _buffer;
        size_t _current_size = 0;
        size_t _segment_size_threshold = 128_MiB;
        model::offset _base_offset;
        model::offset _last_offset;
        cloud_storage_clients::client& _cloud_client;
    };

    class segment_reader {
    public:
        ss::future<ss::input_stream<model::record_batch>>
        create_stream(
            model::ntp ntp,
            model::offset start_offset,
            model::offset end_offset
        ) {
            // Find relevant segments
            auto segments = co_await find_segments_in_range(
                ntp, start_offset, end_offset
            );
            
            // Create reader chain
            co_return make_input_stream<model::record_batch>(
                [this, segments = std::move(segments), 
                 current_offset = start_offset,
                 end_offset]() mutable
                -> ss::future<std::optional<model::record_batch>> {
                    
                    while (!segments.empty()) {
                        auto& segment = segments.front();
                        
                        // Download segment if not cached
                        if (!is_cached(segment)) {
                            co_await download_segment(segment);
                        }
                        
                        // Read from cache
                        auto batch = co_await read_next_batch(
                            segment, current_offset, end_offset
                        );
                        
                        if (batch) {
                            current_offset = batch->last_offset() + model::offset(1);
                            co_return batch;
                        }
                        
                        // Move to next segment
                        segments.pop_front();
                    }
                    
                    co_return std::nullopt;
                }
            );
        }
        
    private:
        ss::future<> download_segment(const segment_metadata& segment) {
            // Check cache first
            if (_cache.contains(segment.name)) {
                co_return;
            }
            
            // Download from cloud storage
            auto data = co_await _cloud_client.get_object(segment.name);
            
            // Decompress if needed
            if (segment.compression) {
                data = co_await decompress(data, *segment.compression);
            }
            
            // Store in cache
            _cache.put(segment.name, std::move(data));
        }
        
        ss::future<std::optional<model::record_batch>>
        read_next_batch(
            const segment_metadata& segment,
            model::offset current_offset,
            model::offset end_offset
        ) {
            auto segment_data = _cache.get(segment.name);
            
            // Use index to seek to offset
            auto reader_offset = seek_to_offset(
                segment_data, segment.index, current_offset
            );
            
            // Read next batch
            if (reader_offset < segment_data.size()) {
                auto batch = parse_batch(segment_data, reader_offset);
                
                if (batch && batch->base_offset() <= end_offset) {
                    co_return batch;
                }
            }
            
            co_return std::nullopt;
        }
        
    private:
        segment_cache _cache;
        cloud_storage_clients::client& _cloud_client;
    };

public:
    ss::future<append_result> append_batches(
        model::ntp ntp,
        std::vector<model::record_batch> batches,
        append_options opts
    ) override {
        // Get or create writer for partition
        auto writer = get_or_create_writer(ntp);
        
        append_result result;
        
        for (auto& batch : batches) {
            // Assign offsets
            auto base_offset = co_await allocate_offset(ntp, batch.record_count());
            batch.set_base_offset(base_offset);
            
            // Append to writer
            co_await writer->append(std::move(batch));
            
            result.base_offset = base_offset;
            result.last_offset = base_offset + model::offset(batch.record_count() - 1);
        }
        
        // Update metadata
        co_await update_partition_metadata(ntp, result.last_offset);
        
        // Replicate if configured
        if (opts.replication_factor > 1) {
            co_await replicate_to_followers(ntp, batches, opts.replication_factor);
        }
        
        co_return result;
    }

private:
    absl::flat_hash_map<model::ntp, std::unique_ptr<segment_writer>> _writers;
    absl::flat_hash_map<model::ntp, partition_metadata> _metadata;
    cloud_storage_clients::client _cloud_client;
    metadata_service_client _metadata_client;
};

} // namespace redpanda::storage_service
```

#### 2. Metadata Service Layer

```cpp
namespace redpanda::metadata_service {

// Metadata service for managing partition and consumer group state
class metadata_service {
public:
    // Partition metadata management
    struct partition_metadata {
        model::ntp ntp;
        model::offset committed_offset;
        model::offset high_water_mark;
        model::term_id leader_epoch;
        std::vector<replica_state> replicas;
        partition_status status;
        std::optional<model::timestamp> last_stable_offset_timestamp;
        size_t log_size_bytes;
        size_t segment_count;
    };
    
    virtual ss::future<partition_metadata> get_partition_metadata(
        model::ntp ntp
    ) = 0;
    
    virtual ss::future<> update_partition_metadata(
        model::ntp ntp,
        partition_metadata metadata
    ) = 0;
    
    // Consumer group management
    struct consumer_group_metadata {
        kafka::group_id group_id;
        kafka::group_state state;
        kafka::protocol_type protocol_type;
        kafka::member_id leader;
        std::vector<member_metadata> members;
        generation_id generation;
    };
    
    virtual ss::future<consumer_group_metadata> get_group_metadata(
        kafka::group_id group_id
    ) = 0;
    
    virtual ss::future<> update_group_metadata(
        kafka::group_id group_id,
        consumer_group_metadata metadata
    ) = 0;
    
    // Offset management
    virtual ss::future<model::offset> get_consumer_offset(
        kafka::group_id group_id,
        model::ntp ntp
    ) = 0;
    
    virtual ss::future<> commit_consumer_offset(
        kafka::group_id group_id,
        model::ntp ntp,
        model::offset offset,
        std::optional<std::string> metadata = std::nullopt
    ) = 0;
    
    // Watch for changes
    virtual ss::future<ss::subscription<partition_metadata_update>>
    watch_partition(model::ntp ntp) = 0;
    
    virtual ss::future<ss::subscription<consumer_group_update>>
    watch_consumer_group(kafka::group_id group_id) = 0;
};

// Implementation using distributed consensus store
class consensus_metadata_service : public metadata_service {
private:
    class metadata_store {
    public:
        ss::future<> init() {
            // Initialize connection to consensus store (etcd/FoundationDB)
            _client = co_await create_consensus_client(_config);
            
            // Set up watches
            co_await setup_watches();
            
            // Load initial state
            co_await load_metadata();
        }
        
        ss::future<partition_metadata> get_partition_metadata(
            model::ntp ntp
        ) {
            auto key = make_partition_key(ntp);
            
            // Try cache first
            if (auto cached = _cache.get(key)) {
                co_return *cached;
            }
            
            // Fetch from consensus store
            auto value = co_await _client.get(key);
            if (!value) {
                throw partition_not_found_exception(ntp);
            }
            
            auto metadata = deserialize_partition_metadata(*value);
            
            // Update cache
            _cache.put(key, metadata);
            
            co_return metadata;
        }
        
        ss::future<> update_partition_metadata(
            model::ntp ntp,
            partition_metadata metadata
        ) {
            auto key = make_partition_key(ntp);
            auto value = serialize_partition_metadata(metadata);
            
            // Use compare-and-swap for consistency
            while (true) {
                auto current = co_await _client.get(key);
                auto revision = current ? current->revision : 0;
                
                auto success = co_await _client.compare_and_swap(
                    key, value, revision
                );
                
                if (success) {
                    // Update cache
                    _cache.put(key, metadata);
                    
                    // Notify watchers
                    notify_partition_watchers(ntp, metadata);
                    
                    co_return;
                }
                
                // Retry with exponential backoff
                co_await ss::sleep(calculate_backoff());
            }
        }
        
        ss::future<> commit_consumer_offset(
            kafka::group_id group_id,
            model::ntp ntp,
            model::offset offset,
            std::optional<std::string> metadata
        ) {
            offset_commit_value commit{
                .offset = offset,
                .metadata = metadata,
                .commit_timestamp = model::timestamp::now()
            };
            
            auto key = make_offset_key(group_id, ntp);
            auto value = serialize_offset_commit(commit);
            
            // Store in consensus system
            co_await _client.put(key, value);
            
            // Update cache
            _offset_cache[{group_id, ntp}] = offset;
            
            // Track for compaction
            track_offset_commit(group_id, ntp, offset);
        }
        
    private:
        ss::future<> setup_watches() {
            // Watch for partition changes
            _partition_watch = co_await _client.watch(
                "/partitions/",
                [this](const watch_event& event) {
                    handle_partition_change(event);
                }
            );
            
            // Watch for consumer group changes
            _group_watch = co_await _client.watch(
                "/consumer_groups/",
                [this](const watch_event& event) {
                    handle_group_change(event);
                }
            );
        }
        
        void handle_partition_change(const watch_event& event) {
            auto ntp = extract_ntp_from_key(event.key);
            auto metadata = deserialize_partition_metadata(event.value);
            
            // Update cache
            _cache.put(event.key, metadata);
            
            // Notify subscribers
            notify_partition_watchers(ntp, metadata);
        }
        
    private:
        consensus_client _client;
        lru_cache<std::string, partition_metadata> _cache;
        absl::flat_hash_map<offset_key, model::offset> _offset_cache;
        watch_handle _partition_watch;
        watch_handle _group_watch;
    };
    
    // Leader election for consumer group coordination
    class group_coordinator {
    public:
        ss::future<> handle_join_group(
            kafka::group_id group_id,
            member_metadata member
        ) {
            // Acquire group lock
            auto lock = co_await acquire_group_lock(group_id);
            
            // Get current group metadata
            auto group = co_await get_or_create_group(group_id);
            
            // Add member
            group.members.push_back(member);
            
            // Trigger rebalance if needed
            if (should_rebalance(group)) {
                co_await trigger_rebalance(group);
            }
            
            // Update metadata
            co_await update_group_metadata(group_id, group);
        }
        
        ss::future<> trigger_rebalance(consumer_group_metadata& group) {
            // Increment generation
            group.generation++;
            
            // Notify all members
            for (const auto& member : group.members) {
                co_await send_rebalance_notification(member.member_id);
            }
            
            // Wait for sync
            co_await wait_for_sync_group(group.group_id, group.generation);
            
            // Update state
            group.state = kafka::group_state::stable;
        }
        
    private:
        ss::future<distributed_lock> acquire_group_lock(
            kafka::group_id group_id
        ) {
            auto lock_key = fmt::format("/locks/groups/{}", group_id);
            co_return co_await _lock_manager.acquire(lock_key);
        }
        
    private:
        distributed_lock_manager _lock_manager;
    };

private:
    metadata_store _store;
    group_coordinator _coordinator;
};

} // namespace redpanda::metadata_service
```

#### 3. Compute Layer

```cpp
namespace redpanda::compute {

// Stateless compute node
class compute_node {
public:
    struct config {
        std::string storage_service_endpoint;
        std::string metadata_service_endpoint;
        size_t cache_size_bytes = 10_GiB;
        size_t max_concurrent_requests = 10000;
        duration metadata_refresh_interval = 10s;
    };

    ss::future<> start(config cfg) {
        _config = cfg;
        
        // Connect to storage service
        _storage_client = co_await storage_service_client::create(
            cfg.storage_service_endpoint
        );
        
        // Connect to metadata service
        _metadata_client = co_await metadata_service_client::create(
            cfg.metadata_service_endpoint
        );
        
        // Initialize cache
        _cache = std::make_unique<compute_cache>(cfg.cache_size_bytes);
        
        // Start metadata refresh loop
        _metadata_refresher = refresh_metadata_loop();
        
        // Start Kafka API server
        co_await start_kafka_server();
    }
    
    ss::future<> stop() {
        // Graceful shutdown
        co_await _kafka_server.stop();
        co_await _metadata_refresher.close();
        co_await _storage_client.close();
        co_await _metadata_client.close();
    }

private:
    // Kafka API handlers
    class kafka_request_handler {
    public:
        ss::future<produce_response> handle_produce(
            produce_request request
        ) {
            produce_response response;
            
            // Validate request
            validate_produce_request(request);
            
            // Get partition metadata
            auto metadata = co_await _metadata_client->get_partition_metadata(
                request.ntp()
            );
            
            // Check if we're the leader (in leaderless mode, anyone can write)
            if (!is_leader_or_leaderless(metadata)) {
                response.set_error(kafka::error_code::not_leader_for_partition);
                co_return response;
            }
            
            // Prepare batches
            std::vector<model::record_batch> batches;
            for (const auto& record : request.records) {
                batches.push_back(convert_to_batch(record));
            }
            
            // Append to storage
            auto append_result = co_await _storage_client->append_batches(
                request.ntp(),
                std::move(batches),
                {.replication_factor = metadata.replication_factor}
            );
            
            response.set_base_offset(append_result.base_offset);
            response.set_log_append_time(append_result.timestamp);
            
            co_return response;
        }
        
        ss::future<fetch_response> handle_fetch(
            fetch_request request
        ) {
            fetch_response response;
            
            // Check cache first
            if (auto cached = _cache->get_records(
                request.ntp(),
                request.fetch_offset,
                request.max_bytes
            )) {
                response.set_records(std::move(*cached));
                co_return response;
            }
            
            // Get partition metadata
            auto metadata = co_await _metadata_client->get_partition_metadata(
                request.ntp()
            );
            
            // Validate offset
            if (request.fetch_offset > metadata.high_water_mark) {
                // Wait for new data or timeout
                co_await wait_for_data_or_timeout(
                    request.ntp(),
                    request.fetch_offset,
                    request.max_wait_ms
                );
            }
            
            // Create reader from storage
            auto reader = co_await _storage_client->create_reader(
                request.ntp(),
                request.fetch_offset,
                request.fetch_offset + model::offset(request.max_bytes),
                {.prefetch_segments = 2}
            );
            
            // Read batches
            std::vector<model::record_batch> batches;
            size_t total_size = 0;
            
            while (auto batch = co_await reader.read()) {
                if (!batch) break;
                
                total_size += batch->size_bytes();
                batches.push_back(std::move(*batch));
                
                if (total_size >= request.max_bytes) {
                    break;
                }
            }
            
            // Update cache
            _cache->put_records(request.ntp(), request.fetch_offset, batches);
            
            response.set_records(std::move(batches));
            response.set_high_water_mark(metadata.high_water_mark);
            
            co_return response;
        }
        
        ss::future<offset_commit_response> handle_offset_commit(
            offset_commit_request request
        ) {
            offset_commit_response response;
            
            for (const auto& topic : request.topics) {
                for (const auto& partition : topic.partitions) {
                    try {
                        co_await _metadata_client->commit_consumer_offset(
                            request.group_id,
                            model::ntp(topic.name, partition.id),
                            partition.offset,
                            partition.metadata
                        );
                        
                        response.add_partition(
                            topic.name,
                            partition.id,
                            kafka::error_code::none
                        );
                    } catch (const std::exception& e) {
                        response.add_partition(
                            topic.name,
                            partition.id,
                            kafka::error_code::unknown_server_error
                        );
                    }
                }
            }
            
            co_return response;
        }
        
    private:
        storage_service_client* _storage_client;
        metadata_service_client* _metadata_client;
        compute_cache* _cache;
    };

    // Intelligent caching layer
    class compute_cache {
    public:
        struct cache_entry {
            model::ntp ntp;
            model::offset base_offset;
            std::vector<model::record_batch> batches;
            std::chrono::steady_clock::time_point last_access;
            size_t access_count = 0;
            size_t size_bytes = 0;
        };
        
        std::optional<std::vector<model::record_batch>>
        get_records(
            model::ntp ntp,
            model::offset start_offset,
            size_t max_bytes
        ) {
            auto key = make_cache_key(ntp, start_offset);
            
            if (auto it = _entries.find(key); it != _entries.end()) {
                // Update access stats
                it->second.last_access = clock::now();
                it->second.access_count++;
                
                // Return matching records
                return extract_records(it->second, start_offset, max_bytes);
            }
            
            return std::nullopt;
        }
        
        void put_records(
            model::ntp ntp,
            model::offset base_offset,
            const std::vector<model::record_batch>& batches
        ) {
            auto size = calculate_size(batches);
            
            // Evict if necessary
            while (_current_size + size > _max_size) {
                evict_lru();
            }
            
            // Add to cache
            auto key = make_cache_key(ntp, base_offset);
            _entries[key] = {
                .ntp = ntp,
                .base_offset = base_offset,
                .batches = batches,
                .last_access = clock::now(),
                .access_count = 1,
                .size_bytes = size
            };
            
            _current_size += size;
        }
        
    private:
        void evict_lru() {
            if (_entries.empty()) return;
            
            // Find least recently used entry
            auto lru_it = std::min_element(
                _entries.begin(), _entries.end(),
                [](const auto& a, const auto& b) {
                    return a.second.last_access < b.second.last_access;
                }
            );
            
            _current_size -= lru_it->second.size_bytes;
            _entries.erase(lru_it);
        }
        
    private:
        absl::flat_hash_map<cache_key, cache_entry> _entries;
        size_t _max_size;
        size_t _current_size = 0;
    };

    // Metadata refresh
    ss::future<> refresh_metadata_loop() {
        while (!_as.abort_requested()) {
            try {
                // Refresh partition metadata
                co_await refresh_partition_metadata();
                
                // Refresh consumer group metadata
                co_await refresh_consumer_group_metadata();
                
            } catch (const std::exception& e) {
                vlog(logger.warn, "Metadata refresh failed: {}", e.what());
            }
            
            co_await ss::sleep_abortable(_config.metadata_refresh_interval, _as);
        }
    }

private:
    config _config;
    std::unique_ptr<storage_service_client> _storage_client;
    std::unique_ptr<metadata_service_client> _metadata_client;
    std::unique_ptr<compute_cache> _cache;
    kafka_request_handler _request_handler;
    kafka_server _kafka_server;
    ss::future<> _metadata_refresher;
    ss::abort_source _as;
};

} // namespace redpanda::compute
```

#### 4. Service Discovery and Load Balancing

```cpp
namespace redpanda::service_mesh {

// Service registry for dynamic discovery
class service_registry {
public:
    struct service_instance {
        std::string service_name;
        std::string instance_id;
        std::string endpoint;
        health_status status;
        std::map<std::string, std::string> metadata;
        std::chrono::system_clock::time_point last_heartbeat;
    };
    
    ss::future<> register_service(
        std::string service_name,
        std::string instance_id,
        std::string endpoint,
        std::map<std::string, std::string> metadata = {}
    ) {
        service_instance instance{
            .service_name = service_name,
            .instance_id = instance_id,
            .endpoint = endpoint,
            .status = health_status::healthy,
            .metadata = metadata,
            .last_heartbeat = clock::now()
        };
        
        // Register with consensus store
        auto key = fmt::format("/services/{}/{}", service_name, instance_id);
        co_await _store.put(key, serialize(instance));
        
        // Start heartbeat
        _heartbeat_tasks[instance_id] = heartbeat_loop(instance);
    }
    
    ss::future<std::vector<service_instance>>
    discover_service(std::string service_name) {
        auto prefix = fmt::format("/services/{}/", service_name);
        auto entries = co_await _store.get_prefix(prefix);
        
        std::vector<service_instance> instances;
        for (const auto& [key, value] : entries) {
            auto instance = deserialize<service_instance>(value);
            
            // Filter out unhealthy instances
            if (instance.status == health_status::healthy) {
                instances.push_back(instance);
            }
        }
        
        co_return instances;
    }
    
private:
    ss::future<> heartbeat_loop(service_instance instance) {
        while (!_as.abort_requested()) {
            try {
                // Update heartbeat
                instance.last_heartbeat = clock::now();
                
                auto key = fmt::format(
                    "/services/{}/{}",
                    instance.service_name,
                    instance.instance_id
                );
                
                co_await _store.put(key, serialize(instance));
                
            } catch (const std::exception& e) {
                vlog(logger.warn, "Heartbeat failed: {}", e.what());
            }
            
            co_await ss::sleep_abortable(5s, _as);
        }
    }
    
private:
    consensus_store _store;
    absl::flat_hash_map<std::string, ss::future<>> _heartbeat_tasks;
    ss::abort_source _as;
};

// Client-side load balancer
class load_balancer {
public:
    enum class strategy {
        round_robin,
        least_connections,
        random,
        consistent_hash
    };
    
    ss::future<service_endpoint> get_endpoint(
        std::string service_name,
        std::optional<std::string> routing_key = std::nullopt
    ) {
        // Get available instances
        auto instances = co_await _registry.discover_service(service_name);
        
        if (instances.empty()) {
            throw no_instances_available_exception(service_name);
        }
        
        // Select instance based on strategy
        service_instance selected;
        
        switch (_strategy) {
        case strategy::round_robin:
            selected = select_round_robin(instances);
            break;
            
        case strategy::least_connections:
            selected = co_await select_least_connections(instances);
            break;
            
        case strategy::consistent_hash:
            selected = select_consistent_hash(instances, routing_key);
            break;
            
        default:
            selected = select_random(instances);
        }
        
        co_return parse_endpoint(selected.endpoint);
    }
    
private:
    service_instance select_round_robin(
        const std::vector<service_instance>& instances
    ) {
        auto index = _round_robin_counter++ % instances.size();
        return instances[index];
    }
    
    service_instance select_consistent_hash(
        const std::vector<service_instance>& instances,
        const std::optional<std::string>& routing_key
    ) {
        if (!routing_key) {
            return select_random(instances);
        }
        
        // Hash the routing key
        auto hash = xxhash_64(routing_key->data(), routing_key->size());
        
        // Select instance based on hash
        auto index = hash % instances.size();
        return instances[index];
    }
    
private:
    service_registry& _registry;
    strategy _strategy = strategy::round_robin;
    std::atomic<size_t> _round_robin_counter{0};
};

} // namespace redpanda::service_mesh
```

### Migration Strategy

#### Phase 1: Storage Service Development (Months 1-6)

```cpp
// Hybrid mode: Support both monolithic and disaggregated
class hybrid_storage_adapter {
    ss::future<append_result> append_batches(
        model::ntp ntp,
        std::vector<model::record_batch> batches,
        append_options opts
    ) {
        if (is_disaggregated_mode(ntp)) {
            // Use storage service
            co_return co_await _storage_service->append_batches(
                ntp, std::move(batches), opts
            );
        } else {
            // Use local storage
            co_return co_await _local_storage->append_batches(
                ntp, std::move(batches), opts
            );
        }
    }
};
```

#### Phase 2: Metadata Service Deployment (Months 6-10)

- Deploy metadata service alongside existing cluster
- Gradually migrate metadata operations
- Implement dual-write for consistency

#### Phase 3: Compute Layer Rollout (Months 10-14)

- Deploy stateless compute nodes
- Route read traffic to compute layer
- Gradually migrate write traffic

#### Phase 4: Full Migration (Months 14-18)

- Complete migration of all workloads
- Decommission monolithic nodes
- Performance optimization

### Performance Optimizations

#### 1. Request Batching

```cpp
class batch_aggregator {
    ss::future<std::vector<append_result>> flush_batch() {
        if (_pending_requests.empty()) {
            co_return {};
        }
        
        // Group by partition
        absl::flat_hash_map<model::ntp, std::vector<batch_request>> grouped;
        for (auto& req : _pending_requests) {
            grouped[req.ntp].push_back(std::move(req));
        }
        
        // Execute in parallel
        std::vector<ss::future<append_result>> futures;
        for (auto& [ntp, requests] : grouped) {
            futures.push_back(execute_batch(ntp, std::move(requests)));
        }
        
        co_return co_await ss::when_all_succeed(futures.begin(), futures.end());
    }
};
```

#### 2. Prefetching and Caching

```cpp
class prefetch_manager {
    ss::future<> prefetch_segments(
        model::ntp ntp,
        model::offset current_offset
    ) {
        // Predict next segments
        auto predicted_offsets = _predictor.predict_next_accesses(
            ntp, current_offset
        );
        
        // Prefetch in parallel
        std::vector<ss::future<>> futures;
        for (auto offset : predicted_offsets) {
            futures.push_back(prefetch_segment(ntp, offset));
        }
        
        co_await ss::when_all(futures.begin(), futures.end());
    }
};
```

## Testing Strategy

### Unit Tests

```cpp
BOOST_AUTO_TEST_CASE(test_storage_service_append) {
    cloud_storage_service service;
    
    model::ntp ntp("test-topic", 0);
    std::vector<model::record_batch> batches = create_test_batches(100);
    
    auto result = service.append_batches(ntp, batches, {}).get();
    
    BOOST_REQUIRE_EQUAL(result.base_offset, model::offset(0));
    BOOST_REQUIRE_EQUAL(result.last_offset, model::offset(99));
}

BOOST_AUTO_TEST_CASE(test_compute_node_caching) {
    compute_cache cache(1_MiB);
    
    model::ntp ntp("test-topic", 0);
    auto batches = create_test_batches(10);
    
    cache.put_records(ntp, model::offset(0), batches);
    
    auto retrieved = cache.get_records(ntp, model::offset(0), 1_KiB);
    BOOST_REQUIRE(retrieved.has_value());
    BOOST_REQUIRE_EQUAL(retrieved->size(), 10);
}
```

### Integration Tests

```cpp
class disaggregated_integration_test {
    ss::future<> test_end_to_end_flow() {
        // Start services
        auto storage_service = co_await start_storage_service();
        auto metadata_service = co_await start_metadata_service();
        auto compute_node = co_await start_compute_node();
        
        // Produce messages
        kafka_client client;
        co_await client.connect(compute_node.endpoint());
        
        produce_request req;
        req.topic = "test-topic";
        req.records = create_test_records(1000);
        
        auto produce_resp = co_await client.produce(req);
        BOOST_REQUIRE(!produce_resp.has_error());
        
        // Consume messages
        fetch_request fetch_req;
        fetch_req.topic = "test-topic";
        fetch_req.partition = 0;
        fetch_req.offset = 0;
        
        auto fetch_resp = co_await client.fetch(fetch_req);
        BOOST_REQUIRE_EQUAL(fetch_resp.records.size(), 1000);
    }
};
```

### Performance Benchmarks

```cpp
PERF_TEST(disaggregated_throughput) {
    auto throughput = co_await measure_throughput(
        disaggregated_config{
            .compute_nodes = 10,
            .storage_nodes = 5,
            .message_size = 1_KiB,
            .batch_size = 100
        }
    );
    
    // Should achieve 1M messages/sec
    BOOST_REQUIRE_GT(throughput, 1'000'000);
}
```

## Security Considerations

### Service-to-Service Authentication

```cpp
class mtls_authenticator {
    ss::future<> authenticate_connection(
        const tls_connection& conn
    ) {
        // Verify client certificate
        auto cert = conn.get_peer_certificate();
        
        if (!verify_certificate(cert)) {
            throw authentication_exception("Invalid certificate");
        }
        
        // Extract service identity
        auto identity = extract_service_identity(cert);
        
        // Check authorization
        if (!is_authorized(identity, conn.requested_service())) {
            throw authorization_exception("Service not authorized");
        }
    }
};
```

### Encryption at Rest

```cpp
class encryption_manager {
    ss::future<encrypted_segment> encrypt_segment(
        const segment& seg,
        const encryption_key& key
    ) {
        // Generate segment-specific key
        auto segment_key = derive_segment_key(key, seg.id());
        
        // Encrypt data
        auto encrypted_data = co_await aes_gcm_encrypt(
            seg.data(),
            segment_key
        );
        
        co_return encrypted_segment{
            .data = std::move(encrypted_data),
            .key_id = key.id(),
            .algorithm = encryption_algorithm::aes_256_gcm
        };
    }
};
```

## Monitoring and Observability

### Metrics

```yaml
# Storage service metrics
storage_append_latency_ms: histogram
storage_read_latency_ms: histogram
storage_segments_created: counter
storage_bytes_written: counter
storage_replication_lag_ms: gauge

# Metadata service metrics
metadata_update_latency_ms: histogram
metadata_consensus_rounds: counter
metadata_watch_subscriptions: gauge

# Compute node metrics
compute_cache_hit_rate: gauge
compute_request_queue_depth: gauge
compute_active_connections: gauge
```

### Health Checks

```cpp
class health_monitor {
    ss::future<health_status> check_system_health() {
        health_status status;
        
        // Check storage service
        status.storage = co_await check_storage_health();
        
        // Check metadata service
        status.metadata = co_await check_metadata_health();
        
        // Check compute nodes
        status.compute = co_await check_compute_health();
        
        // Aggregate status
        status.overall = aggregate_health(status);
        
        co_return status;
    }
};
```

## Configuration

```yaml
# Disaggregated mode configuration
disaggregated_enabled: false
disaggregated_storage_endpoint: "storage.redpanda.internal:9092"
disaggregated_metadata_endpoint: "metadata.redpanda.internal:9093"

# Compute node configuration
compute_cache_size_mb: 10240
compute_max_connections: 10000
compute_metadata_refresh_ms: 10000

# Storage service configuration
storage_segment_size_mb: 128
storage_replication_factor: 3
storage_cloud_provider: "s3"
storage_cloud_bucket: "redpanda-segments"

# Metadata service configuration
metadata_consensus_backend: "raft"  # or "etcd", "foundationdb"
metadata_replication_factor: 5
metadata_snapshot_interval_ms: 60000
```

## Open Questions

1. **Consensus Mechanism**: Use Raft, etcd, or FoundationDB for metadata?
2. **Storage Format**: Keep existing segment format or optimize for cloud?
3. **Caching Strategy**: LRU, LFU, or adaptive replacement?
4. **Compute Elasticity**: How aggressive should auto-scaling be?
5. **Multi-Region**: Active-active or active-passive replication?

## References

- [Amazon S3 Express One Zone](https://aws.amazon.com/s3/storage-classes/express-one-zone/)
- [Snowflake Architecture](https://docs.snowflake.com/en/user-guide/intro-key-concepts)
- [Apache Kafka Tiered Storage KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405)
- [Neon Disaggregated PostgreSQL](https://neon.tech/docs/introduction/architecture)