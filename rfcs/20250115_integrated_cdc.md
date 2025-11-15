# RFC-010: Integrated Change Data Capture (CDC)

**Authors**: Redpanda Engineering Team  
**Status**: Proposed  
**Created**: 2025-01-15  
**Discussion**: [GitHub Issue #XXXX]

## Summary

This RFC proposes native Change Data Capture (CDC) capabilities for Redpanda, enabling seamless streaming of database changes without external connectors, supporting major databases with exactly-once guarantees and minimal performance overhead.

## Motivation

### Current State

Organizations currently rely on external CDC tools like:
- Debezium for Kafka Connect
- Maxwell's Daemon
- AWS Database Migration Service
- Custom CDC implementations

### Problems with External CDC

1. **Complexity**: Additional infrastructure to deploy and manage
2. **Latency**: Extra network hops and serialization overhead
3. **Consistency**: Difficult to maintain exactly-once semantics
4. **Operational Burden**: Separate monitoring, scaling, and failure recovery
5. **Cost**: Additional compute resources and licensing

### Benefits of Integrated CDC

- **Simplicity**: Single system for both streaming and CDC
- **Performance**: Direct integration reduces latency
- **Reliability**: Unified failure handling and recovery
- **Consistency**: Native exactly-once guarantees
- **Efficiency**: Shared resources and optimized data paths

## Detailed Design

### Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│                    Database Sources                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐│
│  │PostgreSQL│  │  MySQL   │  │ MongoDB  │  │  Oracle  ││
│  └─────┬────┘  └─────┬────┘  └─────┬────┘  └─────┬────┘│
└────────┼──────────────┼──────────────┼──────────────┼───┘
         │              │              │              │
         ▼              ▼              ▼              ▼
┌──────────────────────────────────────────────────────────┐
│               CDC Connector Framework                      │
│  ┌─────────────────────────────────────────────────────┐ │
│  │            Source Connector Plugins                  │ │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐  │ │
│  │  │Postgres │ │  MySQL  │ │ MongoDB │ │ Oracle  │  │ │
│  │  │Connector│ │Connector│ │Connector│ │Connector│  │ │
│  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘  │ │
│  └─────────────────────────────────────────────────────┘ │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │           Change Event Processing                    │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────┐ │ │
│  │  │   Schema     │  │  Transform   │  │  Filter  │ │ │
│  │  │  Evolution   │  │   Engine     │  │  Engine  │ │ │
│  │  └──────────────┘  └──────────────┘  └──────────┘ │ │
│  └─────────────────────────────────────────────────────┘ │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │          State Management & Coordination             │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────┐ │ │
│  │  │  Checkpoint  │  │   Offset     │  │Failover  │ │ │
│  │  │   Manager    │  │   Tracking   │  │ Manager  │ │ │
│  │  └──────────────┘  └──────────────┘  └──────────┘ │ │
│  └─────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│                   Redpanda Core                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │                Topic Management                      │ │
│  │   Automatic topic creation and configuration         │ │
│  └─────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

### Core Components

#### 1. CDC Source Framework

```cpp
namespace redpanda::cdc {

// Base CDC source connector interface
class cdc_source {
public:
    struct source_config {
        std::string connection_string;
        std::vector<std::string> tables;
        capture_mode mode = capture_mode::incremental;
        bool include_schema_changes = true;
        bool include_before_image = true;
        std::optional<std::string> start_position;
        std::map<std::string, std::string> properties;
    };
    
    enum class capture_mode {
        snapshot,      // Full table snapshot
        incremental,   // Only changes
        snapshot_and_incremental  // Snapshot then incremental
    };
    
    struct change_event {
        enum class operation {
            insert,
            update,
            delete_,
            truncate,
            schema_change
        };
        
        operation op;
        std::string database;
        std::string schema;
        std::string table;
        std::optional<json::object> before;  // Before image
        std::optional<json::object> after;   // After image
        std::optional<json::object> key;     // Primary key
        std::chrono::system_clock::time_point timestamp;
        std::optional<std::string> transaction_id;
        int64_t sequence_number;
        std::map<std::string, std::string> metadata;
    };
    
    // Lifecycle methods
    virtual ss::future<> start(source_config config) = 0;
    virtual ss::future<> stop() = 0;
    
    // Streaming interface
    virtual ss::future<ss::input_stream<change_event>> 
    create_change_stream() = 0;
    
    // State management
    virtual ss::future<checkpoint> get_checkpoint() = 0;
    virtual ss::future<> restore_from_checkpoint(checkpoint cp) = 0;
    
    // Schema discovery
    virtual ss::future<schema_metadata> 
    discover_schema(const std::string& table) = 0;
    
    // Health monitoring
    virtual ss::future<health_status> check_health() = 0;
};

// PostgreSQL CDC implementation using logical replication
class postgres_cdc_source : public cdc_source {
private:
    struct replication_slot {
        std::string name;
        std::string plugin = "pgoutput";  // or "wal2json"
        lsn position;
        bool temporary = false;
    };
    
    class logical_replication_client {
    public:
        ss::future<> connect(const source_config& config) {
            // Parse connection string
            auto conn_params = parse_connection_string(config.connection_string);
            
            // Connect to PostgreSQL
            _conn = co_await create_connection(conn_params);
            
            // Check server version and capabilities
            co_await verify_server_capabilities();
            
            // Create or reuse replication slot
            _slot = co_await create_or_get_replication_slot(
                generate_slot_name(config)
            );
            
            // Start replication
            co_await start_replication(_slot);
        }
        
        ss::future<> start_replication(const replication_slot& slot) {
            // Send START_REPLICATION command
            auto query = fmt::format(
                "START_REPLICATION SLOT {} LOGICAL {} "
                "(proto_version '2', publication_names '{}')",
                slot.name,
                slot.position.to_string(),
                _publication_name
            );
            
            co_await _conn.send_query(query);
            
            // Enter streaming mode
            _streaming = true;
            _stream_fiber = process_replication_stream();
        }
        
    private:
        ss::future<> process_replication_stream() {
            while (_streaming) {
                auto message = co_await read_replication_message();
                
                switch (message.type) {
                case message_type::begin_transaction:
                    co_await handle_begin(message);
                    break;
                    
                case message_type::commit_transaction:
                    co_await handle_commit(message);
                    break;
                    
                case message_type::insert:
                    co_await handle_insert(message);
                    break;
                    
                case message_type::update:
                    co_await handle_update(message);
                    break;
                    
                case message_type::delete_:
                    co_await handle_delete(message);
                    break;
                    
                case message_type::relation:
                    co_await handle_relation_metadata(message);
                    break;
                    
                case message_type::keepalive:
                    co_await send_standby_status_update();
                    break;
                }
                
                // Track progress
                _last_received_lsn = message.lsn;
                
                // Periodically send feedback
                if (should_send_feedback()) {
                    co_await send_standby_status_update();
                }
            }
        }
        
        ss::future<change_event> handle_insert(const replication_message& msg) {
            change_event event;
            event.op = change_event::operation::insert;
            event.database = msg.database;
            event.schema = msg.schema;
            event.table = msg.table;
            
            // Parse tuple data
            auto tuple = parse_tuple_data(msg.data);
            event.after = tuple_to_json(tuple, get_table_metadata(msg.table));
            
            // Extract primary key
            event.key = extract_primary_key(tuple, msg.table);
            
            event.timestamp = msg.timestamp;
            event.transaction_id = msg.transaction_id;
            event.sequence_number = _sequence_counter++;
            
            // Add to pending changes
            _pending_changes.push_back(event);
            
            co_return event;
        }
        
        ss::future<change_event> handle_update(const replication_message& msg) {
            change_event event;
            event.op = change_event::operation::update;
            event.database = msg.database;
            event.schema = msg.schema;
            event.table = msg.table;
            
            // Parse old and new tuples
            if (msg.has_old_tuple) {
                auto old_tuple = parse_tuple_data(msg.old_data);
                event.before = tuple_to_json(old_tuple, get_table_metadata(msg.table));
            }
            
            auto new_tuple = parse_tuple_data(msg.new_data);
            event.after = tuple_to_json(new_tuple, get_table_metadata(msg.table));
            
            event.key = extract_primary_key(new_tuple, msg.table);
            
            event.timestamp = msg.timestamp;
            event.transaction_id = msg.transaction_id;
            event.sequence_number = _sequence_counter++;
            
            _pending_changes.push_back(event);
            
            co_return event;
        }
        
        ss::future<> send_standby_status_update() {
            // Send feedback to PostgreSQL
            standby_status_update update;
            update.write_lsn = _last_written_lsn;
            update.flush_lsn = _last_flushed_lsn;
            update.apply_lsn = _last_applied_lsn;
            update.reply_requested = false;
            
            co_await _conn.send_replication_feedback(update);
            _last_feedback_time = clock::now();
        }
        
    private:
        pg_connection _conn;
        replication_slot _slot;
        std::string _publication_name;
        bool _streaming = false;
        ss::future<> _stream_fiber;
        
        lsn _last_received_lsn;
        lsn _last_written_lsn;
        lsn _last_flushed_lsn;
        lsn _last_applied_lsn;
        
        std::vector<change_event> _pending_changes;
        int64_t _sequence_counter = 0;
        
        std::chrono::steady_clock::time_point _last_feedback_time;
        static constexpr auto feedback_interval = 10s;
    };
    
public:
    ss::future<> start(source_config config) override {
        _config = config;
        
        // Initialize logical replication client
        _replication_client = std::make_unique<logical_replication_client>();
        co_await _replication_client->connect(config);
        
        // Create publication if needed
        if (_config.mode == capture_mode::snapshot_and_incremental) {
            co_await create_publication();
        }
        
        // Start snapshot if required
        if (needs_initial_snapshot()) {
            co_await perform_initial_snapshot();
        }
        
        // Start streaming changes
        _streaming = true;
    }
    
    ss::future<ss::input_stream<change_event>> 
    create_change_stream() override {
        co_return make_input_stream<change_event>(
            [this]() -> ss::future<std::optional<change_event>> {
                if (!_streaming) {
                    co_return std::nullopt;
                }
                
                // Return buffered changes or wait for new ones
                while (_change_buffer.empty() && _streaming) {
                    co_await _change_available.wait();
                }
                
                if (!_change_buffer.empty()) {
                    auto event = _change_buffer.front();
                    _change_buffer.pop_front();
                    co_return event;
                }
                
                co_return std::nullopt;
            }
        );
    }
    
private:
    ss::future<> perform_initial_snapshot() {
        vlog(logger.info, "Starting initial snapshot");
        
        // Begin transaction with REPEATABLE READ
        co_await _conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ");
        
        // Get snapshot ID for consistency
        auto snapshot_id = co_await get_current_snapshot_id();
        
        for (const auto& table : _config.tables) {
            co_await snapshot_table(table, snapshot_id);
        }
        
        // Commit snapshot transaction
        co_await _conn.execute("COMMIT");
        
        vlog(logger.info, "Initial snapshot completed");
    }
    
    ss::future<> snapshot_table(
        const std::string& table,
        const std::string& snapshot_id
    ) {
        // Get table metadata
        auto metadata = co_await discover_schema(table);
        
        // Prepare snapshot query
        auto query = fmt::format("SELECT * FROM {}", table);
        
        // Stream results
        auto result_stream = co_await _conn.stream_query(query);
        
        while (auto row = co_await result_stream.read()) {
            if (!row) break;
            
            // Convert row to change event
            change_event event;
            event.op = change_event::operation::insert;
            event.database = metadata.database;
            event.schema = metadata.schema;
            event.table = metadata.table;
            event.after = row_to_json(*row, metadata);
            event.key = extract_primary_key(*row, metadata);
            event.timestamp = clock::now();
            event.metadata["snapshot"] = "true";
            event.metadata["snapshot_id"] = snapshot_id;
            
            _change_buffer.push_back(event);
            _change_available.signal();
        }
    }
    
private:
    source_config _config;
    std::unique_ptr<logical_replication_client> _replication_client;
    pg_connection _conn;  // For snapshots and metadata
    
    std::deque<change_event> _change_buffer;
    ss::condition_variable _change_available;
    bool _streaming = false;
};

// MySQL CDC implementation using binlog
class mysql_cdc_source : public cdc_source {
private:
    class binlog_reader {
    public:
        ss::future<> connect(const source_config& config) {
            // Connect to MySQL
            _conn = co_await create_connection(config.connection_string);
            
            // Check binlog configuration
            co_await verify_binlog_enabled();
            
            // Get current binlog position
            if (config.start_position) {
                _position = parse_binlog_position(*config.start_position);
            } else {
                _position = co_await get_current_binlog_position();
            }
            
            // Register as slave
            co_await register_as_slave();
            
            // Start binlog dump
            co_await start_binlog_dump(_position);
        }
        
        ss::future<> start_binlog_dump(const binlog_position& pos) {
            // Send COM_BINLOG_DUMP command
            binlog_dump_command cmd;
            cmd.binlog_pos = pos.position;
            cmd.flags = BINLOG_DUMP_NON_BLOCK;
            cmd.server_id = _server_id;
            cmd.binlog_filename = pos.filename;
            
            co_await _conn.send_command(cmd);
            
            // Start processing binlog events
            _processing = true;
            _process_fiber = process_binlog_stream();
        }
        
        ss::future<> process_binlog_stream() {
            while (_processing) {
                auto event = co_await read_binlog_event();
                
                switch (event.type) {
                case binlog_event_type::rotate_event:
                    co_await handle_rotate(event);
                    break;
                    
                case binlog_event_type::format_description_event:
                    co_await handle_format_description(event);
                    break;
                    
                case binlog_event_type::table_map_event:
                    co_await handle_table_map(event);
                    break;
                    
                case binlog_event_type::write_rows_event:
                case binlog_event_type::write_rows_event_v2:
                    co_await handle_insert_rows(event);
                    break;
                    
                case binlog_event_type::update_rows_event:
                case binlog_event_type::update_rows_event_v2:
                    co_await handle_update_rows(event);
                    break;
                    
                case binlog_event_type::delete_rows_event:
                case binlog_event_type::delete_rows_event_v2:
                    co_await handle_delete_rows(event);
                    break;
                    
                case binlog_event_type::gtid_event:
                    co_await handle_gtid(event);
                    break;
                    
                case binlog_event_type::xid_event:
                    co_await handle_transaction_commit(event);
                    break;
                }
                
                // Update position
                _position.position = event.next_position;
            }
        }
        
        ss::future<change_event> handle_insert_rows(const binlog_event& evt) {
            auto rows_event = parse_write_rows_event(evt);
            
            change_event event;
            event.op = change_event::operation::insert;
            
            // Get table metadata from table map
            auto table_info = _table_map[rows_event.table_id];
            event.database = table_info.database;
            event.schema = table_info.database;  // MySQL uses database as schema
            event.table = table_info.table;
            
            // Parse row data
            for (const auto& row : rows_event.rows) {
                event.after = decode_row(row, table_info);
                event.key = extract_primary_key(event.after, table_info);
                event.timestamp = std::chrono::system_clock::from_time_t(evt.timestamp);
                
                if (_current_gtid) {
                    event.transaction_id = _current_gtid->to_string();
                }
                
                _pending_events.push_back(event);
            }
            
            co_return event;
        }
        
        json::object decode_row(
            const std::vector<uint8_t>& data,
            const table_map_info& table_info
        ) {
            json::object result;
            size_t offset = 0;
            
            for (size_t i = 0; i < table_info.columns.size(); ++i) {
                const auto& col = table_info.columns[i];
                
                // Check NULL bitmap
                if (is_column_null(data, i)) {
                    result[col.name] = json::null();
                    continue;
                }
                
                // Decode based on column type
                switch (col.type) {
                case column_type::tiny:
                    result[col.name] = decode_int8(data, offset);
                    break;
                    
                case column_type::short_:
                    result[col.name] = decode_int16(data, offset);
                    break;
                    
                case column_type::long_:
                    result[col.name] = decode_int32(data, offset);
                    break;
                    
                case column_type::longlong:
                    result[col.name] = decode_int64(data, offset);
                    break;
                    
                case column_type::float_:
                    result[col.name] = decode_float(data, offset);
                    break;
                    
                case column_type::double_:
                    result[col.name] = decode_double(data, offset);
                    break;
                    
                case column_type::varchar:
                case column_type::string:
                    result[col.name] = decode_string(data, offset, col.metadata);
                    break;
                    
                case column_type::datetime2:
                    result[col.name] = decode_datetime(data, offset, col.metadata);
                    break;
                    
                case column_type::json:
                    result[col.name] = decode_json(data, offset);
                    break;
                }
            }
            
            return result;
        }
        
    private:
        mysql_connection _conn;
        binlog_position _position;
        uint32_t _server_id;
        bool _processing = false;
        ss::future<> _process_fiber;
        
        std::map<uint64_t, table_map_info> _table_map;
        std::optional<gtid> _current_gtid;
        std::vector<change_event> _pending_events;
    };
    
public:
    ss::future<> start(source_config config) override {
        _config = config;
        
        // Initialize binlog reader
        _binlog_reader = std::make_unique<binlog_reader>();
        co_await _binlog_reader->connect(config);
        
        // Perform initial snapshot if needed
        if (_config.mode == capture_mode::snapshot_and_incremental) {
            co_await perform_initial_snapshot();
        }
        
        _streaming = true;
    }
    
private:
    source_config _config;
    std::unique_ptr<binlog_reader> _binlog_reader;
    bool _streaming = false;
};

} // namespace redpanda::cdc
```

#### 2. Schema Evolution and Management

```cpp
namespace redpanda::cdc {

// Schema registry integration for CDC
class cdc_schema_manager {
public:
    struct schema_change_event {
        enum class change_type {
            add_column,
            drop_column,
            alter_column,
            rename_column,
            add_table,
            drop_table,
            rename_table
        };
        
        change_type type;
        std::string database;
        std::string schema;
        std::string table;
        std::optional<column_definition> old_definition;
        std::optional<column_definition> new_definition;
        std::chrono::system_clock::time_point timestamp;
        int32_t version;
    };
    
    ss::future<> handle_schema_change(const schema_change_event& event) {
        // Update schema registry
        auto new_schema = co_await evolve_schema(event);
        
        // Register new version
        auto version = co_await _schema_registry.register_schema(
            make_subject_name(event),
            new_schema
        );
        
        // Update compatibility
        co_await update_compatibility_rules(event, new_schema);
        
        // Notify consumers
        co_await notify_schema_change(event, version);
        
        // Update internal metadata
        _schema_cache[make_cache_key(event)] = {
            .schema = new_schema,
            .version = version,
            .timestamp = event.timestamp
        };
    }
    
    ss::future<avro::schema> evolve_schema(const schema_change_event& event) {
        // Get current schema
        auto current = co_await get_current_schema(event);
        
        avro::schema evolved = current;
        
        switch (event.type) {
        case schema_change_event::change_type::add_column:
            evolved = add_field(current, event.new_definition);
            break;
            
        case schema_change_event::change_type::drop_column:
            evolved = remove_field(current, event.old_definition->name);
            break;
            
        case schema_change_event::change_type::alter_column:
            evolved = alter_field(
                current,
                event.old_definition,
                event.new_definition
            );
            break;
        }
        
        // Validate compatibility
        if (!is_compatible(current, evolved)) {
            throw schema_evolution_exception(
                "Schema change breaks compatibility"
            );
        }
        
        co_return evolved;
    }
    
    // Column type mapping
    avro::type map_database_type_to_avro(
        const std::string& db_type,
        database_type db
    ) {
        if (db == database_type::postgresql) {
            return map_postgres_type(db_type);
        } else if (db == database_type::mysql) {
            return map_mysql_type(db_type);
        } else if (db == database_type::mongodb) {
            return map_mongodb_type(db_type);
        }
        
        throw unsupported_type_exception(db_type);
    }
    
private:
    avro::type map_postgres_type(const std::string& pg_type) {
        static const std::map<std::string, avro::type> type_map = {
            {"boolean", avro::type::boolean},
            {"smallint", avro::type::int_},
            {"integer", avro::type::int_},
            {"bigint", avro::type::long_},
            {"real", avro::type::float_},
            {"double precision", avro::type::double_},
            {"text", avro::type::string},
            {"varchar", avro::type::string},
            {"char", avro::type::string},
            {"bytea", avro::type::bytes},
            {"timestamp", avro::type::long_},  // Microseconds since epoch
            {"timestamptz", avro::type::long_},
            {"date", avro::type::int_},        // Days since epoch
            {"time", avro::type::long_},        // Microseconds since midnight
            {"json", avro::type::string},
            {"jsonb", avro::type::string},
            {"uuid", avro::type::string},
            {"numeric", avro::type::string},    // Preserve precision
            {"decimal", avro::type::string}
        };
        
        if (auto it = type_map.find(pg_type); it != type_map.end()) {
            return it->second;
        }
        
        // Array types
        if (pg_type.ends_with("[]")) {
            auto element_type = pg_type.substr(0, pg_type.size() - 2);
            return avro::type::array;
        }
        
        // Default to string
        return avro::type::string;
    }
    
private:
    schema_registry_client _schema_registry;
    absl::flat_hash_map<std::string, cached_schema> _schema_cache;
};

} // namespace redpanda::cdc
```

#### 3. Transform and Filter Engine

```cpp
namespace redpanda::cdc {

// CDC-specific transformations
class cdc_transform_engine {
public:
    struct transform_config {
        std::string name;
        transform_type type;
        json::object parameters;
        std::optional<std::string> filter_expression;
    };
    
    enum class transform_type {
        flatten,           // Flatten nested structures
        rename_fields,     // Rename fields
        add_metadata,      // Add metadata fields
        route,            // Route to different topics
        filter,           // Filter events
        mask_sensitive,   // Mask sensitive data
        extract_key,      // Extract key from payload
        debezium_format,  // Convert to Debezium format
        custom_wasm       // Custom WASM transform
    };
    
    ss::future<std::optional<change_event>> 
    apply_transforms(change_event event) {
        // Apply filter first
        if (_filter && !co_await _filter->matches(event)) {
            co_return std::nullopt;
        }
        
        // Apply transformations in order
        for (const auto& transform : _transforms) {
            event = co_await apply_single_transform(transform, std::move(event));
        }
        
        co_return event;
    }
    
private:
    ss::future<change_event> apply_single_transform(
        const transform_config& config,
        change_event event
    ) {
        switch (config.type) {
        case transform_type::flatten:
            co_return flatten_event(event, config.parameters);
            
        case transform_type::rename_fields:
            co_return rename_fields(event, config.parameters);
            
        case transform_type::add_metadata:
            co_return add_metadata_fields(event, config.parameters);
            
        case transform_type::mask_sensitive:
            co_return mask_sensitive_data(event, config.parameters);
            
        case transform_type::debezium_format:
            co_return convert_to_debezium(event);
            
        case transform_type::custom_wasm:
            co_return co_await apply_wasm_transform(event, config);
        }
        
        co_return event;
    }
    
    change_event flatten_event(
        change_event event,
        const json::object& params
    ) {
        // Flatten nested JSON structures
        if (event.after) {
            event.after = flatten_json(*event.after, params);
        }
        
        if (event.before) {
            event.before = flatten_json(*event.before, params);
        }
        
        return event;
    }
    
    change_event convert_to_debezium(change_event event) {
        // Convert to Debezium-compatible format
        json::object debezium_event;
        
        // Envelope
        debezium_event["schema"] = generate_debezium_schema(event);
        
        // Payload
        json::object payload;
        
        if (event.before) {
            payload["before"] = *event.before;
        } else {
            payload["before"] = json::null();
        }
        
        if (event.after) {
            payload["after"] = *event.after;
        } else {
            payload["after"] = json::null();
        }
        
        // Source metadata
        json::object source;
        source["version"] = "2.0.0";
        source["connector"] = "redpanda-cdc";
        source["name"] = event.database;
        source["db"] = event.database;
        source["schema"] = event.schema;
        source["table"] = event.table;
        source["ts_ms"] = std::chrono::duration_cast<std::chrono::milliseconds>(
            event.timestamp.time_since_epoch()
        ).count();
        
        if (event.transaction_id) {
            source["txId"] = *event.transaction_id;
        }
        
        payload["source"] = source;
        
        // Operation
        switch (event.op) {
        case change_event::operation::insert:
            payload["op"] = "c";  // Create
            break;
        case change_event::operation::update:
            payload["op"] = "u";  // Update
            break;
        case change_event::operation::delete_:
            payload["op"] = "d";  // Delete
            break;
        }
        
        payload["ts_ms"] = source["ts_ms"];
        
        debezium_event["payload"] = payload;
        
        // Update event with Debezium format
        event.after = debezium_event;
        event.before = std::nullopt;
        
        return event;
    }
    
    ss::future<change_event> apply_wasm_transform(
        change_event event,
        const transform_config& config
    ) {
        // Load WASM module
        auto module = co_await _wasm_engine.load_module(config.parameters["module"]);
        
        // Serialize event
        auto input = serialize_event(event);
        
        // Execute transform
        auto output = co_await module->execute("transform", input);
        
        // Deserialize result
        co_return deserialize_event(output);
    }
    
private:
    std::vector<transform_config> _transforms;
    std::unique_ptr<filter_engine> _filter;
    wasm_engine _wasm_engine;
};

// SQL-based filtering
class cdc_filter_engine {
public:
    ss::future<bool> matches(const change_event& event) {
        // Parse SQL WHERE clause
        auto ast = parse_sql_expression(_filter_expression);
        
        // Build context
        evaluation_context ctx;
        ctx.set_variable("op", operation_to_string(event.op));
        ctx.set_variable("database", event.database);
        ctx.set_variable("schema", event.schema);
        ctx.set_variable("table", event.table);
        
        if (event.after) {
            for (const auto& [key, value] : *event.after) {
                ctx.set_variable("after." + key, value);
            }
        }
        
        if (event.before) {
            for (const auto& [key, value] : *event.before) {
                ctx.set_variable("before." + key, value);
            }
        }
        
        // Evaluate expression
        co_return evaluate_expression(ast, ctx);
    }
    
private:
    std::string _filter_expression;
};

} // namespace redpanda::cdc
```

#### 4. State Management and Recovery

```cpp
namespace redpanda::cdc {

// CDC connector state management
class cdc_state_manager {
public:
    struct connector_state {
        std::string connector_id;
        std::string source_type;
        json::object source_position;  // Database-specific position
        std::chrono::system_clock::time_point last_checkpoint;
        int64_t events_processed;
        int64_t bytes_processed;
        std::map<std::string, table_state> tables;
    };
    
    struct table_state {
        std::string table_name;
        snapshot_state snapshot;
        int64_t rows_processed;
        std::optional<std::string> last_key;
    };
    
    enum class snapshot_state {
        not_started,
        in_progress,
        completed
    };
    
    ss::future<> checkpoint(const connector_state& state) {
        // Serialize state
        auto serialized = serialize_state(state);
        
        // Store in internal topic
        co_await store_checkpoint(state.connector_id, serialized);
        
        // Update in-memory state
        _states[state.connector_id] = state;
        
        // Notify listeners
        _checkpoint_listeners.notify(state);
    }
    
    ss::future<connector_state> restore(const std::string& connector_id) {
        // Try to load from internal topic
        if (auto checkpoint = co_await load_checkpoint(connector_id)) {
            auto state = deserialize_state(*checkpoint);
            
            vlog(logger.info, "Restored CDC connector {} from checkpoint: "
                 "position={}, events_processed={}",
                 connector_id,
                 json::stringify(state.source_position),
                 state.events_processed);
            
            _states[connector_id] = state;
            co_return state;
        }
        
        // No checkpoint found, start fresh
        connector_state fresh_state;
        fresh_state.connector_id = connector_id;
        fresh_state.last_checkpoint = clock::now();
        fresh_state.events_processed = 0;
        fresh_state.bytes_processed = 0;
        
        _states[connector_id] = fresh_state;
        co_return fresh_state;
    }
    
    // Failure recovery coordination
    ss::future<> handle_connector_failure(const std::string& connector_id) {
        vlog(logger.warn, "CDC connector {} failed, initiating recovery",
             connector_id);
        
        // Load last checkpoint
        auto state = co_await restore(connector_id);
        
        // Determine recovery strategy
        auto strategy = determine_recovery_strategy(state);
        
        switch (strategy) {
        case recovery_strategy::resume_from_checkpoint:
            co_await resume_from_checkpoint(state);
            break;
            
        case recovery_strategy::restart_snapshot:
            co_await restart_snapshot(state);
            break;
            
        case recovery_strategy::skip_to_latest:
            co_await skip_to_latest(state);
            break;
        }
        
        // Restart connector
        co_await restart_connector(connector_id);
    }
    
private:
    ss::future<> store_checkpoint(
        const std::string& connector_id,
        const iobuf& data
    ) {
        // Use internal topic for state storage
        model::record_batch batch;
        batch.append_record(connector_id, data);
        
        co_await _state_topic_producer.produce(
            cdc_state_topic,
            std::move(batch)
        );
    }
    
    enum class recovery_strategy {
        resume_from_checkpoint,
        restart_snapshot,
        skip_to_latest
    };
    
    recovery_strategy determine_recovery_strategy(const connector_state& state) {
        // If snapshot was in progress, restart it
        for (const auto& [table, table_state] : state.tables) {
            if (table_state.snapshot == snapshot_state::in_progress) {
                return recovery_strategy::restart_snapshot;
            }
        }
        
        // If checkpoint is recent, resume from it
        auto age = clock::now() - state.last_checkpoint;
        if (age < max_checkpoint_age) {
            return recovery_strategy::resume_from_checkpoint;
        }
        
        // Otherwise, skip to latest
        return recovery_strategy::skip_to_latest;
    }
    
private:
    absl::flat_hash_map<std::string, connector_state> _states;
    producer _state_topic_producer;
    event_listeners<connector_state> _checkpoint_listeners;
    
    static constexpr auto cdc_state_topic = "__redpanda_cdc_state";
    static constexpr auto max_checkpoint_age = 24h;
};

} // namespace redpanda::cdc
```

#### 5. CDC Management API

```cpp
namespace redpanda::cdc {

// REST API for CDC management
class cdc_admin_api {
public:
    struct create_connector_request {
        std::string name;
        std::string source_type;
        json::object config;
        std::optional<json::object> transforms;
        std::optional<std::string> filter;
    };
    
    struct connector_status {
        std::string name;
        std::string state;  // running, paused, failed
        connector_metrics metrics;
        std::optional<std::string> error;
        std::chrono::system_clock::time_point last_activity;
    };
    
    struct connector_metrics {
        int64_t events_total;
        int64_t bytes_total;
        double events_per_second;
        double bytes_per_second;
        duration average_latency;
        std::map<std::string, int64_t> events_by_table;
    };
    
    // API endpoints
    ss::future<ss::httpd::json::json_return_type> 
    create_connector(std::unique_ptr<ss::httpd::request> req) {
        auto request = parse_json<create_connector_request>(req->content);
        
        // Validate configuration
        validate_connector_config(request);
        
        // Create connector instance
        auto connector = create_source_connector(request.source_type);
        
        // Configure connector
        cdc_source::source_config config;
        config.connection_string = request.config["connection_string"];
        config.tables = parse_tables(request.config["tables"]);
        config.mode = parse_capture_mode(request.config["mode"]);
        
        // Start connector
        co_await connector->start(config);
        
        // Register connector
        _connectors[request.name] = std::move(connector);
        
        // Create output topic
        co_await create_output_topic(request.name);
        
        // Start processing pipeline
        _pipelines[request.name] = start_pipeline(request.name);
        
        co_return json::object{
            {"status", "created"},
            {"name", request.name}
        };
    }
    
    ss::future<ss::httpd::json::json_return_type>
    list_connectors() {
        json::array connectors;
        
        for (const auto& [name, connector] : _connectors) {
            json::object conn;
            conn["name"] = name;
            conn["type"] = connector->type();
            conn["status"] = get_connector_status(name);
            connectors.push_back(conn);
        }
        
        co_return connectors;
    }
    
    ss::future<ss::httpd::json::json_return_type>
    get_connector(const std::string& name) {
        auto it = _connectors.find(name);
        if (it == _connectors.end()) {
            throw ss::httpd::not_found_exception(
                fmt::format("Connector {} not found", name)
            );
        }
        
        auto status = co_await get_detailed_status(name);
        
        json::object result;
        result["name"] = name;
        result["type"] = it->second->type();
        result["state"] = status.state;
        result["metrics"] = metrics_to_json(status.metrics);
        
        if (status.error) {
            result["error"] = *status.error;
        }
        
        co_return result;
    }
    
    ss::future<ss::httpd::json::json_return_type>
    delete_connector(const std::string& name) {
        auto it = _connectors.find(name);
        if (it == _connectors.end()) {
            throw ss::httpd::not_found_exception(
                fmt::format("Connector {} not found", name)
            );
        }
        
        // Stop pipeline
        if (auto pipeline_it = _pipelines.find(name); 
            pipeline_it != _pipelines.end()) {
            pipeline_it->second.request_abort();
        }
        
        // Stop connector
        co_await it->second->stop();
        
        // Remove from registry
        _connectors.erase(it);
        _pipelines.erase(name);
        
        co_return json::object{
            {"status", "deleted"},
            {"name", name}
        };
    }
    
    ss::future<ss::httpd::json::json_return_type>
    pause_connector(const std::string& name) {
        auto connector = get_connector_checked(name);
        co_await connector->pause();
        
        co_return json::object{
            {"status", "paused"},
            {"name", name}
        };
    }
    
    ss::future<ss::httpd::json::json_return_type>
    resume_connector(const std::string& name) {
        auto connector = get_connector_checked(name);
        co_await connector->resume();
        
        co_return json::object{
            {"status", "resumed"},
            {"name", name}
        };
    }
    
private:
    ss::future<> start_pipeline(const std::string& connector_name) {
        auto connector = _connectors[connector_name];
        auto stream = co_await connector->create_change_stream();
        
        while (!_as.abort_requested()) {
            auto event = co_await stream.read();
            if (!event) break;
            
            // Apply transforms
            if (_transform_engines.contains(connector_name)) {
                event = co_await _transform_engines[connector_name]
                    ->apply_transforms(*event);
                
                if (!event) continue;  // Filtered out
            }
            
            // Produce to topic
            co_await produce_event(connector_name, *event);
            
            // Update metrics
            update_metrics(connector_name, *event);
            
            // Periodic checkpointing
            if (should_checkpoint(connector_name)) {
                auto checkpoint = co_await connector->get_checkpoint();
                co_await _state_manager.checkpoint({
                    .connector_id = connector_name,
                    .source_position = checkpoint.position,
                    .events_processed = _metrics[connector_name].events_total
                });
            }
        }
    }
    
    ss::future<> produce_event(
        const std::string& connector_name,
        const change_event& event
    ) {
        // Determine topic
        std::string topic = fmt::format("{}_{}.{}",
            connector_name,
            event.database,
            event.table
        );
        
        // Serialize event
        iobuf key_buf = event.key ? 
            serialize_json(*event.key) : iobuf();
        iobuf value_buf = serialize_change_event(event);
        
        // Produce
        co_await _producer.produce(
            topic,
            std::move(key_buf),
            std::move(value_buf)
        );
    }
    
private:
    absl::flat_hash_map<std::string, std::unique_ptr<cdc_source>> _connectors;
    absl::flat_hash_map<std::string, ss::abort_source> _pipelines;
    absl::flat_hash_map<std::string, std::unique_ptr<cdc_transform_engine>> _transform_engines;
    absl::flat_hash_map<std::string, connector_metrics> _metrics;
    cdc_state_manager _state_manager;
    producer _producer;
    ss::abort_source _as;
};

} // namespace redpanda::cdc
```

### Testing Strategy

#### Unit Tests

```cpp
BOOST_AUTO_TEST_CASE(test_postgres_cdc_snapshot) {
    // Start PostgreSQL container
    auto pg = start_postgres_container();
    
    // Create test table
    pg.execute("CREATE TABLE test_table (id INT PRIMARY KEY, value TEXT)");
    pg.execute("INSERT INTO test_table VALUES (1, 'foo'), (2, 'bar')");
    
    // Create CDC source
    postgres_cdc_source source;
    source.start({
        .connection_string = pg.connection_string(),
        .tables = {"test_table"},
        .mode = capture_mode::snapshot_and_incremental
    }).get();
    
    // Read snapshot events
    auto stream = source.create_change_stream().get();
    
    std::vector<change_event> events;
    while (auto event = stream.read().get()) {
        if (!event) break;
        events.push_back(*event);
    }
    
    BOOST_REQUIRE_EQUAL(events.size(), 2);
    BOOST_CHECK_EQUAL(events[0].op, change_event::operation::insert);
    BOOST_CHECK_EQUAL(events[0].after["id"], 1);
    BOOST_CHECK_EQUAL(events[0].after["value"], "foo");
}

BOOST_AUTO_TEST_CASE(test_mysql_binlog_replication) {
    // Start MySQL container with binlog enabled
    auto mysql = start_mysql_container({
        .binlog_format = "ROW",
        .binlog_row_image = "FULL"
    });
    
    // Create test table
    mysql.execute("CREATE TABLE test (id INT PRIMARY KEY, data VARCHAR(100))");
    
    // Start CDC
    mysql_cdc_source source;
    source.start({
        .connection_string = mysql.connection_string(),
        .tables = {"test"},
        .mode = capture_mode::incremental
    }).get();
    
    auto stream = source.create_change_stream().get();
    
    // Insert data
    mysql.execute("INSERT INTO test VALUES (1, 'test')");
    
    // Read change event
    auto event = stream.read().get();
    BOOST_REQUIRE(event);
    BOOST_CHECK_EQUAL(event->op, change_event::operation::insert);
    BOOST_CHECK_EQUAL(event->after["id"], 1);
}
```

#### Integration Tests

```cpp
class cdc_integration_test {
    ss::future<> test_end_to_end_cdc() {
        // Start Redpanda
        auto redpanda = co_await start_redpanda();
        
        // Start PostgreSQL with sample data
        auto pg = co_await start_postgres_with_sample_data();
        
        // Create CDC connector via API
        auto response = co_await http_client.post("/v1/cdc/connectors", {
            {"name", "pg-connector"},
            {"source_type", "postgresql"},
            {"config", {
                {"connection_string", pg.connection_string()},
                {"tables", {"orders", "customers"}},
                {"mode", "snapshot_and_incremental"}
            }}
        });
        
        BOOST_REQUIRE_EQUAL(response.status, 201);
        
        // Verify snapshot completed
        co_await wait_for_snapshot_completion("pg-connector");
        
        // Make changes in PostgreSQL
        co_await pg.execute("INSERT INTO orders VALUES (100, 1, 99.99)");
        co_await pg.execute("UPDATE customers SET name = 'Updated' WHERE id = 1");
        co_await pg.execute("DELETE FROM orders WHERE id = 1");
        
        // Consume CDC events from Redpanda
        kafka_consumer consumer;
        consumer.subscribe({"pg-connector_test.orders", "pg-connector_test.customers"});
        
        std::vector<change_event> events;
        while (events.size() < 3) {
            auto records = co_await consumer.poll(1s);
            for (const auto& record : records) {
                events.push_back(deserialize_change_event(record.value));
            }
        }
        
        // Verify events
        BOOST_CHECK_EQUAL(events[0].op, change_event::operation::insert);
        BOOST_CHECK_EQUAL(events[1].op, change_event::operation::update);
        BOOST_CHECK_EQUAL(events[2].op, change_event::operation::delete_);
    }
};
```

## Performance Considerations

### Optimization Strategies

1. **Batch Processing**: Batch multiple change events before producing
2. **Compression**: Use compression for CDC topics
3. **Parallel Processing**: Process multiple tables in parallel
4. **Connection Pooling**: Reuse database connections
5. **Caching**: Cache schema metadata and table mappings

### Benchmarks

```cpp
PERF_TEST(cdc_throughput) {
    // Measure CDC throughput
    auto events_per_second = measure_cdc_throughput({
        .source_type = "postgresql",
        .tables = 10,
        .events_per_table = 10000,
        .event_size = 1_KiB
    });
    
    BOOST_REQUIRE_GT(events_per_second, 50000);  // > 50K events/sec
}

PERF_TEST(cdc_latency) {
    // Measure end-to-end latency
    auto latency = measure_cdc_latency({
        .source_type = "mysql",
        .measure_from = "database_commit",
        .measure_to = "topic_produce"
    });
    
    BOOST_REQUIRE_LT(latency.p99, 100ms);  // < 100ms p99 latency
}
```

## Configuration

```yaml
# CDC configuration
cdc_enabled: true
cdc_max_connectors: 100
cdc_snapshot_fetch_size: 10000
cdc_snapshot_parallel_tables: 4
cdc_binlog_buffer_size_mb: 64
cdc_checkpoint_interval_ms: 60000
cdc_state_topic_replication_factor: 3

# PostgreSQL specific
cdc_postgres_plugin: "pgoutput"
cdc_postgres_publication_autocreate: true
cdc_postgres_slot_drop_on_stop: false

# MySQL specific  
cdc_mysql_server_id: 123456
cdc_mysql_gtid_mode: true
cdc_mysql_include_schema_changes: true
```

## Migration Strategy

### Phase 1: Core Framework (Months 1-4)
- Implement CDC framework
- PostgreSQL connector
- Basic transforms

### Phase 2: Additional Connectors (Months 4-8)
- MySQL connector
- MongoDB connector
- Oracle connector

### Phase 3: Advanced Features (Months 8-10)
- Schema evolution
- Complex transforms
- Performance optimizations

## Open Questions

1. Should we support custom connector plugins?
2. How to handle very large initial snapshots?
3. Which wire protocols to prioritize?
4. How to handle schema conflicts?
5. Should we provide Debezium compatibility mode?

## References

- [PostgreSQL Logical Replication Protocol](https://www.postgresql.org/docs/current/protocol-replication.html)
- [MySQL Binlog Format](https://dev.mysql.com/doc/internals/en/binary-log.html)
- [Debezium Architecture](https://debezium.io/documentation/reference/architecture.html)
- [MongoDB Change Streams](https://docs.mongodb.com/manual/changeStreams/)