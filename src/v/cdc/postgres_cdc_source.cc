// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cdc/postgres_cdc_source.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>

namespace redpanda::cdc {

static ss::logger logger{"postgres_cdc"};

ss::future<> postgres_cdc_source::start(source_config config) {
    _config = std::move(config);
    _streaming = false;
    _paused = false;
    _sequence_counter = 0;

    vlog(logger.info, "Starting PostgreSQL CDC source for tables: {}",
         fmt::join(_config.tables, ", "));

    // TODO: Implement PostgreSQL connection and logical replication setup
    // This is a placeholder implementation

    if (_config.mode == capture_mode::snapshot_and_incremental ||
        _config.mode == capture_mode::snapshot) {
        co_await perform_initial_snapshot();
    }

    if (_config.mode == capture_mode::incremental ||
        _config.mode == capture_mode::snapshot_and_incremental) {
        co_await start_logical_replication();
    }

    _streaming = true;
    vlog(logger.info, "PostgreSQL CDC source started successfully");
}

ss::future<> postgres_cdc_source::stop() {
    vlog(logger.info, "Stopping PostgreSQL CDC source");
    _streaming = false;
    _change_available.broadcast();
    co_return;
}

ss::future<ss::input_stream<change_event>>
postgres_cdc_source::create_change_stream() {
    co_return ss::input_stream<change_event>(
        ss::data_source(std::make_unique<ss::memory_data_source>(
            reinterpret_cast<char*>(_change_buffer.data()),
            _change_buffer.size() * sizeof(change_event)
        ))
    );
}

ss::future<checkpoint> postgres_cdc_source::get_checkpoint() {
    co_return _current_checkpoint;
}

ss::future<> postgres_cdc_source::restore_from_checkpoint(checkpoint cp) {
    _current_checkpoint = std::move(cp);
    _sequence_counter = _current_checkpoint.sequence;
    vlog(logger.info, "Restored from checkpoint at sequence {}",
         _current_checkpoint.sequence);
    co_return;
}

ss::future<schema_metadata>
postgres_cdc_source::discover_schema(const ss::sstring& table) {
    // TODO: Implement schema discovery from PostgreSQL
    schema_metadata metadata;
    metadata.database = _config.properties["database"];
    metadata.schema = "public";
    metadata.table = table;
    co_return metadata;
}

ss::future<health_status> postgres_cdc_source::check_health() {
    health_status status;
    if (_streaming && !_paused) {
        status.status = health_status::state::healthy;
        status.message = "Streaming changes";
    } else if (_paused) {
        status.status = health_status::state::degraded;
        status.message = "Paused";
    } else {
        status.status = health_status::state::unhealthy;
        status.message = "Not streaming";
    }
    status.last_check = std::chrono::system_clock::now();
    co_return status;
}

ss::future<> postgres_cdc_source::pause() {
    vlog(logger.info, "Pausing PostgreSQL CDC source");
    _paused = true;
    co_return;
}

ss::future<> postgres_cdc_source::resume() {
    vlog(logger.info, "Resuming PostgreSQL CDC source");
    _paused = false;
    _change_available.broadcast();
    co_return;
}

ss::future<> postgres_cdc_source::perform_initial_snapshot() {
    vlog(logger.info, "Starting initial snapshot");

    // TODO: Implement actual PostgreSQL snapshot logic
    // This is a placeholder

    vlog(logger.info, "Initial snapshot completed");
    co_return;
}

ss::future<> postgres_cdc_source::snapshot_table(
    const ss::sstring& table,
    const ss::sstring& snapshot_id) {

    vlog(logger.info, "Snapshotting table {} with snapshot_id {}",
         table, snapshot_id);

    // TODO: Implement table snapshot logic

    co_return;
}

ss::future<> postgres_cdc_source::start_logical_replication() {
    vlog(logger.info, "Starting logical replication");

    // TODO: Implement PostgreSQL logical replication
    // - Create replication slot
    // - Start replication stream
    // - Process WAL records

    co_return;
}

ss::future<> postgres_cdc_source::process_replication_stream() {
    // TODO: Implement replication stream processing
    co_return;
}

} // namespace redpanda::cdc
