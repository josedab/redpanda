// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cdc/mysql_cdc_source.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>

namespace redpanda::cdc {

static ss::logger logger{"mysql_cdc"};

ss::future<> mysql_cdc_source::start(source_config config) {
    _config = std::move(config);
    _streaming = false;
    _paused = false;
    _sequence_counter = 0;

    vlog(logger.info, "Starting MySQL CDC source for tables: {}",
         fmt::join(_config.tables, ", "));

    // TODO: Implement MySQL connection and binlog setup

    if (_config.mode == capture_mode::snapshot_and_incremental ||
        _config.mode == capture_mode::snapshot) {
        co_await perform_initial_snapshot();
    }

    if (_config.mode == capture_mode::incremental ||
        _config.mode == capture_mode::snapshot_and_incremental) {
        co_await start_binlog_replication();
    }

    _streaming = true;
    vlog(logger.info, "MySQL CDC source started successfully");
}

ss::future<> mysql_cdc_source::stop() {
    vlog(logger.info, "Stopping MySQL CDC source");
    _streaming = false;
    _change_available.broadcast();
    co_return;
}

ss::future<ss::input_stream<change_event>>
mysql_cdc_source::create_change_stream() {
    co_return ss::input_stream<change_event>(
        ss::data_source(std::make_unique<ss::memory_data_source>(
            reinterpret_cast<char*>(_change_buffer.data()),
            _change_buffer.size() * sizeof(change_event)
        ))
    );
}

ss::future<checkpoint> mysql_cdc_source::get_checkpoint() {
    co_return _current_checkpoint;
}

ss::future<> mysql_cdc_source::restore_from_checkpoint(checkpoint cp) {
    _current_checkpoint = std::move(cp);
    _sequence_counter = _current_checkpoint.sequence;
    vlog(logger.info, "Restored from checkpoint at sequence {}",
         _current_checkpoint.sequence);
    co_return;
}

ss::future<schema_metadata>
mysql_cdc_source::discover_schema(const ss::sstring& table) {
    // TODO: Implement schema discovery from MySQL
    schema_metadata metadata;
    metadata.database = _config.properties["database"];
    metadata.schema = _config.properties["database"];
    metadata.table = table;
    co_return metadata;
}

ss::future<health_status> mysql_cdc_source::check_health() {
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

ss::future<> mysql_cdc_source::pause() {
    vlog(logger.info, "Pausing MySQL CDC source");
    _paused = true;
    co_return;
}

ss::future<> mysql_cdc_source::resume() {
    vlog(logger.info, "Resuming MySQL CDC source");
    _paused = false;
    _change_available.broadcast();
    co_return;
}

ss::future<> mysql_cdc_source::perform_initial_snapshot() {
    vlog(logger.info, "Starting initial snapshot");

    // TODO: Implement actual MySQL snapshot logic

    vlog(logger.info, "Initial snapshot completed");
    co_return;
}

ss::future<> mysql_cdc_source::start_binlog_replication() {
    vlog(logger.info, "Starting binlog replication");

    // TODO: Implement MySQL binlog replication
    // - Register as slave
    // - Start binlog dump
    // - Process binlog events

    co_return;
}

ss::future<> mysql_cdc_source::process_binlog_stream() {
    // TODO: Implement binlog stream processing
    co_return;
}

} // namespace redpanda::cdc
