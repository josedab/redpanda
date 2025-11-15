// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cdc/cdc_source.h"

#include <seastar/core/condition-variable.hh>

#include <deque>

namespace redpanda::cdc {

/// PostgreSQL CDC implementation using logical replication
class postgres_cdc_source : public cdc_source {
public:
    postgres_cdc_source() = default;
    ~postgres_cdc_source() override = default;

    ss::future<> start(source_config config) override;
    ss::future<> stop() override;

    ss::future<ss::input_stream<change_event>>
    create_change_stream() override;

    ss::future<checkpoint> get_checkpoint() override;
    ss::future<> restore_from_checkpoint(checkpoint cp) override;

    ss::future<schema_metadata>
    discover_schema(const ss::sstring& table) override;

    ss::future<health_status> check_health() override;
    ss::future<> pause() override;
    ss::future<> resume() override;

    ss::sstring type() const override { return "postgresql"; }

private:
    ss::future<> perform_initial_snapshot();
    ss::future<> snapshot_table(
        const ss::sstring& table,
        const ss::sstring& snapshot_id);
    ss::future<> start_logical_replication();
    ss::future<> process_replication_stream();

    source_config _config;
    std::deque<change_event> _change_buffer;
    ss::condition_variable _change_available;
    bool _streaming = false;
    bool _paused = false;
    checkpoint _current_checkpoint;
    int64_t _sequence_counter = 0;
};

} // namespace redpanda::cdc
