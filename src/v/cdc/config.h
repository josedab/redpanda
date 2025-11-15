// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <seastar/core/sstring.hh>

#include <chrono>
#include <cstdint>

namespace redpanda::cdc {

/// CDC configuration
struct cdc_config {
    // General settings
    bool enabled = false;
    int32_t max_connectors = 100;

    // Snapshot settings
    int32_t snapshot_fetch_size = 10000;
    int32_t snapshot_parallel_tables = 4;

    // Performance settings
    int32_t binlog_buffer_size_mb = 64;
    std::chrono::milliseconds checkpoint_interval_ms{60000};

    // State topic settings
    int16_t state_topic_replication_factor = 3;

    // PostgreSQL specific
    ss::sstring postgres_plugin = "pgoutput";
    bool postgres_publication_autocreate = true;
    bool postgres_slot_drop_on_stop = false;

    // MySQL specific
    int32_t mysql_server_id = 123456;
    bool mysql_gtid_mode = true;
    bool mysql_include_schema_changes = true;

    // MongoDB specific (placeholder)
    bool mongodb_change_streams = true;

    // Oracle specific (placeholder)
    ss::sstring oracle_log_miner_strategy = "online_catalog";
};

/// Get default CDC configuration
cdc_config default_cdc_config();

} // namespace redpanda::cdc
