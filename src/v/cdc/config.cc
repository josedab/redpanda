// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cdc/config.h"

namespace redpanda::cdc {

cdc_config default_cdc_config() {
    cdc_config config;

    // Use default values defined in struct
    config.enabled = true;
    config.max_connectors = 100;
    config.snapshot_fetch_size = 10000;
    config.snapshot_parallel_tables = 4;
    config.binlog_buffer_size_mb = 64;
    config.checkpoint_interval_ms = std::chrono::milliseconds{60000};
    config.state_topic_replication_factor = 3;

    // PostgreSQL defaults
    config.postgres_plugin = "pgoutput";
    config.postgres_publication_autocreate = true;
    config.postgres_slot_drop_on_stop = false;

    // MySQL defaults
    config.mysql_server_id = 123456;
    config.mysql_gtid_mode = true;
    config.mysql_include_schema_changes = true;

    return config;
}

} // namespace redpanda::cdc
