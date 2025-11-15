// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cdc/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

#include <memory>

namespace redpanda::cdc {

/// Base CDC source connector interface
/// Implementations capture changes from different database systems
class cdc_source {
public:
    virtual ~cdc_source() = default;

    /// Start the CDC source with the given configuration
    virtual ss::future<> start(source_config config) = 0;

    /// Stop the CDC source
    virtual ss::future<> stop() = 0;

    /// Create a stream of change events
    virtual ss::future<ss::input_stream<change_event>>
    create_change_stream() = 0;

    /// Get current checkpoint for state management
    virtual ss::future<checkpoint> get_checkpoint() = 0;

    /// Restore from a previous checkpoint
    virtual ss::future<> restore_from_checkpoint(checkpoint cp) = 0;

    /// Discover schema for a table
    virtual ss::future<schema_metadata>
    discover_schema(const ss::sstring& table) = 0;

    /// Check health status
    virtual ss::future<health_status> check_health() = 0;

    /// Pause the CDC source
    virtual ss::future<> pause() = 0;

    /// Resume the CDC source
    virtual ss::future<> resume() = 0;

    /// Get the source type name
    virtual ss::sstring type() const = 0;
};

/// Factory for creating CDC source instances
class cdc_source_factory {
public:
    static std::unique_ptr<cdc_source> create(const ss::sstring& type);
};

} // namespace redpanda::cdc
