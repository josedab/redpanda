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
#include "json/json.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <vector>

namespace redpanda::cdc {

/// Transform type
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

/// Transform configuration
struct transform_config {
    ss::sstring name;
    transform_type type;
    json::Value parameters;
    std::optional<ss::sstring> filter_expression;
};

/// CDC-specific transformations
class cdc_transform_engine {
public:
    cdc_transform_engine() = default;

    /// Add a transform to the pipeline
    void add_transform(transform_config config);

    /// Apply all transforms to an event
    ss::future<std::optional<change_event>>
    apply_transforms(change_event event);

private:
    ss::future<change_event> apply_single_transform(
        const transform_config& config,
        change_event event);

    change_event flatten_event(
        change_event event,
        const json::Value& params);

    change_event rename_fields(
        change_event event,
        const json::Value& params);

    change_event add_metadata_fields(
        change_event event,
        const json::Value& params);

    change_event mask_sensitive_data(
        change_event event,
        const json::Value& params);

    change_event convert_to_debezium(change_event event);

    std::vector<transform_config> _transforms;
};

/// SQL-based filtering
class cdc_filter_engine {
public:
    explicit cdc_filter_engine(ss::sstring filter_expression)
        : _filter_expression(std::move(filter_expression)) {}

    /// Check if event matches filter
    ss::future<bool> matches(const change_event& event);

private:
    ss::sstring _filter_expression;
};

} // namespace redpanda::cdc
