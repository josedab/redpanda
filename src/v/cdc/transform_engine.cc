// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cdc/transform_engine.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>

namespace redpanda::cdc {

static ss::logger logger{"cdc_transform"};

void cdc_transform_engine::add_transform(transform_config config) {
    vlog(logger.info, "Adding transform: name={}, type={}",
         config.name, static_cast<int>(config.type));
    _transforms.push_back(std::move(config));
}

ss::future<std::optional<change_event>>
cdc_transform_engine::apply_transforms(change_event event) {

    // Apply transformations in order
    for (const auto& transform : _transforms) {
        event = co_await apply_single_transform(transform, std::move(event));
    }

    co_return event;
}

ss::future<change_event> cdc_transform_engine::apply_single_transform(
    const transform_config& config,
    change_event event) {

    switch (config.type) {
    case transform_type::flatten:
        co_return flatten_event(std::move(event), config.parameters);

    case transform_type::rename_fields:
        co_return rename_fields(std::move(event), config.parameters);

    case transform_type::add_metadata:
        co_return add_metadata_fields(std::move(event), config.parameters);

    case transform_type::mask_sensitive:
        co_return mask_sensitive_data(std::move(event), config.parameters);

    case transform_type::debezium_format:
        co_return convert_to_debezium(std::move(event));

    default:
        vlog(logger.warn, "Unknown transform type: {}",
             static_cast<int>(config.type));
        co_return event;
    }
}

change_event cdc_transform_engine::flatten_event(
    change_event event,
    const json::Value& params) {

    // TODO: Implement flattening logic
    return event;
}

change_event cdc_transform_engine::rename_fields(
    change_event event,
    const json::Value& params) {

    // TODO: Implement field renaming
    return event;
}

change_event cdc_transform_engine::add_metadata_fields(
    change_event event,
    const json::Value& params) {

    // Add timestamp if not present
    event.metadata["captured_at"] = fmt::format(
        "{}", event.timestamp.time_since_epoch().count());

    // Add source info
    event.metadata["source_database"] = event.database;
    event.metadata["source_table"] = event.table;

    return event;
}

change_event cdc_transform_engine::mask_sensitive_data(
    change_event event,
    const json::Value& params) {

    // TODO: Implement data masking based on params
    return event;
}

change_event cdc_transform_engine::convert_to_debezium(change_event event) {
    // Create Debezium-compatible envelope
    json::Value debezium_payload;

    // Set operation
    ss::sstring op_str;
    switch (event.op) {
    case operation::insert:
        op_str = "c"; // Create
        break;
    case operation::update:
        op_str = "u"; // Update
        break;
    case operation::delete_:
        op_str = "d"; // Delete
        break;
    default:
        op_str = "r"; // Read (snapshot)
        break;
    }

    // TODO: Build complete Debezium envelope structure

    return event;
}

ss::future<bool> cdc_filter_engine::matches(const change_event& event) {
    // TODO: Implement SQL expression evaluation
    // For now, always return true (no filtering)
    co_return true;
}

} // namespace redpanda::cdc
