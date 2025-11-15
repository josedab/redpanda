// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "sql/materialized_view/view_manager.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

namespace redpanda::sql {

// Memory storage implementation
ss::future<> memory_view_storage::write(const row_batch& batch) {
    _data.push_back(batch);
    _row_count += batch.row_count;
    co_return;
}

ss::future<query_result> memory_view_storage::query(const query_request& req) {
    query_result result;

    for (const auto& batch : _data) {
        // TODO: Apply filters and projections
        result.add_batch(batch);

        if (req.limit && result.total_rows >= *req.limit) {
            break;
        }
    }

    co_return result;
}

ss::future<> memory_view_storage::compact() {
    // No-op for memory storage
    co_return;
}

ss::future<> memory_view_storage::clear() {
    _data.clear();
    _row_count = 0;
    co_return;
}

// Disk storage implementation
ss::future<> disk_view_storage::write(const row_batch& batch) {
    // TODO: Write batch to disk segment files
    co_return;
}

ss::future<query_result> disk_view_storage::query(const query_request& req) {
    query_result result;
    // TODO: Read from disk segments and apply filters
    co_return result;
}

ss::future<> disk_view_storage::compact() {
    // TODO: Compact disk segments
    co_return;
}

ss::future<> disk_view_storage::clear() {
    // TODO: Delete all segment files
    _segment_files.clear();
    co_return;
}

// Materialized view implementation
ss::future<> materialized_view::create(const view_definition& def) {
    // Create view state
    _state = std::make_unique<view_state>();
    _state->definition = def;
    _state->last_refresh = model::timestamp::now();

    // TODO: Parse query and create logical plan

    // Create storage backend
    switch (def.storage.storage_backend) {
    case storage_options::backend::memory:
        _storage = std::make_unique<memory_view_storage>();
        break;
    case storage_options::backend::disk:
        _storage = std::make_unique<disk_view_storage>(def.storage);
        break;
    }

    // Start continuous refresh if needed
    if (def.policy == refresh_policy::continuous) {
        _refresh_fiber = refresh_continuously();
    }

    co_return;
}

ss::future<> materialized_view::drop() {
    _as.request_abort();
    co_await std::move(_refresh_fiber);
    co_await _storage->clear();
}

ss::future<> materialized_view::refresh() {
    if (_state->is_refreshing) {
        co_return;
    }

    _state->is_refreshing = true;

    // TODO: Execute view query and write results to storage
    // For now, just update timestamp
    _state->last_refresh = model::timestamp::now();

    _state->is_refreshing = false;
    co_return;
}

ss::future<query_result> materialized_view::query(const query_request& req) {
    // Check if view needs refresh
    if (needs_refresh()) {
        co_await refresh();
    }

    // Query the storage
    co_return co_await _storage->query(req);
}

ss::future<> materialized_view::start_continuous_refresh() {
    if (!_refresh_fiber.available()) {
        co_return; // Already running
    }

    _refresh_fiber = refresh_continuously();
    co_return;
}

ss::future<> materialized_view::stop_continuous_refresh() {
    _as.request_abort();
    co_await std::move(_refresh_fiber);
    _as = ss::abort_source{}; // Reset abort source
}

bool materialized_view::needs_refresh() const {
    if (_state->definition.policy == refresh_policy::on_demand) {
        return false;
    }

    if (_state->definition.policy == refresh_policy::periodic) {
        auto elapsed = model::timestamp::now() - _state->last_refresh;
        return elapsed.value()
               >= _state->definition.refresh_interval.count();
    }

    return false;
}

ss::future<> materialized_view::refresh_continuously() {
    while (!_as.abort_requested()) {
        try {
            co_await refresh();
            co_await ss::sleep_abortable(
              _state->definition.refresh_interval, _as);
        } catch (const ss::sleep_aborted&) {
            break;
        }
    }
}

ss::future<> materialized_view::incremental_update() {
    // TODO: Implement incremental view maintenance
    co_return;
}

// View manager implementation
ss::future<> view_manager::create_view(const view_definition& def) {
    auto view = std::make_unique<materialized_view>();
    co_await view->create(def);
    _views[def.name] = std::move(view);
}

ss::future<> view_manager::drop_view(const ss::sstring& name) {
    auto it = _views.find(name);
    if (it == _views.end()) {
        co_return;
    }

    co_await it->second->drop();
    _views.erase(it);
}

ss::future<> view_manager::refresh_view(const ss::sstring& name) {
    auto it = _views.find(name);
    if (it == _views.end()) {
        co_return;
    }

    co_await it->second->refresh();
}

ss::future<> view_manager::refresh_all_views() {
    for (auto& [name, view] : _views) {
        co_await view->refresh();
    }
}

ss::future<query_result> view_manager::query_view(
  const ss::sstring& name,
  const query_request& req) {
    auto it = _views.find(name);
    if (it == _views.end()) {
        co_return query_result{};
    }

    co_return co_await it->second->query(req);
}

std::vector<ss::sstring> view_manager::list_views() const {
    std::vector<ss::sstring> names;
    names.reserve(_views.size());
    for (const auto& [name, _] : _views) {
        names.push_back(name);
    }
    return names;
}

ss::future<> view_manager::start() {
    _maintenance_fiber = maintenance_loop();
    co_return;
}

ss::future<> view_manager::stop() {
    _as.request_abort();
    co_await std::move(_maintenance_fiber);

    // Stop all views
    for (auto& [name, view] : _views) {
        co_await view->drop();
    }
    _views.clear();
}

ss::future<> view_manager::maintenance_loop() {
    while (!_as.abort_requested()) {
        try {
            // Refresh periodic views
            co_await refresh_all_views();

            // Sleep for a bit
            co_await ss::sleep_abortable(std::chrono::seconds(60), _as);
        } catch (const ss::sleep_aborted&) {
            break;
        }
    }
}

} // namespace redpanda::sql
