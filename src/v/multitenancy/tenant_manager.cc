// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "multitenancy/tenant_manager.h"

#include <stdexcept>

namespace redpanda::multitenancy {

tenant_manager::tenant_manager(const encryption_config& enc_config)
  : _encryption_manager(enc_config) {}

seastar::future<tenant_ptr>
tenant_manager::create_tenant(tenant_id id, resource_limits limits) {
    auto units = co_await _mutex.lock();

    // Check if tenant already exists
    if (_tenants.contains(id)) {
        throw std::runtime_error(
          "Tenant already exists: " + id.id);
    }

    // Initialize encryption for tenant
    co_await _encryption_manager.initialize_tenant(id);

    // Create tenant
    auto tenant_obj = std::make_shared<tenant>(id, limits);
    _tenants[id] = tenant_obj;

    co_return tenant_obj;
}

seastar::future<std::optional<tenant_ptr>>
tenant_manager::get_tenant(const tenant_id& id) {
    auto units = co_await _mutex.lock_shared();

    auto it = _tenants.find(id);
    if (it != _tenants.end()) {
        co_return it->second;
    }

    co_return std::nullopt;
}

seastar::future<> tenant_manager::update_tenant_limits(
  const tenant_id& id, resource_limits limits) {
    auto units = co_await _mutex.lock();

    auto it = _tenants.find(id);
    if (it == _tenants.end()) {
        throw std::runtime_error("Tenant not found: " + id.id);
    }

    co_await it->second->update_limits(limits);
}

seastar::future<> tenant_manager::delete_tenant(const tenant_id& id) {
    auto units = co_await _mutex.lock();

    auto it = _tenants.find(id);
    if (it == _tenants.end()) {
        throw std::runtime_error("Tenant not found: " + id.id);
    }

    // Remove encryption keys
    co_await _encryption_manager.remove_tenant(id);

    // Remove tenant
    _tenants.erase(it);
}

std::vector<tenant_id> tenant_manager::list_tenants() const {
    std::vector<tenant_id> result;
    result.reserve(_tenants.size());

    for (const auto& [tid, _] : _tenants) {
        result.push_back(tid);
    }

    return result;
}

} // namespace redpanda::multitenancy
