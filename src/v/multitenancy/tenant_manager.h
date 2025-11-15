// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "multitenancy/encryption_manager.h"
#include "multitenancy/tenant.h"
#include "multitenancy/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_mutex.hh>

#include <absl/container/flat_hash_map.h>

#include <optional>

namespace redpanda::multitenancy {

// Manages all tenants in the system
class tenant_manager {
public:
    explicit tenant_manager(const encryption_config& enc_config);

    // Create a new tenant
    seastar::future<tenant_ptr> create_tenant(
      tenant_id id, resource_limits limits);

    // Get an existing tenant
    seastar::future<std::optional<tenant_ptr>> get_tenant(const tenant_id& id);

    // Update tenant limits
    seastar::future<> update_tenant_limits(
      const tenant_id& id, resource_limits limits);

    // Delete a tenant
    seastar::future<> delete_tenant(const tenant_id& id);

    // List all tenants
    std::vector<tenant_id> list_tenants() const;

    // Get encryption manager
    tenant_encryption_manager& encryption() { return _encryption_manager; }

private:
    tenant_encryption_manager _encryption_manager;
    absl::flat_hash_map<tenant_id, tenant_ptr> _tenants;
    mutable seastar::shared_mutex _mutex;
};

} // namespace redpanda::multitenancy
