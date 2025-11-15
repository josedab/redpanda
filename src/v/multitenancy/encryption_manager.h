// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "multitenancy/types.h"

#include <seastar/core/future.hh>

#include <absl/container/flat_hash_map.h>

#include <array>
#include <vector>

namespace redpanda::multitenancy {

// Encryption key representation
class encryption_key {
public:
    static constexpr size_t key_size = 32; // 256 bits

    encryption_key();
    explicit encryption_key(const std::array<uint8_t, key_size>& data);

    const uint8_t* data() const { return _data.data(); }
    size_t size() const { return _data.size(); }

private:
    std::array<uint8_t, key_size> _data;
};

// Per-tenant encryption and key management
class tenant_encryption_manager {
public:
    explicit tenant_encryption_manager(const encryption_config& config);

    // Initialize encryption for a tenant
    seastar::future<> initialize_tenant(const tenant_id& tid);

    // Get data encryption key for tenant
    seastar::future<encryption_key> get_data_key(const tenant_id& tid);

    // Rotate encryption keys for tenant
    seastar::future<> rotate_keys(const tenant_id& tid);

    // Remove tenant encryption
    seastar::future<> remove_tenant(const tenant_id& tid);

private:
    encryption_key generate_master_key();
    encryption_key derive_data_key(
      const encryption_key& master, const tenant_id& tid);

    encryption_config _config;
    absl::flat_hash_map<tenant_id, encryption_key> _key_cache;
};

} // namespace redpanda::multitenancy
