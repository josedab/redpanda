// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "multitenancy/encryption_manager.h"

#include <random>

namespace redpanda::multitenancy {

encryption_key::encryption_key() { _data.fill(0); }

encryption_key::encryption_key(const std::array<uint8_t, key_size>& data)
  : _data(data) {}

tenant_encryption_manager::tenant_encryption_manager(
  const encryption_config& config)
  : _config(config) {}

seastar::future<>
tenant_encryption_manager::initialize_tenant(const tenant_id& tid) {
    // Generate master key for tenant
    auto master_key = generate_master_key();

    // Derive data encryption key
    auto dek = derive_data_key(master_key, tid);

    // Cache DEK
    _key_cache[tid] = dek;

    co_return;
}

seastar::future<encryption_key>
tenant_encryption_manager::get_data_key(const tenant_id& tid) {
    // Check cache first
    if (auto it = _key_cache.find(tid); it != _key_cache.end()) {
        co_return it->second;
    }

    // If not found, initialize
    co_await initialize_tenant(tid);
    co_return _key_cache[tid];
}

seastar::future<>
tenant_encryption_manager::rotate_keys(const tenant_id& tid) {
    // Generate new master key
    auto new_master = generate_master_key();

    // Derive new data key
    auto new_dek = derive_data_key(new_master, tid);

    // Update cache
    _key_cache[tid] = new_dek;

    // TODO: Background re-encryption would happen here
    co_return;
}

seastar::future<>
tenant_encryption_manager::remove_tenant(const tenant_id& tid) {
    _key_cache.erase(tid);
    co_return;
}

encryption_key tenant_encryption_manager::generate_master_key() {
    // Generate random key
    std::array<uint8_t, encryption_key::key_size> key_data;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 255);

    for (auto& byte : key_data) {
        byte = static_cast<uint8_t>(dis(gen));
    }

    return encryption_key(key_data);
}

encryption_key tenant_encryption_manager::derive_data_key(
  const encryption_key& master, const tenant_id& tid) {
    // Simplified key derivation using hash of master key + tenant ID
    // In production, this would use HKDF or similar KDF
    std::array<uint8_t, encryption_key::key_size> derived;

    // Simple XOR-based derivation for demonstration
    for (size_t i = 0; i < encryption_key::key_size; ++i) {
        derived[i] = master.data()[i]
                     ^ static_cast<uint8_t>(
                       tid.id.empty() ? 0 : tid.id[i % tid.id.size()]);
    }

    return encryption_key(derived);
}

} // namespace redpanda::multitenancy
