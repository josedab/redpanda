// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <chrono>
#include <string>

namespace redpanda::multitenancy {

// Core tenant identifier
struct tenant_id {
    std::string id;
    std::string organization;

    bool operator==(const tenant_id& other) const {
        return id == other.id && organization == other.organization;
    }

    bool operator!=(const tenant_id& other) const { return !(*this == other); }
};

// Resource limit structures
struct cpu_quota {
    std::chrono::microseconds quota_per_second{1000000};
    std::chrono::microseconds burst_quota{1100000};
    size_t max_concurrent_requests{100};
    double cpu_shares{1024.0}; // For proportional share scheduling
};

struct memory_quota {
    size_t soft_limit_bytes{1024 * 1024 * 1024};      // 1GB
    size_t hard_limit_bytes{2 * 1024 * 1024 * 1024};  // 2GB
    size_t cache_size_bytes{256 * 1024 * 1024};       // 256MB
    size_t buffer_pool_bytes{128 * 1024 * 1024};      // 128MB
};

struct disk_quota {
    size_t read_bytes_per_second{100 * 1024 * 1024};  // 100 MB/s
    size_t write_bytes_per_second{100 * 1024 * 1024}; // 100 MB/s
    size_t read_iops{1000};
    size_t write_iops{1000};
    size_t max_storage_bytes{100ULL * 1024 * 1024 * 1024}; // 100GB
};

struct network_quota {
    size_t ingress_bytes_per_second{100 * 1024 * 1024}; // 100 MB/s
    size_t egress_bytes_per_second{100 * 1024 * 1024};  // 100 MB/s
    size_t max_connections{1000};
    size_t max_partitions{1000};
};

// Combined resource limits for a tenant
struct resource_limits {
    cpu_quota cpu;
    memory_quota memory;
    disk_quota disk;
    network_quota network;
};

// Encryption configuration
enum class encryption_algorithm {
    aes_256_gcm,
    aes_256_cbc,
};

enum class key_derivation_function {
    pbkdf2,
    hkdf,
};

struct key_rotation_policy {
    std::chrono::days rotation_period{90};
    bool auto_rotate{true};
};

struct encryption_config {
    encryption_algorithm algorithm{encryption_algorithm::aes_256_gcm};
    key_derivation_function kdf{key_derivation_function::pbkdf2};
    size_t kdf_iterations{100000};
    bool encrypt_at_rest{true};
    bool encrypt_in_transit{true};
    key_rotation_policy rotation_policy;
};

// I/O direction
enum class io_direction {
    read,
    write,
};

// Execution context for resource accounting
struct execution_context {
    std::chrono::microseconds estimated_cpu_time{0};
    size_t estimated_memory_bytes{0};
    size_t estimated_io_bytes{0};
};

} // namespace redpanda::multitenancy

// Hash support for tenant_id
namespace std {
template<>
struct hash<redpanda::multitenancy::tenant_id> {
    size_t operator()(const redpanda::multitenancy::tenant_id& tid) const {
        return std::hash<std::string>{}(tid.id)
               ^ (std::hash<std::string>{}(tid.organization) << 1);
    }
};
} // namespace std
