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

// Multi-tenancy configuration
struct multitenancy_config {
    // Enable/disable multi-tenancy
    bool enabled{false};

    // Default CPU quota in microseconds per second
    int64_t default_cpu_quota_us{1000000};

    // Default memory limit in MB
    size_t default_memory_limit_mb{1024};

    // Default disk quota in MB
    size_t default_disk_quota_mb{10240};

    // Default network bandwidth in Mbps
    size_t default_network_bandwidth_mbps{100};

    // Encryption settings
    bool encryption_enabled{true};
    std::string encryption_algorithm{"aes-256-gcm"};
    int key_rotation_days{90};
    std::string kms_endpoint;

    // Resource control
    bool cgroup_enabled{true};
    bool ebpf_enabled{false};
    std::string io_scheduler{"mq-deadline"};

    // Validate configuration
    void validate() const;
};

} // namespace redpanda::multitenancy
