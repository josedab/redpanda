// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "multitenancy/config.h"

#include <stdexcept>

namespace redpanda::multitenancy {

void multitenancy_config::validate() const {
    if (default_cpu_quota_us <= 0) {
        throw std::invalid_argument("default_cpu_quota_us must be positive");
    }

    if (default_memory_limit_mb <= 0) {
        throw std::invalid_argument("default_memory_limit_mb must be positive");
    }

    if (default_disk_quota_mb <= 0) {
        throw std::invalid_argument("default_disk_quota_mb must be positive");
    }

    if (default_network_bandwidth_mbps <= 0) {
        throw std::invalid_argument(
          "default_network_bandwidth_mbps must be positive");
    }

    if (encryption_algorithm != "aes-256-gcm"
        && encryption_algorithm != "aes-256-cbc") {
        throw std::invalid_argument(
          "encryption_algorithm must be aes-256-gcm or aes-256-cbc");
    }

    if (key_rotation_days <= 0) {
        throw std::invalid_argument("key_rotation_days must be positive");
    }
}

} // namespace redpanda::multitenancy
