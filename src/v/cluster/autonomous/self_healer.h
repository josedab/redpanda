// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cluster/autonomous/anomaly_detector.h"
#include "cluster/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <chrono>
#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace cluster::autonomous {

/**
 * Self-healing system that automatically remediates detected issues.
 */
class self_healer {
public:
    struct healing_action {
        std::string name;
        std::function<ss::future<bool>()> execute;
        std::function<bool(const anomaly_detector::anomaly&)> can_handle;
        std::chrono::minutes cooldown_period{5};
        std::chrono::system_clock::time_point last_executed;
    };

    struct healing_config {
        bool enable_auto_healing = true;
        std::chrono::minutes default_cooldown{5};
        size_t max_concurrent_healings = 3;
    };

    explicit self_healer(
        healing_config config,
        ss::sharded<cluster::partition_manager>& pm,
        ss::sharded<cluster::metadata_cache>& mc);

    ss::future<> start();
    ss::future<> stop();

    ss::future<> heal_anomaly(const anomaly_detector::anomaly& anomaly);

private:
    void register_healing_actions();
    std::optional<healing_action*> select_healing_action(
        const anomaly_detector::anomaly& anomaly);
    bool in_cooldown(const healing_action& action) const;
    ss::future<> verify_healing(const anomaly_detector::anomaly& anomaly);
    ss::future<> escalate_issue(const anomaly_detector::anomaly& anomaly);

    // Specific healing actions
    ss::future<bool> restart_slow_partitions();
    ss::future<bool> trigger_emergency_compaction();
    ss::future<bool> reconnect_partitioned_brokers();
    ss::future<bool> repair_corrupted_segments();

    healing_config _config;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    std::vector<healing_action> _healing_actions;
    ss::abort_source _as;
    ss::gate _gate;
};

} // namespace cluster::autonomous
