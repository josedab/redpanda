// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/autonomous/self_healer.h"

#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"

#include <seastar/core/sleep.hh>

namespace cluster::autonomous {

self_healer::self_healer(
    healing_config config,
    ss::sharded<cluster::partition_manager>& pm,
    ss::sharded<cluster::metadata_cache>& mc)
    : _config(std::move(config))
    , _partition_manager(pm)
    , _metadata_cache(mc) {

    register_healing_actions();
}

ss::future<> self_healer::start() {
    vlog(clusterlog.info, "Starting self-healer");
    return ss::now();
}

ss::future<> self_healer::stop() {
    vlog(clusterlog.info, "Stopping self-healer");
    _as.request_abort();
    co_await _gate.close();
}

ss::future<> self_healer::heal_anomaly(
    const anomaly_detector::anomaly& anomaly) {

    auto gate_holder = _gate.hold();

    // Find appropriate healing action
    auto action = select_healing_action(anomaly);

    if (!action) {
        vlog(clusterlog.warn,
             "No healing action available for anomaly: {}",
             anomaly.description);
        co_return;
    }

    // Check cooldown
    if (in_cooldown(**action)) {
        vlog(clusterlog.info,
             "Healing action {} in cooldown period", (*action)->name);
        co_return;
    }

    // Execute healing
    vlog(clusterlog.info, "Executing healing action: {}", (*action)->name);

    try {
        bool success = co_await (*action)->execute();

        if (success) {
            vlog(clusterlog.info,
                 "Healing action {} completed successfully",
                 (*action)->name);
            (*action)->last_executed = std::chrono::system_clock::now();

            // Verify healing worked
            co_await verify_healing(anomaly);
        } else {
            vlog(clusterlog.error,
                 "Healing action {} failed", (*action)->name);

            // Escalate if healing failed
            co_await escalate_issue(anomaly);
        }

    } catch (const std::exception& e) {
        vlog(clusterlog.error,
             "Healing action {} threw exception: {}",
             (*action)->name, e.what());
        co_await escalate_issue(anomaly);
    }
}

void self_healer::register_healing_actions() {
    using anomaly_type = anomaly_detector::anomaly::type;

    // Performance degradation remediation
    _healing_actions.push_back({
        .name = "restart_slow_partition",
        .execute = [this]() { return restart_slow_partitions(); },
        .can_handle = [](const anomaly_detector::anomaly& a) {
            return a.anomaly_type == anomaly_type::performance_degradation;
        },
        .cooldown_period = std::chrono::minutes(5)
    });

    // Resource exhaustion remediation
    _healing_actions.push_back({
        .name = "trigger_compaction",
        .execute = [this]() { return trigger_emergency_compaction(); },
        .can_handle = [](const anomaly_detector::anomaly& a) {
            return a.anomaly_type == anomaly_type::resource_exhaustion &&
                   a.description.find("disk") != std::string::npos;
        },
        .cooldown_period = std::chrono::minutes(30)
    });

    // Network partition remediation
    _healing_actions.push_back({
        .name = "reconnect_broker",
        .execute = [this]() { return reconnect_partitioned_brokers(); },
        .can_handle = [](const anomaly_detector::anomaly& a) {
            return a.anomaly_type == anomaly_type::network_partition;
        },
        .cooldown_period = std::chrono::minutes(1)
    });

    // Data corruption remediation
    _healing_actions.push_back({
        .name = "repair_corrupted_segment",
        .execute = [this]() { return repair_corrupted_segments(); },
        .can_handle = [](const anomaly_detector::anomaly& a) {
            return a.anomaly_type == anomaly_type::data_corruption;
        },
        .cooldown_period = std::chrono::minutes(10)
    });
}

std::optional<self_healer::healing_action*>
self_healer::select_healing_action(
    const anomaly_detector::anomaly& anomaly) {

    for (auto& action : _healing_actions) {
        if (action.can_handle(anomaly)) {
            return &action;
        }
    }
    return std::nullopt;
}

bool self_healer::in_cooldown(const healing_action& action) const {
    auto now = std::chrono::system_clock::now();
    auto elapsed = now - action.last_executed;
    return elapsed < action.cooldown_period;
}

ss::future<> self_healer::verify_healing(
    const anomaly_detector::anomaly& anomaly) {

    // Wait a bit for the healing to take effect
    co_await ss::sleep(std::chrono::seconds(10));

    // Placeholder - would re-check metrics to verify healing
    vlog(clusterlog.info, "Verifying healing for anomaly: {}", anomaly.description);
}

ss::future<> self_healer::escalate_issue(
    const anomaly_detector::anomaly& anomaly) {

    // Log escalation
    vlog(clusterlog.error,
         "Escalating issue: {} (severity: {})",
         anomaly.description,
         static_cast<int>(anomaly.severity_level));

    // In a real implementation, this would:
    // 1. Send alerts to operators
    // 2. Create incident tickets
    // 3. Update monitoring dashboards
    co_return;
}

ss::future<bool> self_healer::restart_slow_partitions() {
    vlog(clusterlog.info, "Restarting slow partitions");
    // Placeholder implementation
    co_return true;
}

ss::future<bool> self_healer::trigger_emergency_compaction() {
    vlog(clusterlog.info, "Triggering emergency compaction");
    // Placeholder implementation
    co_return true;
}

ss::future<bool> self_healer::reconnect_partitioned_brokers() {
    vlog(clusterlog.info, "Reconnecting partitioned brokers");
    // Placeholder implementation
    co_return true;
}

ss::future<bool> self_healer::repair_corrupted_segments() {
    vlog(clusterlog.info, "Repairing corrupted segments");
    // Placeholder implementation
    co_return true;
}

} // namespace cluster::autonomous
