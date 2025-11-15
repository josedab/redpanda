// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/autonomous/autonomous_controller.h"

#include "cluster/health_monitor_frontend.h"
#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"

#include <seastar/core/sleep.hh>

namespace cluster::autonomous {

autonomous_controller::autonomous_controller(
    controller_config config,
    ss::sharded<cluster::partition_manager>& pm,
    ss::sharded<cluster::metadata_cache>& mc,
    ss::sharded<cluster::health_monitor_frontend>& hm)
    : _config(std::move(config))
    , _partition_manager(pm)
    , _metadata_cache(mc)
    , _health_monitor(hm) {}

ss::future<> autonomous_controller::start() {
    vlog(clusterlog.info, "Starting autonomous operations controller");

    // Initialize components based on config
    if (_config.enable_rebalancing) {
        _rebalancer = std::make_unique<autonomous_rebalancer>(
            autonomous_rebalancer::rebalance_config{},
            _partition_manager,
            _metadata_cache,
            _health_monitor
        );
        co_await _rebalancer->start();
    }

    if (_config.enable_scaling) {
        _scaler = std::make_unique<predictive_scaler>(
            predictive_scaler::scaling_config{}
        );
        co_await _scaler->start();
    }

    if (_config.enable_anomaly_detection) {
        _anomaly_detector = std::make_unique<anomaly_detector>(
            anomaly_detector::detection_config{}
        );
        co_await _anomaly_detector->start();
    }

    if (_config.enable_self_healing) {
        _healer = std::make_unique<self_healer>(
            self_healer::healing_config{},
            _partition_manager,
            _metadata_cache
        );
        co_await _healer->start();
    }

    if (_config.enable_auto_tuning) {
        _tuner = std::make_unique<configuration_tuner>(
            configuration_tuner::tuning_config{}
        );
        co_await _tuner->start();
    }

    // Start coordination loop
    _coordination_loop = coordinate_operations();

    vlog(clusterlog.info, "Autonomous operations controller started");
}

ss::future<> autonomous_controller::stop() {
    vlog(clusterlog.info, "Stopping autonomous operations controller");

    _as.request_abort();
    co_await _gate.close();

    // Stop coordination loop
    co_await std::move(_coordination_loop);

    // Stop all components
    if (_rebalancer) {
        co_await _rebalancer->stop();
    }
    if (_scaler) {
        co_await _scaler->stop();
    }
    if (_anomaly_detector) {
        co_await _anomaly_detector->stop();
    }
    if (_healer) {
        co_await _healer->stop();
    }
    if (_tuner) {
        co_await _tuner->stop();
    }

    vlog(clusterlog.info, "Autonomous operations controller stopped");
}

ss::future<> autonomous_controller::coordinate_operations() {
    auto gate_holder = _gate.hold();

    while (!_as.abort_requested()) {
        try {
            // Detect anomalies
            if (_anomaly_detector && _healer) {
                auto anomalies = co_await _anomaly_detector->detect_anomalies();

                // Heal detected issues
                for (const auto& anomaly : anomalies) {
                    co_await _healer->heal_anomaly(anomaly);
                }
            }

            // Check scaling needs
            if (_scaler) {
                // Scaling decisions are handled internally by the scaler
                // This is just a coordination point for future integration
            }

            // Rebalancing is handled internally by the rebalancer
            // This is just a coordination point for future integration

        } catch (const ss::sleep_aborted&) {
            break;
        } catch (const std::exception& e) {
            vlog(clusterlog.warn,
                 "Autonomous controller coordination error: {}", e.what());
        }

        co_await ss::sleep_abortable(_config.coordination_interval, _as);
    }
}

} // namespace cluster::autonomous
