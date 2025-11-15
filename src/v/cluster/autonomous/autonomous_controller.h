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
#include "cluster/autonomous/autonomous_rebalancer.h"
#include "cluster/autonomous/configuration_tuner.h"
#include "cluster/autonomous/predictive_scaler.h"
#include "cluster/autonomous/self_healer.h"
#include "cluster/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <memory>

namespace cluster::autonomous {

/**
 * Main autonomous operations controller that orchestrates all autonomous
 * systems for self-managing cluster operations.
 */
class autonomous_controller {
public:
    struct controller_config {
        bool enable_rebalancing = true;
        bool enable_scaling = false;  // Disabled by default for safety
        bool enable_anomaly_detection = true;
        bool enable_self_healing = true;
        bool enable_auto_tuning = true;
        std::chrono::seconds coordination_interval{30};
    };

    explicit autonomous_controller(
        controller_config config,
        ss::sharded<cluster::partition_manager>& pm,
        ss::sharded<cluster::metadata_cache>& mc,
        ss::sharded<cluster::health_monitor_frontend>& hm);

    ss::future<> start();
    ss::future<> stop();

private:
    ss::future<> coordinate_operations();

    controller_config _config;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    ss::sharded<cluster::health_monitor_frontend>& _health_monitor;

    std::unique_ptr<autonomous_rebalancer> _rebalancer;
    std::unique_ptr<predictive_scaler> _scaler;
    std::unique_ptr<anomaly_detector> _anomaly_detector;
    std::unique_ptr<self_healer> _healer;
    std::unique_ptr<configuration_tuner> _tuner;

    ss::abort_source _as;
    ss::gate _gate;
    ss::future<> _coordination_loop;
};

} // namespace cluster::autonomous
