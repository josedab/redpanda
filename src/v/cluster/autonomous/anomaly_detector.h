// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cluster/fwd.h"
#include "model/fundamental.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <chrono>
#include <string>
#include <vector>

namespace cluster::autonomous {

/**
 * Multi-dimensional anomaly detection system for identifying cluster issues.
 */
class anomaly_detector {
public:
    enum class severity {
        low,
        medium,
        high,
        critical
    };

    struct anomaly {
        enum class type {
            performance_degradation,
            resource_exhaustion,
            network_partition,
            data_corruption,
            security_breach,
            configuration_drift,
            hardware_failure
        };

        type anomaly_type;
        severity severity_level;
        std::string description;
        std::vector<model::node_id> affected_nodes;
        std::chrono::system_clock::time_point detected_at;
        double confidence_score;
    };

    struct detection_config {
        std::chrono::hours detection_window{1};
        double z_score_threshold = 3.0;
        double prediction_threshold = 0.2;
        std::chrono::seconds check_interval{30};
    };

    explicit anomaly_detector(detection_config config);

    ss::future<> start();
    ss::future<> stop();

    ss::future<std::vector<anomaly>> detect_anomalies();

private:
    ss::future<> detection_loop();
    ss::future<std::vector<anomaly>> detect_statistical_anomalies();
    ss::future<std::vector<anomaly>> detect_pattern_anomalies();
    ss::future<std::vector<anomaly>> detect_correlation_anomalies();

    std::vector<anomaly> correlate_anomalies(const std::vector<anomaly>& anomalies);
    severity calculate_severity(double z_score);
    double calculate_confidence(double z_score);

    detection_config _config;
    ss::abort_source _as;
    ss::gate _gate;
    ss::future<> _detection_fiber;
};

} // namespace cluster::autonomous
