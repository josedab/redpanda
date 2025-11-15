// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/autonomous/anomaly_detector.h"

#include "cluster/logger.h"

#include <seastar/core/sleep.hh>

#include <cmath>

namespace cluster::autonomous {

anomaly_detector::anomaly_detector(detection_config config)
    : _config(std::move(config)) {}

ss::future<> anomaly_detector::start() {
    vlog(clusterlog.info, "Starting anomaly detector");
    _detection_fiber = detection_loop();
    return ss::now();
}

ss::future<> anomaly_detector::stop() {
    vlog(clusterlog.info, "Stopping anomaly detector");
    _as.request_abort();
    co_await _gate.close();
    co_await std::move(_detection_fiber);
}

ss::future<> anomaly_detector::detection_loop() {
    auto gate_holder = _gate.hold();

    while (!_as.abort_requested()) {
        try {
            auto anomalies = co_await detect_anomalies();

            if (!anomalies.empty()) {
                vlog(clusterlog.warn,
                     "Detected {} anomalies", anomalies.size());

                for (const auto& anomaly : anomalies) {
                    vlog(clusterlog.warn,
                         "Anomaly: {} (confidence: {:.2f})",
                         anomaly.description, anomaly.confidence_score);
                }
            }

        } catch (const ss::sleep_aborted&) {
            break;
        } catch (const std::exception& e) {
            vlog(clusterlog.warn, "Anomaly detector error: {}", e.what());
        }

        co_await ss::sleep_abortable(_config.check_interval, _as);
    }
}

ss::future<std::vector<anomaly_detector::anomaly>>
anomaly_detector::detect_anomalies() {

    std::vector<anomaly> anomalies;

    // Run multiple detection algorithms
    auto statistical = co_await detect_statistical_anomalies();
    auto pattern = co_await detect_pattern_anomalies();
    auto correlation = co_await detect_correlation_anomalies();

    // Merge results
    anomalies.insert(anomalies.end(), statistical.begin(), statistical.end());
    anomalies.insert(anomalies.end(), pattern.begin(), pattern.end());
    anomalies.insert(anomalies.end(), correlation.begin(), correlation.end());

    // Correlate and deduplicate
    anomalies = correlate_anomalies(anomalies);

    co_return anomalies;
}

ss::future<std::vector<anomaly_detector::anomaly>>
anomaly_detector::detect_statistical_anomalies() {

    std::vector<anomaly> anomalies;

    // Placeholder implementation
    // In a real implementation, this would:
    // 1. Collect recent metrics
    // 2. Calculate statistical measures (mean, stddev)
    // 3. Check for outliers using z-score

    co_return anomalies;
}

ss::future<std::vector<anomaly_detector::anomaly>>
anomaly_detector::detect_pattern_anomalies() {

    std::vector<anomaly> anomalies;

    // Placeholder implementation
    // In a real implementation, this would:
    // 1. Define expected patterns (cyclic, monotonic growth, etc.)
    // 2. Compare current behavior against patterns
    // 3. Flag deviations

    co_return anomalies;
}

ss::future<std::vector<anomaly_detector::anomaly>>
anomaly_detector::detect_correlation_anomalies() {

    std::vector<anomaly> anomalies;

    // Placeholder implementation
    // In a real implementation, this would:
    // 1. Define expected correlations between metrics
    // 2. Calculate actual correlations
    // 3. Flag unexpected correlations

    co_return anomalies;
}

std::vector<anomaly_detector::anomaly>
anomaly_detector::correlate_anomalies(
    const std::vector<anomaly>& anomalies) {

    // Simple deduplication for now
    // In a real implementation, this would correlate related anomalies
    // and reduce false positives

    return anomalies;
}

anomaly_detector::severity
anomaly_detector::calculate_severity(double z_score) {

    double abs_score = std::abs(z_score);

    if (abs_score > 5.0) {
        return severity::critical;
    } else if (abs_score > 4.0) {
        return severity::high;
    } else if (abs_score > 3.0) {
        return severity::medium;
    } else {
        return severity::low;
    }
}

double anomaly_detector::calculate_confidence(double z_score) {
    // Simple confidence calculation based on z-score
    double abs_score = std::abs(z_score);
    return std::min(1.0, abs_score / 5.0);
}

} // namespace cluster::autonomous
