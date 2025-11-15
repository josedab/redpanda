// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/autonomous/predictive_scaler.h"

#include "cluster/logger.h"

#include <seastar/core/sleep.hh>

namespace cluster::autonomous {

predictive_scaler::predictive_scaler(scaling_config config)
    : _config(std::move(config))
    , _last_scale_up(std::chrono::system_clock::time_point::min())
    , _last_scale_down(std::chrono::system_clock::time_point::min()) {}

ss::future<> predictive_scaler::start() {
    vlog(clusterlog.info, "Starting predictive scaler");
    if (_config.enable_auto_scaling) {
        _scaling_fiber = scaling_loop();
    }
    return ss::now();
}

ss::future<> predictive_scaler::stop() {
    vlog(clusterlog.info, "Stopping predictive scaler");
    _as.request_abort();
    co_await _gate.close();
    if (_config.enable_auto_scaling) {
        co_await std::move(_scaling_fiber);
    }
}

ss::future<> predictive_scaler::scaling_loop() {
    auto gate_holder = _gate.hold();

    while (!_as.abort_requested()) {
        try {
            // Collect current metrics
            auto current = co_await collect_current_metrics();

            // Predict future workload
            auto forecast = co_await predict_workload(_config.forecast_window);

            // Make scaling decision
            auto action = co_await make_scaling_decision(forecast, current);

            if (action.action != scaling_action::type::no_action) {
                vlog(clusterlog.info,
                     "Scaling action recommended: {} (reason: {})",
                     action.action == scaling_action::type::scale_up ? "scale_up" : "scale_down",
                     action.reason);
            }

        } catch (const ss::sleep_aborted&) {
            break;
        } catch (const std::exception& e) {
            vlog(clusterlog.warn, "Predictive scaler error: {}", e.what());
        }

        co_await ss::sleep_abortable(std::chrono::minutes(5), _as);
    }
}

ss::future<predictive_scaler::resource_forecast>
predictive_scaler::predict_workload(std::chrono::hours lookahead) {

    resource_forecast forecast;
    forecast.timestamp = std::chrono::system_clock::now() + lookahead;
    forecast.predicted_cpu = 0.5;  // Placeholder
    forecast.predicted_memory = 0.5;
    forecast.predicted_throughput = 1000.0;
    forecast.confidence = 0.8;
    forecast.time_to_threshold = std::chrono::seconds(3600);

    co_return forecast;
}

ss::future<predictive_scaler::scaling_action>
predictive_scaler::make_scaling_decision(
    const resource_forecast& forecast,
    const cluster_metrics& current) {

    scaling_action action;

    // Check if we're in cooldown
    if (in_cooldown()) {
        action.action = scaling_action::type::no_action;
        action.reason = "In cooldown period";
        co_return action;
    }

    // Predictive scale-up
    if (forecast.predicted_cpu > _config.scale_up_threshold ||
        forecast.predicted_memory > _config.scale_up_threshold) {

        action.action = scaling_action::type::scale_up;
        action.target_broker_count = calculate_required_brokers(forecast);
        action.reason = "Predicted resource exhaustion";
        action.confidence = forecast.confidence;
        action.execute_at = std::chrono::system_clock::now() +
            std::chrono::minutes(5);

    }
    // Reactive scale-up
    else if (current.avg_cpu > _config.scale_up_threshold ||
             current.avg_memory > _config.scale_up_threshold) {

        action.action = scaling_action::type::scale_up;
        action.target_broker_count = current.broker_count + 1;
        action.reason = "Current resource utilization high";
        action.confidence = 1.0;
        action.execute_at = std::chrono::system_clock::now();

    }
    // Scale-down
    else if (current.avg_cpu < _config.scale_down_threshold &&
             current.avg_memory < _config.scale_down_threshold &&
             forecast.predicted_cpu < _config.scale_down_threshold) {

        action.action = scaling_action::type::scale_down;
        action.target_broker_count = std::max(
            _config.min_brokers,
            current.broker_count - 1
        );
        action.reason = "Resource utilization consistently low";
        action.confidence = forecast.confidence;
        action.execute_at = std::chrono::system_clock::now() +
            std::chrono::minutes(10);

    } else {
        action.action = scaling_action::type::no_action;
        action.reason = "No scaling needed";
    }

    co_return action;
}

ss::future<predictive_scaler::cluster_metrics>
predictive_scaler::collect_current_metrics() {

    cluster_metrics metrics;
    metrics.broker_count = 3;  // Placeholder
    metrics.avg_cpu = 0.5;
    metrics.avg_memory = 0.5;
    metrics.avg_disk = 0.5;
    metrics.total_throughput = 1000.0;

    co_return metrics;
}

bool predictive_scaler::in_cooldown() const {
    auto now = std::chrono::system_clock::now();
    auto since_last_up = now - _last_scale_up;
    auto since_last_down = now - _last_scale_down;

    return since_last_up < _config.scale_up_cooldown ||
           since_last_down < _config.scale_down_cooldown;
}

size_t predictive_scaler::calculate_required_brokers(
    const resource_forecast& forecast) {

    // Simple calculation - assumes 70% target utilization
    double target_utilization = 0.7;
    size_t required = static_cast<size_t>(
        std::ceil(forecast.predicted_cpu / target_utilization)
    );

    return std::clamp(required, _config.min_brokers, _config.max_brokers);
}

} // namespace cluster::autonomous
