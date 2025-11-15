// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/autonomous/configuration_tuner.h"

#include "cluster/logger.h"

#include <seastar/core/sleep.hh>

namespace cluster::autonomous {

configuration_tuner::configuration_tuner(tuning_config config)
    : _config(std::move(config)) {}

ss::future<> configuration_tuner::start() {
    vlog(clusterlog.info, "Starting configuration tuner");
    if (_config.enable_auto_tuning) {
        _tuning_fiber = tuning_loop();
    }
    return ss::now();
}

ss::future<> configuration_tuner::stop() {
    vlog(clusterlog.info, "Stopping configuration tuner");
    _as.request_abort();
    co_await _gate.close();
    if (_config.enable_auto_tuning) {
        co_await std::move(_tuning_fiber);
    }
}

ss::future<> configuration_tuner::tuning_loop() {
    auto gate_holder = _gate.hold();

    while (!_as.abort_requested()) {
        try {
            // Identify tuning opportunities
            auto opportunities = co_await identify_tuning_opportunities();

            if (!opportunities.empty()) {
                vlog(clusterlog.info,
                     "Found {} tuning opportunities", opportunities.size());

                // Run experiments for each opportunity
                for (const auto& opportunity : opportunities) {
                    co_await run_tuning_experiment(opportunity);
                }

                // Apply successful configurations
                co_await apply_improved_configurations();
            }

        } catch (const ss::sleep_aborted&) {
            break;
        } catch (const std::exception& e) {
            vlog(clusterlog.warn, "Auto-tuning error: {}", e.what());
        }

        co_await ss::sleep_abortable(_config.tuning_interval, _as);
    }
}

ss::future<std::vector<configuration_tuner::tuning_opportunity>>
configuration_tuner::identify_tuning_opportunities() {

    std::vector<tuning_opportunity> opportunities;

    // Collect current performance metrics
    auto metrics = co_await collect_performance_metrics();

    // Check batch size tuning
    if (metrics.avg_batch_size < 8192) {  // Less than 8KB
        opportunities.push_back({
            .parameter = {
                .name = "batch.max.bytes",
                .value = int64_t(16384),
                .min_value = int64_t(1024),
                .max_value = int64_t(10485760),
                .impact_score = 0.3
            },
            .current_performance = metrics.throughput,
            .suggested_value = int64_t(32768),
            .rationale = "Batch size below optimal"
        });
    }

    // Check compression settings
    if (metrics.compression_ratio < 0.5) {
        opportunities.push_back({
            .parameter = {
                .name = "compression.type",
                .value = std::string("none"),
                .min_value = std::string("none"),
                .max_value = std::string("zstd"),
                .impact_score = 0.4
            },
            .current_performance = metrics.throughput,
            .suggested_value = std::string("lz4"),
            .rationale = "High compression potential detected"
        });
    }

    co_return opportunities;
}

ss::future<> configuration_tuner::run_tuning_experiment(
    const tuning_opportunity& opportunity) {

    vlog(clusterlog.info,
         "Running tuning experiment for parameter: {} (reason: {})",
         opportunity.parameter.name, opportunity.rationale);

    // Placeholder implementation
    // In a real implementation, this would:
    // 1. Create A/B test groups
    // 2. Apply configuration to test group
    // 3. Collect and compare metrics
    // 4. Determine if change is beneficial

    co_return;
}

ss::future<> configuration_tuner::apply_improved_configurations() {
    vlog(clusterlog.info, "Applying improved configurations");

    // Placeholder implementation
    // In a real implementation, this would:
    // 1. Review experiment results
    // 2. Apply configurations that showed improvement
    // 3. Monitor for any negative effects

    co_return;
}

ss::future<configuration_tuner::performance_metrics>
configuration_tuner::collect_performance_metrics() {

    performance_metrics metrics;
    metrics.throughput = 1000.0;  // Placeholder
    metrics.latency_p99 = 10.0;
    metrics.avg_batch_size = 4096;
    metrics.compression_ratio = 0.7;
    metrics.segment_roll_frequency = 100.0;

    co_return metrics;
}

} // namespace cluster::autonomous
