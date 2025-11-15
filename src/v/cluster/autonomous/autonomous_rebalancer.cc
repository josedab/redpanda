// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/autonomous/autonomous_rebalancer.h"

#include "cluster/health_monitor_frontend.h"
#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"

#include <seastar/core/sleep.hh>

#include <algorithm>
#include <cmath>

namespace cluster::autonomous {

autonomous_rebalancer::autonomous_rebalancer(
    rebalance_config config,
    ss::sharded<cluster::partition_manager>& pm,
    ss::sharded<cluster::metadata_cache>& mc,
    ss::sharded<cluster::health_monitor_frontend>& hm)
    : _config(std::move(config))
    , _partition_manager(pm)
    , _metadata_cache(mc)
    , _health_monitor(hm) {}

ss::future<> autonomous_rebalancer::start() {
    vlog(clusterlog.info, "Starting autonomous rebalancer");
    _monitoring_fiber = monitor_and_rebalance();
    return ss::now();
}

ss::future<> autonomous_rebalancer::stop() {
    vlog(clusterlog.info, "Stopping autonomous rebalancer");
    _as.request_abort();
    co_await _gate.close();
    co_await std::move(_monitoring_fiber);
}

ss::future<> autonomous_rebalancer::monitor_and_rebalance() {
    auto gate_holder = _gate.hold();

    while (!_as.abort_requested()) {
        try {
            // Collect current cluster state
            auto state = co_await collect_cluster_state();

            // Check if rebalancing is needed
            auto decision = analyze_state(state);

            if (decision.should_rebalance) {
                vlog(clusterlog.info,
                     "Rebalancing triggered: imbalance detected");

                // Create rebalance plan
                auto plan = co_await create_rebalance_plan(state, decision);

                // Validate plan safety
                if (co_await validate_plan(plan)) {
                    // Execute rebalancing
                    co_await execute_rebalance(plan);

                    // Enter cooldown period
                    co_await ss::sleep_abortable(_config.cooldown_period, _as);
                } else {
                    vlog(clusterlog.warn,
                         "Rebalance plan validation failed, skipping");
                }
            }

        } catch (const ss::sleep_aborted&) {
            break;
        } catch (const std::exception& e) {
            vlog(clusterlog.warn, "Rebalancer error: {}", e.what());
        }

        co_await ss::sleep_abortable(_config.check_interval, _as);
    }
}

ss::future<autonomous_rebalancer::cluster_state>
autonomous_rebalancer::collect_cluster_state() {
    cluster_state state;
    state.timestamp = std::chrono::system_clock::now();

    // In a real implementation, this would collect metrics from
    // health monitor and other sources
    vlog(clusterlog.trace, "Collecting cluster state");

    // Placeholder: return empty state for now
    co_return state;
}

autonomous_rebalancer::rebalance_decision
autonomous_rebalancer::analyze_state(const cluster_state& state) {
    rebalance_decision decision;

    if (state.brokers.empty()) {
        return decision;
    }

    // Check for imbalance
    double imbalance = state.calculate_imbalance();
    if (imbalance > _config.imbalance_threshold) {
        decision.should_rebalance = true;

        // Identify overloaded and underutilized brokers
        for (const auto& broker : state.brokers) {
            if (broker.cpu_utilization > _config.cpu_threshold) {
                decision.overloaded_brokers.push_back(broker.broker);
                decision.primary_reason = rebalance_decision::reason::cpu_imbalance;
            } else if (broker.memory_utilization > _config.memory_threshold) {
                decision.overloaded_brokers.push_back(broker.broker);
                decision.primary_reason = rebalance_decision::reason::memory_imbalance;
            } else if (broker.disk_utilization > _config.disk_threshold) {
                decision.overloaded_brokers.push_back(broker.broker);
                decision.primary_reason = rebalance_decision::reason::disk_imbalance;
            } else if (broker.cpu_utilization < 0.3) {
                decision.underutilized_brokers.push_back(broker.broker);
            }
        }
    }

    return decision;
}

ss::future<autonomous_rebalancer::rebalance_plan>
autonomous_rebalancer::create_rebalance_plan(
    const cluster_state& state,
    const rebalance_decision& decision) {

    rebalance_plan plan;

    // Placeholder implementation
    vlog(clusterlog.info, "Creating rebalance plan");

    plan.expected_improvement = 0.0;
    plan.estimated_duration = std::chrono::seconds(0);

    co_return plan;
}

ss::future<bool> autonomous_rebalancer::validate_plan(
    const rebalance_plan& plan) {

    // Basic validation
    if (plan.moves.empty()) {
        co_return false;
    }

    // Check if we exceed max concurrent moves
    if (plan.moves.size() > _config.max_concurrent_moves * 10) {
        vlog(clusterlog.warn,
             "Plan has too many moves: {}", plan.moves.size());
        co_return false;
    }

    co_return true;
}

ss::future<> autonomous_rebalancer::execute_rebalance(
    const rebalance_plan& plan) {

    vlog(clusterlog.info,
         "Executing rebalance plan with {} moves", plan.moves.size());

    // Execute moves
    for (const auto& move : plan.moves) {
        co_await execute_single_move(move);
    }

    vlog(clusterlog.info, "Rebalance completed successfully");
}

ss::future<> autonomous_rebalancer::execute_single_move(
    const partition_move& move) {

    vlog(clusterlog.info,
         "Moving partition {} from broker {} to broker {}",
         move.ntp, move.from_broker, move.to_broker);

    // Placeholder - actual implementation would interact with
    // partition manager and raft
    co_return;
}

double autonomous_rebalancer::cluster_state::calculate_imbalance() const {
    if (brokers.empty()) {
        return 0.0;
    }

    // Calculate standard deviation of load across brokers
    double mean_load = 0;
    for (const auto& broker : brokers) {
        mean_load += calculate_composite_load(broker);
    }
    mean_load /= brokers.size();

    double variance = 0;
    for (const auto& broker : brokers) {
        double load = calculate_composite_load(broker);
        variance += std::pow(load - mean_load, 2);
    }
    variance /= brokers.size();

    return std::sqrt(variance) / mean_load;  // Coefficient of variation
}

double autonomous_rebalancer::cluster_state::calculate_composite_load(
    const broker_load& broker) const {

    // Weighted combination of different metrics
    return 0.3 * broker.cpu_utilization +
           0.3 * broker.memory_utilization +
           0.2 * broker.disk_utilization +
           0.1 * (broker.network_in_mbps / 10000.0) +
           0.1 * (broker.network_out_mbps / 10000.0);
}

void autonomous_rebalancer::rebalance_plan::optimize_move_order() {
    // Sort moves by impact score (least disruptive first)
    std::sort(moves.begin(), moves.end(),
        [](const auto& a, const auto& b) {
            return a.impact_score < b.impact_score;
        });
}

} // namespace cluster::autonomous
