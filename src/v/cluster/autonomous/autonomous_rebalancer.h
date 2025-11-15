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
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <chrono>
#include <vector>

namespace cluster::autonomous {

/**
 * Intelligent partition rebalancing system that automatically optimizes
 * partition placement based on cluster load metrics.
 */
class autonomous_rebalancer {
public:
    struct rebalance_config {
        double cpu_threshold = 0.8;         // Trigger if CPU > 80%
        double memory_threshold = 0.8;      // Trigger if memory > 80%
        double disk_threshold = 0.85;       // Trigger if disk > 85%
        double imbalance_threshold = 0.2;   // 20% imbalance triggers action
        std::chrono::seconds check_interval{30};
        std::chrono::minutes cooldown_period{5};
        size_t max_concurrent_moves = 10;
        bool enable_predictive = true;
    };

    struct broker_load {
        model::node_id broker;
        double cpu_utilization;
        double memory_utilization;
        double disk_utilization;
        double network_in_mbps;
        double network_out_mbps;
        size_t partition_count;
        size_t leader_count;
    };

    struct partition_load {
        model::ntp ntp;
        size_t size_bytes;
        double throughput_mbps;
        double request_rate;
        double cpu_usage;
        bool is_leader;
    };

    struct cluster_state {
        std::vector<broker_load> brokers;
        std::chrono::system_clock::time_point timestamp;

        double calculate_imbalance() const;

    private:
        double calculate_composite_load(const broker_load& broker) const;
    };

    struct rebalance_decision {
        bool should_rebalance = false;
        enum class reason {
            cpu_imbalance,
            memory_imbalance,
            disk_imbalance,
            partition_skew,
            leader_skew,
            predicted_issue
        };
        reason primary_reason;
        std::vector<model::node_id> overloaded_brokers;
        std::vector<model::node_id> underutilized_brokers;
    };

    struct partition_move {
        model::ntp ntp;
        model::node_id from_broker;
        model::node_id to_broker;
        size_t estimated_bytes;
        std::chrono::seconds estimated_time;
        double impact_score;  // Higher score = more impact
    };

    struct rebalance_plan {
        std::vector<partition_move> moves;
        double expected_improvement;
        std::chrono::seconds estimated_duration;

        void optimize_move_order();
    };

    explicit autonomous_rebalancer(
        rebalance_config config,
        ss::sharded<cluster::partition_manager>& pm,
        ss::sharded<cluster::metadata_cache>& mc,
        ss::sharded<cluster::health_monitor_frontend>& hm);

    ss::future<> start();
    ss::future<> stop();

private:
    ss::future<> monitor_and_rebalance();
    ss::future<cluster_state> collect_cluster_state();
    rebalance_decision analyze_state(const cluster_state& state);
    ss::future<rebalance_plan> create_rebalance_plan(
        const cluster_state& state,
        const rebalance_decision& decision);
    ss::future<bool> validate_plan(const rebalance_plan& plan);
    ss::future<> execute_rebalance(const rebalance_plan& plan);
    ss::future<> execute_single_move(const partition_move& move);

    rebalance_config _config;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    ss::sharded<cluster::health_monitor_frontend>& _health_monitor;
    ss::abort_source _as;
    ss::gate _gate;
    ss::future<> _monitoring_fiber;
};

} // namespace cluster::autonomous
