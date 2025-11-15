// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/autonomous/anomaly_detector.h"
#include "cluster/autonomous/autonomous_controller.h"
#include "cluster/autonomous/autonomous_rebalancer.h"
#include "cluster/autonomous/configuration_tuner.h"
#include "cluster/autonomous/predictive_scaler.h"
#include "cluster/autonomous/self_healer.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

using namespace cluster::autonomous;

SEASTAR_THREAD_TEST_CASE(test_anomaly_detector_construction) {
    anomaly_detector::detection_config config;
    anomaly_detector detector(config);
    BOOST_CHECK(true);
}

SEASTAR_THREAD_TEST_CASE(test_predictive_scaler_construction) {
    predictive_scaler::scaling_config config;
    predictive_scaler scaler(config);
    BOOST_CHECK(true);
}

SEASTAR_THREAD_TEST_CASE(test_configuration_tuner_construction) {
    configuration_tuner::tuning_config config;
    configuration_tuner tuner(config);
    BOOST_CHECK(true);
}

SEASTAR_THREAD_TEST_CASE(test_cluster_state_imbalance_calculation) {
    autonomous_rebalancer::cluster_state state;

    // Empty state should have zero imbalance
    BOOST_CHECK_EQUAL(state.calculate_imbalance(), 0.0);

    // Add some brokers
    state.brokers.push_back({
        .broker = model::node_id(0),
        .cpu_utilization = 0.5,
        .memory_utilization = 0.5,
        .disk_utilization = 0.5,
        .network_in_mbps = 100.0,
        .network_out_mbps = 100.0,
        .partition_count = 10,
        .leader_count = 5
    });

    state.brokers.push_back({
        .broker = model::node_id(1),
        .cpu_utilization = 0.5,
        .memory_utilization = 0.5,
        .disk_utilization = 0.5,
        .network_in_mbps = 100.0,
        .network_out_mbps = 100.0,
        .partition_count = 10,
        .leader_count = 5
    });

    // Balanced cluster should have low imbalance
    double imbalance = state.calculate_imbalance();
    BOOST_CHECK(imbalance >= 0.0);
    BOOST_CHECK(imbalance < 0.1);  // Should be very low
}

SEASTAR_THREAD_TEST_CASE(test_scaling_action_types) {
    predictive_scaler::scaling_action action;

    action.action = predictive_scaler::scaling_action::type::scale_up;
    BOOST_CHECK(action.action == predictive_scaler::scaling_action::type::scale_up);

    action.action = predictive_scaler::scaling_action::type::scale_down;
    BOOST_CHECK(action.action == predictive_scaler::scaling_action::type::scale_down);

    action.action = predictive_scaler::scaling_action::type::no_action;
    BOOST_CHECK(action.action == predictive_scaler::scaling_action::type::no_action);
}

SEASTAR_THREAD_TEST_CASE(test_anomaly_severity_levels) {
    using severity = anomaly_detector::severity;

    anomaly_detector::anomaly anomaly;

    anomaly.severity_level = severity::low;
    BOOST_CHECK(anomaly.severity_level == severity::low);

    anomaly.severity_level = severity::medium;
    BOOST_CHECK(anomaly.severity_level == severity::medium);

    anomaly.severity_level = severity::high;
    BOOST_CHECK(anomaly.severity_level == severity::high);

    anomaly.severity_level = severity::critical;
    BOOST_CHECK(anomaly.severity_level == severity::critical);
}

SEASTAR_THREAD_TEST_CASE(test_rebalance_plan_optimization) {
    autonomous_rebalancer::rebalance_plan plan;

    // Add moves with different impact scores
    plan.moves.push_back({
        .ntp = model::ntp("test", "topic", 0),
        .from_broker = model::node_id(0),
        .to_broker = model::node_id(1),
        .estimated_bytes = 1000,
        .estimated_time = std::chrono::seconds(10),
        .impact_score = 0.5
    });

    plan.moves.push_back({
        .ntp = model::ntp("test", "topic", 1),
        .from_broker = model::node_id(1),
        .to_broker = model::node_id(2),
        .estimated_bytes = 2000,
        .estimated_time = std::chrono::seconds(20),
        .impact_score = 0.3
    });

    plan.moves.push_back({
        .ntp = model::ntp("test", "topic", 2),
        .from_broker = model::node_id(2),
        .to_broker = model::node_id(0),
        .estimated_bytes = 1500,
        .estimated_time = std::chrono::seconds(15),
        .impact_score = 0.7
    });

    // Optimize move order
    plan.optimize_move_order();

    // Check that moves are sorted by impact score (ascending)
    BOOST_CHECK(plan.moves.size() == 3);
    BOOST_CHECK(plan.moves[0].impact_score <= plan.moves[1].impact_score);
    BOOST_CHECK(plan.moves[1].impact_score <= plan.moves[2].impact_score);
}
