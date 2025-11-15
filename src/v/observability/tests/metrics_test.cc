// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "metrics/client_metrics.h"
#include "metrics/cost_tracker.h"
#include "metrics/partition_metrics.h"
#include "model/fundamental.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

using namespace std::chrono_literals;

SEASTAR_THREAD_TEST_CASE(test_partition_metrics) {
    metrics::partition_metrics pm;

    model::ntp ntp(
      model::ns("test_ns"),
      model::topic("test_topic"),
      model::partition_id(0));

    // Record some produce operations
    pm.record_produce(ntp, 1024, 10, 100us);
    pm.record_produce(ntp, 2048, 20, 150us);

    // Record some fetch operations
    pm.record_fetch(ntp, 512, 5, 50us);

    // Get metrics
    auto* m = pm.get_metrics(ntp);
    BOOST_REQUIRE(m != nullptr);

    BOOST_CHECK_EQUAL(m->bytes_produced, 1024 + 2048);
    BOOST_CHECK_EQUAL(m->records_produced, 10 + 20);
    BOOST_CHECK_EQUAL(m->bytes_consumed, 512);
    BOOST_CHECK_EQUAL(m->records_consumed, 5);
}

SEASTAR_THREAD_TEST_CASE(test_partition_metrics_errors) {
    metrics::partition_metrics pm;

    model::ntp ntp(
      model::ns("test_ns"),
      model::topic("test_topic"),
      model::partition_id(0));

    // Record errors
    pm.record_produce_error(ntp);
    pm.record_produce_error(ntp);
    pm.record_fetch_error(ntp);

    auto* m = pm.get_metrics(ntp);
    BOOST_REQUIRE(m != nullptr);

    BOOST_CHECK_EQUAL(m->produce_errors, 2);
    BOOST_CHECK_EQUAL(m->fetch_errors, 1);
}

SEASTAR_THREAD_TEST_CASE(test_client_metrics) {
    metrics::client_metrics cm;

    metrics::client_id client{
      .client_id_str = "test-client",
      .user = "test-user",
      .ip_address = seastar::net::inet_address("127.0.0.1"),
    };

    // Record some requests
    cm.record_request(
      client,
      metrics::request_type::produce,
      1024,
      100us,
      true);
    cm.record_request(
      client,
      metrics::request_type::fetch,
      2048,
      50us,
      true);
    cm.record_request(
      client,
      metrics::request_type::produce,
      512,
      120us,
      false); // Error

    // Check metrics
    auto* m = cm.get_metrics(client);
    BOOST_REQUIRE(m != nullptr);

    BOOST_CHECK_EQUAL(m->requests, 3);
    BOOST_CHECK_EQUAL(m->errors, 1);
    BOOST_CHECK_EQUAL(m->bytes_received, 1024 + 512); // Produce requests
    BOOST_CHECK_EQUAL(m->bytes_sent, 2048);           // Fetch requests
}

SEASTAR_THREAD_TEST_CASE(test_client_metrics_top_clients) {
    metrics::client_metrics cm;

    // Create multiple clients with different request counts
    for (int i = 0; i < 10; ++i) {
        metrics::client_id client{
          .client_id_str = fmt::format("client-{}", i),
          .user = "user",
          .ip_address = seastar::net::inet_address("127.0.0.1"),
        };

        // Each client has i * 100 requests
        for (int j = 0; j < i * 10; ++j) {
            cm.record_request(
              client,
              metrics::request_type::produce,
              100,
              100us,
              true);
        }
    }

    // Get top 5 clients by request count
    auto top_clients = cm.top_clients_by_requests(5);

    BOOST_CHECK_EQUAL(top_clients.size(), 5);

    // Check ordering (should be descending)
    BOOST_CHECK(top_clients[0].first.client_id_str == "client-9");
    BOOST_CHECK(top_clients[1].first.client_id_str == "client-8");
    BOOST_CHECK(top_clients[2].first.client_id_str == "client-7");
}

SEASTAR_THREAD_TEST_CASE(test_client_metrics_bandwidth) {
    metrics::client_metrics cm;

    metrics::client_id client1{
      .client_id_str = "high-bandwidth",
      .user = "user",
      .ip_address = seastar::net::inet_address("127.0.0.1"),
    };

    metrics::client_id client2{
      .client_id_str = "low-bandwidth",
      .user = "user",
      .ip_address = seastar::net::inet_address("127.0.0.2"),
    };

    // Client1: high bandwidth
    cm.record_request(
      client1,
      metrics::request_type::produce,
      1024 * 1024,
      100us,
      true);
    cm.record_request(
      client1,
      metrics::request_type::fetch,
      2048 * 1024,
      100us,
      true);

    // Client2: low bandwidth
    cm.record_request(
      client2,
      metrics::request_type::produce,
      1024,
      100us,
      true);

    auto top_by_bandwidth = cm.top_clients_by_bandwidth(10);

    BOOST_CHECK_EQUAL(top_by_bandwidth.size(), 2);
    BOOST_CHECK(top_by_bandwidth[0].first.client_id_str == "high-bandwidth");
    BOOST_CHECK(top_by_bandwidth[1].first.client_id_str == "low-bandwidth");
}

SEASTAR_THREAD_TEST_CASE(test_cost_tracker_basic) {
    metrics::cost_tracker ct;

    metrics::operation_cost cost{
      .cpu_time = 1000us,
      .memory_bytes = 1024 * 1024,
      .disk_io_bytes = 2048,
      .network_io_bytes = 512,
    };

    ct.record_cost("test_operation", cost);

    auto* history = ct.get_operation_history("test_operation");
    BOOST_REQUIRE(history != nullptr);

    BOOST_CHECK_EQUAL(history->operation_count, 1);
    BOOST_CHECK_EQUAL(history->total_cost.cpu_time, 1000us);
    BOOST_CHECK_EQUAL(history->total_cost.memory_bytes, 1024 * 1024);
}

SEASTAR_THREAD_TEST_CASE(test_cost_tracker_multiple_operations) {
    metrics::cost_tracker ct;

    // Record costs for different operations
    for (int i = 0; i < 5; ++i) {
        metrics::operation_cost cost{
          .cpu_time = std::chrono::microseconds(i * 1000),
          .memory_bytes = static_cast<size_t>(i * 1024),
        };
        ct.record_cost("op_a", cost);
    }

    for (int i = 0; i < 3; ++i) {
        metrics::operation_cost cost{
          .cpu_time = std::chrono::microseconds(i * 2000),
          .memory_bytes = static_cast<size_t>(i * 2048),
        };
        ct.record_cost("op_b", cost);
    }

    // Get top operations by CPU
    auto top_cpu = ct.top_operations_by_cpu(10);

    BOOST_CHECK_EQUAL(top_cpu.size(), 2);
}

SEASTAR_THREAD_TEST_CASE(test_cost_tracker_enable_disable) {
    metrics::cost_tracker ct;

    BOOST_CHECK(ct.is_enabled());

    ct.set_enabled(false);
    BOOST_CHECK(!ct.is_enabled());

    ct.set_enabled(true);
    BOOST_CHECK(ct.is_enabled());
}
