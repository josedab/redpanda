// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "observability/tracing/span.h"
#include "observability/tracing/span_recorder.h"
#include "observability/tracing/tracer.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

using namespace observability::tracing;

SEASTAR_THREAD_TEST_CASE(test_trace_id_generation) {
    auto id1 = trace_id{.high = 123, .low = 456};
    auto id2 = trace_id{.high = 123, .low = 456};
    auto id3 = trace_id{.high = 789, .low = 456};

    BOOST_CHECK(id1 == id2);
    BOOST_CHECK(id1 != id3);
}

SEASTAR_THREAD_TEST_CASE(test_span_id_generation) {
    auto id1 = span_id{.value = 123};
    auto id2 = span_id{.value = 123};
    auto id3 = span_id{.value = 456};

    BOOST_CHECK(id1 == id2);
    BOOST_CHECK(id1 != id3);
}

SEASTAR_THREAD_TEST_CASE(test_trace_context_w3c_format) {
    // Create a tracer with in-memory exporter
    auto exporter = std::make_unique<in_memory_span_exporter>();
    auto* exporter_ptr = exporter.get();
    auto recorder = std::make_unique<span_recorder>(std::move(exporter));
    tracer t(std::move(recorder));

    // Create a trace context
    trace_context ctx{
      .id = trace_id{.high = 0x0123456789abcdef, .low = 0xfedcba9876543210},
      .parent_span = span_id{.value = 0},
      .current_span = span_id{.value = 0x1122334455667788},
      .flags = trace_flags{.value = 0x01},
    };

    // Inject into headers
    absl::flat_hash_map<std::string, std::string> headers;
    t.inject_context(headers, ctx);

    // Check traceparent format
    BOOST_REQUIRE(headers.contains("traceparent"));
    auto traceparent = headers["traceparent"];

    // Should be in format: 00-{32 hex}-{16 hex}-{2 hex}
    BOOST_CHECK(traceparent.starts_with("00-"));

    // Extract back
    auto extracted = t.extract_context(headers);
    BOOST_REQUIRE(extracted.has_value());
    BOOST_CHECK(extracted->id == ctx.id);
    BOOST_CHECK(extracted->flags.value == ctx.flags.value);
}

SEASTAR_THREAD_TEST_CASE(test_span_creation_and_attributes) {
    auto exporter = std::make_unique<in_memory_span_exporter>();
    auto* exporter_ptr = exporter.get();
    auto recorder = std::make_unique<span_recorder>(std::move(exporter));
    tracer t(std::move(recorder));

    // Create a span
    auto span = t.start_span("test_operation").get();

    // Set attributes
    span.set_attribute("key1", attribute_value("value1"));
    span.set_attribute("key2", attribute_value(int64_t(42)));
    span.set_attribute("key3", attribute_value(3.14));
    span.set_attribute("key4", attribute_value(true));

    // Add event
    span.add_event("test_event", {{"event_key", attribute_value("event_value")}});

    // Set status
    span.set_status(status_code::ok);

    // Finish span
    span.finish().get();

    // Flush recorder
    t.flush().get();

    // Check exported spans
    auto spans = exporter_ptr->get_spans();
    BOOST_REQUIRE_EQUAL(spans.size(), 1);

    auto& exported_span = spans[0];
    BOOST_CHECK_EQUAL(exported_span.name, "test_operation");
    BOOST_CHECK_EQUAL(exported_span.status, status_code::ok);
    BOOST_CHECK_EQUAL(exported_span.attrs.size(), 4);
    BOOST_CHECK_EQUAL(exported_span.events.size(), 1);
}

SEASTAR_THREAD_TEST_CASE(test_nested_spans) {
    auto exporter = std::make_unique<in_memory_span_exporter>();
    auto* exporter_ptr = exporter.get();
    auto recorder = std::make_unique<span_recorder>(std::move(exporter));
    tracer t(std::move(recorder));

    // Create parent span
    auto parent = t.start_span("parent").get();
    auto parent_id = parent.get_span_id();
    auto trace_id = parent.get_trace_id();

    // Create child span (should inherit trace context)
    auto child = t.start_span("child").get();
    auto child_id = child.get_span_id();

    // Verify child has same trace ID
    BOOST_CHECK(child.get_trace_id() == trace_id);
    BOOST_CHECK(child_id != parent_id);

    // Finish spans
    child.finish().get();
    parent.finish().get();

    // Flush
    t.flush().get();

    // Check spans
    auto spans = exporter_ptr->get_spans();
    BOOST_REQUIRE_EQUAL(spans.size(), 2);
}

SEASTAR_THREAD_TEST_CASE(test_context_propagation) {
    auto exporter = std::make_unique<in_memory_span_exporter>();
    auto recorder = std::make_unique<span_recorder>(std::move(exporter));
    tracer t(std::move(recorder));

    // Create a span
    auto span = t.start_span("operation").get();

    // Get current context
    auto ctx = tracer::get_current_context();
    BOOST_REQUIRE(ctx.has_value());
    BOOST_CHECK(ctx->current_span == span.get_span_id());

    // Inject into headers
    absl::flat_hash_map<std::string, std::string> headers;
    t.inject_context(headers, *ctx);

    // Clear context
    tracer::clear_current_context();
    BOOST_CHECK(!tracer::get_current_context().has_value());

    // Extract from headers
    auto extracted = t.extract_context(headers);
    BOOST_REQUIRE(extracted.has_value());

    // Set as current context
    tracer::set_current_context(*extracted);

    // Create new span (should be child of extracted)
    auto child = t.start_span("child_operation").get();
    BOOST_CHECK(child.get_trace_id() == extracted->id);

    child.finish().get();
    span.finish().get();
}

SEASTAR_THREAD_TEST_CASE(test_sampling) {
    // Test with 0% sampling
    {
        auto exporter = std::make_unique<in_memory_span_exporter>();
        auto* exporter_ptr = exporter.get();
        auto recorder = std::make_unique<span_recorder>(std::move(exporter));
        tracer_config config{.sampling_rate = 0.0, .enabled = true};
        tracer t(std::move(recorder), config);

        // Create spans - they should still be created but not sampled
        for (int i = 0; i < 10; ++i) {
            auto span = t.start_span("test").get();
            span.finish().get();
        }

        t.flush().get();
        auto spans = exporter_ptr->get_spans();
        BOOST_CHECK_EQUAL(spans.size(), 10); // All spans recorded even if not sampled
    }

    // Test with tracing disabled
    {
        auto exporter = std::make_unique<in_memory_span_exporter>();
        auto recorder = std::make_unique<span_recorder>(std::move(exporter));
        tracer_config config{.enabled = false};
        tracer t(std::move(recorder), config);

        auto span = t.start_span("test").get();
        BOOST_CHECK(!span.is_valid()); // Should return invalid span
    }
}

SEASTAR_THREAD_TEST_CASE(test_span_recorder_batching) {
    auto exporter = std::make_unique<in_memory_span_exporter>();
    auto* exporter_ptr = exporter.get();

    // Create recorder with batch size of 3
    auto recorder = std::make_unique<span_recorder>(std::move(exporter), 3);
    tracer t(std::move(recorder));

    // Create 2 spans (below batch size)
    for (int i = 0; i < 2; ++i) {
        auto span = t.start_span("test").get();
        span.finish().get();
    }

    // Spans should still be pending
    auto spans = exporter_ptr->get_spans();
    BOOST_CHECK_EQUAL(spans.size(), 0);

    // Create 3rd span (should trigger batch export)
    {
        auto span = t.start_span("test").get();
        span.finish().get();
    }

    // Now all 3 should be exported
    spans = exporter_ptr->get_spans();
    BOOST_CHECK_EQUAL(spans.size(), 3);
}
