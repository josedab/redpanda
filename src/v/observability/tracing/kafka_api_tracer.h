// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "observability/tracing/tracer.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <functional>
#include <memory>

namespace observability::tracing {

/// Instrumentation for Kafka API requests with distributed tracing
class kafka_api_tracer {
public:
    explicit kafka_api_tracer(std::shared_ptr<tracer> tracer)
      : _tracer(std::move(tracer)) {}

    /// Trace a produce request
    template<typename Request, typename Response, typename Handler>
    seastar::future<Response> trace_produce(
      Request req,
      Handler handler) {
        if (!_tracer || !_tracer->is_enabled()) {
            co_return co_await handler(std::move(req));
        }

        span_options opts{.kind = span_kind::server};
        auto span = co_await _tracer->start_span("kafka.produce", opts);

        // Set Kafka-specific attributes
        span.set_attribute("kafka.api", "produce");
        if constexpr (requires { req.data.topics; }) {
            if (!req.data.topics.empty()) {
                span.set_attribute("kafka.topic", req.data.topics[0].name());
            }
        }

        try {
            auto response = co_await handler(std::move(req));

            span.set_status(status_code::ok);
            co_await span.finish();

            co_return response;
        } catch (const std::exception& e) {
            span.set_status(status_code::error, e.what());
            span.add_event("exception", {{"exception.message", attribute_value(e.what())}});
            co_await span.finish();
            throw;
        }
    }

    /// Trace a fetch request
    template<typename Request, typename Response, typename Handler>
    seastar::future<Response>
    trace_fetch(Request req, Handler handler) {
        if (!_tracer || !_tracer->is_enabled()) {
            co_return co_await handler(std::move(req));
        }

        span_options opts{.kind = span_kind::server};
        auto span = co_await _tracer->start_span("kafka.fetch", opts);

        // Set Kafka-specific attributes
        span.set_attribute("kafka.api", "fetch");
        if constexpr (requires { req.data.max_bytes; }) {
            span.set_attribute("kafka.max_bytes", static_cast<int64_t>(req.data.max_bytes));
        }

        try {
            auto response = co_await handler(std::move(req));

            span.set_status(status_code::ok);
            co_await span.finish();

            co_return response;
        } catch (const std::exception& e) {
            span.set_status(status_code::error, e.what());
            span.add_event("exception", {{"exception.message", attribute_value(e.what())}});
            co_await span.finish();
            throw;
        }
    }

    /// Trace a metadata request
    template<typename Request, typename Response, typename Handler>
    seastar::future<Response>
    trace_metadata(Request req, Handler handler) {
        if (!_tracer || !_tracer->is_enabled()) {
            co_return co_await handler(std::move(req));
        }

        span_options opts{.kind = span_kind::server};
        auto span = co_await _tracer->start_span("kafka.metadata", opts);

        span.set_attribute("kafka.api", "metadata");

        try {
            auto response = co_await handler(std::move(req));

            span.set_status(status_code::ok);
            co_await span.finish();

            co_return response;
        } catch (const std::exception& e) {
            span.set_status(status_code::error, e.what());
            co_await span.finish();
            throw;
        }
    }

    /// Generic trace wrapper for any Kafka API request
    template<typename Request, typename Response, typename Handler>
    seastar::future<Response> trace_request(
      seastar::sstring api_name,
      Request req,
      Handler handler) {
        if (!_tracer || !_tracer->is_enabled()) {
            co_return co_await handler(std::move(req));
        }

        span_options opts{.kind = span_kind::server};
        auto span_name = seastar::sstring("kafka.") + api_name;
        auto span = co_await _tracer->start_span(span_name, opts);

        span.set_attribute("kafka.api", std::string(api_name.data(), api_name.size()));

        try {
            auto response = co_await handler(std::move(req));

            span.set_status(status_code::ok);
            co_await span.finish();

            co_return response;
        } catch (const std::exception& e) {
            span.set_status(status_code::error, e.what());
            span.add_event("exception", {{"exception.message", attribute_value(e.what())}});
            co_await span.finish();
            throw;
        }
    }

private:
    std::shared_ptr<tracer> _tracer;
};

} // namespace observability::tracing
