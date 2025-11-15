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

#include <functional>
#include <memory>

namespace observability::tracing {

/// Instrumentation for Raft operations with distributed tracing
class raft_tracer {
public:
    explicit raft_tracer(std::shared_ptr<tracer> tracer)
      : _tracer(std::move(tracer)) {}

    /// Trace an append_entries operation
    template<typename Request, typename Handler>
    seastar::future<> trace_append_entries(
      const Request& req,
      Handler handler) {
        if (!_tracer || !_tracer->is_enabled()) {
            co_return co_await handler(req);
        }

        span_options opts{.kind = span_kind::internal};
        auto span = co_await _tracer->start_span("raft.append_entries", opts);

        // Set Raft-specific attributes
        if constexpr (requires { req.target_node_id; }) {
            span.set_attribute("raft.target_node", static_cast<int64_t>(req.target_node_id.id()));
        }
        if constexpr (requires { req.node_id; }) {
            span.set_attribute("raft.leader_id", static_cast<int64_t>(req.node_id.id()));
        }

        try {
            co_await handler(req);

            span.set_status(status_code::ok);
            co_await span.finish();
        } catch (const std::exception& e) {
            span.set_status(status_code::error, e.what());
            span.add_event("exception", {{"exception.message", attribute_value(e.what())}});
            co_await span.finish();
            throw;
        }
    }

    /// Trace a vote request
    template<typename Request, typename Response, typename Handler>
    seastar::future<Response>
    trace_vote_request(const Request& req, Handler handler) {
        if (!_tracer || !_tracer->is_enabled()) {
            co_return co_await handler(req);
        }

        span_options opts{.kind = span_kind::internal};
        auto span = co_await _tracer->start_span("raft.vote_request", opts);

        // Set Raft-specific attributes
        if constexpr (requires { req.target_node_id; }) {
            span.set_attribute("raft.target_node", static_cast<int64_t>(req.target_node_id.id()));
        }
        if constexpr (requires { req.node_id; }) {
            span.set_attribute("raft.candidate_id", static_cast<int64_t>(req.node_id.id()));
        }

        try {
            auto response = co_await handler(req);

            span.set_status(status_code::ok);
            co_await span.finish();

            co_return response;
        } catch (const std::exception& e) {
            span.set_status(status_code::error, e.what());
            co_await span.finish();
            throw;
        }
    }

    /// Trace a timeout now request
    template<typename Request, typename Response, typename Handler>
    seastar::future<Response>
    trace_timeout_now(const Request& req, Handler handler) {
        if (!_tracer || !_tracer->is_enabled()) {
            co_return co_await handler(req);
        }

        span_options opts{.kind = span_kind::internal};
        auto span = co_await _tracer->start_span("raft.timeout_now", opts);

        if constexpr (requires { req.target_node_id; }) {
            span.set_attribute("raft.target_node", static_cast<int64_t>(req.target_node_id.id()));
        }

        try {
            auto response = co_await handler(req);

            span.set_status(status_code::ok);
            co_await span.finish();

            co_return response;
        } catch (const std::exception& e) {
            span.set_status(status_code::error, e.what());
            co_await span.finish();
            throw;
        }
    }

    /// Trace log replication
    template<typename Handler>
    seastar::future<> trace_replicate(
      int64_t term,
      size_t batch_count,
      Handler handler) {
        if (!_tracer || !_tracer->is_enabled()) {
            co_return co_await handler();
        }

        span_options opts{.kind = span_kind::internal};
        auto span = co_await _tracer->start_span("raft.replicate", opts);

        span.set_attribute("raft.term", term);
        span.set_attribute("raft.batch_count", static_cast<int64_t>(batch_count));

        try {
            co_await handler();

            span.set_status(status_code::ok);
            co_await span.finish();
        } catch (const std::exception& e) {
            span.set_status(status_code::error, e.what());
            co_await span.finish();
            throw;
        }
    }

private:
    std::shared_ptr<tracer> _tracer;
};

} // namespace observability::tracing
