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

namespace observability::tracing {

span_handle::~span_handle() {
    if (_span && _recorder) {
        // Finish the span synchronously on destruction
        _span->end_time = std::chrono::system_clock::now();
        if (_span->status == status_code::unset) {
            _span->status = status_code::ok;
        }
        // Note: In production, we'd want to handle this asynchronously
        // For now, we'll just record it - the recorder handles batching
        (void)_recorder->record_end(std::move(*_span));
    }
}

void span_handle::set_attribute(std::string_view key, attribute_value value) {
    if (_span) {
        _span->attrs[std::string(key)] = std::move(value);
    }
}

void span_handle::add_event(std::string_view name, const attributes& attrs) {
    if (_span) {
        _span->events.push_back(event{
          .name = std::string(name),
          .timestamp = std::chrono::system_clock::now(),
          .attributes_ = attrs});
    }
}

void span_handle::set_status(
  status_code code, std::string_view description) {
    if (_span) {
        _span->status = code;
        if (!description.empty()) {
            _span->status_message = std::string(description);
        }
    }
}

seastar::future<> span_handle::finish() {
    if (!_span) {
        return seastar::make_ready_future<>();
    }

    _span->end_time = std::chrono::system_clock::now();
    if (_span->status == status_code::unset) {
        _span->status = status_code::ok;
    }

    auto fut = _recorder->record_end(std::move(*_span));
    _span.reset();

    return fut;
}

} // namespace observability::tracing
