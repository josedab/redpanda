// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "observability/tracing/span.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>

#include <memory>
#include <mutex>

namespace observability::tracing {

/// Interface for exporting spans to external systems
class span_exporter {
public:
    virtual ~span_exporter() = default;

    /// Export a batch of spans
    virtual seastar::future<> export_spans(std::vector<span> spans) = 0;
};

/// Simple in-memory span exporter for testing and debugging
class in_memory_span_exporter : public span_exporter {
public:
    seastar::future<> export_spans(std::vector<span> spans) override {
        std::lock_guard<std::mutex> lock(_mutex);
        for (auto& s : spans) {
            _spans.push_back(std::move(s));
        }
        return seastar::make_ready_future<>();
    }

    std::vector<span> get_spans() const {
        std::lock_guard<std::mutex> lock(_mutex);
        return _spans;
    }

    void clear() {
        std::lock_guard<std::mutex> lock(_mutex);
        _spans.clear();
    }

private:
    mutable std::mutex _mutex;
    std::vector<span> _spans;
};

/// Records and batches spans for export
class span_recorder {
public:
    explicit span_recorder(
      std::unique_ptr<span_exporter> exporter,
      size_t batch_size = 100)
      : _exporter(std::move(exporter))
      , _batch_size(batch_size) {}

    /// Record span start (optional, for real-time tracking)
    seastar::future<> record_start(const span& s) {
        // In a full implementation, this could be used for real-time
        // tracking or streaming to external systems
        return seastar::make_ready_future<>();
    }

    /// Record completed span
    seastar::future<> record_end(span s) {
        _pending_spans.push_back(std::move(s));

        // Batch export when we reach the batch size
        if (_pending_spans.size() >= _batch_size) {
            return flush();
        }

        return seastar::make_ready_future<>();
    }

    /// Force export of all pending spans
    seastar::future<> flush() {
        if (_pending_spans.empty()) {
            return seastar::make_ready_future<>();
        }

        std::vector<span> to_export;
        to_export.reserve(_pending_spans.size());
        for (auto& s : _pending_spans) {
            to_export.push_back(std::move(s));
        }
        _pending_spans.clear();

        return _exporter->export_spans(std::move(to_export));
    }

    /// Get number of pending spans
    size_t pending_count() const { return _pending_spans.size(); }

private:
    std::unique_ptr<span_exporter> _exporter;
    size_t _batch_size;
    seastar::circular_buffer<span> _pending_spans;
};

} // namespace observability::tracing
