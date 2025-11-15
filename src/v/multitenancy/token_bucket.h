// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>

#include <atomic>
#include <chrono>

namespace redpanda::multitenancy {

// Token bucket for rate limiting
class token_bucket {
public:
    using clock_type = std::chrono::steady_clock;
    using duration = std::chrono::microseconds;

    token_bucket(size_t rate, size_t burst)
      : _rate(rate)
      , _burst(burst)
      , _tokens(burst)
      , _last_refill(clock_type::now()) {}

    // Try to consume tokens (non-blocking)
    bool try_consume(size_t tokens) {
        refill();
        if (_tokens >= tokens) {
            _tokens -= tokens;
            return true;
        }
        return false;
    }

    // Acquire tokens (blocking if necessary)
    seastar::future<> acquire(size_t tokens) {
        refill();
        if (_tokens >= tokens) {
            _tokens -= tokens;
            return seastar::make_ready_future<>();
        }

        // Calculate wait time
        auto deficit = tokens - _tokens;
        auto wait_time = std::chrono::duration_cast<duration>(
          std::chrono::microseconds(deficit * 1000000 / _rate));

        return seastar::sleep(wait_time).then([this, tokens] {
            refill();
            _tokens -= std::min(_tokens, tokens);
        });
    }

    // Release tokens back to the bucket
    void release(size_t tokens) { _tokens = std::min(_tokens + tokens, _burst); }

    // Get available tokens
    size_t available() const {
        const_cast<token_bucket*>(this)->refill();
        return _tokens;
    }

private:
    void refill() {
        auto now = clock_type::now();
        auto elapsed = std::chrono::duration_cast<duration>(now - _last_refill);

        if (elapsed.count() > 0) {
            // Add tokens based on rate
            auto new_tokens = (elapsed.count() * _rate) / 1000000;
            _tokens = std::min(_tokens + new_tokens, _burst);
            _last_refill = now;
        }
    }

    size_t _rate;  // Tokens per second
    size_t _burst; // Maximum tokens
    size_t _tokens;
    clock_type::time_point _last_refill;
};

} // namespace redpanda::multitenancy
