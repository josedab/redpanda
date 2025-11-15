// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "multitenancy/token_bucket.h"

#include <seastar/core/seastar.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

namespace redpanda::multitenancy {

SEASTAR_THREAD_TEST_CASE(token_bucket_try_consume) {
    token_bucket bucket(1000, 1000); // 1000 tokens/sec, burst of 1000

    // Should be able to consume up to burst
    BOOST_CHECK(bucket.try_consume(500));
    BOOST_CHECK(bucket.try_consume(500));

    // Should fail to consume more than available
    BOOST_CHECK(!bucket.try_consume(100));
}

SEASTAR_THREAD_TEST_CASE(token_bucket_refill) {
    token_bucket bucket(1000, 1000); // 1000 tokens/sec, burst of 1000

    // Consume all tokens
    BOOST_CHECK(bucket.try_consume(1000));
    BOOST_CHECK(!bucket.try_consume(1));

    // Wait for refill
    seastar::sleep(std::chrono::seconds(1)).get();

    // Should be able to consume again
    BOOST_CHECK(bucket.try_consume(500));
}

SEASTAR_THREAD_TEST_CASE(token_bucket_acquire_blocking) {
    token_bucket bucket(100, 100); // 100 tokens/sec, burst of 100

    // Consume all tokens
    bucket.try_consume(100);

    auto start = std::chrono::steady_clock::now();

    // This should block until tokens are available
    bucket.acquire(50).get();

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start);

    // Should have waited approximately 500ms for 50 tokens at 100/sec
    BOOST_CHECK_GE(elapsed.count(), 400);
}

} // namespace redpanda::multitenancy
