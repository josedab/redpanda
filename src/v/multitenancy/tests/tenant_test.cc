// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "multitenancy/tenant.h"
#include "multitenancy/types.h"

#include <boost/test/unit_test.hpp>

namespace redpanda::multitenancy {

BOOST_AUTO_TEST_CASE(tenant_creation) {
    tenant_id tid{"test-tenant", "test-org"};
    resource_limits limits;

    tenant t(tid, limits);

    BOOST_CHECK_EQUAL(t.id().id, "test-tenant");
    BOOST_CHECK_EQUAL(t.id().organization, "test-org");
}

BOOST_AUTO_TEST_CASE(tenant_quota_validation) {
    tenant_id tid{"test-tenant", "test-org"};
    resource_limits limits;

    // Invalid CPU quota
    limits.cpu.quota_per_second = std::chrono::microseconds(0);
    BOOST_CHECK_THROW(tenant(tid, limits), std::invalid_argument);

    // Reset to valid
    limits.cpu.quota_per_second = std::chrono::microseconds(1000000);

    // Invalid memory limits
    limits.memory.soft_limit_bytes = 2048;
    limits.memory.hard_limit_bytes = 1024;
    BOOST_CHECK_THROW(tenant(tid, limits), std::invalid_argument);

    // Reset to valid
    limits.memory.soft_limit_bytes = 1024;
    limits.memory.hard_limit_bytes = 2048;

    // Should not throw
    BOOST_CHECK_NO_THROW(tenant(tid, limits));
}

BOOST_AUTO_TEST_CASE(tenant_statistics) {
    tenant_id tid{"test-tenant", "test-org"};
    resource_limits limits;

    tenant t(tid, limits);

    // Initial statistics should be zero
    BOOST_CHECK_EQUAL(t.usage().produce_requests, 0);
    BOOST_CHECK_EQUAL(t.usage().fetch_requests, 0);
    BOOST_CHECK_EQUAL(t.usage().errors, 0);

    // Record some requests
    t.record_produce_request();
    t.record_fetch_request();
    t.record_error();

    BOOST_CHECK_EQUAL(t.usage().produce_requests, 1);
    BOOST_CHECK_EQUAL(t.usage().fetch_requests, 1);
    BOOST_CHECK_EQUAL(t.usage().errors, 1);
}

} // namespace redpanda::multitenancy
