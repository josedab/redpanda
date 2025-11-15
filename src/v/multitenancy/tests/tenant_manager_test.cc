// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "multitenancy/tenant_manager.h"
#include "multitenancy/types.h"

#include <seastar/core/seastar.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

namespace redpanda::multitenancy {

SEASTAR_THREAD_TEST_CASE(tenant_manager_create_get) {
    encryption_config enc_config;
    tenant_manager mgr(enc_config);

    tenant_id tid{"test-tenant", "test-org"};
    resource_limits limits;

    auto tenant = mgr.create_tenant(tid, limits).get();
    BOOST_REQUIRE(tenant);
    BOOST_CHECK_EQUAL(tenant->id().id, "test-tenant");

    auto retrieved = mgr.get_tenant(tid).get();
    BOOST_REQUIRE(retrieved.has_value());
    BOOST_CHECK_EQUAL(retrieved.value()->id().id, "test-tenant");
}

SEASTAR_THREAD_TEST_CASE(tenant_manager_duplicate) {
    encryption_config enc_config;
    tenant_manager mgr(enc_config);

    tenant_id tid{"test-tenant", "test-org"};
    resource_limits limits;

    mgr.create_tenant(tid, limits).get();

    // Creating duplicate tenant should throw
    BOOST_CHECK_THROW(mgr.create_tenant(tid, limits).get(), std::runtime_error);
}

SEASTAR_THREAD_TEST_CASE(tenant_manager_delete) {
    encryption_config enc_config;
    tenant_manager mgr(enc_config);

    tenant_id tid{"test-tenant", "test-org"};
    resource_limits limits;

    mgr.create_tenant(tid, limits).get();

    auto retrieved = mgr.get_tenant(tid).get();
    BOOST_REQUIRE(retrieved.has_value());

    mgr.delete_tenant(tid).get();

    auto after_delete = mgr.get_tenant(tid).get();
    BOOST_CHECK(!after_delete.has_value());
}

SEASTAR_THREAD_TEST_CASE(tenant_manager_list) {
    encryption_config enc_config;
    tenant_manager mgr(enc_config);

    resource_limits limits;

    mgr.create_tenant({"tenant1", "org1"}, limits).get();
    mgr.create_tenant({"tenant2", "org1"}, limits).get();
    mgr.create_tenant({"tenant3", "org2"}, limits).get();

    auto tenants = mgr.list_tenants();
    BOOST_CHECK_EQUAL(tenants.size(), 3);
}

} // namespace redpanda::multitenancy
