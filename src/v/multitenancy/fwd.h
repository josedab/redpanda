// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

namespace redpanda::multitenancy {

// Forward declarations
class tenant;
class resource_domain;
class cpu_scheduler;
class memory_manager;
class io_scheduler;
class network_shaper;
class tenant_encryption_manager;
class tenant_manager;

} // namespace redpanda::multitenancy
