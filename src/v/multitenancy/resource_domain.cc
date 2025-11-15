// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "multitenancy/resource_domain.h"

namespace redpanda::multitenancy {

resource_domain::resource_domain(const resource_limits& limits)
  : _cpu_scheduler(limits.cpu)
  , _memory_manager(limits.memory)
  , _io_scheduler(limits.disk)
  , _network_shaper(limits.network) {}

seastar::future<> resource_domain::reconfigure(const resource_limits& new_limits) {
    // TODO: Implement reconfiguration of all schedulers/managers
    // This would require adding reconfigure methods to each component
    co_return;
}

void resource_domain::update_usage_stats(
  std::chrono::microseconds duration, const execution_context& ctx) {
    // Release CPU time based on actual usage
    _cpu_scheduler.release_cpu_time(duration);

    // Update other statistics as needed
    // This would integrate with metrics/monitoring
}

} // namespace redpanda::multitenancy
