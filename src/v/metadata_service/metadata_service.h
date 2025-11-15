// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/util/noncopyable_function.hh>

#include <optional>
#include <vector>

namespace redpanda::metadata_service {

// Replica state
struct replica_state {
    model::node_id node_id;
    model::offset committed_offset;
    bool is_leader = false;
    bool is_in_sync = true;
};

// Partition status
enum class partition_status {
    online,
    offline,
    creating,
    deleting,
};

// Partition metadata
struct partition_metadata {
    model::ntp ntp;
    model::offset committed_offset;
    model::offset high_water_mark;
    model::term_id leader_epoch;
    std::vector<replica_state> replicas;
    partition_status status;
    std::optional<model::timestamp> last_stable_offset_timestamp;
    size_t log_size_bytes = 0;
    size_t segment_count = 0;
};

// Member metadata for consumer groups
struct member_metadata {
    kafka::member_id member_id;
    kafka::client_id client_id;
    kafka::client_host client_host;
    std::vector<model::topic> subscribed_topics;
    bytes member_metadata;
    bytes member_assignment;
};

// Consumer group metadata
struct consumer_group_metadata {
    kafka::group_id group_id;
    kafka::group_state state;
    kafka::protocol_type protocol_type;
    kafka::protocol_name protocol_name;
    kafka::member_id leader;
    std::vector<member_metadata> members;
    kafka::generation_id generation;
};

// Partition metadata update event
struct partition_metadata_update {
    model::ntp ntp;
    partition_metadata metadata;
    enum class type { created, updated, deleted };
    type update_type;
};

// Consumer group update event
struct consumer_group_update {
    kafka::group_id group_id;
    consumer_group_metadata metadata;
    enum class type { created, updated, deleted };
    type update_type;
};

// Subscription handle for watching changes
template<typename T>
using subscription = ss::noncopyable_function<void(T)>;

// Main metadata service interface
class metadata_service {
public:
    virtual ~metadata_service() = default;

    // Partition metadata management
    virtual ss::future<partition_metadata>
    get_partition_metadata(model::ntp ntp) = 0;

    virtual ss::future<> update_partition_metadata(
      model::ntp ntp, partition_metadata metadata)
      = 0;

    // Consumer group management
    virtual ss::future<consumer_group_metadata>
    get_group_metadata(kafka::group_id group_id) = 0;

    virtual ss::future<> update_group_metadata(
      kafka::group_id group_id, consumer_group_metadata metadata)
      = 0;

    // Offset management
    virtual ss::future<model::offset> get_consumer_offset(
      kafka::group_id group_id, model::ntp ntp)
      = 0;

    virtual ss::future<> commit_consumer_offset(
      kafka::group_id group_id,
      model::ntp ntp,
      model::offset offset,
      std::optional<ss::sstring> metadata = std::nullopt)
      = 0;

    // Watch for changes
    virtual ss::future<> watch_partition(
      model::ntp ntp, subscription<partition_metadata_update> handler)
      = 0;

    virtual ss::future<> watch_consumer_group(
      kafka::group_id group_id, subscription<consumer_group_update> handler)
      = 0;
};

} // namespace redpanda::metadata_service
