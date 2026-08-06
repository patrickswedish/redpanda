/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <absl/container/btree_map.h>

#include <cstdint>
#include <vector>

namespace kafka {

/// The racks that hold one partition's replicas, so the replication factor
/// bounds the list. It carries one entry per replica, so a rack that holds two
/// of them appears twice and the hash covers replica placement. Kafka builds
/// the same list in KRaftCoordinatorMetadataImage.partitionRacks:
/// https://github.com/apache/kafka/blob/fce22525f7/coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/KRaftCoordinatorMetadataImage.java#L140-L154
struct partition_racks {
    model::partition_id partition;
    std::vector<model::rack_id> racks;

    friend auto
    operator<=>(const partition_racks&, const partition_racks&) = default;
};

/// The metadata of one subscribed topic that this hash reads.
struct subscribed_topic_metadata {
    model::topic_id id;
    model::topic name;
    chunked_vector<partition_racks> partitions;
};

/// Hashes of the subscribed topics that exist, keyed by name.
///
/// The key is the name, because a subscription names its topics and a topic id
/// exists only once the topic does. A topic that the cluster knows gets an
/// entry here, so its creation changes the group's hash. The heartbeat carries
/// the subscription itself, and the group epoch follows from that. Kafka
/// selects the same topics in ModernGroup.computeMetadataHash:
/// https://github.com/apache/kafka/blob/fce22525f7/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/modern/ModernGroup.java#L375-L386
using topic_metadata_hashes = absl::btree_map<model::topic, int64_t>;

/// Hashes one subscribed topic's metadata.
///
/// This function sorts each partition's racks, then orders the partitions by id
/// and racks, and hashes the result. The value follows the content of the
/// argument, whatever order the caller built it in. Two nodes that disagree on
/// a group's hash rebalance the group on every recompute.
///
/// Every group that subscribes to a topic can share one of these values,
/// because `subscription_metadata_hash` is a function of them.
int64_t topic_metadata_hash(subscribed_topic_metadata topic);

/// Hashes a group's whole subscription from its per-topic hashes.
///
/// An empty map hashes to 0.
int64_t subscription_metadata_hash(const topic_metadata_hashes& topic_hashes);

} // namespace kafka
