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

#include "kafka/server/subscription_metadata_hash.h"

#include "hashing/xx.h"
#include "utils/uuid.h"

#include <seastar/core/byteorder.hh>

#include <algorithm>
#include <array>
#include <bit>
#include <concepts>
#include <string_view>

namespace kafka {

namespace {

/// This byte starts every topic hash. Bump it when the inputs or their order
/// change: every stored hash then differs from its fresh value, and every group
/// rebalances once.
constexpr uint8_t hash_version = 0;

/// Writes an integer as fixed-width big-endian, so that nodes of different
/// architecture agree. The `update` overload for an integer writes its
/// in-memory bytes.
template<std::integral T>
void update(incremental_xxhash64& hash, T value) {
    auto bytes = std::bit_cast<std::array<char, sizeof(T)>>(
      ss::cpu_to_be(value));
    hash.update(bytes.data(), bytes.size());
}

/// Writes the length before the bytes, so that the boundary between two strings
/// is part of the digest: "a" then "bc" differs from "ab" then "c".
void update(incremental_xxhash64& hash, std::string_view value) {
    update(hash, static_cast<int32_t>(value.size()));
    hash.update(value);
}

void update(incremental_xxhash64& hash, const uuid_t& value) {
    hash.update(
      // NOLINTNEXTLINE(*reinterpret-cast*)
      reinterpret_cast<const char*>(value.uuid().begin()),
      uuid_t::length);
}

} // namespace

// This function reads the same inputs in the same order as Kafka's
// Utils.computeTopicHash:
// https://github.com/apache/kafka/blob/fce22525f7/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/Utils.java#L491-L522
int64_t topic_metadata_hash(subscribed_topic_metadata topic) {
    // Sort the racks before the partitions, because partition_racks orders on
    // the racks when two entries carry one partition id, and std::sort puts
    // equivalent elements in an unspecified order. The sort keeps duplicate
    // racks, because the list holds one entry per replica and the hash counts
    // the replicas that each rack holds.
    std::ranges::for_each(topic.partitions, [](partition_racks& partition) {
        std::ranges::sort(partition.racks);
    });
    std::ranges::sort(topic.partitions);

    incremental_xxhash64 hash;
    update(hash, hash_version);
    update(hash, topic.id());
    update(hash, std::string_view{topic.name()});
    update(hash, static_cast<int32_t>(topic.partitions.size()));

    for (const auto& partition : topic.partitions) {
        update(hash, partition.partition());
        update(hash, static_cast<int32_t>(partition.racks.size()));
        for (const auto& rack : partition.racks) {
            update(hash, std::string_view{rack()});
        }
    }

    return std::bit_cast<int64_t>(hash.digest());
}

// This function reads the same inputs in the same order as Kafka's
// Utils.computeGroupHash:
// https://github.com/apache/kafka/blob/fce22525f7/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/Utils.java#L453-L468
int64_t subscription_metadata_hash(const topic_metadata_hashes& topic_hashes) {
    if (topic_hashes.empty()) {
        return 0;
    }

    // The fold takes the hashes alone, in order of ascending topic name. Each
    // hash already covers its topic's id and name.
    incremental_xxhash64 hash;
    for (const auto& [_, topic_hash] : topic_hashes) {
        update(hash, topic_hash);
    }
    return std::bit_cast<int64_t>(hash.digest());
}

} // namespace kafka
