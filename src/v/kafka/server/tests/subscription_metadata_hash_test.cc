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
#include "model/fundamental.h"
#include "model/metadata.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <string_view>
#include <vector>

namespace {

model::topic_id topic_id_of(uint8_t first_byte) {
    std::vector<uint8_t> bytes(uuid_t::length, 0);
    bytes.front() = first_byte;
    return model::topic_id{uuid_t{bytes}};
}

std::vector<model::rack_id>
racks_of(const std::vector<std::string_view>& names) {
    std::vector<model::rack_id> racks;
    racks.reserve(names.size());
    for (const auto& name : names) {
        racks.emplace_back(ss::sstring{name});
    }
    return racks;
}

/// A topic with two partitions and one rack each. Most cases below change one
/// field of it.
kafka::subscribed_topic_metadata a_topic() {
    chunked_vector<kafka::partition_racks> partitions;
    partitions.push_back(
      {.partition = model::partition_id{0}, .racks = racks_of({"rack-a"})});
    partitions.push_back(
      {.partition = model::partition_id{1}, .racks = racks_of({"rack-b"})});
    return kafka::subscribed_topic_metadata{
      .id = topic_id_of(1),
      .name = model::topic{"topic-1"},
      .partitions = std::move(partitions),
    };
}

} // namespace

/// The value follows the content, whatever order the caller built it in. Two
/// nodes that read the same metadata by different paths must agree, or the
/// group rebalances on every recompute.
TEST(subscription_metadata_hash, ignores_input_order) {
    auto shuffled = a_topic();
    std::reverse(shuffled.partitions.begin(), shuffled.partitions.end());

    EXPECT_EQ(
      kafka::topic_metadata_hash(a_topic()),
      kafka::topic_metadata_hash(std::move(shuffled)));
}

TEST(subscription_metadata_hash, ignores_rack_order) {
    auto sorted = a_topic();
    sorted.partitions.front().racks = racks_of({"rack-x", "rack-y"});

    auto unsorted = a_topic();
    unsorted.partitions.front().racks = racks_of({"rack-y", "rack-x"});

    EXPECT_EQ(
      kafka::topic_metadata_hash(std::move(sorted)),
      kafka::topic_metadata_hash(std::move(unsorted)));
}

/// Two replicas in one rack give two rack entries, so the hash differs from the
/// hash of one replica in that rack.
TEST(subscription_metadata_hash, counts_a_rack_once_per_replica) {
    auto one_replica = a_topic();
    one_replica.partitions.front().racks = racks_of({"rack-x"});

    auto two_replicas = a_topic();
    two_replicas.partitions.front().racks = racks_of({"rack-x", "rack-x"});

    EXPECT_NE(
      kafka::topic_metadata_hash(std::move(one_replica)),
      kafka::topic_metadata_hash(std::move(two_replicas)));
}

/// A repeated partition id is malformed input, and it still hashes the same
/// whichever order the caller passes the two entries in. The comparator
/// includes the racks for that reason.
TEST(subscription_metadata_hash, orders_repeated_partition_ids) {
    auto one_way = a_topic();
    one_way.partitions.push_back(
      {.partition = model::partition_id{0}, .racks = racks_of({"rack-z"})});

    auto other_way = a_topic();
    std::reverse(other_way.partitions.begin(), other_way.partitions.end());
    other_way.partitions.push_back(
      {.partition = model::partition_id{0}, .racks = racks_of({"rack-z"})});
    std::reverse(other_way.partitions.begin(), other_way.partitions.end());

    EXPECT_EQ(
      kafka::topic_metadata_hash(std::move(one_way)),
      kafka::topic_metadata_hash(std::move(other_way)));
}

/// Every input that the assignor reads must move the hash, so that the group
/// reassigns after a change to the metadata.
TEST(subscription_metadata_hash, detects_every_input_change) {
    const auto baseline = kafka::topic_metadata_hash(a_topic());

    auto renamed = a_topic();
    renamed.name = model::topic{"topic-2"};
    EXPECT_NE(baseline, kafka::topic_metadata_hash(std::move(renamed)));

    auto reidentified = a_topic();
    reidentified.id = topic_id_of(2);
    EXPECT_NE(baseline, kafka::topic_metadata_hash(std::move(reidentified)));

    auto grown = a_topic();
    grown.partitions.push_back(
      {.partition = model::partition_id{2}, .racks = racks_of({"rack-c"})});
    EXPECT_NE(baseline, kafka::topic_metadata_hash(std::move(grown)));

    auto renumbered = a_topic();
    renumbered.partitions.back().partition = model::partition_id{7};
    EXPECT_NE(baseline, kafka::topic_metadata_hash(std::move(renumbered)));

    auto moved_rack = a_topic();
    moved_rack.partitions.front().racks = racks_of({"rack-z"});
    EXPECT_NE(baseline, kafka::topic_metadata_hash(std::move(moved_rack)));

    auto replicated = a_topic();
    replicated.partitions.front().racks = racks_of({"rack-a", "rack-b"});
    EXPECT_NE(baseline, kafka::topic_metadata_hash(std::move(replicated)));

    auto unracked = a_topic();
    unracked.partitions.front().racks = {};
    EXPECT_NE(baseline, kafka::topic_metadata_hash(std::move(unracked)));
}

/// Each rack belongs to the partition it sits under, so an exchange of two
/// partitions' racks changes the hash. A rack-aware assignor then reads each
/// partition from a rack that holds it.
TEST(subscription_metadata_hash, binds_racks_to_their_partition) {
    auto swapped = a_topic();
    swapped.partitions.front().racks = racks_of({"rack-b"});
    swapped.partitions.back().racks = racks_of({"rack-a"});

    EXPECT_NE(
      kafka::topic_metadata_hash(a_topic()),
      kafka::topic_metadata_hash(std::move(swapped)));
}

/// A length precedes each variable-length field, so two inputs that differ give
/// two digests.
TEST(subscription_metadata_hash, distinguishes_regrouped_strings) {
    auto split_early = a_topic();
    split_early.partitions.front().racks = racks_of({"a", "bc"});

    auto split_late = a_topic();
    split_late.partitions.front().racks = racks_of({"ab", "c"});

    EXPECT_NE(
      kafka::topic_metadata_hash(std::move(split_early)),
      kafka::topic_metadata_hash(std::move(split_late)));

    auto long_name = a_topic();
    long_name.name = model::topic{"topic-11"};
    auto short_name = a_topic();
    short_name.name = model::topic{"topic-1"};
    short_name.partitions.front().racks = racks_of({"1rack-a"});
    EXPECT_NE(
      kafka::topic_metadata_hash(std::move(long_name)),
      kafka::topic_metadata_hash(std::move(short_name)));
}

TEST(subscription_metadata_hash, empty_subscription_hashes_to_zero) {
    EXPECT_EQ(kafka::subscription_metadata_hash({}), 0);
}

TEST(subscription_metadata_hash, group_hash_ignores_insertion_order) {
    const auto first = kafka::topic_metadata_hash(a_topic());
    auto second_topic = a_topic();
    second_topic.id = topic_id_of(2);
    const auto second = kafka::topic_metadata_hash(std::move(second_topic));

    kafka::topic_metadata_hashes ascending;
    ascending.emplace(model::topic{"topic-1"}, first);
    ascending.emplace(model::topic{"topic-2"}, second);

    kafka::topic_metadata_hashes descending;
    descending.emplace(model::topic{"topic-2"}, second);
    descending.emplace(model::topic{"topic-1"}, first);

    EXPECT_EQ(
      kafka::subscription_metadata_hash(ascending),
      kafka::subscription_metadata_hash(descending));
}

TEST(subscription_metadata_hash, group_hash_follows_its_topics) {
    const auto topic_hash = kafka::topic_metadata_hash(a_topic());

    kafka::topic_metadata_hashes one_topic;
    one_topic.emplace(model::topic{"topic-1"}, topic_hash);

    kafka::topic_metadata_hashes two_topics = one_topic;
    two_topics.emplace(model::topic{"topic-2"}, topic_hash);

    kafka::topic_metadata_hashes changed_topic;
    changed_topic.emplace(model::topic{"topic-1"}, topic_hash + 1);

    EXPECT_NE(
      kafka::subscription_metadata_hash(one_topic),
      kafka::subscription_metadata_hash(two_topics));
    EXPECT_NE(
      kafka::subscription_metadata_hash(one_topic),
      kafka::subscription_metadata_hash(changed_topic));
}

/// The group hash takes the values alone, and a topic's own hash covers its id
/// and name. A rename therefore changes the value as well as the key.
TEST(subscription_metadata_hash, topic_hash_carries_topic_identity) {
    auto renamed = a_topic();
    renamed.name = model::topic{"topic-2"};

    EXPECT_NE(
      kafka::topic_metadata_hash(a_topic()),
      kafka::topic_metadata_hash(std::move(renamed)));
}

/// These values catch a change to the algorithm. Each group holds a stored
/// hash, so an edit to the inputs or their order rebalances every group once,
/// and `hash_version` records that the edit happened.
TEST(subscription_metadata_hash, algorithm_is_frozen) {
    EXPECT_EQ(kafka::topic_metadata_hash(a_topic()), -9121569554438404953);

    kafka::topic_metadata_hashes group;
    group.emplace(
      model::topic{"topic-1"}, kafka::topic_metadata_hash(a_topic()));
    EXPECT_EQ(kafka::subscription_metadata_hash(group), 6715244579225394956);
}
