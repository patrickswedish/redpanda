// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "container/chunked_vector.h"
#include "kafka/server/subscription_metadata_hash.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <seastar/testing/perf_tests.hh>

#include <algorithm>
#include <random>
#include <string>
#include <vector>

namespace kafka {

namespace {

/// The coordinator shard recomputes a group's hash to find out whether the
/// metadata of its subscribed topics moved. The cost grows with the partition
/// count of each topic, so the cases below sweep it: `topic_metadata_hash`
/// sorts the partitions and then hashes every partition id and rack, and the
/// shard holds the reactor for as long as that takes.
///
/// Every figure is the cost of one call. The build of each input sits outside
/// the measured window, because a caller materializes it from the metadata
/// cache once and hashes it once, and `topic_metadata_hash` consumes it.

constexpr int replication_factor = 3;

std::vector<partition_racks> partition_specs(int count) {
    std::vector<partition_racks> specs;
    specs.reserve(count);
    for (int p = 0; p < count; ++p) {
        std::vector<model::rack_id> racks;
        racks.reserve(replication_factor);
        for (int r = 0; r < replication_factor; ++r) {
            racks.emplace_back(
              ss::sstring{"rack-"}
              + std::to_string((p + r) % replication_factor));
        }
        specs.push_back(
          {.partition = model::partition_id{p}, .racks = std::move(racks)});
    }
    return specs;
}

topic_metadata_hashes topic_hashes(int count) {
    topic_metadata_hashes hashes;
    for (int t = 0; t < count; ++t) {
        hashes.emplace(
          model::topic{ss::sstring{"topic-"} + std::to_string(t)}, t);
    }
    return hashes;
}

struct shapes {
    std::vector<partition_racks> hundred = partition_specs(100);
    std::vector<partition_racks> ten_thousand = partition_specs(10'000);
    std::vector<partition_racks> hundred_thousand = partition_specs(100'000);
    std::vector<partition_racks> ten_thousand_shuffled = [] {
        auto specs = partition_specs(10'000);
        std::mt19937 engine{0x848};
        std::shuffle(specs.begin(), specs.end(), engine);
        return specs;
    }();

    topic_metadata_hashes hundred_topics = topic_hashes(100);
    topic_metadata_hashes ten_thousand_topics = topic_hashes(10'000);

    static subscribed_topic_metadata
    topic_of(const std::vector<partition_racks>& specs) {
        chunked_vector<partition_racks> partitions;
        partitions.reserve(specs.size());
        for (const auto& spec : specs) {
            partitions.push_back(spec);
        }
        return subscribed_topic_metadata{
          .id = model::topic_id::create(),
          .name = model::topic{"benchmark-topic"},
          .partitions = std::move(partitions)};
    }

    static void hash_topic(const std::vector<partition_racks>& specs) {
        auto topic = topic_of(specs);
        perf_tests::start_measuring_time();
        auto hash = topic_metadata_hash(std::move(topic));
        perf_tests::do_not_optimize(hash);
        perf_tests::stop_measuring_time();
    }

    /// The fold reads its argument, so an inner loop keeps the measured window
    /// well clear of the cost of starting and stopping the timers.
    static size_t
    hash_group(const topic_metadata_hashes& hashes, size_t loops) {
        int64_t sink = 0;
        for (size_t i = 0; i < loops; ++i) {
            sink ^= subscription_metadata_hash(hashes);
        }
        perf_tests::do_not_optimize(sink);
        return loops;
    }
};

} // namespace

PERF_TEST_F(shapes, topic_100_partitions) { hash_topic(hundred); }

PERF_TEST_F(shapes, topic_10k_partitions) { hash_topic(ten_thousand); }

PERF_TEST_F(shapes, topic_100k_partitions) { hash_topic(hundred_thousand); }

/// The metadata cache hands out partitions in order of ascending id, so the
/// sort in `topic_metadata_hash` has nothing to move. This case pays for the
/// moves.
PERF_TEST_F(shapes, topic_10k_partitions_unordered) {
    hash_topic(ten_thousand_shuffled);
}

PERF_TEST_F(shapes, group_100_topics) {
    return hash_group(hundred_topics, 10'000);
}

PERF_TEST_F(shapes, group_10k_topics) {
    return hash_group(ten_thousand_topics, 100);
}

} // namespace kafka
