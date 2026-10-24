#include "ha/simple_ha/simple_ha_leader_service.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <ylt/struct_pack.hpp>

#include "ha/simple_ha/simple_ha_types.h"
#include "master_service.h"
#include "master_service/master_service_test_peer.h"
#include "metadata_store.h"
#include "types.h"

namespace mooncake::test {
namespace {

using namespace std::chrono_literals;

constexpr size_t kSegmentBase = 0x500000000;
constexpr size_t kSegmentSize = 16 * 1024 * 1024;

MasterServiceConfig MakeConfig(uint64_t wal_max_batches = 8192,
                               uint32_t batch_entries = 1024) {
    auto config = MasterServiceConfig::builder()
                      .set_enable_ha(true)
                      .set_oplog_batch_max_entries(batch_entries)
                      .build();
    config.enable_simple_ha = true;
    config.simple_ha_wal_max_batches = wal_max_batches;
    config.simple_ha_wal_max_bytes = 0;
    return config;
}

Segment MakeSegment(const std::string& name) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = name;
    segment.base = kSegmentBase;
    segment.size = kSegmentSize;
    segment.te_endpoint = name;
    return segment;
}

struct WalkResult {
    bool ok{true};
    ErrorCode error{ErrorCode::OK};
    std::vector<SimpleHaSnapshotEntry> entries;
    int chunk_calls{0};
    // The request that produced a page whose cursor advanced within a shard.
    std::optional<SimpleHaSnapshotChunkRequest> paged_request;
    std::optional<SimpleHaSnapshotChunkResponse> paged_response;
};

WalkResult WalkSnapshot(SimpleHaLeaderService& leader,
                        const SimpleHaBeginSnapshotResponse& begin,
                        uint32_t max_entries) {
    WalkResult walk;
    uint32_t tenant_index = 0;
    uint32_t shard_index = 0;
    uint32_t offset = 0;
    for (int i = 0; i < 200000; ++i) {
        SimpleHaSnapshotChunkRequest request{
            .snapshot_id = begin.snapshot_id,
            .tenant_index = tenant_index,
            .shard_index = shard_index,
            .offset = offset,
            .max_entries = max_entries,
        };
        const auto response = leader.SnapshotChunk(request);
        ++walk.chunk_calls;
        if (!response.success) {
            walk.ok = false;
            walk.error = static_cast<ErrorCode>(response.error_code);
            return walk;
        }
        if (response.incarnation != begin.incarnation ||
            response.base_batch_id != begin.base_batch_id ||
            response.base_seq != begin.base_seq) {
            walk.ok = false;
            walk.error = ErrorCode::INVALID_PARAMS;
            return walk;
        }
        walk.entries.insert(walk.entries.end(), response.entries.begin(),
                            response.entries.end());
        if (response.next_offset > 0 && !response.entries.empty()) {
            walk.paged_request = request;
            walk.paged_response = response;
        }
        if (response.done) {
            return walk;
        }
        tenant_index = response.next_tenant_index;
        shard_index = response.next_shard_index;
        offset = response.next_offset;
    }
    walk.ok = false;
    walk.error = ErrorCode::INTERNAL_ERROR;
    return walk;
}

std::map<std::string, int> CountKeys(
    const std::vector<SimpleHaSnapshotEntry>& entries) {
    std::map<std::string, int> counts;
    for (const auto& entry : entries) {
        ++counts[entry.key];
    }
    return counts;
}

}  // namespace

class SimpleHaSnapshotCursorTest : public ::testing::Test {
   protected:
    explicit SimpleHaSnapshotCursorTest(uint64_t wal_max_batches = 8192)
        : service_(MakeConfig(wal_max_batches)),
          leader_(service_, /*port=*/0, /*address=*/"127.0.0.1",
                  /*snapshot_chunk_entries=*/1024) {}

    void MountSegment(
        const std::string& name = "simple_ha_snapshot_segment") {
        segment_name_ = name;
        ASSERT_TRUE(
            service_.MountSegment(MakeSegment(name), client_id_).has_value());
    }

    void PutCompleted(const std::string& key) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segments = {segment_name_};
        auto start = service_.PutStart(client_id_, key, TenantId::Default(),
                                       /*slice_length=*/1024, config);
        ASSERT_TRUE(start.has_value());
        ASSERT_TRUE(service_
                        .PutEnd(client_id_, key, TenantId::Default(),
                                ReplicaType::MEMORY)
                        .has_value());
    }

    void PutStarted(const std::string& key) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segments = {segment_name_};
        ASSERT_TRUE(service_
                        .PutStart(client_id_, key, TenantId::Default(),
                                  /*slice_length=*/1024, config)
                        .has_value());
    }

    bool WaitForWalLastBatch(
        uint64_t last_batch_id,
        std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (leader_.GetState().last_batch_id >= last_batch_id) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return leader_.GetState().last_batch_id >= last_batch_id;
    }

    MasterService service_;
    SimpleHaLeaderService leader_;
    UUID client_id_{generate_uuid()};
    std::string segment_name_;
};

class SimpleHaSnapshotRingOverwriteTest : public SimpleHaSnapshotCursorTest {
   protected:
    SimpleHaSnapshotRingOverwriteTest()
        : SimpleHaSnapshotCursorTest(/*wal_max_batches=*/1) {}
};

TEST_F(SimpleHaSnapshotCursorTest, EmptySnapshotCompletesImmediately) {
    const auto begin = leader_.BeginSnapshot();
    ASSERT_TRUE(begin.success);
    EXPECT_TRUE(begin.segments.empty());

    SimpleHaSnapshotChunkRequest request{.snapshot_id = begin.snapshot_id};
    const auto chunk = leader_.SnapshotChunk(request);
    ASSERT_TRUE(chunk.success);
    EXPECT_TRUE(chunk.done);
    EXPECT_TRUE(chunk.entries.empty());
    EXPECT_EQ(chunk.base_batch_id, begin.base_batch_id);
    EXPECT_EQ(chunk.base_seq, begin.base_seq);
    EXPECT_EQ(chunk.incarnation, begin.incarnation);
}

TEST_F(SimpleHaSnapshotCursorTest,
       DisableSnapshotsRejectsAndInvalidatesSessions) {
    MountSegment();
    PutCompleted("disable_snapshots_key");
    ASSERT_TRUE(WaitForWalLastBatch(1));

    const auto begin = leader_.BeginSnapshot();
    ASSERT_TRUE(begin.success);

    // Called before the serving map is drained.
    leader_.DisableSnapshots();

    // A session that started before the drain cannot be resumed, so its
    // consumer can never mark a truncated scan as complete.
    SimpleHaSnapshotChunkRequest request{.snapshot_id = begin.snapshot_id};
    const auto chunk = leader_.SnapshotChunk(request);
    EXPECT_FALSE(chunk.success);
    EXPECT_EQ(static_cast<ErrorCode>(chunk.error_code),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);

    // No new snapshot may start against the draining map.
    const auto second = leader_.BeginSnapshot();
    EXPECT_FALSE(second.success);
    EXPECT_EQ(static_cast<ErrorCode>(second.error_code),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);

    // The frozen WAL stays readable for the promoting standby's tail
    // catch-up; only snapshot serving is disabled.
    const auto state = leader_.GetState();
    ASSERT_TRUE(state.success);
    EXPECT_TRUE(state.is_leader);
    const auto pull =
        leader_.PullWal(SimpleHaPullWalRequest{.from_batch_id = 1});
    ASSERT_TRUE(pull.success);
    EXPECT_FALSE(pull.batches.empty());
}

TEST_F(SimpleHaSnapshotCursorTest, WalksEveryKeyExactlyOnce) {
    MountSegment();
    constexpr int kObjects = 24;
    std::set<std::string> expected;
    for (int i = 0; i < kObjects; ++i) {
        const std::string key = "snapshot-cursor-key-" + std::to_string(i);
        PutCompleted(key);
        expected.insert(key);
    }

    const auto begin = leader_.BeginSnapshot();
    ASSERT_TRUE(begin.success);

    const auto walk = WalkSnapshot(leader_, begin, /*max_entries=*/1);
    ASSERT_TRUE(walk.ok) << toString(walk.error);
    const auto counts = CountKeys(walk.entries);
    EXPECT_EQ(counts.size(), expected.size());
    for (const auto& key : expected) {
        const auto it = counts.find(key);
        ASSERT_NE(it, counts.end()) << key;
        EXPECT_EQ(it->second, 1) << key;
    }
    EXPECT_GT(walk.chunk_calls, 1);
}

TEST_F(SimpleHaSnapshotCursorTest, RejectsStaleCursorAndInvalidatesSession) {
    MountSegment();
    // Two keys in one shard guarantee that the server pages within a shard.
    MasterServiceTestPeer peer(service_);
    std::map<size_t, std::string> first_by_shard;
    std::pair<std::string, std::string> same_shard_keys;
    bool found_pair = false;
    for (int i = 0; i < 8192 && !found_pair; ++i) {
        const std::string key = "snapshot-same-shard-" + std::to_string(i);
        const size_t shard = peer.getShardIndex(key);
        auto it = first_by_shard.find(shard);
        if (it != first_by_shard.end()) {
            same_shard_keys = {it->second, key};
            found_pair = true;
        } else {
            first_by_shard.emplace(shard, key);
        }
    }
    ASSERT_TRUE(found_pair);
    PutCompleted(same_shard_keys.first);
    PutCompleted(same_shard_keys.second);

    const auto begin = leader_.BeginSnapshot();
    ASSERT_TRUE(begin.success);
    const auto walk = WalkSnapshot(leader_, begin, /*max_entries=*/1);
    ASSERT_TRUE(walk.ok) << toString(walk.error);
    ASSERT_TRUE(walk.paged_request.has_value());
    ASSERT_TRUE(walk.paged_response.has_value());

    // Replaying the request whose cursor already advanced must fail and
    // invalidate the session rather than silently skipping entries.
    const auto stale = leader_.SnapshotChunk(*walk.paged_request);
    EXPECT_FALSE(stale.success);
    EXPECT_EQ(stale.error_code, toInt(ErrorCode::INVALID_PARAMS));

    // The session was reset, so even the correct next cursor is rejected.
    SimpleHaSnapshotChunkRequest next{
        .snapshot_id = begin.snapshot_id,
        .tenant_index = walk.paged_response->next_tenant_index,
        .shard_index = walk.paged_response->next_shard_index,
        .offset = walk.paged_response->next_offset,
        .max_entries = 1,
    };
    const auto after_reset = leader_.SnapshotChunk(next);
    EXPECT_FALSE(after_reset.success);
    EXPECT_EQ(after_reset.error_code, toInt(ErrorCode::INVALID_PARAMS));
}

TEST_F(SimpleHaSnapshotCursorTest, NewBeginSnapshotInvalidatesPreviousSession) {
    MountSegment();
    PutCompleted("snapshot-session-old");

    const auto first = leader_.BeginSnapshot();
    ASSERT_TRUE(first.success);

    const uint64_t last_before_new_key = leader_.GetState().last_batch_id;
    PutCompleted("snapshot-session-new");
    ASSERT_TRUE(WaitForWalLastBatch(last_before_new_key + 1));

    const auto second = leader_.BeginSnapshot();
    ASSERT_TRUE(second.success);
    ASSERT_NE(first.snapshot_id, second.snapshot_id);

    SimpleHaSnapshotChunkRequest request{.snapshot_id = first.snapshot_id};
    const auto response = leader_.SnapshotChunk(request);
    EXPECT_FALSE(response.success);
    EXPECT_EQ(response.error_code, toInt(ErrorCode::INVALID_PARAMS));
}

TEST_F(SimpleHaSnapshotCursorTest, SkipsObjectsWithoutCompletedReplicas) {
    MountSegment();
    PutCompleted("snapshot-completed-key");
    PutStarted("snapshot-processing-key");

    const auto begin = leader_.BeginSnapshot();
    ASSERT_TRUE(begin.success);
    const auto walk = WalkSnapshot(leader_, begin, /*max_entries=*/16);
    ASSERT_TRUE(walk.ok) << toString(walk.error);
    const auto counts = CountKeys(walk.entries);
    EXPECT_EQ(counts.count("snapshot-completed-key"), 1u);
    EXPECT_EQ(counts.count("snapshot-processing-key"), 0u);

    for (const auto& entry : walk.entries) {
        if (entry.key != "snapshot-completed-key") {
            continue;
        }
        MetadataPayload payload;
        ASSERT_EQ(struct_pack::deserialize_to(payload, entry.payload),
                  struct_pack::errc::ok);
        ASSERT_FALSE(payload.replicas.empty());
        for (const auto& replica : payload.replicas) {
            EXPECT_EQ(replica.status, ReplicaStatus::COMPLETE);
        }
    }
}

TEST_F(SimpleHaSnapshotRingOverwriteTest, RejectsBaseOverwrittenByRing) {
    MountSegment();
    const auto begin = leader_.BeginSnapshot();
    ASSERT_TRUE(begin.success);

    uint64_t last = leader_.GetState().last_batch_id;
    PutCompleted("snapshot-ring-a");
    ASSERT_TRUE(WaitForWalLastBatch(last + 1));

    last = leader_.GetState().last_batch_id;
    PutCompleted("snapshot-ring-b");
    ASSERT_TRUE(WaitForWalLastBatch(last + 1));

    SimpleHaSnapshotChunkRequest request{.snapshot_id = begin.snapshot_id};
    const auto response = leader_.SnapshotChunk(request);
    EXPECT_FALSE(response.success);
    EXPECT_EQ(response.error_code,
              toInt(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP));
}

}  // namespace mooncake::test
