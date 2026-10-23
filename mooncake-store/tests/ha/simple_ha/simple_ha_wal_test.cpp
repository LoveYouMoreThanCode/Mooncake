#include "ha/simple_ha/simple_ha_wal.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <string>
#include <thread>
#include <vector>

#include "ha/oplog/oplog_batch_codec.h"

namespace mooncake::test {
namespace {

OpLogEntry MakeEntry(uint64_t sequence_id, const std::string& key) {
    OpLogEntry entry;
    entry.sequence_id = sequence_id;
    entry.op_type = OpType::PUT_END;
    entry.tenant_id = "default";
    entry.object_key = key;
    entry.payload = "value-" + key;
    return entry;
}

// One entry per batch. `MakeBatch` keeps batch_id == first_seq == last_seq,
// which makes the ring assertions easy to read; `MakeBatchAt` covers cases
// where the durable prefix has advanced past zero and the two differ.
OpLogBatchRecord MakeBatchAt(uint64_t batch_id, uint64_t first_seq,
                             const std::string& key) {
    OpLogBatchRecord batch;
    batch.batch_id = batch_id;
    batch.first_seq = first_seq;
    batch.last_seq = first_seq;
    batch.entries.push_back(MakeEntry(first_seq, key));
    return batch;
}

OpLogBatchRecord MakeBatch(uint64_t batch_id, const std::string& key) {
    return MakeBatchAt(batch_id, batch_id, key);
}

DurablePrefix PrefixOf(uint64_t batch_id) {
    return DurablePrefix{.batch_id = batch_id, .last_seq = batch_id};
}

constexpr size_t kUnlimited = 0;

}  // namespace

TEST(SimpleHaWalTest, EmptyInitialState) {
    SimpleHaWal wal(/*max_batches=*/4, /*max_bytes=*/1024, DurablePrefix{});

    const auto state = wal.SnapshotState();
    EXPECT_EQ(state.first_batch_id, 1u);
    EXPECT_EQ(state.last_batch_id, 0u);
    EXPECT_EQ(state.durable_prefix.batch_id, 0u);
    EXPECT_EQ(state.durable_prefix.last_seq, 0u);
    EXPECT_FALSE(state.has_data);
    EXPECT_FALSE(wal.has_data());
    EXPECT_FALSE(wal.incarnation().empty());
    EXPECT_TRUE(wal.Pull(1, 16).empty());
}

TEST(SimpleHaWalTest, NonEmptyInitialPrefix) {
    const DurablePrefix initial{.batch_id = 7, .last_seq = 42};
    SimpleHaWal wal(/*max_batches=*/4, /*max_bytes=*/1024, initial);

    const auto state = wal.SnapshotState();
    EXPECT_EQ(state.first_batch_id, 8u);
    EXPECT_EQ(state.last_batch_id, 7u);
    EXPECT_EQ(state.durable_prefix, initial);
    EXPECT_TRUE(state.has_data);
    EXPECT_TRUE(wal.has_data());

    // The first batch must continue both halves of the durable prefix.
    EXPECT_EQ(wal.WriteBatchAndAdvancePrefix(MakeBatchAt(7, 43, "k7"), initial),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(wal.WriteBatchAndAdvancePrefix(MakeBatchAt(8, 8, "k8"), initial),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(wal.WriteBatchAndAdvancePrefix(MakeBatchAt(8, 43, "k8"), initial),
              ErrorCode::OK);
    const auto after_write = wal.SnapshotState();
    EXPECT_EQ(after_write.last_batch_id, 8u);
    EXPECT_EQ(after_write.durable_prefix,
              (DurablePrefix{.batch_id = 8, .last_seq = 43}));
}

TEST(SimpleHaWalTest, WriteAdvancesDurablePrefix) {
    SimpleHaWal wal(/*max_batches=*/8, /*max_bytes=*/kUnlimited,
                    DurablePrefix{});

    EXPECT_EQ(wal.WriteBatchAndAdvancePrefix(MakeBatch(1, "k1"), PrefixOf(0)),
              ErrorCode::OK);
    EXPECT_EQ(wal.WriteBatchAndAdvancePrefix(MakeBatch(2, "k2"), PrefixOf(1)),
              ErrorCode::OK);
    EXPECT_EQ(wal.WriteBatchAndAdvancePrefix(MakeBatch(3, "k3"), PrefixOf(2)),
              ErrorCode::OK);

    const auto state = wal.SnapshotState();
    EXPECT_EQ(state.first_batch_id, 1u);
    EXPECT_EQ(state.last_batch_id, 3u);
    EXPECT_EQ(state.durable_prefix.batch_id, 3u);
    EXPECT_EQ(state.durable_prefix.last_seq, 3u);
    EXPECT_TRUE(state.has_data);
}

TEST(SimpleHaWalTest, RejectsStaleExpectedPrefix) {
    SimpleHaWal wal(/*max_batches=*/8, /*max_bytes=*/kUnlimited,
                    DurablePrefix{});

    EXPECT_EQ(wal.WriteBatchAndAdvancePrefix(MakeBatch(1, "k1"), PrefixOf(0)),
              ErrorCode::OK);
    // Reusing the already-consumed prefix must not append or advance.
    EXPECT_EQ(wal.WriteBatchAndAdvancePrefix(MakeBatch(2, "k2"), PrefixOf(0)),
              ErrorCode::INVALID_PARAMS);
    // An ahead-of-time prefix is just as wrong.
    EXPECT_EQ(wal.WriteBatchAndAdvancePrefix(MakeBatch(2, "k2"), PrefixOf(2)),
              ErrorCode::INVALID_PARAMS);

    const auto state = wal.SnapshotState();
    EXPECT_EQ(state.first_batch_id, 1u);
    EXPECT_EQ(state.last_batch_id, 1u);
    EXPECT_EQ(state.durable_prefix, PrefixOf(1));
    EXPECT_EQ(wal.Pull(1, 16).size(), 1u);
}

TEST(SimpleHaWalTest, RejectsDiscontinuousBatchOrSequence) {
    SimpleHaWal wal(/*max_batches=*/8, /*max_bytes=*/kUnlimited,
                    DurablePrefix{});

    auto skipped_batch = MakeBatch(2, "k2");
    EXPECT_EQ(wal.WriteBatchAndAdvancePrefix(skipped_batch, PrefixOf(0)),
              ErrorCode::INVALID_PARAMS);

    auto skipped_seq = MakeBatch(1, "k1");
    skipped_seq.first_seq = 2;
    skipped_seq.last_seq = 2;
    skipped_seq.entries.front().sequence_id = 2;
    EXPECT_EQ(wal.WriteBatchAndAdvancePrefix(skipped_seq, PrefixOf(0)),
              ErrorCode::INVALID_PARAMS);

    EXPECT_FALSE(wal.has_data());
    EXPECT_EQ(wal.SnapshotState().last_batch_id, 0u);
}

TEST(SimpleHaWalTest, PullFiltersAndLimits) {
    SimpleHaWal wal(/*max_batches=*/16, /*max_bytes=*/kUnlimited,
                    DurablePrefix{});
    for (uint64_t id = 1; id <= 5; ++id) {
        ASSERT_EQ(wal.WriteBatchAndAdvancePrefix(
                      MakeBatch(id, "k" + std::to_string(id)),
                      PrefixOf(id - 1)),
                  ErrorCode::OK);
    }

    auto from_three = wal.Pull(3, 16);
    ASSERT_EQ(from_three.size(), 3u);
    for (size_t i = 0; i < from_three.size(); ++i) {
        EXPECT_EQ(from_three[i].batch_id, 3u + i);
    }

    auto limited = wal.Pull(1, 2);
    ASSERT_EQ(limited.size(), 2u);
    EXPECT_EQ(limited[0].batch_id, 1u);
    EXPECT_EQ(limited[1].batch_id, 2u);

    // max_batches == 0 disables the pull rather than meaning "all".
    EXPECT_TRUE(wal.Pull(1, 0).empty());
    // A cursor past the ring head returns nothing.
    EXPECT_TRUE(wal.Pull(6, 16).empty());
    // A cursor before the ring head is served from the oldest retained batch.
    auto from_zero = wal.Pull(0, 16);
    ASSERT_EQ(from_zero.size(), 5u);
    EXPECT_EQ(from_zero.front().batch_id, 1u);
}

TEST(SimpleHaWalTest, PullReturnsIndependentCopies) {
    SimpleHaWal wal(/*max_batches=*/8, /*max_bytes=*/kUnlimited,
                    DurablePrefix{});
    ASSERT_EQ(wal.WriteBatchAndAdvancePrefix(MakeBatch(1, "k1"), PrefixOf(0)),
              ErrorCode::OK);

    auto first_pull = wal.Pull(1, 16);
    ASSERT_EQ(first_pull.size(), 1u);
    first_pull.front().entries.front().payload = "mutated";

    auto second_pull = wal.Pull(1, 16);
    ASSERT_EQ(second_pull.size(), 1u);
    EXPECT_EQ(second_pull.front().entries.front().payload, "value-k1");
}

TEST(SimpleHaWalTest, PullRoundTripsThroughCodec) {
    const DurablePrefix initial{.batch_id = 8, .last_seq = 8};
    SimpleHaWal wal(/*max_batches=*/8, /*max_bytes=*/kUnlimited, initial);
    ASSERT_EQ(wal.WriteBatchAndAdvancePrefix(MakeBatchAt(9, 9, "object-key"),
                                             initial),
              ErrorCode::OK);

    auto pulled = wal.Pull(9, 16);
    ASSERT_EQ(pulled.size(), 1u);

    OpLogBatchRecord decoded;
    std::string reason;
    ASSERT_TRUE(DecodeOpLogBatchRecord(EncodeOpLogBatchRecord(pulled.front()),
                                       &decoded, &reason))
        << reason;
    EXPECT_EQ(decoded.batch_id, 9u);
    EXPECT_EQ(decoded.first_seq, 9u);
    EXPECT_EQ(decoded.last_seq, 9u);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries.front().sequence_id, 9u);
    EXPECT_EQ(decoded.entries.front().op_type, OpType::PUT_END);
    EXPECT_EQ(decoded.entries.front().tenant_id, "default");
    EXPECT_EQ(decoded.entries.front().object_key, "object-key");
    EXPECT_EQ(decoded.entries.front().payload, "value-object-key");
    EXPECT_NE(decoded.checksum, 0u);
}

TEST(SimpleHaWalTest, EvictsOldestByBatchCount) {
    SimpleHaWal wal(/*max_batches=*/2, /*max_bytes=*/kUnlimited,
                    DurablePrefix{});
    for (uint64_t id = 1; id <= 5; ++id) {
        ASSERT_EQ(wal.WriteBatchAndAdvancePrefix(MakeBatch(id, "k"), PrefixOf(id - 1)),
                  ErrorCode::OK);
    }

    const auto state = wal.SnapshotState();
    EXPECT_EQ(state.first_batch_id, 4u);
    EXPECT_EQ(state.last_batch_id, 5u);
    EXPECT_EQ(state.durable_prefix, PrefixOf(5));

    auto retained = wal.Pull(1, 16);
    ASSERT_EQ(retained.size(), 2u);
    EXPECT_EQ(retained[0].batch_id, 4u);
    EXPECT_EQ(retained[1].batch_id, 5u);
}

TEST(SimpleHaWalTest, EvictsOldestByByteBudget) {
    // All three batches encode to the same size: same-length keys, payloads,
    // and batch/sequence identifiers.
    const size_t one_batch =
        EncodeOpLogBatchRecord(MakeBatch(1, "k1")).size();

    SimpleHaWal wal(/*max_batches=*/kUnlimited,
                    /*max_bytes=*/one_batch * 2, DurablePrefix{});
    for (uint64_t id = 1; id <= 3; ++id) {
        ASSERT_EQ(wal.WriteBatchAndAdvancePrefix(
                      MakeBatch(id, "k" + std::to_string(id)), PrefixOf(id - 1)),
                  ErrorCode::OK);
    }

    const auto state = wal.SnapshotState();
    EXPECT_EQ(state.first_batch_id, 2u);
    EXPECT_EQ(state.last_batch_id, 3u);
    EXPECT_EQ(wal.Pull(1, 16).size(), 2u);
}

TEST(SimpleHaWalTest, KeepsNewestBatchEvenWhenItExceedsBudget) {
    SimpleHaWal wal(/*max_batches=*/kUnlimited, /*max_bytes=*/1,
                    DurablePrefix{});
    for (uint64_t id = 1; id <= 3; ++id) {
        ASSERT_EQ(wal.WriteBatchAndAdvancePrefix(MakeBatch(id, "k"), PrefixOf(id - 1)),
                  ErrorCode::OK);
    }

    const auto state = wal.SnapshotState();
    EXPECT_EQ(state.first_batch_id, 3u);
    EXPECT_EQ(state.last_batch_id, 3u);
    auto retained = wal.Pull(1, 16);
    ASSERT_EQ(retained.size(), 1u);
    EXPECT_EQ(retained.front().batch_id, 3u);
    EXPECT_TRUE(wal.has_data());
}

TEST(SimpleHaWalTest, ByteBudgetSpansMultipleBatches) {
    const size_t one_batch =
        EncodeOpLogBatchRecord(MakeBatch(1, "k1")).size();

    // Budget covers two batches; the third write evicts the oldest one only.
    SimpleHaWal wal(/*max_batches=*/kUnlimited,
                    /*max_bytes=*/one_batch * 2, DurablePrefix{});
    for (uint64_t id = 1; id <= 4; ++id) {
        ASSERT_EQ(wal.WriteBatchAndAdvancePrefix(
                      MakeBatch(id, "k" + std::to_string(id)), PrefixOf(id - 1)),
                  ErrorCode::OK);
        const auto state = wal.SnapshotState();
        EXPECT_LE(state.last_batch_id - state.first_batch_id + 1, 2u);
    }
}

TEST(SimpleHaWalTest, IncarnationIsStable) {
    SimpleHaWal wal(/*max_batches=*/4, /*max_bytes=*/kUnlimited,
                    DurablePrefix{});
    const std::string first = wal.incarnation();
    EXPECT_FALSE(first.empty());
    EXPECT_EQ(wal.incarnation(), first);
    EXPECT_EQ(wal.SnapshotState().incarnation, first);
}

TEST(SimpleHaWalTest, ConcurrentReadersSeeConsistentState) {
    constexpr uint64_t kBatchCount = 500;
    SimpleHaWal wal(/*max_batches=*/kBatchCount, /*max_bytes=*/kUnlimited,
                    DurablePrefix{});

    std::atomic<bool> done{false};
    std::thread reader([&] {
        while (!done.load(std::memory_order_acquire)) {
            const auto state = wal.SnapshotState();
            EXPECT_LE(state.first_batch_id, state.last_batch_id + 1);
            EXPECT_EQ(state.durable_prefix.batch_id, state.last_batch_id);

            auto pulled = wal.Pull(state.first_batch_id, 32);
            uint64_t previous = 0;
            for (const auto& batch : pulled) {
                EXPECT_GE(batch.batch_id, state.first_batch_id);
                if (previous != 0) {
                    EXPECT_EQ(batch.batch_id, previous + 1);
                }
                previous = batch.batch_id;
            }
        }
    });

    for (uint64_t id = 1; id <= kBatchCount; ++id) {
        ASSERT_EQ(wal.WriteBatchAndAdvancePrefix(
                      MakeBatch(id, "k" + std::to_string(id)), PrefixOf(id - 1)),
                  ErrorCode::OK);
    }
    done.store(true, std::memory_order_release);
    reader.join();

    EXPECT_EQ(wal.SnapshotState().last_batch_id, kBatchCount);
    EXPECT_EQ(wal.Pull(1, 16).size(), 16u);
}

}  // namespace mooncake::test
