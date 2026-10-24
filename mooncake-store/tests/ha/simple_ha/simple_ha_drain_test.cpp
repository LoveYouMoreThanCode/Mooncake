#include "ha/standby_controller.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "master_service.h"
#include "master_service/master_service_test_peer.h"
#include "metadata_store.h"
#include "types.h"

namespace mooncake::test {
namespace {

constexpr size_t kSegmentBase = 0x600000000;
constexpr size_t kSegmentSize = 16 * 1024 * 1024;

MasterServiceConfig MakeConfig() {
    auto config = MasterServiceConfig::builder()
                      .set_enable_ha(true)
                      .set_oplog_batch_max_entries(1024)
                      .build();
    config.simple_ha.enable = true;
    config.simple_ha.wal_max_batches = 8192;
    config.simple_ha.wal_max_bytes = 0;
    return config;
}

Segment MakeSegment(const std::string& name, size_t base = kSegmentBase) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = name;
    segment.base = base;
    segment.size = kSegmentSize;
    segment.te_endpoint = name;
    return segment;
}

size_t CountObjects(MasterService& service) {
    MasterServiceTestPeer peer(service);
    size_t count = 0;
    peer.VisitTenants([&](const TenantId& tenant) {
        for (auto object : peer.ReadCursor(tenant)) {
            (void)object;
            ++count;
        }
    });
    return count;
}

void PutCompleted(MasterService& service, const UUID& client_id,
                  const std::string& segment_name, const std::string& key) {
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segments = {segment_name};
    ASSERT_TRUE(service
                    .PutStart(client_id, key, TenantId::Default(),
                              /*slice_length=*/1024, config)
                    .has_value());
    ASSERT_TRUE(service
                    .PutEnd(client_id, key, TenantId::Default(),
                            ReplicaType::MEMORY)
                    .has_value());
}

StandbyObjectEntry MakeStandbyObject(const std::string& key,
                                     const std::string& endpoint,
                                     uintptr_t address) {
    StandbyObjectMetadata metadata;
    metadata.client_id = generate_uuid();
    metadata.size = 1024;
    Replica::Descriptor replica;
    replica.id = 1;
    replica.status = ReplicaStatus::COMPLETE;
    MemoryDescriptor memory;
    memory.buffer_descriptor.transport_endpoint_ = endpoint;
    memory.buffer_descriptor.buffer_address_ = address;
    memory.buffer_descriptor.size_ = metadata.size;
    replica.descriptor_variant = std::move(memory);
    metadata.replicas.push_back(std::move(replica));
    return StandbyObjectEntry{"default", key, std::move(metadata)};
}

StandbySegmentInfo MakeStandbySegment(const std::string& endpoint) {
    StandbySegmentInfo segment;
    segment.segment_name = endpoint;
    segment.transport_endpoint = endpoint;
    segment.capacity = kSegmentSize;
    segment.is_memory_segment = true;
    return segment;
}

}  // namespace

TEST(SimpleHaDrainTest, MovesObjectsIntoPromotionContext) {
    MasterService service(MakeConfig());
    const UUID client_id = generate_uuid();
    const std::string segment_name = "simple_ha_drain_segment";
    ASSERT_TRUE(
        service.MountSegment(MakeSegment(segment_name), client_id).has_value());
    PutCompleted(service, client_id, segment_name, "drain-key-a");
    PutCompleted(service, client_id, segment_name, "drain-key-b");

    service.StopBatchOpLogWriter();

    ha::PromotionContext context;
    const auto drained = service.DrainIntoPromotionContext(context);
    ASSERT_TRUE(drained.has_value()) << toString(drained.error());

    ASSERT_NE(context.metadata_store, nullptr);
    EXPECT_EQ(context.metadata_store->GetKeyCount(), 2u);
    EXPECT_EQ(CountObjects(service), 0u);
    EXPECT_GT(context.applied_cursor.batch_id, 0u);
    EXPECT_EQ(context.applied_seq_id, context.applied_cursor.last_seq);
    ASSERT_EQ(context.segments.size(), 1u);
    EXPECT_EQ(context.segments.front().segment_name, segment_name);
    EXPECT_EQ(context.segments.front().transport_endpoint, segment_name);

    ReplicaID stored_max = 0;
    ASSERT_TRUE(context.metadata_store->ValidateReplicaIds(stored_max));
    EXPECT_EQ(context.max_replica_id, stored_max);
    EXPECT_GT(context.max_replica_id, 0u);

    // The context is directly consumable by the promotion restore path.
    MasterService restored(
        MasterServiceConfig::builder().set_enable_ha(false).build());
    ASSERT_TRUE(restored
                    .RestoreFromBatchOpLogPromotion(
                        *context.metadata_store, context.applied_cursor,
                        context.segments, context.max_replica_id,
                        /*chunk_object_count=*/1)
                    .has_value());
    EXPECT_EQ(CountObjects(restored), 2u);

    // Replica ids continue after the retained maximum.
    const std::string new_segment_name = "simple_ha_drain_new_segment";
    ASSERT_TRUE(
        restored
            .MountSegment(MakeSegment(new_segment_name, kSegmentBase + kSegmentSize),
                          client_id)
            .has_value());
    PutCompleted(restored, client_id, new_segment_name, "drain-key-new");

    MasterServiceTestPeer peer(restored);
    bool saw_new = false;
    for (auto object : peer.ReadCursor(TenantId::Default())) {
        if (object.key() != "drain-key-new") {
            continue;
        }
        saw_new = true;
        for (const auto& replica : object.metadata().GetAllReplicas()) {
            EXPECT_GT(replica.id(), context.max_replica_id);
        }
    }
    EXPECT_TRUE(saw_new);
}

TEST(SimpleHaDrainTest, DropsObjectsWithoutCompletedReplicas) {
    MasterService service(MakeConfig());
    const UUID client_id = generate_uuid();
    const std::string segment_name = "simple_ha_drain_processing_segment";
    ASSERT_TRUE(
        service.MountSegment(MakeSegment(segment_name), client_id).has_value());
    PutCompleted(service, client_id, segment_name, "drain-completed");
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segments = {segment_name};
    ASSERT_TRUE(service
                    .PutStart(client_id, "drain-processing",
                              TenantId::Default(), /*slice_length=*/1024,
                              config)
                    .has_value());

    service.StopBatchOpLogWriter();
    ha::PromotionContext context;
    const auto drained = service.DrainIntoPromotionContext(context);
    ASSERT_TRUE(drained.has_value()) << toString(drained.error());
    ASSERT_NE(context.metadata_store, nullptr);
    EXPECT_TRUE(context.metadata_store->Exists("default", "drain-completed"));
    EXPECT_FALSE(context.metadata_store->Exists("default", "drain-processing"));
    EXPECT_EQ(context.metadata_store->GetKeyCount(), 1u);
    EXPECT_EQ(CountObjects(service), 0u);
}

TEST(SimpleHaDrainTest, PreservesGroupHardPinAndSession) {
    MasterService service(MakeConfig());
    const UUID client_id = generate_uuid();
    const std::string segment_name = "simple_ha_drain_fields_segment";
    ASSERT_TRUE(
        service.MountSegment(MakeSegment(segment_name), client_id).has_value());

    const std::string key = "drain-fields-key";
    auto object = MakeStandbyObject(key, segment_name, /*address=*/0);
    object.metadata.group_id = "drain-group";
    object.metadata.hard_pinned = true;
    object.metadata.session_id = "drain-session";
    ASSERT_TRUE(service
                    .RestoreFromStandbySnapshot(
                        {object}, 7, {MakeStandbySegment(segment_name)})
                    .has_value());

    service.StopBatchOpLogWriter();
    ha::PromotionContext context;
    const auto drained = service.DrainIntoPromotionContext(context);
    ASSERT_TRUE(drained.has_value()) << toString(drained.error());
    ASSERT_NE(context.metadata_store, nullptr);
    const auto stored = context.metadata_store->GetMetadata("default", key);
    ASSERT_TRUE(stored.has_value());
    EXPECT_EQ(stored->group_id, "drain-group");
    EXPECT_TRUE(stored->hard_pinned.value_or(false));
    EXPECT_EQ(stored->session_id.value_or(""), "drain-session");
    ASSERT_EQ(stored->replicas.size(), 1u);
    EXPECT_EQ(stored->replicas.front().status, ReplicaStatus::COMPLETE);
    EXPECT_EQ(CountObjects(service), 0u);
}

}  // namespace mooncake::test
