#include "master_service.h"
#include "master_service/master_service_test_peer.h"

#include <gtest/gtest.h>

#include <string>
#include <utility>

#include "metadata_store.h"
#include "types.h"

namespace mooncake::test {
namespace {

constexpr size_t kSegmentCapacity = 1 << 20;

Replica::Descriptor MakeMemoryReplica(const std::string& endpoint, size_t size,
                                      uintptr_t address) {
    Replica::Descriptor replica;
    replica.id = 1;
    replica.status = ReplicaStatus::COMPLETE;

    MemoryDescriptor memory;
    memory.buffer_descriptor.transport_endpoint_ = endpoint;
    memory.buffer_descriptor.buffer_address_ = address;
    memory.buffer_descriptor.size_ = size;
    replica.descriptor_variant = std::move(memory);
    return replica;
}

StandbyObjectEntry MakeStandbyObject(const std::string& key,
                                     const std::string& endpoint,
                                     uintptr_t address, size_t size = 1024) {
    StandbyObjectMetadata metadata;
    metadata.client_id = generate_uuid();
    metadata.size = size;
    metadata.replicas.push_back(
        MakeMemoryReplica(endpoint, size, address));
    return StandbyObjectEntry{"default", key, std::move(metadata)};
}

StandbySegmentInfo MakeStandbySegment(const std::string& endpoint) {
    StandbySegmentInfo segment;
    segment.segment_name = endpoint;
    segment.transport_endpoint = endpoint;
    segment.capacity = kSegmentCapacity;
    segment.is_memory_segment = true;
    return segment;
}

struct LeaseObservation {
    bool saw_plain{false};
    bool saw_grouped{false};
    bool plain_lease_active{false};
    bool grouped_lease_active{false};
};

LeaseObservation ObserveRestoredLeases(MasterService& service,
                                       const std::string& plain_key,
                                       const std::string& grouped_key) {
    LeaseObservation observation;
    MasterServiceTestPeer peer(service);
    for (auto object : peer.ReadCursor(TenantId::Default())) {
        if (object.key() == plain_key) {
            observation.saw_plain = true;
            observation.plain_lease_active = !object.metadata().IsLeaseExpired();
        } else if (object.key() == grouped_key) {
            observation.saw_grouped = true;
            observation.grouped_lease_active =
                !object.metadata().IsLeaseExpired();
        }
    }
    return observation;
}

}  // namespace

TEST(SimpleHaRestoreLeaseTest, RestoreGrantsReadLeaseToRestoredObjects) {
    MasterServiceConfig config =
        MasterServiceConfig::builder().set_enable_ha(false).build();
    config.enable_simple_ha = true;
    MasterService service(config);

    const std::string endpoint = "simple_ha_restore_lease_segment";
    const std::string plain_key = "simple_ha_restore_lease_plain";
    const std::string grouped_key = "simple_ha_restore_lease_grouped";

    auto plain = MakeStandbyObject(plain_key, endpoint, /*address=*/0);
    auto grouped =
        MakeStandbyObject(grouped_key, endpoint, /*address=*/4096);
    grouped.metadata.group_id = "simple_ha_restore_lease_group";

    ASSERT_TRUE(service
                    .RestoreFromStandbySnapshot(
                        {plain, grouped}, 7, {MakeStandbySegment(endpoint)})
                    .has_value());

    const auto observation =
        ObserveRestoredLeases(service, plain_key, grouped_key);
    EXPECT_TRUE(observation.saw_plain);
    EXPECT_TRUE(observation.saw_grouped);
    // A client may still be reading under the lease granted by the previous
    // primary. Restored objects (and their group) must not look evictable.
    EXPECT_TRUE(observation.plain_lease_active);
    EXPECT_TRUE(observation.grouped_lease_active);
}

TEST(SimpleHaRestoreLeaseTest, LegacyRestoreKeepsObjectsImmediatelyEvictable) {
    // Only Simple HA grants a lease on restore. The batch-OpLog and legacy
    // standby paths must keep their pre-existing behavior.
    MasterService service(
        MasterServiceConfig::builder().set_enable_ha(false).build());

    const std::string endpoint = "simple_ha_restore_no_lease_segment";
    const std::string plain_key = "simple_ha_restore_no_lease_plain";
    const std::string grouped_key = "simple_ha_restore_no_lease_grouped";

    auto plain = MakeStandbyObject(plain_key, endpoint, /*address=*/0);
    auto grouped =
        MakeStandbyObject(grouped_key, endpoint, /*address=*/4096);
    grouped.metadata.group_id = "simple_ha_restore_no_lease_group";

    ASSERT_TRUE(service
                    .RestoreFromStandbySnapshot(
                        {plain, grouped}, 7, {MakeStandbySegment(endpoint)})
                    .has_value());

    const auto observation =
        ObserveRestoredLeases(service, plain_key, grouped_key);
    EXPECT_TRUE(observation.saw_plain);
    EXPECT_TRUE(observation.saw_grouped);
    EXPECT_FALSE(observation.plain_lease_active);
    EXPECT_FALSE(observation.grouped_lease_active);
}

}  // namespace mooncake::test
