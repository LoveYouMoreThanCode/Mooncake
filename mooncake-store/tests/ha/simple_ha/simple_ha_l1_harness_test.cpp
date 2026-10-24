#include "ha/simple_ha/simple_ha_standby_controller.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ha/simple_ha/simple_ha_leader_service.h"
#include "master_service.h"
#include "metadata_store.h"
#include "types.h"

namespace mooncake::test {

// In-process harness: the peer leader is a real MasterService +
// SimpleHaLeaderService on localhost, and the test plays the role of the
// supervisor (starting the standby, stopping the peer, triggering promotion).
// No etcd is involved, so merge/promotion interleavings are deterministic.
class SimpleHaStandbyControllerTestPeer {
   public:
    explicit SimpleHaStandbyControllerTestPeer(
        ha::SimpleHaStandbyController& controller)
        : controller_(controller) {}

    bool MergeIncomplete() const {
        return controller_.merge_incomplete_.load(std::memory_order_acquire);
    }

    bool StoreContains(const std::string& tenant,
                       const std::string& key) const {
        std::lock_guard<std::mutex> lock(controller_.store_mutex_);
        return controller_.metadata_store_ &&
               controller_.metadata_store_->Exists(tenant, key);
    }

    size_t StoreKeyCount() const {
        std::lock_guard<std::mutex> lock(controller_.store_mutex_);
        return controller_.metadata_store_
                   ? controller_.metadata_store_->GetKeyCount()
                   : 0;
    }

    uint64_t AppliedBatchId() const {
        return controller_.applied_batch_id_.load(std::memory_order_acquire);
    }

    bool HasSegment(const std::string& transport_endpoint) const {
        std::lock_guard<std::mutex> lock(controller_.store_mutex_);
        return controller_.applier_ &&
               controller_.applier_->GetSegmentRegistry().HasSegment(
                   transport_endpoint);
    }

   private:
    ha::SimpleHaStandbyController& controller_;
};

namespace {

using namespace std::chrono_literals;

constexpr char kClusterId[] = "simple-ha-l1";
constexpr size_t kSegmentBase = 0x700000000;
constexpr size_t kSegmentSize = 16 * 1024 * 1024;

uint16_t AllocateFreePort() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return 0;
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) !=
        0) {
        ::close(fd);
        return 0;
    }
    socklen_t length = sizeof(address);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) !=
        0) {
        ::close(fd);
        return 0;
    }
    ::close(fd);
    return ntohs(address.sin_port);
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

MasterServiceConfig MakeMasterConfig(uint64_t wal_max_batches = 8192,
                                     uint32_t batch_entries = 1024) {
    auto config = MasterServiceConfig::builder()
                      .set_enable_ha(true)
                      .set_oplog_batch_max_entries(batch_entries)
                      .build();
    config.enable_simple_ha = true;
    config.simple_ha_wal_max_batches = wal_max_batches;
    config.simple_ha_wal_max_bytes = 0;
    config.cluster_id = kClusterId;
    return config;
}

MasterServiceSupervisorConfig MakeControllerConfig(uint16_t peer_port,
                                                   uint16_t probe_port) {
    MasterServiceSupervisorConfig config;
    config.enable_simple_ha = true;
    config.simple_ha_peer_address =
        "127.0.0.1:" + std::to_string(peer_port);
    config.simple_ha_rpc_address = "127.0.0.1";
    config.simple_ha_rpc_port = probe_port;
    config.simple_ha_pull_interval_ms = 20;
    config.simple_ha_snapshot_chunk_entries = 1;
    config.cluster_id = kClusterId;
    return config;
}

bool WaitFor(const std::function<bool()>& predicate,
             std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return predicate();
}

bool HasSegmentEndpoint(const std::vector<StandbySegmentInfo>& segments,
                        const std::string& transport_endpoint) {
    return std::any_of(
        segments.begin(), segments.end(),
        [&transport_endpoint](const StandbySegmentInfo& segment) {
            return segment.transport_endpoint == transport_endpoint;
        });
}

class LeaderHarness {
   public:
    // A small WAL ring makes the peer's first_batch_id jump ahead of the
    // local baseline, which forces the controller into a snapshot merge
    // instead of an incremental WAL catch-up.
    explicit LeaderHarness(uint64_t wal_max_batches = 8192,
                           uint32_t batch_entries = 1024)
        : master_(MakeMasterConfig(wal_max_batches, batch_entries)) {}
    ~LeaderHarness() { Stop(); }

    LeaderHarness(const LeaderHarness&) = delete;
    LeaderHarness& operator=(const LeaderHarness&) = delete;

    void Mount(const std::string& name = "l1-peer-segment",
               size_t base = kSegmentBase) {
        Segment segment = MakeSegment(name, base);
        ASSERT_TRUE(master_.MountSegment(segment, client_id_).has_value());
        segments_[name] = segment;
        segment_name_ = name;
    }

    void PutCompleted(const std::string& key,
                      const std::string& segment = {}) {
        const std::string& target = segment.empty() ? segment_name_ : segment;
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segments = {target};
        ASSERT_TRUE(master_
                        .PutStart(client_id_, key, TenantId::Default(),
                                  /*slice_length=*/1024, config)
                        .has_value());
        ASSERT_TRUE(master_
                        .PutEnd(client_id_, key, TenantId::Default(),
                                ReplicaType::MEMORY)
                        .has_value());
        // Let the writer seal the entry before the next PutStart: seeding
        // uses a one-batch admission window in some tests.
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }

    void Remove(const std::string& key) {
        ASSERT_TRUE(
            master_.Remove(key, TenantId::Default()).has_value());
    }

    void UnmountSegment(const std::string& name) {
        auto it = segments_.find(name);
        ASSERT_NE(it, segments_.end());
        ASSERT_TRUE(
            master_.UnmountSegment(it->second.id, client_id_).has_value());
        segments_.erase(it);
        if (segment_name_ == name) {
            segment_name_.clear();
        }
    }

    bool Start() {
        for (int attempt = 0; attempt < 8; ++attempt) {
            const uint16_t port = AllocateFreePort();
            if (port == 0) {
                continue;
            }
            service_ = std::make_unique<SimpleHaLeaderService>(
                master_, port, /*address=*/"127.0.0.1",
                /*snapshot_chunk_entries=*/1);
            if (service_->Start()) {
                port_ = port;
                return true;
            }
            service_.reset();
        }
        return false;
    }

    void Stop() { service_.reset(); }

    uint16_t port() const { return port_; }

    MasterService& master() { return master_; }

    void StopWriter() { master_.StopBatchOpLogWriter(); }

    ha::PromotionContext Drain() {
        ha::PromotionContext context;
        EXPECT_TRUE(master_.DrainIntoPromotionContext(context).has_value());
        return context;
    }

   private:
    MasterService master_;
    std::unique_ptr<SimpleHaLeaderService> service_;
    UUID client_id_{generate_uuid()};
    std::unordered_map<std::string, Segment> segments_;
    std::string segment_name_;
    uint16_t port_{0};
};

class StandbyHarness {
   public:
    explicit StandbyHarness(uint16_t peer_port)
        : config_(MakeControllerConfig(peer_port, AllocateFreePort())),
          controller_(
              std::make_unique<ha::SimpleHaStandbyController>(config_)) {}

    ~StandbyHarness() { controller_->StopStandby(); }

    StandbyHarness(const StandbyHarness&) = delete;
    StandbyHarness& operator=(const StandbyHarness&) = delete;

    ha::SimpleHaStandbyController& controller() { return *controller_; }

    void Install(ha::PromotionContext context) {
        ASSERT_EQ(controller_->RestorePromotionBaseline(std::move(context)),
                  ErrorCode::OK);
    }

    bool Start() {
        return controller_->StartStandby(std::nullopt) == ErrorCode::OK;
    }

    tl::expected<ha::PromotionContext, ErrorCode> Promote() {
        return controller_->PromoteStandbyAndExport();
    }

   private:
    MasterServiceSupervisorConfig config_;
    std::unique_ptr<ha::SimpleHaStandbyController> controller_;
};

}  // namespace

TEST(SimpleHaL1HarnessTest, BaselineSurvivesUnreachablePeer) {
    LeaderHarness local;
    local.Mount("l1-local-segment");
    local.PutCompleted("local-a");
    local.PutCompleted("local-b");
    local.StopWriter();
    auto baseline = local.Drain();
    ASSERT_NE(baseline.metadata_store, nullptr);

    const uint16_t dead_peer = AllocateFreePort();
    ASSERT_NE(dead_peer, 0);
    StandbyHarness standby(dead_peer);
    standby.Install(std::move(baseline));
    ASSERT_TRUE(standby.Start());

    auto promoted = standby.Promote();
    ASSERT_TRUE(promoted.has_value()) << toString(promoted.error());
    EXPECT_TRUE(promoted->metadata_store->Exists("default", "local-a"));
    EXPECT_TRUE(promoted->metadata_store->Exists("default", "local-b"));
    EXPECT_EQ(promoted->metadata_store->GetKeyCount(), 2u);
}

TEST(SimpleHaL1HarnessTest, MergeKeepsLocalOnlyAndPeerWinsOnConflict) {
    LeaderHarness peer(/*wal_max_batches=*/1, /*batch_entries=*/4);
    peer.Mount("l1-peer-segment");
    for (int i = 0; i < 32; ++i) {
        peer.PutCompleted("peer-" + std::to_string(i));
    }
    peer.PutCompleted("shared");
    peer.PutCompleted("peer-only");
    ASSERT_TRUE(peer.Start());

    LeaderHarness local;
    local.Mount("l1-local-segment");
    local.PutCompleted("shared");
    local.PutCompleted("local-only");
    local.StopWriter();
    auto baseline = local.Drain();
    ASSERT_NE(baseline.metadata_store, nullptr);

    StandbyHarness standby(peer.port());
    standby.Install(std::move(baseline));
    ASSERT_TRUE(standby.Start());

    SimpleHaStandbyControllerTestPeer test_peer(standby.controller());
    ASSERT_TRUE(WaitFor([&] {
        return !test_peer.MergeIncomplete() &&
               test_peer.StoreContains("default", "peer-only");
    })) << "store keys=" << test_peer.StoreKeyCount();

    auto promoted = standby.Promote();
    ASSERT_TRUE(promoted.has_value()) << toString(promoted.error());
    EXPECT_TRUE(promoted->metadata_store->Exists("default", "local-only"));
    EXPECT_TRUE(promoted->metadata_store->Exists("default", "peer-only"));

    // peer wins on the shared key: its replica lives on the peer's segment.
    const auto shared =
        promoted->metadata_store->GetMetadata("default", "shared");
    ASSERT_TRUE(shared.has_value());
    bool peer_segment = false;
    for (const auto& replica : shared->replicas) {
        if (replica.is_memory_replica() &&
            replica.get_memory_descriptor()
                    .buffer_descriptor.transport_endpoint_ ==
                "l1-peer-segment") {
            peer_segment = true;
        }
    }
    EXPECT_TRUE(peer_segment);
}

TEST(SimpleHaL1HarnessTest, WalRemoveDeletesMergedKey) {
    LeaderHarness peer;
    peer.Mount();
    peer.PutCompleted("doomed");
    peer.PutCompleted("keeper");
    ASSERT_TRUE(peer.Start());

    StandbyHarness standby(peer.port());
    ASSERT_TRUE(standby.Start());

    SimpleHaStandbyControllerTestPeer test_peer(standby.controller());
    ASSERT_TRUE(WaitFor([&] {
        return !test_peer.MergeIncomplete() &&
               test_peer.StoreContains("default", "doomed") &&
               test_peer.StoreContains("default", "keeper");
    }));

    peer.Remove("doomed");
    ASSERT_TRUE(WaitFor(
        [&] { return !test_peer.StoreContains("default", "doomed"); }));
    EXPECT_TRUE(test_peer.StoreContains("default", "keeper"));
}

TEST(SimpleHaL1HarnessTest, WalSegmentOpsReachPromotion) {
    LeaderHarness peer;
    peer.Mount("l1-unmount-segment");
    peer.PutCompleted("unmount-doomed");
    ASSERT_TRUE(peer.Start());

    StandbyHarness standby(peer.port());
    ASSERT_TRUE(standby.Start());

    SimpleHaStandbyControllerTestPeer test_peer(standby.controller());
    ASSERT_TRUE(WaitFor([&] {
        return !test_peer.MergeIncomplete() &&
               test_peer.StoreContains("default", "unmount-doomed") &&
               test_peer.HasSegment("l1-unmount-segment");
    }));

    // Mounted after the standby's baseline, so this segment reaches the
    // standby through WAL replay instead of the snapshot's segment list.
    peer.Mount("l1-keep-segment", kSegmentBase + kSegmentSize);
    peer.PutCompleted("keep-alive", "l1-keep-segment");
    ASSERT_TRUE(WaitFor([&] {
        return test_peer.HasSegment("l1-keep-segment") &&
               test_peer.StoreContains("default", "keep-alive");
    }));

    // Unmounting drops the objects on the segment (through the REMOVE /
    // PUT_END entries emitted by ClearStaleHandles) and then the segment
    // itself (SEGMENT_UNMOUNT). Both must be replayed before promotion.
    peer.UnmountSegment("l1-unmount-segment");
    ASSERT_TRUE(WaitFor([&] {
        return !test_peer.HasSegment("l1-unmount-segment") &&
               !test_peer.StoreContains("default", "unmount-doomed");
    }));

    auto promoted = standby.Promote();
    ASSERT_TRUE(promoted.has_value()) << toString(promoted.error());
    EXPECT_TRUE(promoted->metadata_store->Exists("default", "keep-alive"));
    EXPECT_FALSE(promoted->metadata_store->Exists("default", "unmount-doomed"));
    EXPECT_TRUE(HasSegmentEndpoint(promoted->segments, "l1-keep-segment"));
    EXPECT_FALSE(HasSegmentEndpoint(promoted->segments, "l1-unmount-segment"));
}

#if defined(MOONCAKE_ENABLE_TEST_FAILPOINTS)
TEST(SimpleHaL1HarnessTest, MidMergePeerDeathKeepsLocalRecords) {
    LeaderHarness peer(/*wal_max_batches=*/1, /*batch_entries=*/4);
    peer.Mount();
    for (int i = 0; i < 32; ++i) {
        peer.PutCompleted("peer-" + std::to_string(i));
    }
    ASSERT_TRUE(peer.Start());

    LeaderHarness local;
    local.Mount("l1-local-segment");
    local.PutCompleted("local-only");
    local.StopWriter();
    auto baseline = local.Drain();
    ASSERT_NE(baseline.metadata_store, nullptr);

    const auto failpoint_dir =
        std::filesystem::temp_directory_path() /
        ("simple-ha-l1-" + std::to_string(::getpid()));
    std::filesystem::remove_all(failpoint_dir);
    std::filesystem::create_directories(failpoint_dir);
    ASSERT_EQ(::setenv("MOONCAKE_TEST_FAILPOINT_DIR",
                       failpoint_dir.c_str(), 1),
              0);
    const auto arm = failpoint_dir / "simple_ha_snapshot_chunk_merged.arm";
    const auto hit = failpoint_dir / "simple_ha_snapshot_chunk_merged.hit";
    const auto release =
        failpoint_dir / "simple_ha_snapshot_chunk_merged.release";
    {
        std::ofstream output(arm);
        ASSERT_TRUE(output.good());
    }

    StandbyHarness standby(peer.port());
    standby.Install(std::move(baseline));
    ASSERT_TRUE(standby.Start());

    ASSERT_TRUE(WaitFor([&] { return std::filesystem::exists(hit); }))
        << "merge never reached the failpoint";
    peer.Stop();
    {
        std::ofstream output(release);
        ASSERT_TRUE(output.good());
    }

    auto promoted = standby.Promote();
    ASSERT_TRUE(promoted.has_value()) << toString(promoted.error());
    EXPECT_TRUE(promoted->metadata_store->Exists("default", "local-only"));
    EXPECT_GE(promoted->metadata_store->GetKeyCount(), 1u);

    ::unsetenv("MOONCAKE_TEST_FAILPOINT_DIR");
    std::filesystem::remove_all(failpoint_dir);
}
#endif

}  // namespace mooncake::test
