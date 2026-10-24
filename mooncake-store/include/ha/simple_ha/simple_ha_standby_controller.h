#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/util/tl/expected.hpp>

#include "ha/oplog/oplog_applier.h"
#include "ha/simple_ha/simple_ha_types.h"
#include "ha/standby_controller.h"
#include "ha/standby_metadata_store.h"
#include "master_config.h"

namespace mooncake {
class SimpleHaLeaderService;
}

namespace mooncake::ha {

// Standby side of Simple HA. It pulls a leader-served snapshot first, then
// replays the leader's in-memory WAL ring. It never serves reads. On promotion
// it exports the same PromotionContext the batch OpLog path uses, so the
// supervisor's RestoreFromBatchOpLogPromotion remains unchanged.
class SimpleHaStandbyController final : public StandbyController {
   public:
    explicit SimpleHaStandbyController(
        const MasterServiceSupervisorConfig& config);
    ~SimpleHaStandbyController() override;

    ErrorCode StartStandby(
        const std::optional<MasterView>& observed_leader) override;
    void StopStandby() override;
    ErrorCode PromoteStandby() override;
    tl::expected<PromotionContext, ErrorCode> PromoteStandbyAndExport()
        override;
    void UpdateObservedLeader(
        const std::optional<MasterView>& observed_leader) override;
    MasterRuntimeState GetStandbyRuntimeState() const override;
    void SetStandbyRuntimeStateCallback(RuntimeStateCallback callback) override;
    ErrorCode RestorePromotionBaseline(PromotionContext context) override;

   private:
    void RunLoop();
    void StopLoop();
    void WaitForNextPoll(std::chrono::milliseconds delay);
    void SetRuntimeState(MasterRuntimeState state);
    bool IsEmpty() const;

    bool EnsureConnected();
    void DropConnection();
    tl::expected<SimpleHaGetStateResponse, ErrorCode> CallGetState();
    tl::expected<SimpleHaPullWalResponse, ErrorCode> CallPullWal(
        uint64_t from_batch_id, uint32_t max_batches);
    tl::expected<SimpleHaBeginSnapshotResponse, ErrorCode> CallBeginSnapshot();
    tl::expected<SimpleHaSnapshotChunkResponse, ErrorCode> CallSnapshotChunk(
        const std::string& snapshot_id, uint32_t tenant_index,
        uint32_t shard_index, uint32_t offset, uint32_t max_entries);
    tl::expected<SimpleHaProbeResponse, ErrorCode> CallProbe();

    ErrorCode LoadSnapshotFromPeer();
    ErrorCode PullOnce();

    const MasterServiceSupervisorConfig config_;
    std::string peer_host_;
    std::string peer_port_;

    std::mutex client_mutex_;
    coro_rpc::coro_rpc_client client_;
    bool connected_{false};

    std::unique_ptr<StandbyMetadataStore> metadata_store_;
    std::unique_ptr<OpLogApplier> applier_;
    // Protects the store/applier pointers against the promotion readiness
    // check racing a snapshot swap in the pull loop.
    mutable std::mutex store_mutex_;
    // Standby-side GetState/Probe endpoint. Both roles bind the configured
    // Simple HA port, so an empty candidate can ask this node whether it holds
    // data before deciding to serve empty.
    std::unique_ptr<SimpleHaLeaderService> probe_service_;

    mutable std::mutex incarnation_mutex_;
    std::string incarnation_;

    std::atomic<uint64_t> applied_batch_id_{0};
    std::atomic<uint64_t> applied_seq_{0};
    std::atomic<uint64_t> peer_durable_seq_{0};
    std::atomic<bool> has_lineage_{false};
    std::atomic<bool> synced_{false};
    std::atomic<bool> needs_resync_{false};
    // True while a snapshot merge is in flight or failed partway: the pull
    // loop keeps retrying the merge, but the records already present remain
    // promotable.
    std::atomic<bool> merge_incomplete_{false};
    std::atomic<bool> running_{false};

    std::thread loop_thread_;
    std::mutex loop_mutex_;
    std::condition_variable loop_cv_;

    std::mutex lifecycle_mutex_;
    bool preserve_baseline_{false};
    DurablePrefix preserved_cursor_{};
    std::vector<StandbySegmentInfo> preserved_segments_;

    std::mutex observed_mutex_;
    std::optional<MasterView> observed_leader_;

    mutable std::mutex callback_mutex_;
    RuntimeStateCallback runtime_state_callback_;
    std::atomic<MasterRuntimeState> runtime_state_{
        MasterRuntimeState::kStandby};
};

}  // namespace mooncake::ha
