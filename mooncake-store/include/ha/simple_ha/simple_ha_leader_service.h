#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "ha/oplog/oplog_batch_types.h"
#include "ha/simple_ha/simple_ha_types.h"
#include "ha/simple_ha/simple_ha_wal.h"
#include "types.h"

namespace mooncake {

class MasterService;
class SimpleHaWal;

// Leader-side RPC endpoint for Simple HA. The supervisor starts it only while
// this process is the serving primary; standbys pull snapshots and WAL batches
// from it. The service does not replicate anything by itself: every write
// already reaches the in-memory SimpleHaWal through OrderedOpLogWriter.
class SimpleHaLeaderService {
   public:
    struct StandbyState {
        bool has_data{false};
        uint64_t applied_batch_id{0};
        uint64_t applied_seq{0};
        uint64_t key_count{0};
        std::string incarnation;
    };
    using StandbyStateFn = std::function<StandbyState()>;

    SimpleHaLeaderService(MasterService& master, uint16_t port,
                          std::string address,
                          uint32_t snapshot_chunk_entries);
    // Standby-side probe endpoint. It serves GetState/Probe from the local
    // applied state and rejects WAL/snapshot calls with NOT_LEADER. Both nodes
    // bind the same configured port in their respective roles, so an empty
    // candidate can ask a standby whether it holds data before serving empty.
    SimpleHaLeaderService(uint16_t port, std::string address,
                          uint32_t snapshot_chunk_entries,
                          StandbyStateFn standby_state);
    ~SimpleHaLeaderService();

    SimpleHaLeaderService(const SimpleHaLeaderService&) = delete;
    SimpleHaLeaderService& operator=(const SimpleHaLeaderService&) = delete;

    bool Start();
    void Stop();

    // Stop serving snapshot sessions and invalidate any in-flight one. The
    // caller uses this when the serving map is about to be drained: a
    // partially drained map must not be captured under the pre-drain base.
    // GetState/PullWal stay available so a promoting standby can still catch
    // up the frozen WAL tail. Waiters already inside a chunk finish against
    // the pre-drain map; no chunk can observe the drain.
    void DisableSnapshots();

    // RPC handlers.
    SimpleHaGetStateResponse GetState();
    SimpleHaPullWalResponse PullWal(SimpleHaPullWalRequest request);
    SimpleHaBeginSnapshotResponse BeginSnapshot();
    SimpleHaSnapshotChunkResponse SnapshotChunk(
        SimpleHaSnapshotChunkRequest request);
    SimpleHaProbeResponse Probe();

   private:
    struct SnapshotSession {
        std::string snapshot_id;
        std::string incarnation;
        DurablePrefix base;
        std::vector<std::string> tenants;
        size_t tenant_index{0};
        size_t shard_index{0};
        std::vector<SimpleHaSnapshotEntry> staged;
        size_t staged_offset{0};
        bool done{false};
    };

    std::vector<SimpleHaSegmentEntry> SnapshotSegments() const;
    static SimpleHaGetStateResponse MakeStateResponse(
        const SimpleHaWalState& state);

    MasterService* master_{nullptr};
    std::shared_ptr<SimpleHaWal> wal_;
    StandbyStateFn standby_state_fn_;
    uint32_t snapshot_chunk_entries_{1024};
    coro_rpc::coro_rpc_server server_;
    std::atomic<bool> running_{false};

    std::mutex snapshot_mutex_;
    std::optional<SnapshotSession> snapshot_;
    std::atomic<bool> snapshots_enabled_{true};
};

}  // namespace mooncake
