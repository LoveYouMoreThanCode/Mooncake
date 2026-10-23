#include "ha/simple_ha/simple_ha_leader_service.h"

#include <algorithm>
#include <chrono>
#include <utility>
#include <glog/logging.h>

#include "ha/oplog/oplog_batch_codec.h"
#include "ha/simple_ha/simple_ha_wal.h"
#include "master_service.h"

namespace mooncake {

SimpleHaLeaderService::SimpleHaLeaderService(
    MasterService& master, uint16_t port, std::string address,
    uint32_t snapshot_chunk_entries)
    : master_(&master),
      wal_(master.simple_ha_wal_),
      snapshot_chunk_entries_(snapshot_chunk_entries == 0
                                  ? 1024
                                  : snapshot_chunk_entries),
      server_(/*thread_num=*/4, port, std::move(address),
              std::chrono::seconds(0), /*tcp_no_delay=*/true) {}

SimpleHaLeaderService::SimpleHaLeaderService(uint16_t port, std::string address,
                                             uint32_t snapshot_chunk_entries,
                                             StandbyStateFn standby_state)
    : standby_state_fn_(std::move(standby_state)),
      snapshot_chunk_entries_(snapshot_chunk_entries == 0
                                  ? 1024
                                  : snapshot_chunk_entries),
      server_(/*thread_num=*/4, port, std::move(address),
              std::chrono::seconds(0), /*tcp_no_delay=*/true) {}

SimpleHaLeaderService::~SimpleHaLeaderService() { Stop(); }

bool SimpleHaLeaderService::Start() {
    if (running_.exchange(true)) {
        return true;
    }
    if (!wal_ && !standby_state_fn_) {
        LOG(ERROR) << "Simple HA service cannot start without a WAL or "
                      "standby state source";
        running_.store(false);
        return false;
    }

    server_.register_handler<&SimpleHaLeaderService::GetState,
                             &SimpleHaLeaderService::PullWal,
                             &SimpleHaLeaderService::BeginSnapshot,
                             &SimpleHaLeaderService::SnapshotChunk,
                             &SimpleHaLeaderService::Probe>(this);

    auto future = server_.async_start();
    if (future.hasResult()) {
        auto error = std::move(future).get();
        if (error) {
            LOG(ERROR) << "Simple HA RPC server failed to start: "
                       << error.message();
            running_.store(false);
            return false;
        }
    }
    LOG(INFO) << "Simple HA RPC service listening on port " << server_.port();
    return true;
}

void SimpleHaLeaderService::Stop() {
    if (!running_.exchange(false)) {
        return;
    }
    server_.stop();
}

SimpleHaGetStateResponse SimpleHaLeaderService::MakeStateResponse(
    const SimpleHaWalState& state) {
    SimpleHaGetStateResponse response;
    response.success = true;
    response.incarnation = state.incarnation;
    response.first_batch_id = state.first_batch_id;
    response.last_batch_id = state.last_batch_id;
    response.durable_batch_id = state.durable_prefix.batch_id;
    response.durable_last_seq = state.durable_prefix.last_seq;
    response.has_data = state.has_data;
    response.is_leader = true;
    return response;
}

SimpleHaGetStateResponse SimpleHaLeaderService::GetState() {
    if (wal_) {
        return MakeStateResponse(wal_->SnapshotState());
    }
    if (standby_state_fn_) {
        const auto state = standby_state_fn_();
        SimpleHaGetStateResponse response;
        response.success = true;
        response.incarnation = state.incarnation;
        response.first_batch_id = state.applied_batch_id;
        response.last_batch_id = state.applied_batch_id;
        response.durable_batch_id = state.applied_batch_id;
        response.durable_last_seq = state.applied_seq;
        response.has_data = state.has_data;
        response.is_leader = false;
        return response;
    }
    SimpleHaGetStateResponse response;
    response.error_code = toInt(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    return response;
}

SimpleHaPullWalResponse SimpleHaLeaderService::PullWal(
    SimpleHaPullWalRequest request) {
    SimpleHaPullWalResponse response;
    if (!wal_) {
        response.error_code = toInt(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        return response;
    }
    const size_t max_batches =
        std::min<size_t>(request.max_batches == 0 ? 64 : request.max_batches,
                         1024);
    const auto state = wal_->SnapshotState();
    response.success = true;
    response.incarnation = state.incarnation;
    response.first_batch_id = state.first_batch_id;
    response.last_batch_id = state.last_batch_id;
    response.durable_batch_id = state.durable_prefix.batch_id;
    response.durable_last_seq = state.durable_prefix.last_seq;
    for (const auto& batch :
         wal_->Pull(request.from_batch_id, max_batches)) {
        response.batches.push_back(EncodeOpLogBatchRecord(batch));
    }
    return response;
}

std::vector<SimpleHaSegmentEntry> SimpleHaLeaderService::SnapshotSegments()
    const {
    std::vector<SimpleHaSegmentEntry> result;
    std::vector<std::pair<Segment, UUID>> segments;
    if (master_->segment_manager_.getSegmentAccess().GetAllSegments(segments) !=
        ErrorCode::OK) {
        LOG(WARNING) << "Simple HA snapshot could not enumerate segments";
        return result;
    }
    result.reserve(segments.size());
    for (const auto& [segment, client_id] : segments) {
        (void)client_id;
        result.push_back(SimpleHaSegmentEntry{
            .segment_name = segment.name,
            .transport_endpoint = segment.te_endpoint,
            .capacity = segment.size,
            .is_memory_segment = true,
            .file_path = {},
        });
    }
    return result;
}

SimpleHaBeginSnapshotResponse SimpleHaLeaderService::BeginSnapshot() {
    SimpleHaBeginSnapshotResponse response;
    if (!wal_) {
        response.error_code = toInt(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        return response;
    }

    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    const auto state = wal_->SnapshotState();
    SnapshotSession session;
    session.snapshot_id = std::to_string(state.durable_prefix.batch_id) + "-" +
                          state.incarnation;
    session.incarnation = state.incarnation;
    session.base = state.durable_prefix;
    master_->VisitTenants([&session](const TenantId& tenant) {
        session.tenants.push_back(tenant.value());
    });
    std::sort(session.tenants.begin(), session.tenants.end());
    // Only one standby consumes a snapshot at a time; replace a stale session
    // rather than wedging the next attempt.
    snapshot_ = std::move(session);

    response.success = true;
    response.snapshot_id = snapshot_->snapshot_id;
    response.incarnation = snapshot_->incarnation;
    response.base_batch_id = snapshot_->base.batch_id;
    response.base_seq = snapshot_->base.last_seq;
    response.segments = SnapshotSegments();
    return response;
}

SimpleHaSnapshotChunkResponse SimpleHaLeaderService::SnapshotChunk(
    SimpleHaSnapshotChunkRequest request) {
    SimpleHaSnapshotChunkResponse response;
    if (!wal_ || !master_) {
        response.error_code = toInt(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        return response;
    }
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    if (!snapshot_ || snapshot_->snapshot_id != request.snapshot_id) {
        response.error_code = toInt(ErrorCode::INVALID_PARAMS);
        return response;
    }

    const auto state = wal_->SnapshotState();
    if (state.incarnation != snapshot_->incarnation ||
        (snapshot_->base.batch_id != UINT64_MAX &&
         state.first_batch_id > snapshot_->base.batch_id + 1)) {
        // The ring overwrote base+1 while the scan was in flight. The standby
        // must restart from a new snapshot instead of replaying a gap.
        snapshot_.reset();
        response.error_code = toInt(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP);
        return response;
    }

    auto& session = *snapshot_;
    // The session has one consumer. A retry with a stale cursor (for example
    // after a lost response) cannot be served without re-staging a shard, so
    // reject it and require a fresh BeginSnapshot instead of silently skipping
    // entries.
    if (request.tenant_index != session.tenant_index ||
        request.shard_index != session.shard_index ||
        request.offset != session.staged_offset) {
        snapshot_.reset();
        response.error_code = toInt(ErrorCode::INVALID_PARAMS);
        return response;
    }
    const uint32_t max_entries =
        request.max_entries == 0 ? snapshot_chunk_entries_
                                 : request.max_entries;

    while (session.staged_offset >= session.staged.size() && !session.done) {
        session.staged.clear();
        session.staged_offset = 0;
        bool staged = false;
        while (session.tenant_index < session.tenants.size()) {
            if (session.shard_index >= MasterService::kNumShards) {
                ++session.tenant_index;
                session.shard_index = 0;
                continue;
            }
            TenantId tenant(session.tenants[session.tenant_index]);
            auto cursor = master_->ReadCursor(
                tenant, {session.shard_index, MasterService::kNumShards});
            for (auto object : cursor) {
                // Snapshot the same post-image the OpLog would carry: only
                // completed replicas are readable. An in-flight PutStart is
                // represented by PROCESSING replicas and must not leak into a
                // recovery baseline; its later PUT_END arrives through the
                // WAL.
                std::vector<Replica::Descriptor> completed;
                for (const auto& replica : object.metadata().GetAllReplicas()) {
                    if (replica.is_completed()) {
                        completed.push_back(replica.get_descriptor());
                    }
                }
                if (completed.empty()) {
                    continue;
                }
                session.staged.push_back(SimpleHaSnapshotEntry{
                    .tenant_id = session.tenants[session.tenant_index],
                    .key = object.key(),
                    .payload =
                        master_->SerializeMetadataForOpLogFromReplicaDescriptors(
                            object.metadata(), completed),
                });
            }
            ++session.shard_index;
            if (!session.staged.empty()) {
                staged = true;
                break;
            }
        }
        if (!staged) {
            session.done = true;
        }
    }

    const size_t available = session.staged.size() - session.staged_offset;
    const size_t count = std::min<size_t>(max_entries, available);
    response.success = true;
    response.incarnation = session.incarnation;
    response.base_batch_id = session.base.batch_id;
    response.base_seq = session.base.last_seq;
    response.entries.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        response.entries.push_back(
            std::move(session.staged[session.staged_offset + i]));
    }
    session.staged_offset += count;
    response.next_tenant_index = static_cast<uint32_t>(session.tenant_index);
    response.next_shard_index = static_cast<uint32_t>(session.shard_index);
    response.next_offset = static_cast<uint32_t>(session.staged_offset);
    response.done = session.done && session.staged_offset >= session.staged.size();
    if (response.done) {
        snapshot_.reset();
    }
    return response;
}

SimpleHaProbeResponse SimpleHaLeaderService::Probe() {
    SimpleHaProbeResponse response;
    if (wal_) {
        const auto state = wal_->SnapshotState();
        response.success = true;
        response.incarnation = state.incarnation;
        response.has_data = state.has_data;
        response.is_leader = true;
        return response;
    }
    if (standby_state_fn_) {
        const auto state = standby_state_fn_();
        response.success = true;
        response.incarnation = state.incarnation;
        response.has_data = state.has_data;
        response.applied_batch_id = state.applied_batch_id;
        response.applied_seq = state.applied_seq;
        response.is_leader = false;
        return response;
    }
    response.error_code = toInt(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    return response;
}

}  // namespace mooncake
