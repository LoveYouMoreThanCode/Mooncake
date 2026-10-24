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
    uint32_t checkpoint_chunk_entries)
    : master_(&master),
      wal_(master.simple_ha_wal_),
      checkpoint_chunk_entries_(checkpoint_chunk_entries == 0
                                  ? 1024
                                  : checkpoint_chunk_entries),
      server_(/*thread_num=*/4, port, std::move(address),
              std::chrono::seconds(0), /*tcp_no_delay=*/true) {}

SimpleHaLeaderService::SimpleHaLeaderService(uint16_t port, std::string address,
                                             uint32_t checkpoint_chunk_entries,
                                             StandbyStateFn standby_state)
    : standby_state_fn_(std::move(standby_state)),
      checkpoint_chunk_entries_(checkpoint_chunk_entries == 0
                                  ? 1024
                                  : checkpoint_chunk_entries),
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
                             &SimpleHaLeaderService::BeginCheckpoint,
                             &SimpleHaLeaderService::CheckpointChunk,
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

void SimpleHaLeaderService::DisableCheckpoints() {
    // Take the same lock the chunk handlers hold for their whole body, so
    // once this returns no scan can still be reading the serving map. From
    // here on new sessions are rejected and the current one cannot be
    // resumed, which keeps a draining map from being captured as a checkpoint.
    std::lock_guard<std::mutex> lock(checkpoint_mutex_);
    checkpoints_enabled_.store(false, std::memory_order_release);
    checkpoint_.reset();
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
        return MakeStateResponse(wal_->GetState());
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
        response.key_count = state.key_count;
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
    const auto state = wal_->GetState();
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

std::vector<SimpleHaSegmentEntry> SimpleHaLeaderService::CheckpointSegments()
    const {
    std::vector<SimpleHaSegmentEntry> result;
    std::vector<std::pair<Segment, UUID>> segments;
    if (master_->segment_manager_.getSegmentAccess().GetAllSegments(segments) !=
        ErrorCode::OK) {
        LOG(WARNING) << "Simple HA checkpoint could not enumerate segments";
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

SimpleHaBeginCheckpointResponse SimpleHaLeaderService::BeginCheckpoint() {
    SimpleHaBeginCheckpointResponse response;
    if (!wal_ || !master_) {
        response.error_code = toInt(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        return response;
    }

    std::lock_guard<std::mutex> lock(checkpoint_mutex_);
    if (!checkpoints_enabled_.load(std::memory_order_acquire)) {
        response.error_code = toInt(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        return response;
    }
    const auto state = wal_->GetState();
    // A durable-finalize callback may still be running for an entry at or
    // below the durable prefix. Wait for those callbacks so the scan cannot
    // capture state the OpLog has already superseded (for example an erase
    // that only lands in the live map after the record became durable).
    constexpr auto kQuiesceTimeout = std::chrono::milliseconds(2000);
    if (!master_->WaitForSimpleHaCallbacks(state.durable_prefix.last_seq,
                                           kQuiesceTimeout)) {
        LOG(WARNING) << "Simple HA checkpoint base did not quiesce, base_seq="
                     << state.durable_prefix.last_seq;
        response.error_code = toInt(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        return response;
    }
    CheckpointSession session;
    session.checkpoint_id =
        std::to_string(state.durable_prefix.batch_id) + "-" +
        state.incarnation;
    session.incarnation = state.incarnation;
    session.base = state.durable_prefix;
    master_->VisitTenants([&session](const TenantId& tenant) {
        session.tenants.push_back(tenant.value());
    });
    std::sort(session.tenants.begin(), session.tenants.end());
    // Only one standby consumes a checkpoint at a time; replace a stale session
    // rather than wedging the next attempt.
    checkpoint_ = std::move(session);

    response.success = true;
    response.checkpoint_id = checkpoint_->checkpoint_id;
    response.incarnation = checkpoint_->incarnation;
    response.base_batch_id = checkpoint_->base.batch_id;
    response.base_seq = checkpoint_->base.last_seq;
    response.segments = CheckpointSegments();
    return response;
}

SimpleHaCheckpointChunkResponse SimpleHaLeaderService::CheckpointChunk(
    SimpleHaCheckpointChunkRequest request) {
    SimpleHaCheckpointChunkResponse response;
    if (!wal_ || !master_) {
        response.error_code = toInt(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        return response;
    }
    std::lock_guard<std::mutex> lock(checkpoint_mutex_);
    if (!checkpoints_enabled_.load(std::memory_order_acquire)) {
        response.error_code = toInt(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        return response;
    }
    if (!checkpoint_ || checkpoint_->checkpoint_id != request.checkpoint_id) {
        response.error_code = toInt(ErrorCode::INVALID_PARAMS);
        return response;
    }

    const auto state = wal_->GetState();
    if (state.incarnation != checkpoint_->incarnation ||
        (checkpoint_->base.batch_id != UINT64_MAX &&
         state.first_batch_id > checkpoint_->base.batch_id + 1)) {
        // The ring overwrote base+1 while the scan was in flight. The standby
        // must restart from a new checkpoint instead of replaying a gap.
        checkpoint_.reset();
        response.error_code = toInt(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP);
        return response;
    }

    auto& session = *checkpoint_;
    // The session has one consumer. A retry with a stale cursor (for example
    // after a lost response) cannot be served without re-staging a shard, so
    // reject it and require a fresh BeginCheckpoint instead of silently
    // skipping entries.
    if (request.tenant_index != session.tenant_index ||
        request.shard_index != session.shard_index ||
        request.offset != session.staged_offset) {
        checkpoint_.reset();
        response.error_code = toInt(ErrorCode::INVALID_PARAMS);
        return response;
    }
    const uint32_t max_entries =
        request.max_entries == 0 ? checkpoint_chunk_entries_
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
                // Capture the same post-image the OpLog would carry: only
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
                session.staged.push_back(SimpleHaCheckpointEntry{
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
        checkpoint_.reset();
    }
    return response;
}

SimpleHaProbeResponse SimpleHaLeaderService::Probe() {
    SimpleHaProbeResponse response;
    if (wal_) {
        const auto state = wal_->GetState();
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
