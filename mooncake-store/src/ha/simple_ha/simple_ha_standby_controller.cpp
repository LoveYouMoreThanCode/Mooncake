#include "ha/simple_ha/simple_ha_standby_controller.h"

#include <algorithm>
#include <chrono>
#include <unordered_map>
#include <utility>

#include <async_simple/coro/SyncAwait.h>
#include <glog/logging.h>

#include "ha/oplog/oplog_batch_codec.h"
#include "ha/simple_ha/simple_ha_leader_service.h"
#include "ha_metric_manager.h"
#include "metadata_store.h"

namespace mooncake::ha {
namespace {

using namespace std::chrono_literals;

ErrorCode MapRpcError(const coro_rpc::rpc_error& error) {
    return error.code == coro_rpc::errc::timed_out ? ErrorCode::RPC_TIMEOUT
                                                   : ErrorCode::RPC_FAIL;
}

StandbySegmentInfo ToStandbySegment(const SimpleHaSegmentEntry& entry) {
    StandbySegmentInfo info;
    info.segment_name = entry.segment_name;
    info.transport_endpoint = entry.transport_endpoint;
    info.capacity = entry.capacity;
    info.is_memory_segment = entry.is_memory_segment;
    info.file_path = entry.file_path;
    return info;
}

}  // namespace

SimpleHaStandbyController::SimpleHaStandbyController(
    const MasterServiceSupervisorConfig& config)
    : config_(config) {
    const auto& peer = config_.simple_ha_peer_address;
    const auto separator = peer.rfind(':');
    if (separator == std::string::npos) {
        peer_host_ = peer;
        peer_port_ = std::to_string(config_.simple_ha_rpc_port);
    } else {
        peer_host_ = peer.substr(0, separator);
        peer_port_ = peer.substr(separator + 1);
    }
}

SimpleHaStandbyController::~SimpleHaStandbyController() { StopStandby(); }

ErrorCode SimpleHaStandbyController::StartStandby(
    const std::optional<MasterView>& observed_leader) {
    {
        std::lock_guard<std::mutex> lock(observed_mutex_);
        observed_leader_ = observed_leader;
    }
    std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);
    if (running_.load(std::memory_order_acquire)) {
        return ErrorCode::OK;
    }

    if (preserve_baseline_) {
        if (!metadata_store_) {
            return ErrorCode::INTERNAL_ERROR;
        }
        if (!applier_) {
            applier_ = std::make_unique<OpLogApplier>(metadata_store_.get(),
                                                      config_.cluster_id);
        }
        applier_->Recover(applied_seq_.load(std::memory_order_acquire));
        applier_->LoadSegmentRegistry(preserved_segments_);
        preserved_segments_.clear();
        {
            std::lock_guard<std::mutex> incarnation_lock(incarnation_mutex_);
            incarnation_.clear();
        }
        preserve_baseline_ = false;
        // The retained baseline is a consistent serving state at its cursor.
        // A newer leader still wins: the incarnation check turns the first
        // pull into a resync. This flag only allows re-promotion when no new
        // leader can be reached.
        synced_.store(true, std::memory_order_release);
        needs_resync_.store(false, std::memory_order_release);
    }

    if (!metadata_store_) {
        metadata_store_ = std::make_unique<StandbyMetadataStore>();
    }
    if (!applier_) {
        applier_ = std::make_unique<OpLogApplier>(metadata_store_.get(),
                                                  config_.cluster_id);
    }

    probe_service_ = std::make_unique<SimpleHaLeaderService>(
        static_cast<uint16_t>(config_.simple_ha_rpc_port),
        config_.simple_ha_rpc_address,
        config_.simple_ha_snapshot_chunk_entries, [this] {
            SimpleHaLeaderService::StandbyState state;
            state.has_data = has_lineage_.load(std::memory_order_acquire);
            state.applied_batch_id =
                applied_batch_id_.load(std::memory_order_acquire);
            state.applied_seq = applied_seq_.load(std::memory_order_acquire);
            {
                std::lock_guard<std::mutex> lock(incarnation_mutex_);
                state.incarnation = incarnation_;
            }
            return state;
        });
    if (!probe_service_->Start()) {
        LOG(ERROR) << "Simple HA standby probe service failed to start on port "
                   << config_.simple_ha_rpc_port;
        probe_service_.reset();
        return ErrorCode::INTERNAL_ERROR;
    }

    running_.store(true, std::memory_order_release);
    loop_thread_ = std::thread(&SimpleHaStandbyController::RunLoop, this);
    SetRuntimeState(MasterRuntimeState::kRecovering);
    return ErrorCode::OK;
}

void SimpleHaStandbyController::StopStandby() {
    std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);
    StopLoop();
}

void SimpleHaStandbyController::StopLoop() {
    running_.store(false, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lock(loop_mutex_);
        loop_cv_.notify_all();
    }
    if (loop_thread_.joinable()) {
        loop_thread_.join();
    }
    DropConnection();
    probe_service_.reset();
    SetRuntimeState(MasterRuntimeState::kStandby);
}

void SimpleHaStandbyController::WaitForNextPoll(
    std::chrono::milliseconds delay) {
    std::unique_lock<std::mutex> lock(loop_mutex_);
    loop_cv_.wait_for(lock, delay, [this] {
        return !running_.load(std::memory_order_acquire);
    });
}

void SimpleHaStandbyController::SetRuntimeState(MasterRuntimeState state) {
    const auto previous = runtime_state_.exchange(state);
    if (previous == state) {
        return;
    }
    RuntimeStateCallback callback;
    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        callback = runtime_state_callback_;
    }
    if (callback) {
        callback(state);
    }
}

void SimpleHaStandbyController::UpdateObservedLeader(
    const std::optional<MasterView>& observed_leader) {
    std::lock_guard<std::mutex> lock(observed_mutex_);
    observed_leader_ = observed_leader;
}

MasterRuntimeState SimpleHaStandbyController::GetStandbyRuntimeState() const {
    if (!running_.load(std::memory_order_acquire)) {
        return MasterRuntimeState::kStandby;
    }
    if (!synced_.load(std::memory_order_acquire)) {
        return MasterRuntimeState::kRecovering;
    }
    if (applied_seq_.load(std::memory_order_acquire) <
        peer_durable_seq_.load(std::memory_order_acquire)) {
        return MasterRuntimeState::kCatchingUp;
    }
    return MasterRuntimeState::kStandby;
}

void SimpleHaStandbyController::SetStandbyRuntimeStateCallback(
    RuntimeStateCallback callback) {
    RuntimeStateCallback current;
    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        runtime_state_callback_ = std::move(callback);
        current = runtime_state_callback_;
    }
    if (current) {
        current(runtime_state_.load());
    }
}

bool SimpleHaStandbyController::IsEmpty() const {
    return !has_lineage_.load(std::memory_order_acquire) &&
           applied_batch_id_.load(std::memory_order_acquire) == 0 &&
           applied_seq_.load(std::memory_order_acquire) == 0 &&
           (!metadata_store_ || metadata_store_->GetKeyCount() == 0);
}

bool SimpleHaStandbyController::EnsureConnected() {
    std::lock_guard<std::mutex> lock(client_mutex_);
    if (connected_) {
        return true;
    }
    client_.close();
    auto error = async_simple::coro::syncAwait(client_.connect(
        peer_host_, peer_port_, std::chrono::milliseconds(1000)));
    if (error) {
        LOG(WARNING) << "Simple HA standby cannot connect to peer "
                     << peer_host_ << ":" << peer_port_ << ": "
                     << error.message();
        connected_ = false;
        return false;
    }
    connected_ = true;
    return true;
}

void SimpleHaStandbyController::DropConnection() {
    std::lock_guard<std::mutex> lock(client_mutex_);
    if (connected_) {
        client_.close();
        connected_ = false;
    }
}

tl::expected<SimpleHaGetStateResponse, ErrorCode>
SimpleHaStandbyController::CallGetState() {
    std::lock_guard<std::mutex> lock(client_mutex_);
    if (!connected_) {
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }
    auto result = async_simple::coro::syncAwait(
        client_.call_for<&SimpleHaLeaderService::GetState>(std::chrono::milliseconds(2000)));
    if (!result) {
        connected_ = false;
        client_.close();
        return tl::make_unexpected(MapRpcError(result.error()));
    }
    if (!result->success) {
        return tl::make_unexpected(static_cast<ErrorCode>(result->error_code));
    }
    return *result;
}

tl::expected<SimpleHaPullWalResponse, ErrorCode>
SimpleHaStandbyController::CallPullWal(uint64_t from_batch_id,
                                       uint32_t max_batches) {
    std::lock_guard<std::mutex> lock(client_mutex_);
    if (!connected_) {
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }
    SimpleHaPullWalRequest request{
        .from_batch_id = from_batch_id,
        .max_batches = max_batches,
    };
    auto result = async_simple::coro::syncAwait(
        client_.call_for<&SimpleHaLeaderService::PullWal>(std::chrono::milliseconds(5000), request));
    if (!result) {
        connected_ = false;
        client_.close();
        return tl::make_unexpected(MapRpcError(result.error()));
    }
    if (!result->success) {
        return tl::make_unexpected(static_cast<ErrorCode>(result->error_code));
    }
    return *result;
}

tl::expected<SimpleHaBeginSnapshotResponse, ErrorCode>
SimpleHaStandbyController::CallBeginSnapshot() {
    std::lock_guard<std::mutex> lock(client_mutex_);
    if (!connected_) {
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }
    auto result = async_simple::coro::syncAwait(
        client_.call_for<&SimpleHaLeaderService::BeginSnapshot>(std::chrono::milliseconds(5000)));
    if (!result) {
        connected_ = false;
        client_.close();
        return tl::make_unexpected(MapRpcError(result.error()));
    }
    if (!result->success) {
        return tl::make_unexpected(static_cast<ErrorCode>(result->error_code));
    }
    return *result;
}

tl::expected<SimpleHaSnapshotChunkResponse, ErrorCode>
SimpleHaStandbyController::CallSnapshotChunk(const std::string& snapshot_id,
                                             uint32_t tenant_index,
                                             uint32_t shard_index,
                                             uint32_t offset,
                                             uint32_t max_entries) {
    std::lock_guard<std::mutex> lock(client_mutex_);
    if (!connected_) {
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }
    SimpleHaSnapshotChunkRequest request{
        .snapshot_id = snapshot_id,
        .tenant_index = tenant_index,
        .shard_index = shard_index,
        .offset = offset,
        .max_entries = max_entries,
    };
    auto result = async_simple::coro::syncAwait(
        client_.call_for<&SimpleHaLeaderService::SnapshotChunk>(std::chrono::milliseconds(10000), request));
    if (!result) {
        connected_ = false;
        client_.close();
        return tl::make_unexpected(MapRpcError(result.error()));
    }
    if (!result->success) {
        return tl::make_unexpected(static_cast<ErrorCode>(result->error_code));
    }
    return *result;
}

tl::expected<SimpleHaProbeResponse, ErrorCode>
SimpleHaStandbyController::CallProbe() {
    std::lock_guard<std::mutex> lock(client_mutex_);
    if (!connected_) {
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }
    auto result = async_simple::coro::syncAwait(
        client_.call_for<&SimpleHaLeaderService::Probe>(std::chrono::milliseconds(1000)));
    if (!result) {
        connected_ = false;
        client_.close();
        return tl::make_unexpected(MapRpcError(result.error()));
    }
    if (!result->success) {
        return tl::make_unexpected(static_cast<ErrorCode>(result->error_code));
    }
    return *result;
}

ErrorCode SimpleHaStandbyController::LoadSnapshotFromPeer() {
    auto begin = CallBeginSnapshot();
    if (!begin) {
        return begin.error();
    }

    // Merge semantics: entries present in the new leader's snapshot overwrite
    // matching local keys (the leader is authoritative), while keys absent
    // from the snapshot are kept. Snapshot absence never deletes; only an
    // explicit REMOVE/PUT_REVOKE in the replayed WAL does.
    //
    // Adopt the new leader's identity and cursor up front: if this node wins
    // an election while the merge is still running, it serves the records
    // merged so far plus everything it already had, instead of starting empty.
    {
        std::lock_guard<std::mutex> incarnation_lock(incarnation_mutex_);
        incarnation_ = begin->incarnation;
    }
    applied_batch_id_.store(begin->base_batch_id, std::memory_order_release);
    applied_seq_.store(begin->base_seq, std::memory_order_release);
    peer_durable_seq_.store(begin->base_seq, std::memory_order_release);
    has_lineage_.store(true, std::memory_order_release);
    needs_resync_.store(false, std::memory_order_release);
    synced_.store(true, std::memory_order_release);
    merge_incomplete_.store(true, std::memory_order_release);

    StandbyMetadataStore* store = nullptr;
    OpLogApplier* applier = nullptr;
    {
        std::lock_guard<std::mutex> store_lock(store_mutex_);
        if (!metadata_store_) {
            metadata_store_ = std::make_unique<StandbyMetadataStore>();
        }
        if (!applier_) {
            applier_ = std::make_unique<OpLogApplier>(metadata_store_.get(),
                                                      config_.cluster_id);
        }
        store = metadata_store_.get();
        applier = applier_.get();
    }
    applier->Recover(begin->base_seq);

    uint32_t tenant_index = 0;
    uint32_t shard_index = 0;
    uint32_t offset = 0;
    const uint32_t chunk_entries = config_.simple_ha_snapshot_chunk_entries;
    for (;;) {
        if (!running_.load(std::memory_order_acquire)) {
            return ErrorCode::ETCD_CTX_CANCELLED;
        }
        auto chunk = CallSnapshotChunk(begin->snapshot_id, tenant_index,
                                       shard_index, offset, chunk_entries);
        if (!chunk) {
            return chunk.error();
        }
        if (chunk->incarnation != begin->incarnation ||
            chunk->base_batch_id != begin->base_batch_id ||
            chunk->base_seq != begin->base_seq) {
            return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
        }
        for (const auto& entry : chunk->entries) {
            MetadataPayload payload;
            if (struct_pack::deserialize_to(payload, entry.payload) !=
                struct_pack::errc::ok) {
                return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
            }
            StandbyObjectMetadata metadata = payload.ToStandbyMetadata();
            // Upsert: the new leader wins on a key it still has; a rejected
            // write is not expected for an in-memory store.
            if (!store->PutMetadata(entry.tenant_id, entry.key, metadata)) {
                return ErrorCode::INTERNAL_ERROR;
            }
        }
        tenant_index = chunk->next_tenant_index;
        shard_index = chunk->next_shard_index;
        offset = chunk->next_offset;
        if (chunk->done) {
            break;
        }
    }

    // Union the segment registries: retained local-only objects can reference
    // segments the new leader's snapshot does not list, while the snapshot may
    // describe newer endpoints for the rest.
    std::vector<StandbySegmentInfo> segments =
        applier->GetSegmentRegistry().GetAllSegments();
    std::unordered_map<std::string, size_t> segment_by_endpoint;
    segment_by_endpoint.reserve(segments.size() + begin->segments.size());
    for (size_t i = 0; i < segments.size(); ++i) {
        segment_by_endpoint[segments[i].transport_endpoint] = i;
    }
    for (const auto& entry : begin->segments) {
        const StandbySegmentInfo segment = ToStandbySegment(entry);
        auto it = segment_by_endpoint.find(segment.transport_endpoint);
        if (it == segment_by_endpoint.end()) {
            segment_by_endpoint.emplace(segment.transport_endpoint,
                                        segments.size());
            segments.push_back(segment);
        } else {
            segments[it->second] = segment;
        }
    }
    applier->LoadSegmentRegistry(segments);

    merge_incomplete_.store(false, std::memory_order_release);
    LOG(INFO) << "Simple HA standby snapshot merged: base_batch="
              << begin->base_batch_id << ", base_seq=" << begin->base_seq
              << ", objects=" << store->GetKeyCount()
              << ", segments=" << segments.size();
    return ErrorCode::OK;
}

ErrorCode SimpleHaStandbyController::PullOnce() {
    auto state = CallGetState();
    if (!state) {
        return state.error();
    }
    if (!state->is_leader) {
        // The peer is a standby probe endpoint, not a WAL owner. Do not treat
        // its applied cursor as authority: that would make two standbys chase
        // each other's local state instead of waiting for a leader.
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    {
        std::lock_guard<std::mutex> lock(incarnation_mutex_);
        if (incarnation_.empty()) {
            incarnation_ = state->incarnation;
        } else if (incarnation_ != state->incarnation) {
            needs_resync_.store(true, std::memory_order_release);
            return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
        }
    }

    if (!state->has_data) {
        if (applied_batch_id_.load(std::memory_order_acquire) != 0 ||
            applied_seq_.load(std::memory_order_acquire) != 0) {
            // Merge semantics: an empty leader never makes us discard local
            // records. Keep them and remain promotable; if this node wins an
            // election it serves the records it already has.
            LOG(WARNING) << "Simple HA leader reports no data while standby "
                            "holds a non-empty cursor; keeping local records";
        }
        synced_.store(true, std::memory_order_release);
        return ErrorCode::OK;
    }

    if (state->first_batch_id >
        applied_batch_id_.load(std::memory_order_acquire) + 1) {
        needs_resync_.store(true, std::memory_order_release);
        return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
    }

    for (int page = 0; page < 16; ++page) {
        const uint64_t applied =
            applied_batch_id_.load(std::memory_order_acquire);
        if (applied >= state->durable_batch_id) {
            break;
        }
        auto pull = CallPullWal(applied + 1, 128);
        if (!pull) {
            return pull.error();
        }
        if (pull->incarnation != state->incarnation) {
            needs_resync_.store(true, std::memory_order_release);
            return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
        }
        if (pull->batches.empty()) {
            break;
        }
        for (const auto& encoded : pull->batches) {
            OpLogBatchRecord batch;
            std::string reason;
            if (!DecodeOpLogBatchRecord(encoded, &batch, &reason)) {
                needs_resync_.store(true, std::memory_order_release);
                return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
            }
            if (batch.batch_id !=
                applied_batch_id_.load(std::memory_order_acquire) + 1) {
                needs_resync_.store(true, std::memory_order_release);
                return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
            }
            if (batch.first_seq !=
                applied_seq_.load(std::memory_order_acquire) + 1) {
                needs_resync_.store(true, std::memory_order_release);
                return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
            }
            for (const auto& entry : batch.entries) {
                if (!applier_->ApplyOpLogEntry(entry)) {
                    needs_resync_.store(true, std::memory_order_release);
                    return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
                }
                applied_seq_.store(entry.sequence_id,
                                   std::memory_order_release);
            }
            applied_batch_id_.store(batch.batch_id,
                                    std::memory_order_release);
        }
    }

    peer_durable_seq_.store(state->durable_last_seq,
                            std::memory_order_release);
    synced_.store(applied_seq_.load(std::memory_order_acquire) >=
                      state->durable_last_seq,
                  std::memory_order_release);
    return ErrorCode::OK;
}

void SimpleHaStandbyController::RunLoop() {
    const auto base_delay =
        std::chrono::milliseconds(std::max(config_.simple_ha_pull_interval_ms, 1));
    auto retry_delay = base_delay;
    while (running_.load(std::memory_order_acquire)) {
        if (!EnsureConnected()) {
            WaitForNextPoll(retry_delay);
            retry_delay = std::min(retry_delay * 2, std::chrono::milliseconds(5000));
            continue;
        }

        const bool need_snapshot =
            needs_resync_.load(std::memory_order_acquire) ||
            !has_lineage_.load(std::memory_order_acquire) ||
            !metadata_store_ ||
            merge_incomplete_.load(std::memory_order_acquire);
        ErrorCode error = ErrorCode::OK;
        if (need_snapshot) {
            SetRuntimeState(MasterRuntimeState::kRecovering);
            error = LoadSnapshotFromPeer();
            // A failed merge leaves every record already present in place, so
            // it must not gate promotion; the loop simply retries the merge.
        } else {
            error = PullOnce();
            if (error == ErrorCode::INCOMPLETE_OPLOG_CATCH_UP) {
                needs_resync_.store(true, std::memory_order_release);
            }
        }

        if (error == ErrorCode::ETCD_CTX_CANCELLED) {
            break;
        }
        if (error != ErrorCode::OK) {
            WaitForNextPoll(retry_delay);
            retry_delay = std::min(retry_delay * 2, std::chrono::milliseconds(5000));
            continue;
        }

        retry_delay = base_delay;
        SetRuntimeState(GetStandbyRuntimeState());
        WaitForNextPoll(base_delay);
    }
    SetRuntimeState(MasterRuntimeState::kStandby);
}

ErrorCode SimpleHaStandbyController::PromoteStandby() {
    auto result = PromoteStandbyAndExport();
    return result ? ErrorCode::OK : result.error();
}

tl::expected<PromotionContext, ErrorCode>
SimpleHaStandbyController::PromoteStandbyAndExport() {
    std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);

    bool empty = false;
    {
        std::lock_guard<std::mutex> store_lock(store_mutex_);
        if (!metadata_store_) {
            return tl::make_unexpected(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP);
        }
        empty = IsEmpty();
        if (!empty && (!synced_.load(std::memory_order_acquire) ||
                       needs_resync_.load(std::memory_order_acquire))) {
            // Keep the pull loop running: the node will be elected again
            // after the supervisor releases, and stopping the loop here would
            // restart a long snapshot from scratch on every attempt.
            return tl::make_unexpected(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP);
        }
    }
    StopLoop();

    // The loop may have installed a snapshot or hit a gap while stopping.
    {
        std::lock_guard<std::mutex> store_lock(store_mutex_);
        empty = IsEmpty();
    }
    if (!empty && (!synced_.load(std::memory_order_acquire) ||
                   needs_resync_.load(std::memory_order_acquire))) {
        return tl::make_unexpected(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP);
    }

    if (merge_incomplete_.load(std::memory_order_acquire)) {
        LOG(WARNING)
            << "Simple HA promoting with an incomplete snapshot merge; "
               "serving the records merged so far plus every local record";
    }

    if (empty) {
        if (EnsureConnected()) {
            auto probe = CallProbe();
            if (probe && probe->has_data) {
                LOG(INFO) << "Simple HA empty node yields leadership: peer has "
                             "data, incarnation="
                          << probe->incarnation;
                return tl::make_unexpected(
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            }
            if (!probe) {
                LOG(WARNING) << "Simple HA empty promotion peer probe failed; "
                                "starting empty";
                HAMetricManager::instance()
                    .inc_simple_ha_degraded_promotions();
            }
        } else {
            // A cold two-node cluster has no leader service yet. Refusing to
            // start would deadlock both nodes. Best-effort HA accepts the
            // empty start and logs it loudly; a partitioned peer with data is
            // recovered only by restarting this node against that peer.
            LOG(WARNING) << "Simple HA empty promotion cannot reach peer "
                         << peer_host_ << ":" << peer_port_
                         << "; starting empty";
            HAMetricManager::instance().inc_simple_ha_degraded_promotions();
        }
    }

    if (!applier_) {
        applier_ = std::make_unique<OpLogApplier>(metadata_store_.get(),
                                                  config_.cluster_id);
    }

    if (!empty) {
        // Best-effort final catch-up. A crashed leader is unreachable and the
        // applied state is already the newest recoverable one; a live leader
        // lets us pull the remaining tail before exporting.
        bool catch_up_failed = false;
        for (int attempt = 0; attempt < 4; ++attempt) {
            if (!EnsureConnected()) {
                catch_up_failed = true;
                break;
            }
            const ErrorCode error = PullOnce();
            if (error == ErrorCode::INCOMPLETE_OPLOG_CATCH_UP) {
                return tl::make_unexpected(error);
            }
            if (error != ErrorCode::OK) {
                catch_up_failed = true;
                break;
            }
            if (applied_seq_.load(std::memory_order_acquire) >=
                peer_durable_seq_.load(std::memory_order_acquire)) {
                break;
            }
        }

        const uint64_t applied_seq =
            applied_seq_.load(std::memory_order_acquire);
        const uint64_t peer_durable_seq =
            peer_durable_seq_.load(std::memory_order_acquire);
        const uint64_t lag = peer_durable_seq > applied_seq
                                 ? peer_durable_seq - applied_seq
                                 : 0;
        if (catch_up_failed || lag != 0) {
            // The previous primary is gone or unreachable, so the missing
            // suffix cannot be recovered. Serving the newest recoverable state
            // beats failing closed, but operators must be able to see that this
            // promotion may have lost more than the steady-state pull window.
            LOG(WARNING)
                << "Simple HA promotion without a completed WAL catch-up: "
                << "applied_seq=" << applied_seq
                << ", applied_batch="
                << applied_batch_id_.load(std::memory_order_acquire)
                << ", last_known_peer_durable_seq=" << peer_durable_seq
                << ", lag=" << lag
                << ", peer_unreachable=" << (catch_up_failed ? 1 : 0)
                << "; the unreplicated suffix is unrecoverable";
            HAMetricManager::instance().inc_simple_ha_degraded_promotions();
        }
    }

    ReplicaID max_replica_id = 0;
    if (!metadata_store_->ValidateReplicaIds(max_replica_id)) {
        needs_resync_.store(true, std::memory_order_release);
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const uint64_t applied_batch_id =
        applied_batch_id_.load(std::memory_order_acquire);
    const uint64_t applied_seq = applied_seq_.load(std::memory_order_acquire);
    if ((applied_batch_id == 0) != (applied_seq == 0)) {
        needs_resync_.store(true, std::memory_order_release);
        return tl::make_unexpected(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP);
    }

    PromotionContext context;
    context.applied_seq_id = applied_seq;
    context.applied_cursor = DurablePrefix{
        .batch_id = applied_batch_id,
        .last_seq = applied_seq,
    };
    context.segments = applier_->GetSegmentRegistry().GetAllSegments();
    context.max_replica_id = max_replica_id;
    context.metadata_store = std::move(metadata_store_);
    applier_.reset();
    preserve_baseline_ = false;

    applied_batch_id_.store(0, std::memory_order_release);
    applied_seq_.store(0, std::memory_order_release);
    peer_durable_seq_.store(0, std::memory_order_release);
    has_lineage_.store(false, std::memory_order_release);
    synced_.store(false, std::memory_order_release);
    needs_resync_.store(false, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lock(incarnation_mutex_);
        incarnation_.clear();
    }
    return context;
}

ErrorCode SimpleHaStandbyController::RestorePromotionBaseline(
    PromotionContext context) {
    if (!context.metadata_store) {
        return ErrorCode::OK;
    }
    std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);
    metadata_store_ = std::move(context.metadata_store);
    preserved_cursor_ = context.applied_cursor;
    preserved_segments_ = std::move(context.segments);
    applied_batch_id_.store(preserved_cursor_.batch_id,
                            std::memory_order_release);
    applied_seq_.store(preserved_cursor_.last_seq, std::memory_order_release);
    has_lineage_.store(true, std::memory_order_release);
    preserve_baseline_ = true;
    // The retained baseline is a consistent state at its cursor, so it is
    // promotable when no newer leader can be reached. If one exists, the
    // incarnation check in PullOnce forces a resync before promotion.
    synced_.store(true, std::memory_order_release);
    needs_resync_.store(false, std::memory_order_release);
    applier_.reset();
    {
        std::lock_guard<std::mutex> lock(incarnation_mutex_);
        incarnation_.clear();
    }
    return ErrorCode::OK;
}

}  // namespace mooncake::ha
