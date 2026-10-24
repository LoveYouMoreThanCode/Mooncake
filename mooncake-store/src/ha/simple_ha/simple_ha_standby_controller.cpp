#include "ha/simple_ha/simple_ha_standby_controller.h"

#include <algorithm>
#include <chrono>
#include <unordered_map>
#include <utility>

#include <async_simple/coro/SyncAwait.h>
#include <glog/logging.h>

#include "ha/oplog/oplog_batch_codec.h"
#include "ha/oplog/oplog_test_failpoint.h"
#include "ha/simple_ha/simple_ha_leader_service.h"
#include "ha_metric_manager.h"
#include "metadata_store.h"

namespace mooncake::ha {
namespace {

using namespace std::chrono_literals;

// Bound the promotion probe so an unresponsive peer cannot spend the whole
// failover budget; a timeout fails open (serve locally) with a warning.
constexpr std::chrono::milliseconds kPromotionProbeTimeout{500};

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
    const auto& peer = config_.simple_ha.peer_address;
    const auto separator = peer.rfind(':');
    if (separator == std::string::npos) {
        peer_host_ = peer;
        peer_port_ = std::to_string(config_.simple_ha.rpc_port);
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
        // It still has to merge the next leader's checkpoint: the cursor is
        // from an older WAL instance and can be ahead of the new leader's.
        baseline_needs_merge_.store(true, std::memory_order_release);
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
        static_cast<uint16_t>(config_.simple_ha.rpc_port),
        config_.simple_ha.rpc_address,
        config_.simple_ha.checkpoint_chunk_entries, [this] {
            SimpleHaLeaderService::StandbyState state;
            state.has_data = has_lineage_.load(std::memory_order_acquire);
            state.applied_batch_id =
                applied_batch_id_.load(std::memory_order_acquire);
            state.applied_seq = applied_seq_.load(std::memory_order_acquire);
            {
                std::lock_guard<std::mutex> lock(store_mutex_);
                state.key_count = metadata_store_
                                      ? metadata_store_->GetKeyCount()
                                      : 0;
            }
            {
                std::lock_guard<std::mutex> lock(incarnation_mutex_);
                state.incarnation = incarnation_;
            }
            return state;
        });
    if (!probe_service_->Start()) {
        LOG(ERROR) << "Simple HA standby probe service failed to start on port "
                   << config_.simple_ha.rpc_port;
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
SimpleHaStandbyController::CallGetState(std::chrono::milliseconds timeout) {
    std::lock_guard<std::mutex> lock(client_mutex_);
    if (!connected_) {
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }
    auto result = async_simple::coro::syncAwait(
        client_.call_for<&SimpleHaLeaderService::GetState>(timeout));
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

tl::expected<SimpleHaBeginCheckpointResponse, ErrorCode>
SimpleHaStandbyController::CallBeginCheckpoint() {
    std::lock_guard<std::mutex> lock(client_mutex_);
    if (!connected_) {
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }
    auto result = async_simple::coro::syncAwait(
        client_.call_for<&SimpleHaLeaderService::BeginCheckpoint>(
            std::chrono::milliseconds(5000)));
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

tl::expected<SimpleHaCheckpointChunkResponse, ErrorCode>
SimpleHaStandbyController::CallCheckpointChunk(const std::string& checkpoint_id,
                                             uint32_t tenant_index,
                                             uint32_t shard_index,
                                             uint32_t offset,
                                             uint32_t max_entries) {
    std::lock_guard<std::mutex> lock(client_mutex_);
    if (!connected_) {
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }
    SimpleHaCheckpointChunkRequest request{
        .checkpoint_id = checkpoint_id,
        .tenant_index = tenant_index,
        .shard_index = shard_index,
        .offset = offset,
        .max_entries = max_entries,
    };
    auto result = async_simple::coro::syncAwait(
        client_.call_for<&SimpleHaLeaderService::CheckpointChunk>(
            std::chrono::milliseconds(10000), request));
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

ErrorCode SimpleHaStandbyController::LoadCheckpointFromPeer() {
    auto begin = CallBeginCheckpoint();
    if (!begin) {
        return begin.error();
    }

    // Merge semantics: entries present in the new leader's checkpoint overwrite
    // matching local keys (the leader is authoritative), while keys absent
    // from the checkpoint are kept. Checkpoint absence never deletes; only an
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
    const uint32_t chunk_entries = config_.simple_ha.checkpoint_chunk_entries;
    for (;;) {
        if (!running_.load(std::memory_order_acquire)) {
            return ErrorCode::ETCD_CTX_CANCELLED;
        }
        auto chunk = CallCheckpointChunk(begin->checkpoint_id, tenant_index,
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
        // Test-only seam for the L1 harness: stop the peer while a merge is
        // in flight. Compiled out unless MOONCAKE_ENABLE_TEST_FAILPOINTS.
        TestFailPoint::Wait("simple_ha_checkpoint_chunk_merged");
        tenant_index = chunk->next_tenant_index;
        shard_index = chunk->next_shard_index;
        offset = chunk->next_offset;
        if (chunk->done) {
            break;
        }
    }

    // Union the segment registries: retained local-only objects can reference
    // segments the new leader's checkpoint does not list, while the
    // checkpoint may describe newer endpoints for the rest.
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
    baseline_needs_merge_.store(false, std::memory_order_release);
    LOG(INFO) << "Simple HA standby checkpoint merged: base_batch="
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
    return PullOnce(*state);
}

ErrorCode SimpleHaStandbyController::PullOnce(
    const SimpleHaGetStateResponse& state) {
    if (!state.is_leader) {
        // The peer is a standby probe endpoint, not a WAL owner. Do not treat
        // its applied cursor as authority: that would make two standbys chase
        // each other's local state instead of waiting for a leader.
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    if (baseline_needs_merge_.load(std::memory_order_acquire)) {
        // The preserved cursor belongs to an older WAL instance and can be
        // numerically ahead of the new leader's WAL. Following the WAL from
        // that cursor would silently skip the new leader's writes, so merge
        // its checkpoint first; the merge keeps every local record.
        LOG(INFO) << "Simple HA preserved baseline: forcing a checkpoint merge "
                     "from the new leader";
        needs_resync_.store(true, std::memory_order_release);
        return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
    }

    {
        std::lock_guard<std::mutex> lock(incarnation_mutex_);
        if (incarnation_.empty()) {
            incarnation_ = state.incarnation;
        } else if (incarnation_ != state.incarnation) {
            needs_resync_.store(true, std::memory_order_release);
            return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
        }
    }

    if (!state.has_data) {
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

    if (state.first_batch_id >
        applied_batch_id_.load(std::memory_order_acquire) + 1) {
        needs_resync_.store(true, std::memory_order_release);
        return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
    }

    for (int page = 0; page < 16; ++page) {
        const uint64_t applied =
            applied_batch_id_.load(std::memory_order_acquire);
        if (applied >= state.durable_batch_id) {
            break;
        }
        auto pull = CallPullWal(applied + 1, 128);
        if (!pull) {
            return pull.error();
        }
        if (pull->incarnation != state.incarnation) {
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

    peer_durable_seq_.store(state.durable_last_seq,
                            std::memory_order_release);
    synced_.store(applied_seq_.load(std::memory_order_acquire) >=
                      state.durable_last_seq,
                  std::memory_order_release);
    return ErrorCode::OK;
}

void SimpleHaStandbyController::RunLoop() {
    const auto base_delay =
        std::chrono::milliseconds(std::max(config_.simple_ha.pull_interval_ms, 1));
    auto retry_delay = base_delay;
    while (running_.load(std::memory_order_acquire)) {
        if (!EnsureConnected()) {
            WaitForNextPoll(retry_delay);
            retry_delay = std::min(retry_delay * 2, std::chrono::milliseconds(5000));
            continue;
        }

        const bool need_checkpoint =
            needs_resync_.load(std::memory_order_acquire) ||
            !has_lineage_.load(std::memory_order_acquire) ||
            !metadata_store_ ||
            merge_incomplete_.load(std::memory_order_acquire);
        ErrorCode error = ErrorCode::OK;
        if (need_checkpoint) {
            SetRuntimeState(MasterRuntimeState::kRecovering);
            error = LoadCheckpointFromPeer();
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
    size_t local_key_count = 0;
    {
        std::lock_guard<std::mutex> store_lock(store_mutex_);
        if (!metadata_store_) {
            return tl::make_unexpected(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP);
        }
        empty = IsEmpty();
        local_key_count = metadata_store_->GetKeyCount();
        if (!empty && !synced_.load(std::memory_order_acquire)) {
            // Keep the pull loop running: the node will be elected again
            // after the supervisor releases, and stopping the loop here would
            // restart a long checkpoint from scratch on every attempt.
            return tl::make_unexpected(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP);
        }
    }
    StopLoop();

    // The loop may have installed a checkpoint or hit a gap while stopping.
    {
        std::lock_guard<std::mutex> store_lock(store_mutex_);
        empty = IsEmpty();
        local_key_count = metadata_store_->GetKeyCount();
    }
    if (!empty && !synced_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP);
    }

    if (merge_incomplete_.load(std::memory_order_acquire)) {
        LOG(WARNING)
            << "Simple HA promoting with an incomplete checkpoint merge; "
               "serving the records merged so far plus every local record";
    }

    // Probe the peer unconditionally. Leadership follows data: a node that
    // would serve an older or empty state must let the fresher side win. The
    // response is reused by the catch-up below.
    std::optional<SimpleHaGetStateResponse> peer_state;
    if (EnsureConnected()) {
        auto state = CallGetState(kPromotionProbeTimeout);
        if (state) {
            peer_state = std::move(state.value());
        } else {
            LOG(WARNING) << "Simple HA promotion peer probe failed: "
                         << toString(state.error());
        }
    } else {
        LOG(WARNING) << "Simple HA promotion cannot reach peer "
                     << peer_host_ << ":" << peer_port_;
    }

    if (!peer_state && empty) {
        // A cold two-node cluster has no leader service yet. Refusing to
        // start would deadlock both nodes. Best-effort HA accepts the empty
        // start and logs it loudly; a partitioned peer with data is recovered
        // only by restarting this node against that peer.
        LOG(WARNING) << "Simple HA empty promotion cannot reach peer; "
                        "starting empty";
        HAMetricManager::instance().inc_simple_ha_degraded_promotions();
    }

    if (peer_state) {
        // Let the peer serve when it holds records and this node holds none.
        // A retained empty baseline has lineage but no records, so key count
        // is the right signal; IsEmpty() would classify it as non-empty. A
        // standby that is only partway through a merge has not written keys
        // yet but already adopted the leader's non-zero cursor, so count that
        // as records too rather than starting empty while it catches up.
        // Only a standby reports key_count; a live leader's has_data tracks
        // its WAL prefix.
        const bool peer_has_records =
            peer_state->is_leader
                ? peer_state->has_data
                : (peer_state->key_count > 0 ||
                   (peer_state->has_data &&
                    (peer_state->durable_batch_id != 0 ||
                     peer_state->durable_last_seq != 0)));
        if (local_key_count == 0 && peer_has_records) {
            LOG(INFO) << "Simple HA promotion yields leadership: peer holds "
                         "records, peer_is_leader="
                      << peer_state->is_leader
                      << ", incarnation=" << peer_state->incarnation;
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }

        // Cursors are comparable only within one WAL incarnation. A standby
        // that is strictly ahead on the same lineage is the fresher source;
        // across lineages neither side is provably newer, so serve locally and
        // let the merge rules reconcile. Strict inequality keeps two nodes
        // from yielding to each other forever.
        std::string local_incarnation;
        {
            std::lock_guard<std::mutex> lock(incarnation_mutex_);
            local_incarnation = incarnation_;
        }
        if (!peer_state->is_leader && !local_incarnation.empty() &&
            peer_state->incarnation == local_incarnation &&
            peer_state->durable_last_seq >
                applied_seq_.load(std::memory_order_acquire)) {
            LOG(INFO) << "Simple HA promotion yields leadership: peer is ahead "
                         "in the same lineage, peer_seq="
                      << peer_state->durable_last_seq
                      << ", applied_seq="
                      << applied_seq_.load(std::memory_order_acquire);
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
    }

    // A preserved baseline (or a merge interrupted by leadership loss) needs
    // the leader's checkpoint before its cursor can be trusted. The pull loop
    // is stopped here, so it cannot complete that merge. If a leader is
    // reachable, release the term and let the loop finish; if it is not (or is
    // only a standby), the retained baseline is the newest recoverable state
    // and serving it beats failing closed.
    const bool state_needs_merge =
        needs_resync_.load(std::memory_order_acquire) ||
        merge_incomplete_.load(std::memory_order_acquire) ||
        baseline_needs_merge_.load(std::memory_order_acquire);
    if (peer_state && peer_state->is_leader && state_needs_merge) {
        LOG(INFO) << "Simple HA promotion yields leadership: a reachable "
                     "leader still has state to merge";
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    if (state_needs_merge) {
        LOG(WARNING) << "Simple HA promoting with an unresolved baseline "
                        "merge; serving the newest recoverable state, "
                        "peer_unreachable="
                     << (!peer_state ? 1 : 0)
                     << ", peer_is_leader="
                     << (peer_state && peer_state->is_leader ? 1 : 0);
        HAMetricManager::instance().inc_simple_ha_degraded_promotions();
    }

    if (!applier_) {
        applier_ = std::make_unique<OpLogApplier>(metadata_store_.get(),
                                                  config_.cluster_id);
    }

    if (!empty && !state_needs_merge) {
        // Best-effort final catch-up. A crashed leader is unreachable and the
        // applied state is already the newest recoverable one; a live leader
        // lets us pull the remaining tail before exporting. The first attempt
        // reuses the probe response; later attempts refresh it to chase a
        // leader that keeps advancing.
        bool catch_up_failed = false;
        std::optional<SimpleHaGetStateResponse> next_state =
            std::move(peer_state);
        for (int attempt = 0; attempt < 4; ++attempt) {
            if (!next_state) {
                if (!EnsureConnected()) {
                    catch_up_failed = true;
                    break;
                }
                auto state = CallGetState(kPromotionProbeTimeout);
                if (!state) {
                    catch_up_failed = true;
                    break;
                }
                next_state = std::move(state.value());
            }
            if (!next_state->is_leader) {
                // Only a leader owns a WAL to pull from. A standby peer was
                // already compared above; it cannot feed the tail catch-up.
                break;
            }
            const ErrorCode error = PullOnce(*next_state);
            next_state.reset();
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
    merge_incomplete_.store(false, std::memory_order_release);
    baseline_needs_merge_.store(false, std::memory_order_release);
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
    baseline_needs_merge_.store(true, std::memory_order_release);
    // The retained baseline is a consistent state at its cursor, so it is
    // promotable when no newer leader can be reached. If one exists, the
    // baseline_needs_merge gate forces a merge before following its WAL.
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
