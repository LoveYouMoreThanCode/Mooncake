#include "ha/simple_ha/simple_ha_wal.h"

#include <algorithm>
#include <chrono>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

#include "ha/oplog/oplog_batch_codec.h"
#include "ha/oplog/ordered_oplog_writer.h"
#include "ha/standby_controller.h"
#include "master_service.h"

namespace mooncake {
namespace {

std::string MakeIncarnation() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    std::random_device device;
    std::ostringstream out;
    out << std::chrono::duration_cast<std::chrono::milliseconds>(now).count()
        << '-' << device();
    return out.str();
}

}  // namespace

SimpleHaWal::SimpleHaWal(size_t max_batches, size_t max_bytes,
                         DurablePrefix initial_prefix)
    : incarnation_(MakeIncarnation()),
      durable_prefix_(initial_prefix),
      max_batches_(max_batches),
      max_bytes_(max_bytes),
      initial_has_data_(initial_prefix.batch_id != 0 ||
                        initial_prefix.last_seq != 0) {
    first_batch_id_ = initial_prefix.batch_id + 1;
}

ErrorCode SimpleHaWal::WriteBatchAndAdvancePrefix(
    const OpLogBatchRecord& batch, const DurablePrefix& expected_prefix) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (expected_prefix != durable_prefix_) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (batch.batch_id != durable_prefix_.batch_id + 1 ||
        batch.first_seq != durable_prefix_.last_seq + 1) {
        return ErrorCode::INVALID_PARAMS;
    }

    size_t encoded_bytes = EncodeOpLogBatchRecord(batch).size();
    encoded_bytes_ += encoded_bytes;
    batches_.push_back(StoredBatch{.batch = batch,
                                   .encoded_bytes = encoded_bytes});
    durable_prefix_ = DurablePrefix{.batch_id = batch.batch_id,
                                    .last_seq = batch.last_seq};

    // Keep the newest batch even if it alone exceeds the byte budget: dropping
    // it would advance the durable prefix without retaining its record.
    while (!batches_.empty() &&
           ((max_batches_ > 0 && batches_.size() > max_batches_) ||
            (max_bytes_ > 0 && encoded_bytes_ > max_bytes_ &&
             batches_.size() > 1))) {
        encoded_bytes_ -= batches_.front().encoded_bytes;
        batches_.pop_front();
    }
    first_batch_id_ = batches_.empty()
                          ? durable_prefix_.batch_id + 1
                          : batches_.front().batch.batch_id;
    return ErrorCode::OK;
}

std::vector<OpLogBatchRecord> SimpleHaWal::Pull(uint64_t from_batch_id,
                                                size_t max_batches) const {
    std::vector<OpLogBatchRecord> result;
    if (max_batches == 0) {
        return result;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    result.reserve(std::min(max_batches, batches_.size()));
    for (const auto& stored : batches_) {
        if (stored.batch.batch_id < from_batch_id) {
            continue;
        }
        result.push_back(stored.batch);
        if (result.size() >= max_batches) {
            break;
        }
    }
    return result;
}

SimpleHaWalState SimpleHaWal::SnapshotState() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return SimpleHaWalState{
        .incarnation = incarnation_,
        .first_batch_id = first_batch_id_,
        .last_batch_id = durable_prefix_.batch_id,
        .durable_prefix = durable_prefix_,
        .has_data = initial_has_data_ || !batches_.empty() ||
                    durable_prefix_.batch_id != 0 ||
                    durable_prefix_.last_seq != 0,
    };
}

std::string SimpleHaWal::incarnation() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return incarnation_;
}

bool SimpleHaWal::has_data() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return initial_has_data_ || !batches_.empty() ||
           durable_prefix_.batch_id != 0 || durable_prefix_.last_seq != 0;
}

ErrorCode MasterService::InitializeSimpleHaWriter() {
    if (!simple_ha_.enable) {
        return ErrorCode::INVALID_PARAMS;
    }
    auto wal = std::make_shared<SimpleHaWal>(
        simple_ha_.wal_max_batches, simple_ha_.wal_max_bytes,
        simple_ha_.initial_durable_prefix);

    OrderedOpLogWriterConfig writer_config;
    writer_config.max_entries_per_batch = oplog_batch_max_entries_;
    writer_config.initial_durable_prefix = simple_ha_.initial_durable_prefix;

    auto writer = batch_oplog_writer_factory_(
        writer_config,
        [wal](const OpLogBatchRecord& batch,
              const DurablePrefix& expected_prefix) {
            return wal->WriteBatchAndAdvancePrefix(batch, expected_prefix);
        });
    if (!writer) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (!writer->IsAccepting()) {
        return writer->LastError();
    }
    writer->Start();

    if (ordered_oplog_writer_) {
        ordered_oplog_writer_->Stop();
    }
    simple_ha_wal_ = std::move(wal);
    ordered_oplog_writer_ = std::move(writer);
    ordered_oplog_writer_->ActivateRuntimeMetrics();
    return ErrorCode::OK;
}

void MasterService::InitializeSimpleHaWriterOrThrow() {
    const ErrorCode err = InitializeSimpleHaWriter();
    if (err != ErrorCode::OK) {
        throw std::runtime_error(std::string("failed to create Simple HA "
                                             "writer: ") +
                                 toString(err));
    }
}

bool MasterService::WaitForSimpleHaCallbacks(
    uint64_t sequence_id, std::chrono::milliseconds timeout) {
    if (!ordered_oplog_writer_) {
        return true;
    }
    return ordered_oplog_writer_->WaitForCallbacksThrough(sequence_id, timeout);
}

tl::expected<void, ErrorCode> MasterService::DrainIntoPromotionContext(
    ha::PromotionContext& context) {
    if (!simple_ha_.enable || !simple_ha_wal_) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // The writer must already be stopped: its durable prefix has to describe
    // the same state as the map we are about to drain.
    const auto wal_state = simple_ha_wal_->SnapshotState();

    auto store = std::make_unique<StandbyMetadataStore>();
    ReplicaID max_replica_id = 0;

    // Quiesce the service while retaining it: the RPC server and writer are
    // already stopped by the supervisor; taking both locks keeps the
    // background workers from mutating metadata between shards.
    std::unique_lock<std::shared_mutex> client_lock(client_mutex_);
    std::unique_lock<std::shared_mutex> snapshot_lock(master_state_mutex_);

    std::vector<TenantId> tenants;
    VisitTenants([&tenants](const TenantId& tenant) {
        tenants.push_back(tenant);
    });

    for (const TenantId& tenant : tenants) {
        for (size_t shard_index = 0; shard_index < kNumShards; ++shard_index) {
            // A cursor holds the shard lock for its whole body, so collect the
            // keys first and erase them one at a time afterwards. Only keys are
            // buffered here, keeping the peak at one shard's key list plus one
            // object's metadata instead of a second full copy of the dataset.
            std::vector<std::string> keys;
            for (auto object : ReadCursor(tenant, {shard_index, kNumShards})) {
                keys.push_back(object.key());
            }
            for (const std::string& key : keys) {
                MetadataAccessorRW accessor(
                    this, ObjectIdentity{tenant, key},
                    MetadataAccessorRW::AsIs{});
                if (!accessor.IsPublished()) {
                    continue;
                }
                ObjectMetadata& metadata = accessor.Get();

                StandbyObjectMetadata standby;
                standby.client_id = metadata.client_id;
                standby.size = metadata.TotalLength();
                for (const auto& replica : metadata.GetAllReplicas()) {
                    if (!replica.is_completed()) {
                        continue;
                    }
                    const Replica::Descriptor descriptor =
                        replica.get_descriptor();
                    max_replica_id =
                        std::max(max_replica_id, descriptor.id);
                    standby.replicas.push_back(descriptor);
                }
                standby.group_id = metadata.group_id;
                standby.data_type = metadata.data_type;
                standby.hard_pinned = metadata.IsHardPinned();
                standby.session_id = metadata.session_id;

                // Objects with only in-flight replicas cannot be represented
                // in a standby baseline; they are dropped, exactly as the
                // snapshot scan would drop them.
                if (!standby.replicas.empty() &&
                    !store->PutMetadata(tenant.value(), key, standby)) {
                    return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                }
                accessor.Erase(QuotaEraseMode::kDrain);
            }
        }
    }

    std::vector<StandbySegmentInfo> segments;
    std::vector<std::pair<Segment, UUID>> mounted;
    if (segment_manager_.getSegmentAccess().GetAllSegments(mounted) !=
        ErrorCode::OK) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    segments.reserve(mounted.size());
    for (const auto& [segment, client_id] : mounted) {
        (void)client_id;
        if (segment.name.empty() || segment.te_endpoint.empty() ||
            segment.size == 0) {
            continue;
        }
        segments.push_back(StandbySegmentInfo{
            .segment_name = segment.name,
            .transport_endpoint = segment.te_endpoint,
            .capacity = segment.size,
            .is_memory_segment = true,
            .file_path = {},
        });
    }

    context = ha::PromotionContext{};
    context.applied_cursor = wal_state.durable_prefix;
    context.applied_seq_id = wal_state.durable_prefix.last_seq;
    context.segments = std::move(segments);
    context.max_replica_id = max_replica_id;
    context.metadata_store = std::move(store);
    context.producer_view_version = view_version_;
    return {};
}

}  // namespace mooncake
