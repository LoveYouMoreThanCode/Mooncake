#include "ha/simple_ha/simple_ha_wal.h"

#include <algorithm>
#include <chrono>
#include <random>
#include <sstream>
#include <utility>

#include "ha/oplog/oplog_batch_codec.h"
#include "ha/oplog/ordered_oplog_writer.h"
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
    if (!enable_simple_ha_) {
        return ErrorCode::INVALID_PARAMS;
    }
    auto wal = std::make_shared<SimpleHaWal>(
        simple_ha_wal_max_batches_, simple_ha_wal_max_bytes_,
        initial_durable_prefix_);

    OrderedOpLogWriterConfig writer_config;
    writer_config.max_entries_per_batch = oplog_batch_max_entries_;
    writer_config.initial_durable_prefix = initial_durable_prefix_;

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

bool MasterService::WaitForSimpleHaCallbacks(
    uint64_t sequence_id, std::chrono::milliseconds timeout) {
    if (!ordered_oplog_writer_) {
        return true;
    }
    return ordered_oplog_writer_->WaitForCallbacksThrough(sequence_id, timeout);
}

}  // namespace mooncake
