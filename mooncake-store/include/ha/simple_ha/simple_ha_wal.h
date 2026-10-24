#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <vector>

#include "ha/oplog/oplog_batch_types.h"
#include "types.h"

namespace mooncake {

struct SimpleHaWalState {
    std::string incarnation;
    uint64_t first_batch_id{0};
    uint64_t last_batch_id{0};
    DurablePrefix durable_prefix{};
    bool has_data{false};
};

// In-memory WAL ring for Simple HA. OrderedOpLogWriter writes batches here
// instead of to etcd; a standby pulls them over the leader's RPC service.
// Capacity is bounded by both batch count and encoded bytes. Evicting the
// oldest batch advances first_batch_id; a standby that is behind first_batch_id
// must full-resync from a checkpoint.
class SimpleHaWal {
   public:
    SimpleHaWal(size_t max_batches, size_t max_bytes,
                DurablePrefix initial_prefix);

    // Sink for OrderedOpLogWriter::WriteBatchFn. The expected prefix is the
    // prefix the writer last advanced to; a mismatch means another writer
    // touched this WAL, which must not happen in the single-leader design.
    ErrorCode WriteBatchAndAdvancePrefix(const OpLogBatchRecord& batch,
                                         const DurablePrefix& expected_prefix);

    // Batches with batch_id >= from_batch_id, in ascending order, at most
    // `max_batches`. The response must be read together with GetState():
    // if `from_batch_id < first_batch_id`, batches were overwritten.
    std::vector<OpLogBatchRecord> Pull(uint64_t from_batch_id,
                                       size_t max_batches) const;

    SimpleHaWalState GetState() const;
    std::string incarnation() const;
    bool has_data() const;

   private:
    struct StoredBatch {
        OpLogBatchRecord batch;
        size_t encoded_bytes{0};
    };

    mutable std::mutex mutex_;
    std::string incarnation_;
    DurablePrefix durable_prefix_;
    uint64_t first_batch_id_{0};
    size_t max_batches_{0};
    size_t max_bytes_{0};
    size_t encoded_bytes_{0};
    bool initial_has_data_{false};
    std::deque<StoredBatch> batches_;
};

}  // namespace mooncake
