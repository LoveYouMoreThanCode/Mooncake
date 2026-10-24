#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <ylt/reflection/user_reflect_macro.hpp>

namespace mooncake {

// Wire types for the leader-served Simple HA protocol. Payload blobs are the
// same struct_pack MetadataPayload used by the batch OpLog, so a standby can
// apply checkpoint entries and WAL entries without a second codec.

struct SimpleHaGetStateResponse {
    bool success{false};
    int32_t error_code{0};
    std::string incarnation;
    uint64_t first_batch_id{0};
    uint64_t last_batch_id{0};
    uint64_t durable_batch_id{0};
    uint64_t durable_last_seq{0};
    bool has_data{false};
    bool is_leader{false};
    // Records held locally. A serving leader does not maintain an O(1) count,
    // so it reports 0 and callers use has_data (its WAL prefix) instead. A
    // standby reports its store's key count, which distinguishes "holds
    // records" from "has lineage but no records".
    uint64_t key_count{0};
};
YLT_REFL(SimpleHaGetStateResponse, success, error_code, incarnation,
         first_batch_id, last_batch_id, durable_batch_id, durable_last_seq,
         has_data, is_leader, key_count);

struct SimpleHaPullWalRequest {
    uint64_t from_batch_id{1};
    uint32_t max_batches{64};
};
YLT_REFL(SimpleHaPullWalRequest, from_batch_id, max_batches);

struct SimpleHaPullWalResponse {
    bool success{false};
    int32_t error_code{0};
    std::string incarnation;
    uint64_t first_batch_id{0};
    uint64_t last_batch_id{0};
    uint64_t durable_batch_id{0};
    uint64_t durable_last_seq{0};
    // JSON-encoded OpLogBatchRecord, one per batch, in ascending batch order.
    std::vector<std::string> batches;
};
YLT_REFL(SimpleHaPullWalResponse, success, error_code, incarnation,
         first_batch_id, last_batch_id, durable_batch_id, durable_last_seq,
         batches);

struct SimpleHaSegmentEntry {
    std::string segment_name;
    std::string transport_endpoint;
    uint64_t capacity{0};
    bool is_memory_segment{true};
    std::string file_path;
};
YLT_REFL(SimpleHaSegmentEntry, segment_name, transport_endpoint, capacity,
         is_memory_segment, file_path);

struct SimpleHaCheckpointEntry {
    std::string tenant_id;
    std::string key;
    // struct_pack MetadataPayload bytes.
    std::string payload;
};
YLT_REFL(SimpleHaCheckpointEntry, tenant_id, key, payload);

struct SimpleHaBeginCheckpointResponse {
    bool success{false};
    int32_t error_code{0};
    std::string checkpoint_id;
    std::string incarnation;
    uint64_t base_batch_id{0};
    uint64_t base_seq{0};
    std::vector<SimpleHaSegmentEntry> segments;
};
YLT_REFL(SimpleHaBeginCheckpointResponse, success, error_code, checkpoint_id,
         incarnation, base_batch_id, base_seq, segments);

struct SimpleHaCheckpointChunkRequest {
    std::string checkpoint_id;
    uint32_t tenant_index{0};
    uint32_t shard_index{0};
    uint32_t offset{0};
    uint32_t max_entries{1024};
};
YLT_REFL(SimpleHaCheckpointChunkRequest, checkpoint_id, tenant_index,
         shard_index, offset, max_entries);

struct SimpleHaCheckpointChunkResponse {
    bool success{false};
    int32_t error_code{0};
    std::string incarnation;
    uint64_t base_batch_id{0};
    uint64_t base_seq{0};
    std::vector<SimpleHaCheckpointEntry> entries;
    uint32_t next_tenant_index{0};
    uint32_t next_shard_index{0};
    uint32_t next_offset{0};
    bool done{false};
};
YLT_REFL(SimpleHaCheckpointChunkResponse, success, error_code, incarnation,
         base_batch_id, base_seq, entries, next_tenant_index, next_shard_index,
         next_offset, done);

struct SimpleHaProbeResponse {
    bool success{false};
    int32_t error_code{0};
    std::string incarnation;
    bool has_data{false};
    uint64_t applied_batch_id{0};
    uint64_t applied_seq{0};
    bool is_leader{false};
};
YLT_REFL(SimpleHaProbeResponse, success, error_code, incarnation, has_data,
         applied_batch_id, applied_seq, is_leader);

}  // namespace mooncake
