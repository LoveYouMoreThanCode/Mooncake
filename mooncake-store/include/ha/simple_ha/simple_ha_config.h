#pragma once

#include <cstdint>
#include <string>

#include "ha/oplog/oplog_batch_types.h"

namespace mooncake {

// All Simple HA tunables in one struct so the existing config structs only
// need a single field (and one copy line) to carry them. Keeping this in a
// new header also keeps the diff on the shared config classes small.
struct SimpleHaConfig {
    bool enable = false;
    std::string peer_address;
    std::string rpc_address = "0.0.0.0";
    uint32_t rpc_port = 0;
    int pull_interval_ms = 1000;
    uint64_t wal_max_batches = 8192;
    uint64_t wal_max_bytes = 256ULL * 1024 * 1024;
    uint32_t snapshot_chunk_entries = 1024;
    // Seeds the first WAL instance when a promoted standby is restored.
    DurablePrefix initial_durable_prefix{};
};

}  // namespace mooncake
