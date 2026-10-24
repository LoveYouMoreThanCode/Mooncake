#include "ha/simple_ha/simple_ha_flags.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "default_config.h"
#include "master_config.h"

// Simple HA: leader-served in-memory WAL + checkpoint, no etcd data plane.
DEFINE_bool(enable_simple_ha, false,
            "Enable Simple HA: leader-served in-memory WAL and checkpoint "
            "replication instead of etcd-stored OpLog");
DEFINE_string(simple_ha_peer_address, "",
              "Peer master's Simple HA RPC endpoint (host:port)");
DEFINE_string(simple_ha_rpc_address, "0.0.0.0",
              "Bind address for the Simple HA RPC service");
DEFINE_uint32(simple_ha_rpc_port, 0,
              "Port for the Simple HA RPC service (required when enabled)");
DEFINE_int32(simple_ha_pull_interval_ms, 1000,
             "Standby pull interval for Simple HA, in milliseconds");
DEFINE_uint64(simple_ha_wal_max_batches, 8192,
              "Maximum batches retained in the Simple HA in-memory WAL ring");
DEFINE_uint64(simple_ha_wal_max_bytes, 256ULL * 1024 * 1024,
              "Maximum encoded bytes retained in the Simple HA WAL ring");
DEFINE_uint32(simple_ha_checkpoint_chunk_entries, 1024,
              "Maximum checkpoint entries returned per Simple HA RPC chunk");

namespace mooncake {

void InitSimpleHaConf(const DefaultConfig& default_config,
                      MasterConfig& master_config) {
    default_config.GetBool("enable_simple_ha", &master_config.simple_ha.enable,
                           FLAGS_enable_simple_ha);
    default_config.GetString("simple_ha_peer_address",
                             &master_config.simple_ha.peer_address,
                             FLAGS_simple_ha_peer_address);
    default_config.GetString("simple_ha_rpc_address",
                             &master_config.simple_ha.rpc_address,
                             FLAGS_simple_ha_rpc_address);
    default_config.GetUInt32("simple_ha_rpc_port",
                             &master_config.simple_ha.rpc_port,
                             FLAGS_simple_ha_rpc_port);
    default_config.GetInt32("simple_ha_pull_interval_ms",
                            &master_config.simple_ha.pull_interval_ms,
                            FLAGS_simple_ha_pull_interval_ms);
    default_config.GetUInt64("simple_ha_wal_max_batches",
                             &master_config.simple_ha.wal_max_batches,
                             FLAGS_simple_ha_wal_max_batches);
    default_config.GetUInt64("simple_ha_wal_max_bytes",
                             &master_config.simple_ha.wal_max_bytes,
                             FLAGS_simple_ha_wal_max_bytes);
    default_config.GetUInt32("simple_ha_checkpoint_chunk_entries",
                             &master_config.simple_ha.checkpoint_chunk_entries,
                             FLAGS_simple_ha_checkpoint_chunk_entries);
}

void LoadSimpleHaConfFromCmdline(MasterConfig& master_config, bool conf_set) {
    google::CommandLineFlagInfo info;
    if ((google::GetCommandLineFlagInfo("enable_simple_ha", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.simple_ha.enable = FLAGS_enable_simple_ha;
    }
    if ((google::GetCommandLineFlagInfo("simple_ha_peer_address", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.simple_ha.peer_address = FLAGS_simple_ha_peer_address;
    }
    if ((google::GetCommandLineFlagInfo("simple_ha_rpc_address", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.simple_ha.rpc_address = FLAGS_simple_ha_rpc_address;
    }
    if ((google::GetCommandLineFlagInfo("simple_ha_rpc_port", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.simple_ha.rpc_port = FLAGS_simple_ha_rpc_port;
    }
    if ((google::GetCommandLineFlagInfo("simple_ha_pull_interval_ms", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.simple_ha.pull_interval_ms =
            FLAGS_simple_ha_pull_interval_ms;
    }
    if ((google::GetCommandLineFlagInfo("simple_ha_wal_max_batches", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.simple_ha.wal_max_batches =
            FLAGS_simple_ha_wal_max_batches;
    }
    if ((google::GetCommandLineFlagInfo("simple_ha_wal_max_bytes", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.simple_ha.wal_max_bytes = FLAGS_simple_ha_wal_max_bytes;
    }
    if ((google::GetCommandLineFlagInfo("simple_ha_checkpoint_chunk_entries",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.simple_ha.checkpoint_chunk_entries =
            FLAGS_simple_ha_checkpoint_chunk_entries;
    }
}

void ValidateSimpleHaConfig(const MasterConfig& master_config) {
    if (!master_config.simple_ha.enable) {
        return;
    }
    if (!master_config.enable_ha) {
        LOG(FATAL) << "enable_simple_ha requires enable_ha=true";
    }
    if (master_config.enable_oplog) {
        LOG(FATAL)
            << "enable_simple_ha and enable_oplog are mutually exclusive";
    }
    if (master_config.enable_oplog_snapshot) {
        LOG(FATAL) << "enable_simple_ha does not use batch OpLog snapshots";
    }
    if (master_config.ha_backend_type != "etcd") {
        LOG(FATAL) << "enable_simple_ha requires ha_backend_type=etcd for "
                      "election and presence";
    }
    if (master_config.simple_ha.peer_address.empty()) {
        LOG(FATAL) << "enable_simple_ha requires simple_ha_peer_address";
    }
    if (master_config.cluster_id.empty()) {
        LOG(FATAL) << "enable_simple_ha requires a non-empty cluster_id";
    }
    if (master_config.simple_ha.rpc_port == 0) {
        LOG(FATAL) << "enable_simple_ha requires simple_ha_rpc_port";
    }
    if (master_config.simple_ha.rpc_port > 65535) {
        LOG(FATAL) << "simple_ha_rpc_port must be <= 65535";
    }
    if (master_config.simple_ha.rpc_port == master_config.rpc_port) {
        LOG(FATAL) << "simple_ha_rpc_port must differ from the client rpc_port";
    }
    if (master_config.simple_ha.wal_max_batches == 0 &&
        master_config.simple_ha.wal_max_bytes == 0) {
        LOG(FATAL) << "enable_simple_ha requires a non-zero WAL budget";
    }
}

}  // namespace mooncake
