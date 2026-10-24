#pragma once

namespace mooncake {

class DefaultConfig;
struct MasterConfig;

// Simple HA command-line/config plumbing lives in its own translation unit so
// the shared master entry point only needs three call sites.
void InitSimpleHaConf(const DefaultConfig& default_config,
                      MasterConfig& master_config);
void LoadSimpleHaConfFromCmdline(MasterConfig& master_config, bool conf_set);
// Fails the process with LOG(FATAL) when the Simple HA configuration is
// inconsistent. No-op when Simple HA is disabled.
void ValidateSimpleHaConfig(const MasterConfig& master_config);

}  // namespace mooncake
