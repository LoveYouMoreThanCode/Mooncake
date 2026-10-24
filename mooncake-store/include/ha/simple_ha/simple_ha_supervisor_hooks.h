#pragma once

#include <memory>

namespace mooncake {

class MasterServiceSupervisorConfig;
class SimpleHaLeaderService;
class WrappedMasterService;
class WrappedMasterServiceConfig;

namespace ha {
class StandbyController;
struct PromotionContext;
}  // namespace ha

// Applies the Simple HA part of the supervisor config to the per-term wrapped
// config and seeds the new WAL instance with the promoted cursor.
void ApplySimpleHaPromotionConfig(WrappedMasterServiceConfig& wrapped_config,
                                  const MasterServiceSupervisorConfig& config,
                                  const ha::PromotionContext& promotion_ctx);

// Returns nullptr when Simple HA is disabled or the RPC service fails to
// start; failures are logged.
std::unique_ptr<SimpleHaLeaderService> StartSimpleHaLeaderService(
    WrappedMasterService& service, const MasterServiceSupervisorConfig& config);

// Stops serving snapshot sessions while keeping GetState/PullWal available.
void DisableSimpleHaSnapshots(SimpleHaLeaderService* service);

// Moves the serving state into `retained_context` when the term ends without a
// promotion handoff. Returns true when a baseline was captured.
bool MaybeRetainSimpleHaServingState(
    const MasterServiceSupervisorConfig& config, WrappedMasterService& service,
    const ha::PromotionContext& promotion_ctx, bool shutting_down,
    ha::PromotionContext& retained_context);

// Installs a retained baseline on the standby controller, logging failures.
void InstallSimpleHaBaseline(ha::StandbyController& controller,
                             ha::PromotionContext context);

}  // namespace mooncake
