#include "ha/simple_ha/simple_ha_supervisor_hooks.h"

#include <glog/logging.h>

#include <utility>

#include "ha/simple_ha/simple_ha_leader_service.h"
#include "ha/standby_controller.h"
#include "master_config.h"
#include "rpc_service.h"
#include "types.h"

namespace mooncake {

void ApplySimpleHaPromotionConfig(WrappedMasterServiceConfig& wrapped_config,
                                  const MasterServiceSupervisorConfig& config,
                                  const ha::PromotionContext& promotion_ctx) {
    wrapped_config.simple_ha = config.simple_ha;
    if (promotion_ctx.metadata_store) {
        // The writer is constructed before RestoreFromBatchOpLogPromotion, so
        // the promoted cursor must seed its initial durable prefix here.
        wrapped_config.simple_ha.initial_durable_prefix =
            promotion_ctx.applied_cursor;
    }
}

std::unique_ptr<SimpleHaLeaderService> StartSimpleHaLeaderService(
    WrappedMasterService& service,
    const MasterServiceSupervisorConfig& config) {
    if (!config.simple_ha.enable) {
        return nullptr;
    }
    auto simple_ha_service = std::make_unique<SimpleHaLeaderService>(
        service.GetMasterService(),
        static_cast<uint16_t>(config.simple_ha.rpc_port),
        config.simple_ha.rpc_address, config.simple_ha.snapshot_chunk_entries);
    if (!simple_ha_service->Start()) {
        LOG(ERROR) << "Failed to start Simple HA RPC service";
        return nullptr;
    }
    return simple_ha_service;
}

void DisableSimpleHaSnapshots(SimpleHaLeaderService* service) {
    if (service) {
        service->DisableSnapshots();
    }
}

bool MaybeRetainSimpleHaServingState(
    const MasterServiceSupervisorConfig& config, WrappedMasterService& service,
    const ha::PromotionContext& promotion_ctx, bool shutting_down,
    ha::PromotionContext& retained_context) {
    if (!config.simple_ha.enable || promotion_ctx.metadata_store ||
        shutting_down) {
        return false;
    }
    // Leadership is already released, so the O(objects) walk does not hold
    // the term; the writer is stopped, so the map matches the durable prefix.
    auto drain =
        service.GetMasterService().DrainIntoPromotionContext(retained_context);
    if (!drain) {
        LOG(WARNING) << "Simple HA failed to retain serving state: "
                     << toString(drain.error());
        return false;
    }
    LOG(INFO) << "Simple HA retained serving state: objects="
              << retained_context.metadata_store->GetKeyCount()
              << ", base_batch=" << retained_context.applied_cursor.batch_id
              << ", base_seq=" << retained_context.applied_cursor.last_seq;
    return true;
}

void InstallSimpleHaBaseline(ha::StandbyController& controller,
                             ha::PromotionContext context) {
    const ErrorCode restore_err =
        controller.RestorePromotionBaseline(std::move(context));
    if (restore_err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to install retained Simple HA baseline: "
                   << toString(restore_err);
    }
}

}  // namespace mooncake
