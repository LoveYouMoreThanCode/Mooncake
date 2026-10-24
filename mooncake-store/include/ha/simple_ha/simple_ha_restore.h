#pragma once

#include <chrono>
#include <cstdint>

#include "object_metadata.h"

namespace mooncake {

// Simple HA grants every restored object a fresh read lease: a client may
// still be inside the lease it received from the previous primary, and an
// eviction must not race that in-flight read. Other HA modes keep their
// pre-existing behavior of restoring objects without a lease.
inline void MaybeGrantSimpleHaRestoreLease(bool enabled,
                                           const ObjectMetadata& metadata,
                                           uint64_t lease_ttl_ms) {
    if (!enabled) {
        return;
    }
    metadata.GrantReadLease(std::chrono::milliseconds(lease_ttl_ms));
}

}  // namespace mooncake
