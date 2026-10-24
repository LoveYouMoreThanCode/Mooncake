#include "master_client.h"

#include <iostream>
#include <string>
#include <vector>

#include "rpc_types.h"
#include "types.h"

namespace {

using namespace mooncake;

constexpr size_t kSegmentSize = 64ULL * 1024 * 1024;
constexpr size_t kSegmentBase = 0x900000000;
// A fixed identity so the shell smoke can mount in one invocation and remount
// as the same owner in later invocations.
const UUID kClientId{0x4c335f534d4f4bULL, 0x0000000000000001ULL};

Segment MakeSegment() {
    Segment segment;
    segment.id = UUID{0x4c335f5345474dULL, 0x0000000000000001ULL};
    segment.name = "l3-smoke-segment";
    segment.base = kSegmentBase;
    segment.size = kSegmentSize;
    segment.te_endpoint = "127.0.0.1:1";
    return segment;
}

int Usage() {
    std::cerr
        << "usage: simple_ha_smoke_client <put|exist> <host:port> <key>...\n";
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 4) {
        return Usage();
    }
    const std::string mode = argv[1];
    const std::string rpc_address = argv[2];
    std::vector<std::string> keys(argv + 3, argv + argc);

    MasterClient client(kClientId);
    if (client.Connect(rpc_address) != ErrorCode::OK) {
        std::cerr << "connect failed: " << rpc_address << "\n";
        return 1;
    }
    const Segment segment = MakeSegment();

    if (mode == "put") {
        auto mounted = client.MountSegment(segment);
        if (!mounted.has_value()) {
            std::cerr << "mount failed: " << toString(mounted.error()) << "\n";
            return 1;
        }
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segments = {segment.name};
        for (const auto& key : keys) {
            auto started = client.PutStart(key, {1024}, config);
            if (!started.has_value()) {
                std::cerr << "put start failed for " << key << ": "
                          << toString(started.error()) << "\n";
                return 1;
            }
            ObjectMeta meta;
            meta.key = key;
            auto ended = client.PutEnd(meta, ReplicaType::MEMORY);
            if (!ended.has_value()) {
                std::cerr << "put end failed for " << key << ": "
                          << toString(ended.error()) << "\n";
                return 1;
            }
        }
        std::cout << "put ok: " << keys.size() << " keys\n";
        return 0;
    }

    if (mode == "exist") {
        auto remounted = client.ReMountSegment({segment});
        if (!remounted.has_value()) {
            std::cerr << "remount failed: " << toString(remounted.error())
                      << "\n";
            return 1;
        }
        int missing = 0;
        for (const auto& key : keys) {
            auto exists = client.ExistKey(key);
            if (!exists.has_value()) {
                std::cerr << "exist key failed for " << key << ": "
                          << toString(exists.error()) << "\n";
                return 1;
            }
            if (!*exists) {
                std::cerr << "missing key: " << key << "\n";
                ++missing;
            }
        }
        if (missing != 0) {
            std::cerr << "exist failed: " << missing << " missing\n";
            return 1;
        }
        std::cout << "exist ok: " << keys.size() << " keys\n";
        return 0;
    }

    return Usage();
}
