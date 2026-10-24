#include "master_client.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <ylt/coro_http/coro_http_client.hpp>

#include "rpc_types.h"
#include "types.h"

#ifndef MOONCAKE_MASTER_BIN
#error "MOONCAKE_MASTER_BIN must point at the built mooncake_master binary"
#endif

namespace mooncake::test {
namespace {

using namespace std::chrono_literals;

constexpr size_t kSegmentSize = 64ULL * 1024 * 1024;
constexpr size_t kSegmentBase = 0x800000000;
constexpr const char* kMasterBin = MOONCAKE_MASTER_BIN;

uint16_t AllocateFreePort() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return 0;
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) !=
        0) {
        ::close(fd);
        return 0;
    }
    socklen_t length = sizeof(address);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) !=
        0) {
        ::close(fd);
        return 0;
    }
    ::close(fd);
    return ntohs(address.sin_port);
}

std::vector<uint16_t> AllocatePorts(size_t count) {
    std::set<uint16_t> used;
    std::vector<uint16_t> ports;
    while (used.size() < count) {
        const uint16_t port = AllocateFreePort();
        if (port != 0 && used.insert(port).second) {
            ports.push_back(port);
        }
    }
    return ports;
}

std::string ResolveExecutable(const std::string& name) {
    if (name.find('/') != std::string::npos) {
        return ::access(name.c_str(), X_OK) == 0 ? name : std::string{};
    }
    const char* path_env = std::getenv("PATH");
    if (path_env == nullptr) {
        return {};
    }
    const std::string path(path_env);
    size_t begin = 0;
    while (begin <= path.size()) {
        const size_t end = path.find(':', begin);
        const std::string dir =
            path.substr(begin, end == std::string::npos ? std::string::npos
                                                        : end - begin);
        if (!dir.empty()) {
            const std::string candidate = dir + "/" + name;
            if (::access(candidate.c_str(), X_OK) == 0) {
                return candidate;
            }
        }
        if (end == std::string::npos) {
            break;
        }
        begin = end + 1;
    }
    return {};
}

bool WaitForTcp(const std::string& host, uint16_t port,
                std::chrono::milliseconds timeout) {    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            return false;
        }
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(port);
        ::inet_pton(AF_INET, host.c_str(), &address.sin_addr);
        const bool ok =
            ::connect(fd, reinterpret_cast<sockaddr*>(&address),
                      sizeof(address)) == 0;
        ::close(fd);
        if (ok) {
            return true;
        }
        std::this_thread::sleep_for(50ms);
    }
    return false;
}

std::string HttpGet(uint16_t port, const std::string& path) {
    coro_http::coro_http_client client;
    auto result = client.get("http://127.0.0.1:" + std::to_string(port) + path);
    if (result.status != 200) {
        return {};
    }
    return std::string(result.resp_body);
}

bool WaitForHealth(uint16_t admin_port, const std::string& state,
                   std::chrono::milliseconds timeout,
                   bool require_ready = false) {
    const std::string needle = "\"ha_state\":\"" + state + "\"";
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        const auto body = HttpGet(admin_port, "/health");
        if (body.find(needle) != std::string::npos &&
            (!require_ready ||
             body.find("\"service_ready\":true") != std::string::npos)) {
            return true;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(50ms);
    }
}

bool WaitForNotServing(uint16_t admin_port,
                       std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        const auto body = HttpGet(admin_port, "/health");
        if (body.find("\"ha_state\":\"serving\"") == std::string::npos ||
            body.find("\"service_ready\":true") == std::string::npos) {
            return true;
        }
        std::this_thread::sleep_for(50ms);
    }
    return false;
}

class ChildProcess {
   public:
    ~ChildProcess() { Stop(); }

    bool Start(const std::vector<std::string>& args,
               const std::string& log_path) {
        std::vector<char*> argv;
        argv.reserve(args.size() + 1);
        for (const auto& arg : args) {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }
        argv.push_back(nullptr);

        int log_fd = -1;
        if (!log_path.empty()) {
            log_fd = ::open(log_path.c_str(),
                            O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
            if (log_fd < 0) {
                return false;
            }
        }

        pid_ = ::fork();
        if (pid_ == 0) {
            if (log_fd >= 0) {
                ::dup2(log_fd, STDERR_FILENO);
            }
            ::execv(argv[0], argv.data());
            ::_exit(127);
        }
        if (log_fd >= 0) {
            ::close(log_fd);
        }
        return pid_ > 0;
    }

    bool IsRunning() {
        if (pid_ <= 0) {
            return false;
        }
        int status = 0;
        if (::waitpid(pid_, &status, WNOHANG) == 0) {
            return true;
        }
        pid_ = 0;
        return false;
    }

    void Stop() {
        if (pid_ <= 0) {
            return;
        }
        ::kill(pid_, SIGKILL);
        ::waitpid(pid_, nullptr, 0);
        pid_ = 0;
    }

   private:
    pid_t pid_{0};
};

struct MasterNode {
    uint16_t rpc_port{0};
    uint16_t admin_port{0};
    uint16_t simple_ha_port{0};
    std::string root_dir;
    ChildProcess process;

    std::string RpcAddress() const {
        return "127.0.0.1:" + std::to_string(rpc_port);
    }
};

class SimpleHaSupervisorE2ETest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (!std::filesystem::exists(kMasterBin)) {
            GTEST_SKIP() << "master binary not found: " << kMasterBin;
        }
        const char* etcd_env = std::getenv("ETCD_BIN");
        etcd_bin_ =
            ResolveExecutable(etcd_env != nullptr ? etcd_env : "etcd");
        if (etcd_bin_.empty()) {
            GTEST_SKIP() << "etcd binary not found";
        }

        // The etcd wrapper is a shared library next to the build tree; make
        // sure the spawned master can load it even without an rpath.
        const auto master_path = std::filesystem::path(kMasterBin);
        const auto build_root = master_path.parent_path().parent_path()
                                    .parent_path();
        const auto wrapper_dir = build_root / "mooncake-common" / "etcd";
        if (std::filesystem::exists(wrapper_dir / "libetcd_wrapper.so")) {
            const char* current = std::getenv("LD_LIBRARY_PATH");
            std::string value = wrapper_dir.string();
            if (current != nullptr && *current != '\0') {
                value += ":" + std::string(current);
            }
            ::setenv("LD_LIBRARY_PATH", value.c_str(), 1);
        }

        work_dir_ = std::filesystem::temp_directory_path() /
                    ("simple-ha-l2-" + std::to_string(::getpid()) + "-" +
                     std::to_string(++instance_counter_));
        std::filesystem::remove_all(work_dir_);
        std::filesystem::create_directories(work_dir_);
        LOG(INFO) << "L2 work dir: " << work_dir_;

        const auto ports = AllocatePorts(8);
        ASSERT_EQ(ports.size(), 8u);
        etcd_client_port_ = ports[0];
        etcd_peer_port_ = ports[1];
        masters_[0].rpc_port = ports[2];
        masters_[0].admin_port = ports[3];
        masters_[0].simple_ha_port = ports[4];
        masters_[1].rpc_port = ports[5];
        masters_[1].admin_port = ports[6];
        masters_[1].simple_ha_port = ports[7];
        cluster_id_ = "simple-ha-l2-" + std::to_string(::getpid()) + "-" +
                      std::to_string(instance_counter_);

        for (auto& master : masters_) {
            master.root_dir = (work_dir_ / ("node-" +
                                            std::to_string(&master -
                                                           &masters_[0])))
                                  .string();
            std::filesystem::create_directories(master.root_dir);
        }

        ASSERT_TRUE(StartEtcd());
        ASSERT_TRUE(StartMaster(0));
        ASSERT_TRUE(StartMaster(1));
        ASSERT_TRUE(WaitForLeader(30s));
        ASSERT_TRUE(WaitForStandby(15s));
    }

    void TearDown() override {
        for (auto& master : masters_) {
            master.process.Stop();
        }
        etcd_process_.Stop();
        if (std::getenv("KEEP_L2_WORK") == nullptr) {
            std::filesystem::remove_all(work_dir_);
        } else {
            LOG(INFO) << "L2 work dir kept: " << work_dir_;
        }
    }

    bool StartEtcd() {
        const std::vector<std::string> args = {
            etcd_bin_,
            "--name=simple-ha-l2-etcd",
            "--data-dir=" + (work_dir_ / "etcd-data").string(),
            "--listen-client-urls=http://127.0.0.1:" +
                std::to_string(etcd_client_port_),
            "--advertise-client-urls=http://127.0.0.1:" +
                std::to_string(etcd_client_port_),
            "--listen-peer-urls=http://127.0.0.1:" +
                std::to_string(etcd_peer_port_),
            "--initial-advertise-peer-urls=http://127.0.0.1:" +
                std::to_string(etcd_peer_port_),
            "--initial-cluster=simple-ha-l2-etcd=http://127.0.0.1:" +
                std::to_string(etcd_peer_port_),
        };
        return etcd_process_.Start(args, (work_dir_ / "etcd.log").string()) &&
               WaitForTcp("127.0.0.1", etcd_client_port_, 15s);
    }

    bool StartMaster(int index) {
        auto& node = masters_[index];
        const auto& peer = masters_[1 - index];
        const std::vector<std::string> args = {
            kMasterBin,
            "--enable_ha=true",
            "--enable_simple_ha=true",
            "--enable_oplog=false",
            "--ha_backend_type=etcd",
            "--ha_backend_connstring=127.0.0.1:" +
                std::to_string(etcd_client_port_),
            "--ha_leader_lease_ttl_sec=2",
            "--cluster_id=" + cluster_id_,
            "--rpc_address=127.0.0.1",
            "--rpc_port=" + std::to_string(node.rpc_port),
            "--metrics_host=127.0.0.1",
            "--metrics_port=" + std::to_string(node.admin_port),
            "--enable_metric_reporting=true",
            "--root_fs_dir=" + node.root_dir,
            "--simple_ha_peer_address=127.0.0.1:" +
                std::to_string(peer.simple_ha_port),
            "--simple_ha_rpc_address=127.0.0.1",
            "--simple_ha_rpc_port=" + std::to_string(node.simple_ha_port),
            "--simple_ha_pull_interval_ms=50",
            "--simple_ha_snapshot_chunk_entries=8",
            "--logtostderr=1",
        };
        return node.process.Start(
            args, (work_dir_ / ("node-" + std::to_string(index) + ".log"))
                      .string());
    }

    Segment MakeTestSegment() const {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = "l2-segment";
        segment.base = kSegmentBase;
        segment.size = kSegmentSize;
        segment.te_endpoint = "127.0.0.1:1";
        return segment;
    }

    bool WaitForLeader(std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            for (int i = 0; i < 2; ++i) {
                if (WaitForHealth(masters_[i].admin_port, "serving", 0ms,
                                  /*require_ready=*/true)) {
                    leader_index_ = i;
                    return true;
                }
            }
            std::this_thread::sleep_for(100ms);
        }
        return false;
    }

    bool WaitForServing(int index, std::chrono::milliseconds timeout) {
        return WaitForHealth(masters_[index].admin_port, "serving", timeout,
                             /*require_ready=*/true);
    }

    bool WaitForStandby(std::chrono::milliseconds timeout) {
        const int standby = 1 - leader_index_;
        return WaitForHealth(masters_[standby].admin_port, "standby", timeout);
    }

    bool PutObjects(const std::vector<std::string>& keys) {
        MasterClient client(client_id_);
        if (client.Connect(masters_[leader_index_].RpcAddress()) !=
            ErrorCode::OK) {
            return false;
        }
        const Segment segment = MakeTestSegment();
        if (!client.MountSegment(segment).has_value()) {
            return false;
        }
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segments = {segment.name};
        for (const auto& key : keys) {
            auto started = client.PutStart(key, {1024}, config);
            if (!started.has_value()) {
                return false;
            }
            ObjectMeta meta;
            meta.key = key;
            if (!client.PutEnd(meta, ReplicaType::MEMORY).has_value()) {
                return false;
            }
        }
        return true;
    }

    bool KeysExist(int index, const std::vector<std::string>& keys) {
        MasterClient client(client_id_);
        if (client.Connect(masters_[index].RpcAddress()) != ErrorCode::OK) {
            LOG(ERROR) << "L2: connect to node " << index << " failed";
            return false;
        }
        // A promoted standby restores segment placeholders; the owner must
        // remount before the restored replicas become readable again.
        auto remounted = client.ReMountSegment({MakeTestSegment()});
        if (!remounted.has_value()) {
            LOG(ERROR) << "L2: remount on node " << index
                       << " failed: " << toString(remounted.error());
            return false;
        }
        for (const auto& key : keys) {
            auto exists = client.ExistKey(key);
            if (!exists.has_value()) {
                LOG(ERROR) << "L2: ExistKey(" << key << ") failed: "
                           << toString(exists.error());
                return false;
            }
            if (!*exists) {
                LOG(ERROR) << "L2: key missing on node " << index << ": "
                           << key;
                return false;
            }
        }
        return true;
    }

    std::string etcd_bin_;
    ChildProcess etcd_process_;
    std::filesystem::path work_dir_;
    std::string cluster_id_;
    UUID client_id_{generate_uuid()};
    uint16_t etcd_client_port_{0};
    uint16_t etcd_peer_port_{0};
    MasterNode masters_[2];
    int leader_index_{0};

    inline static int instance_counter_{0};
};

}  // namespace

TEST_F(SimpleHaSupervisorE2ETest, LeaderCrashKeepsStandbyData) {
    const std::vector<std::string> keys = {"l2-crash-a", "l2-crash-b"};
    ASSERT_TRUE(PutObjects(keys));
    // Give the in-memory WAL a moment to reach the standby, then wait for it
    // to report a caught-up standby state.
    std::this_thread::sleep_for(1s);
    ASSERT_TRUE(WaitForStandby(15s));

    const int survivor = 1 - leader_index_;
    masters_[leader_index_].process.Stop();
    ASSERT_TRUE(WaitForServing(survivor, 30s));
    ASSERT_TRUE(KeysExist(survivor, keys));
}

TEST_F(SimpleHaSupervisorE2ETest, EtcdRestartKeepsRetainedState) {
    const std::vector<std::string> keys = {"l2-lease-a", "l2-lease-b"};
    ASSERT_TRUE(PutObjects(keys));
    std::this_thread::sleep_for(1s);
    ASSERT_TRUE(WaitForStandby(15s));

    etcd_process_.Stop();
    ASSERT_TRUE(WaitForNotServing(masters_[0].admin_port, 15s));
    ASSERT_TRUE(WaitForNotServing(masters_[1].admin_port, 15s));

    ASSERT_TRUE(StartEtcd());
    ASSERT_TRUE(WaitForLeader(30s));
    ASSERT_TRUE(KeysExist(leader_index_, keys));
}

}  // namespace mooncake::test
