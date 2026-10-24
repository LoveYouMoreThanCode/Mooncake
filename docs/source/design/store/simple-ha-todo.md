---
orphan: true
---

# Simple HA 待办

*2026-10 · 内部使用，不作为对外文档*

## 已完成

- 核心复制链路：内存 WAL ring、leader 快照/WAL、备端拉取、升主 restore、配置与 flag。
- 快照静默屏障、升主 restore 租约、降级升主指标。
- Move-drain 跨任期保留 + peer-wins merge（快照缺席不删除，WAL REMOVE 生效）。
- 单元测试：WAL / callback barrier / restore lease / snapshot cursor / drain。
- **L1 harness（无 etcd）**：进程内 leader + 真实 controller，覆盖
  baseline 保留、peer-wins merge、WAL REMOVE、mid-merge 对端挂掉。
  目标：`simple_ha_l1_harness_test`（mid-merge 用例需
  `MOONCAKE_ENABLE_TEST_FAILPOINTS=ON`）。
- **L2 supervisor e2e**：自起 etcd + 两个真实 master 进程，覆盖
  leader crash 后备升主保留数据、etcd 重启后旧主用保留状态重新升主。
  目标：`simple_ha_supervisor_e2e_test`，仅 `STORE_USE_ETCD=ON` 时构建。
- **L3 shell smoke**：`simple_ha_smoke_e2e.sh` + `simple_ha_smoke_client`，
  覆盖 kill -9 leader 后备升主、SIGSTOP/SIGCONT 冻结/恢复
  （备升主 → 老主恢复后 merge → 再杀新主 → 老主带合并状态重新升主）。

## 待办（按优先级）

### P0

- [ ] **L2 补充：supervisor 级 mid-merge 故障与反复 flap**
  - [ ] 新主在旧主 merge 中途故障 → 旧主带本地记录重新升主
    （L1 覆盖语义；L3 的 freeze/resume 覆盖了接近的流程）
  - [ ] 反复 flap 不出现双空
- [ ] 把 `scripts/run_simple_ha_e2e.sh` 接入默认本地 CI / nightly
  （目前是 opt-in 脚本，需要 Go/etcd；CI 的 etcd 作业已会自动跑 L2）

### P1

- [ ] merge 增长上限 + 指标（本地独有 key 数、merge 次数、partial-merge 升主告警）
- [ ] Simple HA standby lag 接到 `ha_oplog_standby_lag`
- [ ] 空节点 presence 检查（对端不可达时目前 fail-open 空启动）
- [ ] 升主暂停时间：`StopLoop` join 在途 RPC（5s/10s）可能超出预算

### P2

- [ ] `SnapshotSegments` 跳过无效/重复 endpoint；merge 时过滤陈旧 segment 的 replica
- [ ] 确认 Simple HA 下 weight mutations 永久关闭是否可接受
- [ ] 清理 merge 后遗留的旧 snapshot 路径/字段

## 一键脚本

`scripts/run_simple_ha_e2e.sh`（opt-in）串起整条链路：

```bash
./scripts/run_simple_ha_e2e.sh            # prereq → build → L1 → L2 → L3
./scripts/run_simple_ha_e2e.sh --skip-l3  # 只跑 gtest
./scripts/run_simple_ha_e2e.sh --build-dir build-l2 --skip-build
```

- 构建目录默认 `build-ha-e2e`，自动带
  `STORE_USE_ETCD=ON`、`BUILD_UNIT_TESTS=ON`、
  `MOONCAKE_ENABLE_TEST_FAILPOINTS=ON`；
- 缺少 Go/etcd/curl 时默认 SKIP（`--strict` 则失败）；
- 日志在 `local_test/run-simple-ha-e2e-logs/<timestamp>/`；
- `--keep-work` 保留 L3 的临时目录和日志。

## L2 复现命令

```bash
cmake -S . -B build-l2 \
  -DWITH_STORE=ON -DWITH_TE=OFF -DBUILD_UNIT_TESTS=ON \
  -DSTORE_USE_ETCD=ON -DMOONCAKE_ENABLE_TEST_FAILPOINTS=ON \
  -DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=/tmp/googletest-1.17.0
cmake --build build-l2 --target simple_ha_supervisor_e2e_test -j4
./build-l2/mooncake-store/tests/simple_ha_supervisor_e2e_test

# L3 shell smoke
cmake --build build-l2 --target mooncake_master simple_ha_smoke_client -j4
KEEP_WORK=1 ./mooncake-store/tests/ha/simple_ha/simple_ha_smoke_e2e.sh
```

需要 etcd/etcdctl（`apt install etcd-server etcd-client`），以及 Go
（构建 `mooncake-common/etcd` 的 c-shared wrapper）。

## 已知取舍（已接受）

- 快照缺席不删除；只有 WAL 里显式 `REMOVE` / `PUT_REVOKE` 删除。
- 同 key 冲突 peer wins；不引入 per-key 版本号。
- 新主快照 base 之前删除的 key 可能复活；本地独有记录会单调增长。
- replica ID 只保证单对象内唯一，不保证跨 lineage 全局唯一。
