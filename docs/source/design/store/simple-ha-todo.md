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

## 进行中

- [ ] **L1 harness（无 etcd）**：进程内 leader + 真实 controller
  - [ ] baseline + 对端不可达 → 升主保留本地记录
  - [ ] 完整 merge：peer wins + 本地独有 key 保留
  - [ ] WAL REMOVE 生效、快照缺席不删除
  - [ ] mid-merge 对端挂掉 → 部分 merge 仍可升主（需 `MOONCAKE_ENABLE_TEST_FAILPOINTS=ON`）

## 待办（按优先级）

### P0

- [ ] **L2：etcd + supervisor e2e**（不依赖 etcd 精确时序；用 failpoint + 短 TTL）
  - [ ] 丢 lease → drain → 重新当选 → 用保留状态升主
  - [ ] 同步中途新主再故障 → 本节点带本地记录升主
  - [ ] 反复 flap 不出现双空
- [ ] **L3：shell smoke**
  - [ ] kill -9 leader → 备升主
  - [ ] 端口封堵/恢复 → 重连与 merge

### P1

- [ ] merge 增长上限 + 指标（本地独有 key 数、merge 次数、partial-merge 升主告警）
- [ ] Simple HA standby lag 接到 `ha_oplog_standby_lag`
- [ ] 空节点 presence 检查（对端不可达时目前 fail-open 空启动）
- [ ] 升主暂停时间：`StopLoop` join 在途 RPC（5s/10s）可能超出预算

### P2

- [ ] `SnapshotSegments` 跳过无效/重复 endpoint；merge 时过滤陈旧 segment 的 replica
- [ ] 确认 Simple HA 下 weight mutations 永久关闭是否可接受
- [ ] 清理 merge 后遗留的旧 snapshot 路径/字段

## 已知取舍（已接受）

- 快照缺席不删除；只有 WAL 里显式 `REMOVE` / `PUT_REVOKE` 删除。
- 同 key 冲突 peer wins；不引入 per-key 版本号。
- 新主快照 base 之前删除的 key 可能复活；本地独有记录会单调增长。
- replica ID 只保证单对象内唯一，不保证跨 lineage 全局唯一。
