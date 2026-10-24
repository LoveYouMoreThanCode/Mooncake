---
orphan: true
---

# Simple HA 复制设计

*Design note · 2026-10*

双节点元数据 KV：etcd 选主，内存 WAL 环形缓冲，备从主拉取。目标是最佳努力地少丢记录，而不是完备的一致性状态机。

**最佳努力** · **两节点均为候选** · **失去 lease 即停服** · **lease 4s（需显式配置，见 05）** · **备 1s pull** · **全内存**

## 01 架构

> etcd 只管选主（leader key + lease）；数据在两边内存里。备从主拉快照和 WAL。独立的 presence key 尚未实现，见遗留问题。

![etcd 下方两个节点，主节点持有 KV 与 WAL，备节点从主拉取快照和 WAL](../../image/simple-ha-architecture.svg)

*同一套代码，角色由 election 决定；快照由当前 leader 提供。*

## 02 写路径

> 纯异步：主 apply + 追加 WAL 后立即 ack；备每 1s 拉一次，不参与写路径，也不对外提供读。落后 ≤1s 可接受。

![写请求经过 leader 与 ready 门禁、分配索引、应用 map、追加 WAL，再返回客户端；备定时拉取并应用](../../image/simple-ha-write-path.svg)

*备的 pull 是 out-of-band 的；主不等待备，也不做 group commit。ack 后的写仍可能丢最后一段未复制的尾巴。*

备不对外提供读；读写都走主。当前实现不内联返回 redirect：非 leader 的节点由集成系统的 LB / 客户端重试切到新主（按 ready 路由），leader 但未 ready 时同样由客户端重试。设计目标是非主返回 redirect（target 带当前 leader + epoch）、leader 未 ready 返回 not-ready / retry-after；具体错误码与字段待集成系统确定。

## 03 追平

> 追得上就走 WAL；发现被覆盖不跳过去继续追，而是在进程内 stage-and-swap 重装：握手 + 全量快照 + 从 base+1 追。

![WAL 环形缓冲区：first 101、last 200；applied 150 追得上，applied 95 出现 gap 需要全量快照](../../image/simple-ha-catchup.svg)

*快照要带 base_index（`BeginSnapshot` 时随 WAL 前缀一起取得）；副本随后按 shard 读锁分批读取，扫描期间超过 base 的变化由 base+1 的 WAL 修正。装好之前不要对外服务。备发现 from < first 或 incarnation 变化，丢弃当前快照会话并在进程内 stage-and-swap 重装，不跳过去继续追。*

pull 响应带 first / last / incarnation；from < first、incarnation 变了、或 entries 索引不连续 → 作废当前快照会话，重新 `BeginSnapshot` 并把新快照 merge 进本地 store，不跳过去继续追。merge 期间节点保留全部本地记录；即使新主此时再挂，节点仍可用“本地记录 + 已合并部分”升主。失败按 backoff 重试。与早期设计不同：当前实现不退出进程，也没有 `WAL_OVERWRITTEN` 退出码。快照本身不需要 pin；不做 WAL pin，S+1 被覆盖就重新 merge。ring 要覆盖整个扫描时长，否则会反复重扫。

**全量扫描：** `BeginSnapshot` 返回 `snapshot_id` / `incarnation` / `base_batch_id` / `base_seq` 和 segment 列表；取到 base 后先等待 writer 的 durable-finalize 回调执行到 base（静默前缀）再开始扫描——否则延迟擦除（例如 discard expired processing replica）可能被扫进快照，而重放从 base+1 开始不会补删。扫描按 tenant（排序）→ shard（共 1024 片）逐个推进，每次 `SnapshotChunk` 带 `snapshot_id` / `tenant_index` / `shard_index` / `offset`，主侧在 shard 读锁内把该分片序列化成 entries 后按 offset 分页返回，响应带 `next_tenant_index` / `next_shard_index` / `next_offset` / `done`。只序列化 completed replica；扫描期间的新增、删除、修改都由 base+1 之后的 WAL 修正，批次之间不做快照隔离。分片在 shard 锁内一次取全，分页只发生在已固定的 staged 数据上，所以 offset 分页不会漏 key。扫描期间 base+1 被 ring 覆盖会作废会话（`INCOMPLETE_OPLOG_CATCH_UP`），备重装。会话只有一个消费者；游标不匹配（比如响应丢失后重试）会让会话作废并返回错误，备重新 `BeginSnapshot`。扫完、装好再回放 WAL；不要边扫边回放，否则旧 chunk 可能覆盖新 WAL 值。

## 04 升主

> 升主前无条件 probe 对端：本节点无记录而对端有记录，或同一 incarnation 下对端 applied seq 严格更大时让位；对端是 live leader 时走 best-effort WAL 追赶；探测失败时 fail-open 直接服务并告警（未查询 presence，见 07）。preserved baseline 欠 merge 时，对端若是可达 leader 就让位、让 pull loop 继续 merge；对端不可达或只是 standby 时直接用自己的 baseline 服务并记 degraded。

![升主决策流程：先关 ready；state 有效则服务；空则确认对端；对端有数据就 Resign 让位并同步](../../image/simple-ha-promotion.svg)

*空节点默认让位给有数据/更新的对端；只有 leader && ready 才接流量。让位后等对端升主，再从新主同步。分区恢复：老主失去 lease 立即 ready=false；**实现现状：**term 结束时把 serving 状态 move 到 standby baseline（不复制整表），对端不可达时重新当选可以直接用自己的状态升主；发现新 leader 时把它的快照 merge 进本地 store，本地记录不释放——见下文。*

term 结束的保留路径（Move-drain）：writer 已 Stop、durable callback 已 drain 之后，supervisor 调用 `DrainIntoPromotionContext()`，逐 tenant/shard 把对象构造成 `StandbyObjectMetadata` 写入新的 store，然后立刻从 master erase。drain 前先关闭本节点的 snapshot 会话（`DisableSnapshots()`），否则备可能按 drain 前的 base 合并一份被截断的快照，而 erase 掉的 key 不会出现在 base 之后的 WAL 里；`GetState`/`PullWal` 保持可用，新主仍可追冻结的 WAL 尾部。同一时刻最多只有“一个对象”存在两份，峰值 ≈ 1x + 单个对象，而不是 2x。得到的 `PromotionContext` 交给已有的 `RestorePromotionBaseline()`；重新当选时按普通 standby 升主路径导出并 restore。

如果对端出现了新的 leader，节点**不释放本地 store**，而是把新主快照原地 merge 进来：

- 新主快照里存在的 key：peer wins，覆盖为新主的值；
- 新主快照里不存在的本地 key：保留（快照缺席不作为删除依据）；
- 新主 WAL 回放：`PUT_END` 覆盖、`REMOVE` / `PUT_REVOKE` 删除（显式删除是权威证据）。

merge 过程中节点保持可升主：如果新主再次故障、本节点当选，它直接服务“旧记录 + 已合并的部分新记录”，不会回到空状态。代价是：新主在快照 base 之前删除、且不在重放 WAL 里的 key 会复活；本地独有记录会单调累积，需要容量上限/告警。

**cursor 不能跨任期直接比较。** 降备后的 cursor 属于旧 WAL instance，可能比新主当前的 durable 还大；新主的 WAL 从它自己的 applied cursor 开始编号，两边的 batch_id / seq 是两个命名空间。如果只按数值比较，旧主会误判“已追平”，既 merge 不到新主快照，也拉不到新主后来写的低序号批次。因此 preserved baseline 第一次接触 leader 时必须先 merge 快照，把 cursor 重置到新主 lineage，之后才能按 first_batch / durable / batch_id / seq 做数值比较和 WAL 追赶。

升主 restore 时，新主会给每个恢复对象统一 grant 一个 `default_kv_lease_ttl` 的读租约（组对象使用共享的组租约）。这覆盖客户端在旧主上持有的剩余租约窗口，避免新主的 eviction 与客户端仍在进行的直接读竞争。

扫描完成但 WAL 尚未追平时老主故障，备会以扫描得到的混合状态升主（可用性优先）。该路径会打印 WARN（`applied_seq` / `last_known_peer_durable_seq` / `lag` / `peer_unreachable`）并累加 `ha_simple_ha_degraded_promotions_total`；缺失的尾部在此架构下不可恢复。

## 05 故障切换预算

> 外部目标：进程 crash / 机器掉电 / 网络失联，备 ≤5s 感知，≤6s 开始服务（含客户端路由）。注意 etcd lease TTL 默认是 5s，要满足该预算需显式设置 `ha_leader_lease_ttl_sec=4`。

| 阶段 | 最坏 | 取值 |
| --- | --- | --- |
| crash → lease 过期 / 备感知 | 4.3s | T=4s，ε≤0.3s |
| 感知 → ready | 0.5s | P=0.5s |
| ready → 客户端路由 | 1.0s | R=1s |
| 合计 | 5.8s | 满足 5s 感知 / 6s 服务 |

```text
etcd lease TTL         ha_leader_lease_ttl_sec，默认 5s；本页预算按 4s 计算（需显式配置）
keepalive              etcd 内置 keepalive（设计目标 ~1s）
local serve deadline   设计目标 3s / 3.2s；当前无独立开关，停服由 leadership monitor 触发
presence               同一 session，TTL 4s（未实现）
peer_probe_timeout     设计目标 200ms × 2；当前 connect 1s + 升主 probe 500ms（周期 GetState 2s）
promotion_budget       设计目标 0.5s（受 StopLoop join 影响，见遗留问题）
client retry / LB      ≤1s（客户端侧）
simple_ha_pull_interval 1000ms（--simple_ha_pull_interval_ms）
```

- **为什么 TTL=4s：** 最坏情况是 crash 紧跟一次 keepalive，lease 要等满 T 才过期。感知 = T + ε；T=5s 时 5+ε 已经破 5s。T=4s 留 0.7s 余量。如果只要求平均而非最坏，T=5s 也能用（平均感知 ≈4.3–4.5s）。
- **为什么 keepalive≈1s：** deadline 必须大于 2×keepalive，才能容忍一次延迟/丢失；又必须小于 TTL，才能抢在服务端过期前停服。T=4s、K=1s → D=3s，2K=2s，留 1s 余量。client 固定 T/3≈1.3s 时，2K=2.6s，D 取 3.2s。
- **为什么 deadline 是 3–3.2s：** 约束是 2K < D < T。D < T 保证活着的旧主在备可以升主之前停服，避免双主；D > 2K 保证一次 keepalive 抖动不误停。T−D≈0.8–1s 是分区场景的代价：旧主已停、备还不能升主，短空窗。把 D 推向 T 能缩小空窗，但安全余量也缩小；D ≥ T 就会重叠。
- **为什么 P=0.5s：** 备已经在 campaign 队列里，leader key 一消失就上位；它非空、不需要追数据，只做 state check + role switch。1s 的 pull 落后不影响这一步。
- **为什么 R=1s：** 备 ready ≠ 客户端能打过来。LB 检查间隔或客户端重试必须 ≤1s；如果 LB 5s 探一次，备 4.5s ready，客户端 9.5s 才恢复。
- **crash 与分区的空窗不同：** crash 时旧进程立即没了，空窗 = T − 相位 ≈3–4s；分区时旧主在 D 停服、备在 T 上位，空窗 = T−D ≈0.8–1s。新主的启动时间都是 T+ε+P，D 只影响分区空窗和防重叠。
- **1s pull 是另一条预算：** 它决定新主的状态最多旧 1s，不决定多久能服务。备升主时直接用当前 applied 的状态，不等旧主。
- **presence 的做法（未实现）：** 设计上计划与 election 共用 session，TTL=4s，不额外加 keepalive。key 在但 RPC 不通 → fail-stop/重试；key 没了且 RPC 不通 → 按进程已死处理（残余风险）。当前升主前对端做一次 direct RPC probe（`GetState`，超时 500ms），不可达时 fail-open 直接服务。

前提：etcd quorum 健康且 keepalive p99 ≪1s；备健康、非空、不在全量 resync；LB / 客户端重试 ≤1s。etcd 撑不住 4s 时，只能放宽目标或给这套服务独立 etcd。

## 06 状态与不变量

> 角色是意图，leader 是 lease，ready 是服务闸门。

| 信号 | 含义 |
| --- | --- |
| role | 意图（配置） |
| leader | leader key + lease 健康 |
| ready | 对外服务 |
| serve | `leader && ready` |

空节点允许 role=primary，但必须 leader=false、ready=false。空 = 未初始化（applied=0 / 无 lineage），不是 map 没有 key。lease 健康由 etcd keepalive + leadership monitor 判定；设计目标是本地 deadline 小于 etcd TTL（本页按 TTL=4s、deadline=3–3.2s 分析），超时立即 ready=false，不等网络恢复。当前实现没有独立的 `local_serve_deadline` 开关，停服由 monitor 触发。

| # | 不变量 |
| --- | --- |
| 01 | WAL 只记绝对 post-image（SET/DEL），可重复应用。 |
| 02 | 快照带 base_index，从 base+1 追；base 在 `BeginSnapshot` 时随 WAL 前缀取得，并等待 durable-finalize 回调执行到 base（静默前缀）后才开始扫描。 |
| 03 | 备发现 gap 或 incarnation 变化：作废快照会话，重新 `BeginSnapshot` 并把新快照 merge 进本地 store（不换库、不释放本地记录），再从 base+1 重放 WAL。 |
| 04 | incarnation 变了 = 对端换了一个 WAL 实例（进程重启或重新升主都会换），不能盲信索引连续。 |
| 05 | ring 窗口要覆盖 1s pull 间隔、备短暂停机与全量扫描耗时；按峰值写速率/字节数定容，不能只按条数。 |
| 06 | 领导权跟数据走；升主前无条件探测对端：本节点无记录而对端有记录、或同一 incarnation 下对端严格更新时 Resign；对端是 leader 时追 WAL；不可达时 fail-open（见 07）。 |
| 07 | snapshot / WAL 只由 leader 提供；只有 leader && ready 接流量。 |
| 08 | 空 = 未初始化（applied=0 / 无 lineage），不是 map 没有 key。 |
| 09 | 失去 lease 立即停服；term 结束把 serving 状态 move 到 standby baseline（边搬边删，峰值 ≈1x）；发现新 leader 时把其快照原地 merge 进本地 store，不释放、不换库。 |
| 10 | 全量扫描按 tenant / shard 分批（每片在 shard 读锁内取全）；批次间不做快照隔离；先扫完再回放 base+1 的 WAL。 |
| 11 | 升主 restore 时为每个恢复对象 grant 一个 `default_kv_lease_ttl` 的读租约（组对象 grant 组租约），覆盖客户端在旧主上持有的剩余租约窗口。 |
| 12 | 全量同步期间允许以混合状态升主（可用性优先），但必须记录 degraded 指标并告警；不承诺跨 key 一致性。 |
| 13 | term 结束用 Move-drain 保留状态：逐对象搬入 standby store 并立即 erase，同一时刻最多多一份单对象；baseline 不主动释放。 |
| 14 | 快照 merge 规则：同 key 冲突 peer wins；快照中缺席的本地 key 保留；只有 WAL 里显式 `REMOVE` / `PUT_REVOKE` 才删除本地记录。merge 中途失败不阻塞升主。 |
| 15 | preserved baseline 的 cursor 属于旧 WAL instance，不能和新主 cursor 直接做数值比较；第一次接触 leader 先 merge 快照重置 cursor，之后才追 WAL。 |

## 07 故障与代价

> 真正的丢窗口是未复制的尾部（正常 ≤1s），以及脑裂期间老主 ack 的写；ring 覆盖只增加重同步成本。不做本地持久化，两边同时挂 = 全丢。term 结束不再丢弃内存状态：serving 状态被 move 到 standby baseline，只有确认新 leader 后才释放；Simple HA 下 weight mutations 关闭，weight metadata 不复制、不进快照。

| 场景 | 行为 | 数据影响 |
| --- | --- | --- |
| 主进程崩溃 | 备升主 | 丢未复制的尾巴（正常 ≤1s） |
| 备落后被 ring 覆盖 | 备进程内重装 → 重新加入全量快照 | 不丢；备短暂不可用 |
| 备在全量同步刚完成、WAL 未追平时升主 | 以混合状态服务；记录 degraded 指标并告警 | 丢各 key 在扫描后未复制的写，窗口可达扫描时长；无跨 key 一致性 |
| 主重启且空 | 让位给备，自己同步 | 丢未复制的尾巴（正常 ≤1s） |
| 两边都重启且空 | tie-break 空启动 | 全丢（内存，不做本地持久化） |
| 全量扫描慢于 ring | 扩 ring / 重建会话退避重试 | 不丢，可能反复重装 |
| 主备短暂断连 | 重连后追 WAL | 不丢；延迟增加 |
| 老主脑裂后重连 | 失去 lease 停服并保留 baseline；发现新 leader 后 merge 其快照 | 脑裂期间 ack 的写会被新主快照覆盖 / 回退；本地独有 key 保留 |
| 在任主丢 lease 且对端不可达 | term 结束 move 到 baseline；重新当选用自己的状态升主 | 丢本地 WAL 未覆盖的尾巴（通常 <1s），不再全丢 |
| Simple HA + weight 管理 | weight mutations 关闭 | weight metadata 不复制、不进快照 |

**丢窗口：** 纯异步，已 ack 的写可能丢最后一段未复制的尾巴；正常 ≤ 一个 pull 间隔（约 1s）。脑裂期间老主 ack 的写会在重连重装时被丢弃。ring 覆盖不会丢历史，只会触发重同步。

降级升主：全量同步刚完成、WAL 尚未追平时老主故障，备以扫描得到的混合状态服务。此时每个 key 都是某个合法 post-image，但不存在全局一致的 cursor；丢失范围是各 key 在扫描之后、崩溃之前未复制的写。该路径输出 WARN 并累加 `ha_simple_ha_degraded_promotions_total`，不承诺跨 key 一致性。

保留与 merge 的代价：term 结束多一次 O(objects) 的 Move-drain（节点已不服务）；发现新 leader 时原地 merge，不产生第二份完整 store。merge 的语义代价是：新主在快照 base 之前删除、且不在重放 WAL 里的 key 会复活；本地独有记录会单调累积，需要容量上限与告警；同 key 冲突以新主为准，老主尾巴里同 key 的新值会被覆盖。

按本页参数：最坏感知 ≈4.3s，服务 ≈5.8s；crash 空窗 ≈3–4s，分区空窗 ≈0.8–1s。

## 遗留问题与后续工作

主路径已按当前实现校准；跨任期 cursor 的修复见不变量 15。L1/L2/L3 由
`scripts/run_simple_ha_e2e.sh` 串起（opt-in，需要 Go/etcd），CI 的 etcd
作业会自动跑 L2。下面按优先级记录未实现或还没拍板的点。

### P1（可靠性 / 可运维性）

1. **presence 检查（中风险，未实现）** —— 升主前无条件 probe（`GetState`）；对端不可达时 fail-open 直接服务，没有按设计查询 presence key。standby 的探测响应带 `key_count` 和非零 lineage cursor：正在 merge、cursor 已推进但 key 还没写入的对端也会被视为有数据；leader 侧仍用 `has_data`（WAL prefix）作为“有记录”的代理。presence key 未实现，对端尚未 `BeginSnapshot` 的极短窗口仍无法区分“空”和“即将同步”。
2. **merge 增长上限与指标（中风险）** —— 本地独有 key 会随每次 merge 单调累积；需要容量上限、`merged_extra_keys` / merge 次数 / partial-merge 升主告警，以及超限策略（停止 merge 或告警继续）。
3. **standby lag / merge 可观测性** —— 当前只记录 degraded promotion；把 `peer_durable_seq - applied_seq` 接到 `ha_oplog_standby_lag`，并暴露 merge 是否完整，便于告警。
4. **升主暂停时间（低风险）** —— 升主时 `StopLoop` 会 join 正在进行的 pull / snapshot chunk RPC（超时 5s / 10s），对端卡住时可能突破 P=0.5s 的预算；可考虑缩短超时或可中断的 RPC。
5. **客户端 / LB 切流（redirect）** —— 备不支持读，读写都走主。LB 按 ready 路由；设计目标是非主时请求返回 redirect，target 带当前 leader + epoch；leader 未 ready 返回 not-ready / retry-after。当前 master 不内联返回 redirect，依赖 LB / 客户端重试；具体错误码 / 字段待集成系统确定。
6. **运维文档 / runbook** —— 两节点参数（`simple_ha_*`）、etcd TTL=4s 的建议、merge 语义（删除复活窗口、本地独有记录增长）、常见故障恢复步骤。

### P2（低风险 / 清理）

7. **`SnapshotSegments` 防御性过滤** —— 跳过无效 endpoint / 重复 segment；merge 时过滤陈旧 segment 上不可读的 replica，避免 restore 整体失败。
8. **weight mutations** —— Simple HA 下被强制关闭；确认是否可接受，或后续把 weight metadata 纳入快照 / OpLog。
9. **pull / ring 容量与全量扫描重试** —— ring 仍需覆盖全量扫描耗时；备发现 gap 在进程内重装，失败按 backoff 重试，需配告警；chunk 校验和与格式版本兼容尚未做。
10. **旧路径清理** —— 检查 `preserve_baseline_` / `preserved_segments_` 等旧分支和 merge 后不再使用的 snapshot 路径。

### 已接受（不做或暂不处理）

- **全量 resync 期间的降级升主** —— 扫描完成但 WAL 未追平老主就故障时，备以扫描得到的混合状态升主：每个 key 是某个合法 post-image，但不存在全局一致的 cursor。选择可用性优先——老主已故障时 fail-close 没有恢复路径。已输出 WARN（applied / peer_durable / lag / peer_unreachable）并累加 `ha_simple_ha_degraded_promotions_total`；若上层依赖跨 key 同版本，需要自行做版本校验或改用 batch OpLog 路径。
- **快照 merge 的代价** —— 新主在快照 base 之前删除、且不在重放 WAL 里的 key 会复活；本地独有记录会单调累积（见 P1-2）；同 key 冲突采用 peer wins，老主尾巴里同 key 的新值会被覆盖。不引入 per-key 版本号，key 复用很少，认为这个代价可接受。
- **replica ID 作用域** —— 只保证单对象内唯一，不保证跨 lineage 全局唯一；新 ID 在 restore 后抬到 merged max 之上。

*2026-10 · 已按当前实现校准*
