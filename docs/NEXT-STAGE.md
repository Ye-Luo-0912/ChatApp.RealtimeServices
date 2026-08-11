# 下一阶段与接手状态

## 职责

Realtime 负责消息、会话、回执、同步投影、Outbox/JetStream 和跨 Gateway 事件；数据库与消息队列的一致性语义必须显式、可重放。

## 下一步执行与交接

`REL-GATE-1`（关系投影重建隔离门禁）已完成（2026-08-11），reconcile 门禁 PASSED；下一批是 `REL-WIRE-2`（切 Shared 读）。

1. **冻结输入。** ✅ 已写 `.artifacts/relationship-projection-reconcile/20260811T084447Z/run-manifest.json`（Server `3b4224c`、Realtime `b53dc69`、Contracts `2.5.2`、Integration `3.1.3`、Migration 060–062、`databaseSnapshotId=relgate-seed-v1`）。
2. **验证编排。** ✅ Linux `192.168.5.49` 隔离环境：Postgres 16.8(Garnet 6379/NATS 4222) + Server(8080, 投影导出源) + Realtime ×2(8081/8082，Rebuilder 启用、Reads 关闭、共享同库)。双实例交替持租约且零冲突，Rebuilder 达 `passNumber=3→4`、`stablePasses=2→3`、`lastError=null`，投影落库 versions=27/items=38/snapshots=27/inbox=0。
3. **执行门禁。** ✅ `Invoke-RelationshipProjectionReconcile.ps1 -BaseUri http://127.0.0.1:8081`，`gatePassed=true`，2 个 clean pass 全 27/27 匹配、0 差异，连续两轮指纹一致 `CF08320F…9CB3E9`，无 gap/503、全页 200。报告归档 `.artifacts/relationship-projection-reconcile/20260811T084447Z/reconcile-report.json`。
4. **交给 Shared。** ✅ 交付物齐备（manifest、reconcile report、故障矩阵，见下）。修复了 REL-GATE-1 发现的契约 bug：Server 导出端点为 camelCase，原 Realtime 客户端按默认 PascalCase 反序列化导致 `page.Items is null`（`source_contract_invalid`）；新增 `ServerJsonOptions`（CamelCase + 大小写不敏感 + 复用 `RealtimeJsonSerializerContext`），并补回归测试 `ServerSource_DeserializesCamelCaseServerPayload`。此修复后门禁才通过，现全部测试通过（见基线行）。允许启动 `REL-WIRE-2`。
5. **失败/回滚。** ❌ 未触发（本次无需要回滚的失败）。

**本批故障矩阵（REL-GATE-1，2026-08-11）：**
| 阶段 | 故障 | 根因 | 修复/结论 |
|---|---|---|---|
| 环境/构建 | `NU1004` 项目 RID 已更改 | 发布时 `-p:RestoreLockedMode=false` 把 `linux-x64` 写入 lock 文件 | `dotnet restore --force-evaluate` 重新生成干净 lock 文件；并顺带把 lock 中 Contracts/Integration 版本修正为 `2.5.2`/`3.1.3` |
| 部署 | `BadImageFormatException` | 覆盖 DLL 时旧进程仍在运行（时序） | 停进程→重拷→重启；远端 DLL SHA-256 与本地一致，排除传输损坏 |
| 编排（核心） | `source_contract_invalid: page.Items is null` | Server 导出端点 camelCase 输出 vs Realtime PascalCase 反序列化 | `RelationshipProjectionSnapshotSource.cs` 新增 `ServerJsonOptions`（CamelCase+case-insensitive+`RealtimeJsonSerializerContext`），重新发布双实例后 Rebuilder 正常拉取并多轮 stable pass |
| 门禁 | 无 | — | 2 clean pass 指纹一致 `CF08320F…9CB3E9`，`gatePassed=true` |

**稳定错误码清单（本批验证）：** `source_http_{code}`（429/5xx 退避）、`source_timeout`、`source_transport_failed`、`source_contract_invalid`、`snapshot_version_mismatch`、`rebuild_failed`；服务未配置时 `source_unavailable`。所有错误均按 `FailureRetry` 释放租约并可续跑。

下一位 Agent 从 `REL-WIRE-2` 开始（切 Shared 读），不先改 Gateway/Client，也不在本批次混入消息性能、二进制或媒体改动。

## 接手状态

- P0（在线入口已收口）：默认关系 mutation/list/sync 均 fail-closed，旧表不再是在线权威。
- P0（版本化增量消费已完成）：`RelationshipProjectionDelta v1` 已进入 `ChatApp.Realtime.Contracts 2.5.2`，序列化边界由 `ChatApp.Realtime.Integration 3.1.3` 提供；Server Outbox 经 JetStream 到达 `RealtimeEventWorker` 后先校验 envelope，再在 PostgreSQL 单事务内提交 projection item、owner/list version 和 event inbox，成功后才 ACK。Realtime 自有 Outbox 的 pre-publisher 只作为第二道一致性守卫，不再被误认为 Server 生产链路。重复 event id 返回 Duplicate，只有 `current+1` 可推进，gap 会回滚并进入现有 NAK/重投/DLQ 路径；applied/duplicate/gap 均有独立指标。
  - `relationship_projection_items/versions/inbox` 由 Migration 060 建立；测试覆盖 upsert→重复→delete、并发重复 exactly-once、断档全回滚、旧无 Projection 通知兼容和 typed UTF-8 Outbox 路径。默认关系 mutation/list/sync 仍 fail-closed，旧 `realtime.friendships/friend_requests` 不恢复在线权威。
- P0（快照导入、自动 Rebuilder 与只读候选已完成，默认关闭，生产切读仍是 TODO）：Migration 061 增加 stream snapshot checkpoint；受 Ops API key 保护的导入端点按 owner/list 锁定版本行，原子替换 items、记录 checkpoint，并可把较旧/空投影直接推进到 Server 快照 version。重复同版本快照不再只信 checkpoint，而会核对当前 item count/资源键 hash；发现缺项或键集合漂移时只重建该 stream，下一次重复导入才返回 verified。比 current version 更旧的快照返回冲突；被快照覆盖的迟到 delta 按 Duplicate ACK，紧随快照的 `version+1` delta 可继续推进。Migration 062 的 Rebuilder 使用 PostgreSQL 数据库时钟租约、owner+claim-token fencing、持久化复合 cursor、整页提交、失败续跑、重复整轮扫描和连续两轮稳定判定；已记录 active/stable-pass、轮次/stream 结果和延迟、failure reason、lease-lost stage，HTTP 源使用 source-generated JSON、独立服务密钥与有界超时。
  - 编排自动覆盖已补齐（2026-08-11）：`RelationshipProjectionRebuildWorkerTests` 新增租约 fencing（renew/commit-page 失败即失租，`renew`/`commit-page` 阶段）、扫描中新增更小 owner id 被拒（`InvalidDataException`）、429/5xx/超时/传输/契约错误分类（`ClassifyError` 映射 `source_http_{code}`/`source_timeout`/`source_transport_failed`/`source_contract_invalid`/`snapshot_version_mismatch`/`rebuild_failed`）与失败后按 `FailureRetry` 释放租约；`RelationshipProjectionRebuildStateStoreTests` 新增**数据库时钟租约过期接管**（短租约过期后另一实例可接管、旧租约被 fencing）。密钥轮换由 `ServerRelationshipProjectionSnapshotSource` 的 `X-Relationship-Projection-Key` 头断言覆盖。真实隔离环境（多实例同库、secret store 注入、服务密钥轮换、极限页）仍待执行。
  - 只读候选：`RelationshipProjectionReads:Enabled=false`；只有同时启用 Rebuilder 且使用持久化 Npgsql 才允许装配。每个 owner/list 必须已有 snapshot checkpoint，version 与按资源键排序的页面在同一个 `REPEATABLE READ` 快照内读取；opaque cursor 固定携带 version+resource id，分页期间 version 变化返回 `relationship_projection_changed`，无快照基线返回 `relationship_read_projection_unavailable`。默认 processor 仍 fail-closed，mutation 永久留在 Server HTTP。
  - 编排上线 ✅（2026-08-11 隔离环境验证通过）：受 Ops API key 保护的 `GET /ops/relationship-projection/status` 已返回持久化 cursor、pass/stable、租约有效性、最后错误和整体覆盖率；`GET /ops/relationship-projection/streams` 以 owner/list keyset 分页返回 current/snapshot version、item/checkpoint count、checkpoint hash、快照后 inbox count/max version 与本地连续性；`GET /ops/relationship-projection/reconcile` 再与 Server 的 privacy-minimized digest 做有界合并，全程不读取或返回 resource id、消息、claim token。双实例共享同库抢租/交替持租零冲突，Rebuilder 达 stable pass=3、`lastError=null`，投影落库 versions=27/items=38/snapshots=27/inbox=0。camelCase 契约 bug 已修复并补测试。剩余未在本批覆盖的专场景（取消重启、密钥轮换、极限页、扫描中新增较小 owner id）在真机多实例上仍需专项验证，已由单测/编排测试兜底。
  - 对账执行 ✅（2026-08-11）：`pwsh scripts/Invoke-RelationshipProjectionReconcile.ps1 -BaseUri http://127.0.0.1:8081`（Ops key 经 `CHATAPP_OPS_API_KEY` 注入）。从空 cursor 有界分页到 `hasMore=false`，Rebuilder 已 ≥2 次 stable pass 且 status token 未变化，连续两轮全量 SHA-256 指纹一致 `CF08320F…9CB3E9`；2 clean pass 全 27/27 匹配、0 差异、无 gap/503、全页 200，`gatePassed=true`。报告只保留分页摘要与差异项，不写 key；源码/包 hash、配置已由同目录 `run-manifest.json` 记录。`databaseSnapshotId=relgate-seed-v1`。故障恢复后复跑仍通过（本批未触发 409/503）。仅剩只读 canary 与切读在 `REL-WIRE-2` 推进。
  - 切读 TODO：连续两轮稳定、差异为零、积压恢复、服务密钥轮换、HTTP 授权对照和分页版本漂移均通过后，才在隔离环境打开只读 list canary；随后再设计 Shared list/sync wire。Sync 字节预算超限必须返回显式 partial/reset 与可继续水位，任何门禁失败立即关闭读开关并继续走 Server HTTP。
- P1（默认值已门禁）：Outbox hint 合并窗口保留 `0..50 ms` 开关，但默认 `0`；`2 ms` 虽减少约 20% DB ops，却显著恶化 delivery 尾延迟，只能由明确接受该取舍的部署启用。
- P1：继续压低消息写入的 SQL/WAL/managed allocation，并用短时 admission + capacity 验证；冻结后再做 30 分钟候选测试。
  - 每轮先用 trace/数据库采样确认 Top 路径，只改变一个批处理、SQL 或索引因素；共享仅限线程安全连接池、不可变 metadata 和有界 worker，不跨事务复用 command/reader/写会话。
  - 完成标准：同快照 A/B 的 ACK/投递、重复/漏投、Outbox/JetStream/死信均正确，SQL/WAL/分配有稳定收益且 p95/p99、CPU 和 GC 不回退；短测不过不进入长测。
- P1：补 sync reset/编辑/反应/提及的端到端恢复，以及 Outbox lease、死信和投影重放门禁。
  - 覆盖 cursor 失效、空页但 HasMore、重复 cursor、编辑/撤回/Reaction 与 mention 的 changed-at 分页；只有整批落库和发布状态一致后才推进水位。
  - Outbox 覆盖 claim/续租/过期恢复、发布成功但完成前崩溃、死信重放与租约 owner/token 校验；门禁报告必须能关联消息、事件、checkpoint 和恢复原因。
- P1（附件闭环 wiring 已完成，2026-08-11）：未绑定附件过期清理已接入 DI 与后台 worker。新增 `AttachmentSweepOptions`（`AttachmentSweep` 配置节：`Enabled`/`IntervalMs`/`RetentionDays`），`AttachmentSweepWorker`（`PeriodicTimer` 周期调用 `IAttachmentSweeper.SweepAsync`，停用空闲、单轮异常不阻断后续周期），并在 `RealtimeServicesRegistration` 绑定 options、注册 `IAttachmentSweeper→AttachmentSweeper`（保留期取 `RetentionDays`，运行时未注入 `IObjectStorage` 时仅标记状态、物理删除由对象存储兜底）以及 `AddHostedService<AttachmentSweepWorker>`。新增 `AttachmentSweepWorkerTests` 3 例（启用调用/停用空闲/异常存活）。
- P2（二进制评估边界）：Shared 已建立但未启用的 tagged codec 只服务首轮 Client↔Gateway 评估；Realtime 持久化 Outbox/NATS wire 保持现状。只有 TCP 双格式灰度证明收益且能保留历史事件重放、版本识别和可观测性后，才单独评估内部 event wire，禁止与外部协议同批迁移，也不直接复用外部字段号。
- P2（通话事件）：只保存必要的信令状态、审计和 QoE 汇总，不持久化或经 JetStream 转发音频包；媒体资源计入独立 TURN/SFU 容量模型。

## 功能路线

语音消息作为带元数据的附件消息进入现有消息/Outbox 流程；实时通话只承载临时信令、在线状态和审计事件，媒体由 WebRTC/TURN/SFU 处理，不经 Postgres/JetStream 转发音频包。

## 性能边界

可共享连接池、source-generated serializer、有界 worker 和不可变配置；不得跨事务共享 command/reader/`DbContext`，池化对象必须有清晰归还与清零规则。

## 验证顺序

聚焦单测/契约测试 → Release 构建 → 短时 admission/smoke；阶段长测与发布 soak 只在功能和数据模型冻结后执行。

当前基线：Release build `0 warning / 0 error`；Unit `318/318`（315 + 3 个 `AttachmentSweepWorkerTests`）、PostgreSQL/Docker Integration
`85/85` 通过（原 70 + 新增 14 个 Rebuilder 编排场景 + 1 个 camelCase 契约回归测试）；关系读/指标/默认门禁聚焦 `14/14`，digest source/Rebuilder/reconcile `7/7`，Ops 查询 `2/2`，投影存储 `8/8`，reconcile gate 脚本 `7/7`。
