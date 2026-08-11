# 下一阶段与接手状态

## 职责

Realtime 负责消息、会话、回执、同步投影、Outbox/JetStream 和跨 Gateway 事件；数据库与消息队列的一致性语义必须显式、可重放。

## 下一步执行与交接

当前接手批次是 `REL-GATE-1`，Realtime 与 Server 共同负责，代码实现已完成但真实隔离门禁尚未执行。

1. **冻结输入。** 记录 Server/Realtime commit、Release 二进制 SHA-256、Contracts `2.5.2`、Integration `3.1.3`、Migration 060–062、数据库快照标识和所有非敏感选项。写入独立运行目录的 `run-manifest.json`；密钥只由 secret store 注入环境变量。
2. **验证编排。** 两个 Rebuilder 实例共享同一 PostgreSQL，保持 `RelationshipProjectionReads:Enabled=false`；依次验证单租约 owner、过期接管、取页/导入/提交 cursor 前中断、429/5xx/超时退避、密钥轮换、并发 mutation 和扫描中新增较小 owner id。
3. **执行门禁。** Rebuilder 连续稳定后运行 `pwsh scripts/Invoke-RelationshipProjectionReconcile.ps1 -BaseUri <realtime>`；工具报告与 manifest 同目录归档。工具自动采分页摘要、差异和指纹，但**不自动采源码/包 hash**，这些必须由 manifest 提供。
4. **交给 Shared。** 仅当故障前后门禁都零退出、所有页 200、连续两轮指纹相同且无 gap/503，才交付 manifest、reconcile report、故障矩阵和稳定错误码清单，允许启动 `REL-WIRE-2`。
5. **失败/回滚。** 关闭 Rebuilder/Reads，保留 Server HTTP 权威和现有 projection 数据用于诊断；不清表、不回读 legacy 关系表、不自动重跑失败批次。

下一位 Agent 从本节第 1 项开始，不先改 Shared/Gateway/Client，也不在本批次混入消息性能、二进制或媒体改动。

## 接手状态

- P0（在线入口已收口）：默认关系 mutation/list/sync 均 fail-closed，旧表不再是在线权威。
- P0（版本化增量消费已完成）：`RelationshipProjectionDelta v1` 已进入 `ChatApp.Realtime.Contracts 2.5.2`，序列化边界由 `ChatApp.Realtime.Integration 3.1.3` 提供；Server Outbox 经 JetStream 到达 `RealtimeEventWorker` 后先校验 envelope，再在 PostgreSQL 单事务内提交 projection item、owner/list version 和 event inbox，成功后才 ACK。Realtime 自有 Outbox 的 pre-publisher 只作为第二道一致性守卫，不再被误认为 Server 生产链路。重复 event id 返回 Duplicate，只有 `current+1` 可推进，gap 会回滚并进入现有 NAK/重投/DLQ 路径；applied/duplicate/gap 均有独立指标。
  - `relationship_projection_items/versions/inbox` 由 Migration 060 建立；测试覆盖 upsert→重复→delete、并发重复 exactly-once、断档全回滚、旧无 Projection 通知兼容和 typed UTF-8 Outbox 路径。默认关系 mutation/list/sync 仍 fail-closed，旧 `realtime.friendships/friend_requests` 不恢复在线权威。
- P0（快照导入、自动 Rebuilder 与只读候选已完成，默认关闭，生产切读仍是 TODO）：Migration 061 增加 stream snapshot checkpoint；受 Ops API key 保护的导入端点按 owner/list 锁定版本行，原子替换 items、记录 checkpoint，并可把较旧/空投影直接推进到 Server 快照 version。重复同版本快照不再只信 checkpoint，而会核对当前 item count/资源键 hash；发现缺项或键集合漂移时只重建该 stream，下一次重复导入才返回 verified。比 current version 更旧的快照返回冲突；被快照覆盖的迟到 delta 按 Duplicate ACK，紧随快照的 `version+1` delta 可继续推进。Migration 062 的 Rebuilder 使用 PostgreSQL 数据库时钟租约、owner+claim-token fencing、持久化复合 cursor、整页提交、失败续跑、重复整轮扫描和连续两轮稳定判定；已记录 active/stable-pass、轮次/stream 结果和延迟、failure reason、lease-lost stage，HTTP 源使用 source-generated JSON、独立服务密钥与有界超时。
  - 编排自动覆盖已补齐（2026-08-11）：`RelationshipProjectionRebuildWorkerTests` 新增租约 fencing（renew/commit-page 失败即失租，`renew`/`commit-page` 阶段）、扫描中新增更小 owner id 被拒（`InvalidDataException`）、429/5xx/超时/传输/契约错误分类（`ClassifyError` 映射 `source_http_{code}`/`source_timeout`/`source_transport_failed`/`source_contract_invalid`/`snapshot_version_mismatch`/`rebuild_failed`）与失败后按 `FailureRetry` 释放租约；`RelationshipProjectionRebuildStateStoreTests` 新增**数据库时钟租约过期接管**（短租约过期后另一实例可接管、旧租约被 fencing）。密钥轮换由 `ServerRelationshipProjectionSnapshotSource` 的 `X-Relationship-Projection-Key` 头断言覆盖。真实隔离环境（多实例同库、secret store 注入、服务密钥轮换、极限页）仍待执行。
  - 只读候选：`RelationshipProjectionReads:Enabled=false`；只有同时启用 Rebuilder 且使用持久化 Npgsql 才允许装配。每个 owner/list 必须已有 snapshot checkpoint，version 与按资源键排序的页面在同一个 `REPEATABLE READ` 快照内读取；opaque cursor 固定携带 version+resource id，分页期间 version 变化返回 `relationship_projection_changed`，无快照基线返回 `relationship_read_projection_unavailable`。默认 processor 仍 fail-closed，mutation 永久留在 Server HTTP。
  - 编排上线 TODO：受 Ops API key 保护的 `GET /ops/relationship-projection/status` 已返回持久化 cursor、pass/stable、租约有效性、最后错误和整体覆盖率；`GET /ops/relationship-projection/streams` 以 owner/list keyset 分页返回 current/snapshot version、item/checkpoint count、checkpoint hash、快照后 inbox count/max version 与本地连续性；`GET /ops/relationship-projection/reconcile` 再与 Server 的 privacy-minimized digest 做有界合并，全程不读取或返回 resource id、消息、claim token。隔离环境仍需验证多实例抢租、过期接管、取消重启、整页失败、429/5xx/超时、密钥轮换、极限页与扫描中新增较小 owner id。
  - 对账执行 TODO：隔离实例从 secret store 把 Ops key 注入 `CHATAPP_OPS_API_KEY`，运行 `pwsh scripts/Invoke-RelationshipProjectionReconcile.ps1 -BaseUri <realtime>`。工具会从空 cursor 有界分页到 `hasMore=false`，在每轮前后确认 Rebuilder 已有至少两次 stable pass 且状态 token 未变化，并要求连续两轮全量 SHA-256 指纹一致；报告只保留分页摘要与差异项，不写 key。源码/包 hash、配置和数据库快照标识由同目录 `run-manifest.json` 单独记录。200 表示页面匹配；409 按 `server/realtime stream missing`、version、item count、snapshot version/count/hash 或 local gap 分类；503 表示能力/上游不可用。任一游标停滞、扫描中变更、两轮漂移、409/503 都非零退出。先让自动重复扫描修复同版本 count/hash 损坏，再复跑；禁止回读 legacy 表补洞或用旧快照覆盖更高水位。只有工具通过且故障恢复后再次通过，才接告警并进入只读 canary。
  - 切读 TODO：连续两轮稳定、差异为零、积压恢复、服务密钥轮换、HTTP 授权对照和分页版本漂移均通过后，才在隔离环境打开只读 list canary；随后再设计 Shared list/sync wire。Sync 字节预算超限必须返回显式 partial/reset 与可继续水位，任何门禁失败立即关闭读开关并继续走 Server HTTP。
- P1（默认值已门禁）：Outbox hint 合并窗口保留 `0..50 ms` 开关，但默认 `0`；`2 ms` 虽减少约 20% DB ops，却显著恶化 delivery 尾延迟，只能由明确接受该取舍的部署启用。
- P1：继续压低消息写入的 SQL/WAL/managed allocation，并用短时 admission + capacity 验证；冻结后再做 30 分钟候选测试。
  - 每轮先用 trace/数据库采样确认 Top 路径，只改变一个批处理、SQL 或索引因素；共享仅限线程安全连接池、不可变 metadata 和有界 worker，不跨事务复用 command/reader/写会话。
  - 完成标准：同快照 A/B 的 ACK/投递、重复/漏投、Outbox/JetStream/死信均正确，SQL/WAL/分配有稳定收益且 p95/p99、CPU 和 GC 不回退；短测不过不进入长测。
- P1：补 sync reset/编辑/反应/提及的端到端恢复，以及 Outbox lease、死信和投影重放门禁。
  - 覆盖 cursor 失效、空页但 HasMore、重复 cursor、编辑/撤回/Reaction 与 mention 的 changed-at 分页；只有整批落库和发布状态一致后才推进水位。
  - Outbox 覆盖 claim/续租/过期恢复、发布成功但完成前崩溃、死信重放与租约 owner/token 校验；门禁报告必须能关联消息、事件、checkpoint 和恢复原因。
- P2（二进制评估边界）：Shared 已建立但未启用的 tagged codec 只服务首轮 Client↔Gateway 评估；Realtime 持久化 Outbox/NATS wire 保持现状。只有 TCP 双格式灰度证明收益且能保留历史事件重放、版本识别和可观测性后，才单独评估内部 event wire，禁止与外部协议同批迁移，也不直接复用外部字段号。
- P2（通话事件）：只保存必要的信令状态、审计和 QoE 汇总，不持久化或经 JetStream 转发音频包；媒体资源计入独立 TURN/SFU 容量模型。

## 功能路线

语音消息作为带元数据的附件消息进入现有消息/Outbox 流程；实时通话只承载临时信令、在线状态和审计事件，媒体由 WebRTC/TURN/SFU 处理，不经 Postgres/JetStream 转发音频包。

## 性能边界

可共享连接池、source-generated serializer、有界 worker 和不可变配置；不得跨事务共享 command/reader/`DbContext`，池化对象必须有清晰归还与清零规则。

## 验证顺序

聚焦单测/契约测试 → Release 构建 → 短时 admission/smoke；阶段长测与发布 soak 只在功能和数据模型冻结后执行。

当前基线：Release build `0 warning / 0 error`；Unit `315/315`、PostgreSQL/Docker Integration
`84/84` 通过（原 70 + 新增 14 个 Rebuilder 编排场景）；关系读/指标/默认门禁聚焦 `14/14`，digest source/Rebuilder/reconcile `7/7`，Ops 查询 `2/2`，投影存储 `8/8`，reconcile gate 脚本 `7/7`。
