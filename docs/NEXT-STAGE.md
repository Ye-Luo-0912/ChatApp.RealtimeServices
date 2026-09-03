# 下一阶段与接手状态

## 职责与优先级

Realtime 负责消息、会话、回执、同步投影、Outbox/JetStream 和跨 Gateway 事件；数据库与消息队列的一致性语义必须显式、可重放。

当前优先把已经完成的领域底座接成可用产品功能：关系读取 → 语音消息 → 1:1 通话。Outbox/数据库性能进入维护模式，只修复正确性缺口或新功能暴露出的已归因热点。

## 已具备的基础

- 关系 delta、projection item/version/history/inbox、snapshot/checkpoint、重建租约、digest 对账和 list/catch-up 同源读取已经闭环；无 checkpoint、gap 与 retention 越界均 fail-closed，legacy 关系表不参与在线结果。
- 消息 changed-at 分页、超预算空页和重复 cursor 已有韧性保护。
- Outbox claim/lease/fencing、崩溃恢复、死信重放审计与低基数指标已有实现；数据库索引、fillfactor 和批量排水已经完成一轮有证据的优化。
- 语音附件元数据、`Available` 绑定门禁和稳定错误已进入附件/消息路径；临时通话状态机、非持久化 signal、Redis/InMemory 状态与授权校验已有基础。

上述内容不再作为下一阶段重复建设。详细数据库测量保留在 [`measurements/`](measurements/)，性能决策见 [`p1-perf-stability.md`](p1-perf-stability.md)。

## 当前 P0：`REL-E2E-4` 关系读取交付

1. 将 projected list/catch-up 作为 Gateway 的唯一 Realtime 读取源，固定 request/response mapper 输入；禁止重新注入 legacy `IRelationshipStore` 或直接暴露投影表结构。
2. 与 Shared/Gateway 对齐 list type、resource key、version、opaque cursor/watermark、partial/reset 和错误；预算裁剪、分页 version 变化、空页 `HasMore` 与重复 cursor 必须保持同一语义。
3. 提供可重复的 Server HTTP authority 对照 fixture，覆盖好友、申请、黑名单、并发 mutation、gap 重建、断线续页、无 checkpoint 与权限失败。
4. 完成 Client 首次 snapshot、增量 catch-up、reset 与账户切换联调；任何错误都不得伪装为空列表或推进设备水位。

完成标准：Gateway/Client 只通过投影读路径即可收敛到 Server 权威；关闭能力后安全 unavailable，Realtime 内不存在第二套在线关系读写源。

## 下一阶段功能

### P1：`VOICE-MSG-2` 语音附件消息交付

1. 与 Shared 固定 codec/container、duration、sample rate、channels、size 与可选 waveform；Realtime DTO 到外部 wire 使用显式 mapper，不把数据库列或内部状态直接当协议。
2. 保持发送前 `Available`、归属和绑定校验；重复 client message id/attachment id 幂等，扫描中、拒绝、过期、非本人和已绑定冲突返回稳定错误。
3. 历史、同步、撤回、保留清理和对象回收复用普通附件消息语义；PostgreSQL、Outbox 和 JetStream 只保存元数据、对象引用与状态事件，不保存音频包。
4. 用真实跨 Gateway 流程覆盖正常发送、扫描状态变化、重复发送、断线、历史/同步恢复和回收，不另建语音专用消息队列。

完成标准：Gateway/Client 能从同一消息事件重建完整语音附件状态，失败可安全重试且不绕过附件安全门禁。

> 状态（2026-09-02）：**已交付并真机验收**。附件绑定链路（`AttachmentWriteCommands` 绑定 UPDATE）
> 写入语音 6 字段（is_voice/codec/container/duration_ms/sample_rate_hz/channels，Migration065 建列），
> `IRealtimeAttachmentStore.EnrichAsync` 历史回查带出——Gateway/Client 从同一消息事件与历史重建
> 均能还原完整语音附件状态（真机 e2e VoiceE2E 41/0，relgate 全栈）。上行元数据经
> `IncomingMessageCommand.Attachments` 快照传递（Realtime.Abstractions 2.5.3 / Integration 3.1.4，
> additive wire 兼容）；残缺语音声明按无元数据处理，保消息必达、不触碰 `ck_attachments_voice_metadata`。
> 本地测试已恢复无 Docker 运行（夹具双模式：`CHATAPP_TEST_POSTGRES/NATS/GARNET`），391 + 135 全绿。

### P1：`CALL-E2E-2` 通话信令交付

1. 接入 Server 正式签发的短期 call grant，校验 issuer/audience、参与者、设备/会话、nonce、过期和撤销边界；测试 verifier 不能成为默认运行路径。
2. 把现有 invite/ringing/accept/reject/cancel/end/timeout/reconnect 状态机接到 Shared/Gateway 外部命令，保持 call id + command id 幂等、revision 单调和唯一终态。
3. SDP/ICE 只走有预算、短 TTL、非持久化的 signal 路径；结束/超时后清理状态与路由。PostgreSQL、持久化 Outbox 和聊天 JetStream 不保存 SDP、ICE 或媒体。
4. 覆盖跨 Gateway、多设备竞争、双方同时操作、依赖短暂中断、重复/乱序、过期 grant 和 Gateway 重连；指标只记录状态、延迟和失败分类。

完成标准：控制面故障可恢复且终态唯一，权限变化 fail-closed，媒体始终留在 WebRTC/STUN/TURN/SFU。

## 可靠性维护：`OUTBOX-OPS-2`

只补真实运行或新功能暴露的缺口：claim 后退出、发布后完成前退出、续租失败、死信重放和 checkpoint 关联必须保持可重现。新增恢复行为要有故障注入、稳定 event id 和消费者幂等证明，不自动重试完成状态不明确的非幂等操作。

## 性能支撑：`PERF-SUPPORT-1`

先读现有 `measurements/`，仅对 profiler 或 `pg_stat_statements` 排名靠前且影响当前功能的路径做单因素修改。固定语料短 A/B 同时记录 DB ops/message、WAL/message、allocation、CPU、GC 和 p95/p99；收益不稳定或复杂度过高时保留现状。不得为了二进制外部协议改造内部 NATS/Outbox wire。

## 跨仓衔接

Server 提供唯一关系权威、附件安全策略和 call grant；Realtime 提供投影、消息/Outbox 与临时信令状态；Shared 固定客户端 wire；Gateway 显式映射与路由；Client 事务应用投影并拥有设备与媒体体验。

## 本阶段非目标

- 不恢复 legacy 关系 mutation/list/sync，也不让 Redis、JetStream 或客户端缓存成为关系权威。
- 不在 Postgres、Outbox、JetStream 或 TCP Gateway 中转音频媒体。
- 不为追求统一而同时迁移 Realtime 内部事件和 Client↔Gateway 二进制协议。

## 验证顺序

聚焦单测/契约测试 → PostgreSQL/NATS 集成测试 → Release 构建 → 必要的 5–10 分钟联调或短 A/B。当前阶段到功能联调验收为止。
