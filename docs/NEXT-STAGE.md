# 下一阶段与接手状态

## 职责

Realtime 负责消息、会话、回执、同步投影、Outbox/JetStream 和跨 Gateway 事件；数据库与消息队列的一致性语义必须显式、可重放。

## 已完成前置

Server 关系增量已能在 JetStream ACK 前原子应用到 projection item/version/inbox；重复事件幂等、版本 gap 回滚，snapshot/checkpoint、数据库时钟租约 Rebuilder 和 digest 对账已完成基础验证。snapshot-gated list processor 已存在但默认关闭；消息 changed-at 分页、超预算空页和重复 cursor 的韧性测试已补齐。下一阶段从投影 list + catch-up 的同源闭环开始，不恢复旧关系表为在线权威。

## 下一阶段 TODO

### P0：`REL-READ-3` 关系投影 list + catch-up 同源闭环

当前 list 开关可以切到 `ProjectedRelationshipListQueryProcessor`，但同步 bootstrap 不能重新注入 legacy `IRelationshipStore` 来补增量。list、catch-up、reset 和设备水位必须来自同一套 Server 权威投影。

1. 为每个 owner/list 建立显式的版本化 change history（或语义等价的持久化历史），与 projection item、version 和 inbox 在应用 delta 的同一事务内提交；以 owner/list/version 唯一约束保证重复事件不重复产生变更。
2. list 从 snapshot checkpoint + 当前 version 读取；catch-up 只读投影 history，并提供 retention floor。旧 Realtime `friendships/friend_requests` 只能作为待删除历史数据，不能参与在线结果或 gap 修复。
3. 对 list/sync 统一处理 unavailable、projection changed、gap、invalid cursor、retention exceeded、request too large 和 bad request；opaque cursor 只承诺继续或明确失效，不泄漏数据库主键、checkpoint 或内部事件编码。
4. 严格执行响应字节预算：超预算返回显式 partial/reset 与可继续水位，不能静默截断；用于重试的服务端水位只能在整页选定并完成预算裁剪后推进，客户端显式水位始终优先。
5. 用 Server HTTP 权威列表逐项对照好友、申请、黑名单；覆盖 snapshot 期间并发 mutation、重复/乱序 delta、gap 后重建、分页 version 变化、空页但 HasMore、重复 cursor、断线续页、无 checkpoint 和授权失败。

完成标准：list 与 catch-up 对同一 owner/list 共享版本语义，客户端从 snapshot 后可只靠增量收敛；任何 gap/保留期越界都有明确 reset，不返回伪空成功；关闭读取开关后仍 fail-closed，且旧关系写表始终不参与结果。

**当前进度（2026-08-12）**：
- 同源对照集成测试已落地（`RelationshipProjectionSourceParityTests`，6/6 通过）：以内存 Server authority 驱动真实 Postgres 投影，逐项验证 `NpgsqlRelationshipProjectionQueryStore` 读取与权威一致。覆盖：快照后仅靠增量收敛（好友/申请/黑名单）、分页期间并发 mutation → `VersionChanged`、重复 cursor 稳定延续（断线续页）、无 checkpoint fail-closed `Unavailable`、删除项消失且 history 仍推进、重复 delta 幂等重放仍收敛。
- 写入/读取双端同源闭环均已闭合：`RelationshipProjectionReconcileGate`（真实 Server HTTP 权威快照 → 投影，两轮 mismatch=0）覆盖 Server→投影；本测试覆盖投影→list/catch-up 读取。客户端从 snapshot 后可只靠增量收敛，gap/保留期越界由 catch-up 层 fail-closed 置 `ResetRequired`，关闭读取开关后 `UnavailableRelationshipProjectionStore` 仍 fail-closed，旧关系写表不参与任何在线结果。

### P0：`OUTBOX-DB-1` 消息与 Outbox 数据库瘦身

1. 用固定消息语料、连接数和随机种子做 5–10 分钟短 A/B；结合 `pg_stat_statements`、`pg_stat_wal` 和应用指标，把每消息成本拆成 message、conversation/unread、attachment bind、Outbox insert、claim、complete/retry 和 cleanup。
2. 先按总时间、calls、rows、WAL bytes 排出 Top SQL，再一次只修改一个批处理、SQL 或索引因素。优先评估减少重复读取、合并同事务写入、有界批量 claim/complete、部分索引，以及避免无变化 UPDATE。
3. 检查 Outbox 状态更新的 HOT 比例、fillfactor、dead tuples、autovacuum、已完成行清理和索引增长；只有数据量与查询模式证明必要时才评估分区，不能先用复杂分区掩盖高写放大。
4. 保持 claim owner/token fencing、ACK-after-commit、数据库唯一约束幂等和单批隔离；一个重复或失败事件不得让无关行被完成、删除或进入死信。
5. 热路径共享仅限线程安全连接池、不可变 metadata、source-generated serializer 和有界 worker；不得跨事务共享 connection 上的 command/reader、写会话或可变批次。

完成标准：逐消息 SQL、WAL 或 managed allocation 至少一项有可重复收益；ACK/跨 Gateway 投递、重复/漏投、JetStream backlog、死信、p95/p99、CPU 和 GC 均不回退。短测不满足正确性时立即撤销该单项优化。

**当前进度（2026-08-13）**：
- 需求 1（PG 级测量基石）：新增 `Measurement/PostgresPerfHarness`——以 `shared_preload_libraries=pg_stat_statements` + `pg_stat_statements.track=top` 启动真实 PostgreSQL 16 Testcontainer，端口等待 + 连接重试消除就绪竞态；`SnapshotAsync` 采集 `pg_stat_statements`（按 queryid 聚合的 calls/time/rows/blks/WAL）、`pg_stat_wal` 与 `pg_stat_user_tables` 前后快照。`.WithCommand` 只传 `-c` 参数（镜像 entrypoint 自动附加 `postgres`，带前导可致 `postgres postgres ...` 启动失败）。
- 固定语料/随机种子驱动真实 `NpgsqlRealtimeMessageStore.SaveAsync` 热路径（message + conversation/unread + outbox insert），消息/客户端/事件 id 带种子前缀避免预热与测量窗口幂等内容冲突；再驱动 Outbox claim + complete 排水。`PostgresPerfDiffCalculator` 按 queryid/表名对齐求窗口增量。
- 报告：`OutboxDbMeasurementTests` 单测生成 `docs/measurements/outbox-db-baseline.md`（A/B 用 A 基线），按每消息拆分 WAL 字节/记录、Top SQL（按耗时与 WAL 双排序）、表级 HOT/死元组。基线验证运行通过（2000 条 inbound：每消息 2,949 WAL 字节 / 21.8 WAL 记录；`INSERT messages` 5.2MB、`upsert_conversation` 705KB；conversation_members/conversations 各 1,966 次 HOT 更新；outbox 1,966 插入）。注意 `pg_stat_*` 统计收集器存在异步滞后，超短窗口下全局 WAL 增量可能为 0，A/B 应以更长窗口（5–10 分钟）为准并以语句级 WAL 为权威归因。
- 需求 2 单项优化（索引瘦身）：`Migration066_RemoveLegacyUserHistoryIndexes` 以 `DROP INDEX CONCURRENTLY` 剔除 `messages` 表两个未被任何查询路径使用的 legacy 每用户历史索引（`ix_messages_receiver_history`、`ix_messages_sender_history`），`RequiresTransaction=false` 避免长事务锁表。A（含索引）→B（剔除）同种子同 2000 条 inbound A/B：每消息 WAL 字节 2,949 → 2,732（-7.35%）、WAL 记录 21.8 → 20.3；`INSERT messages` 语句 WAL 5,237,806/34,242 记录 → 4,656,894/30,192 记录，即写放大收益几乎全部来自该语句的索引维护。A/B 报告分别存档于 `docs/measurements/outbox-db-baseline-A-indexed.md` 与 `docs/measurements/outbox-db-baseline.md`。
- 需求 2 单项优化（排水 HOT）：`Migration067_OutboxHotDrainFillfactor` 将 outbox 表 fillfactor 90 → 75，提升 claim/complete 非索引列更新的 HOT 命中、降低排水路径 WAL 写放大。A/B（fillfactor=90 vs 75，同 2000 条 inbound + 排水）：claim WAL/消息 1,085 → 928、complete 949 → 775，排水合计 2,034 → 1,703（-16.3%）。报告存档 `docs/measurements/outbox-db-fillfactor-ab.md`。
- 需求 2 单项优化（冗余部分索引）：`Migration068_RemoveUnusedReplyForwardIndexes` 以 `DROP INDEX CONCURRENTLY` 剔除 `ix_messages_reply_to` / `ix_messages_forwarded_from` 两个部分索引（Migration013/015 随字段创建，代码库无任何 WHERE 按这两列过滤/排序/连接，仅贡献插入与撤回置 NULL 的索引写放大）。A/B 用每 4 条 1 条携带 reply_to_*+forwarded_from_* 引用的语料，两窗口前均 TRUNCATE messages 从空表起始隔离页分配混杂因素：全局每消息 WAL 字节 2,953 → 2,645（-10.4%）、WAL 记录 21.8 → 19.6；`INSERT messages` 语句级归因持平，收益为 pg_stat_statements 语句级未捕获的索引维护 WAL，以 `pg_stat_wal` 全局为权威。报告存档 `docs/measurements/outbox-db-index-ab.md`。
- 需求 2 单项优化（排水 HOT 终值）：诊断定位 claim HOT 命中率 40% 是排水写放大主因（报告 `docs/measurements/outbox-db-claim-wal-breakdown.md`），并在 delete-on-complete 模式下做 fillfactor A/B（75 vs 50，两窗口前均 TRUNCATE 从空表起始、先 CHECKPOINT 隔离 FPI）：fillfactor=50 时 claim WAL/消息 917 → 359（-61%）、claim WAL 记录 5.6 → 2.7、排水合计 979 → 425（-57%）、outbox HOT 命中率 30% → 87%，与页填充率模型 HOT ≈ (100−fillfactor)/fillfactor 一致。据此落地 `Migration069_OutboxHotDrainFillfactor50` 将 outbox fillfactor 最终定为 50（067 已提交并可能在既有库生效，故新增 069 而非回改）。报告存档 `docs/measurements/outbox-db-fillfactor-hot-ab.md`；回归守卫 `MigrationCatalog_OutboxFillfactorEndsAt50` 断言新库迁移后 outbox reloptions 为 fillfactor=50。下一步可从减少重复读取、合并同事务写入、有界批量 claim/complete 等方向继续压测收益。

**当前进度（2026-08-14）**：
- 需求 2 单项验证（减少重复读取/往返）：A/B 对比 fallback（`idempotencyLedger: null`）与生产合并路径（注入 `NpgsqlCommandIdempotencyLedger`，即 `RealtimePostgresRegistration` 的默认接线）的 SaveAsync 热路径往返结构。`PostgresPerfHarness` 扩展 `MergedMessageStore`/`SeedAuthTablesAsync`/`RunMergedSaveWorkloadAsync`（播种 `public."AspNetUsers"`/`"T_BlockRecords"`/`"T_UserFriendEntry"` 使合并路径 `direct_authorization` 判定 Allowed），新增 `SaveAsync_AdmissionMergeAb_ReportsRoundTripSavings` 测量测试。三次运行结果稳定：热路径 SQL/消息 3 → 2、SQL 往返/消息 12.0 → 11.0（精确 −1 次往返）；收益来自 `MessageWriteAdmissionReader.AcquireDirectAndAllocateSequenceAsync` 把「生命周期锁/状态 + canonical 账本读取 + 事务内授权 + 会话序号分配」合并为单条 CTE，消除 fallback 的独立 `upsert_conversation` 序号分配往返。WAL/耗时不可直接对比：B 额外承担幂等账本 canonical 插入（每消息 1 行，A 完全没有），且短窗口 WAL 有 ±10% 级容器波动，故以语句级 calls 与热路径语句数为权威归因。合并路径即为生产默认，故本项为「量化既有收益」而非新增改动；报告存档 `docs/measurements/outbox-db-admission-merge-ab.md`。
- 需求 2 单项验证（有界批量 claim/complete）：A/B 对比排水批量上限 1 vs 200（`RunOutboxDrainDeleteAsync` 直接驱动 `ClaimBatchAsync` + `DeleteClaimedPublishedBatchAsync`，各 2000 条同种子 delete-on-complete 排水）。claim/delete 均为单语句（`FOR UPDATE ... SKIP LOCKED ... LIMIT @batch_size` / `DELETE ... USING UNNEST`），批量上限只改变每条消息的平均往返次数与事务提交次数（autocommit 下每语句一个事务）：批量 200 时排水 SQL 往返/消息 2.00 → 0.01（-99.5%）、排水耗时/消息 0.48 → 0.02 ms、claim/delete 调用 2000 → 10；排水 WAL 字节/消息 336 → 438 略升（批量下完成更新集中于同页、dead tuples 随批量摊薄前略高，非热路径关注项）。上限本身保证单事务/单 claim 跨度有界，积压时不会形成失控大事务。生产 `OutboxPublisherWorker` 默认 `BatchSize=200` 即 B 配置，故本项为「量化既有配置收益」而非新增改动；报告存档 `docs/measurements/outbox-db-batch-size-ab.md`。
- 需求 2 单项验证（合并同事务写入）：A/B 对比生产合并路径（每次 SaveAsync 仍是 2 条数据语句：admission CTE 生命周期/授权/幂等 + 会话与 member 写入 + 序号分配，bundle CTE message + outbox + ledger）与 super-bundle（把这两个同事务写入合并为单条 CTE——复用 `write_gate` 门控与 `upsert_conversation`/`sender_upsert` 序号分配，让 message INSERT 直接取 `last_sequence`/`sent_count`，热路径数据往返 2 → 1）。`PostgresPerfHarness` 新增 `RunSuperBundleWorkloadAsync`（单条 38 参数 super-bundle CTE，advisory lock 随单语句隐式事务提交释放），`SaveAsync_SuperBundleAb_ReportsRoundTripSavings` 测量测试。结果：热路径 SQL/消息 2 → 1（确定性收益，精确 −1 次数据往返）、SQL 往返/消息 11.0 → 8.0；两路径写入行集完全相同（conversations/members/messages/outbox/command_idempotency_ledger 各 1 行），故 WAL 列预期接近，收益集中于往返削减。总执行时间/消息 0.47 → 0.49 ms 几乎持平（单条 CTE 计划更复杂，可忽略）。B 为实验性合并形态（仅压测用），生产 `SaveAsync` 仍保持 admission+bundle 双语句以隔离生命周期/授权与写入职责；本项结论支持后续若需再减往返可将两语句合并为单条。报告存档 `docs/measurements/outbox-db-super-bundle-ab.md`。

### P0：`OUTBOX-RECOVERY-1` 租约、崩溃与死信重放

1. 补齐 claim、续租、租约过期接管和 owner/token fencing；旧 worker 在失租后不得完成、重试或删除记录。
2. 覆盖“消息已发布但数据库完成前崩溃”，依靠稳定 event id 和消费者幂等收敛，不自动重试完成状态不明确的非幂等操作。
3. 死信重放必须记录原 event id、尝试次数、失败分类、操作者/原因和新 checkpoint；重放单条不能改变无关记录，成功后可从消息、事件、checkpoint 追到恢复结果。
4. 增加积压年龄、claim 冲突、续租失败、重复投递、重放结果的低基数指标；日志不记录消息正文、附件地址或凭据。

完成标准：进程在 claim 后、发布后、完成前任一点退出都可通过测试重现并安全收敛；无永久 Pending、无越权完成、无不可解释的重复或漏投。

**当前进度（2026-08-13）**：
- 需求 3：新增 `outbox_replay_audit` 审计表（Migration064，schema 属性 `OutboxReplayAuditTableSql`）。`IRealtimeOutboxStore` 增加 `ReplayDeadWithAuditAsync`（单事务内锁定 Dead 行 → 重置 Pending → 写审计，任一步失败整体回滚）与 `ListReplayAuditsAsync`（按 event_id 过滤、时间倒序分页）。Noop 返回 not-applicable，Npgsql 原子实现。审计记录原 event id、重放前尝试次数、失败分类、操作者/原因与新 checkpoint。
- 需求 4：`RealtimeMetrics` 增加 `realtime.outbox.claim_conflicts` / `realtime.outbox.lease_renew_failures` / `realtime.outbox.replay.result` 低基数指标；store（重放结果）与 worker（续租失败）已接入，日志不含消息正文/附件/凭据。
- 需求 2/3 测试：`OutboxRecoveryAuditTests` 6 项集成测试覆盖重放审计原子性、单条隔离、非 Dead/缺失返回 false 不写审计、崩溃后（发布后/完成后）收敛到 Published 无永久 Pending、审计过滤与时间倒序。完整集成套件 101/101 通过，单元套件 332/332 通过。

### P1：`VOICE-MSG-1` 语音附件消息闭环

1. 语音作为带 codec/container、duration、sample rate、channels 和 size 元数据的附件消息进入现有上传、扫描、绑定、消息和 Outbox 流程。
2. 发送前再次确认附件为 `Available` 且归属/绑定合法；重复命令使用 client message id 和 attachment id 幂等，扫描中、拒绝、过期和已绑定冲突返回稳定错误。
3. PostgreSQL 与 JetStream 仅保存有界元数据、对象引用和状态事件，不保存或转发音频包；历史、同步、撤回和保留清理必须复用普通附件消息语义。

完成标准：正常发送、重复发送、扫描状态变化、撤回、同步恢复、保留期清理和对象回收形成端到端聚焦测试，且不会绕过附件安全状态。

**当前进度（2026-08-13）**：
- 需求 1/3：Migration065 为 `attachments` 增加 `is_voice` 与 `voice_codec/container/duration_ms/sample_rate_hz/channels` 列，并加 `ck_attachments_voice_metadata` 约束（`is_voice=TRUE` 时这些字段必须非空且为正），Postgres 只存有界语音元数据与对象引用、无音频包列。`RealtimeAttachmentRecord`/`AttachmentRef`/`AttachmentRefMapper` 与 `NpgsqlRealtimeAttachmentStore` 全程携带语音元数据（插入、扫描完成写入、地图到线协议）。
- 需求 2：`AttachmentWriteCommands.BindConfirmedToMessageAsync` 改为发送前可用性校验——`Available`（或 legacy `Confirmed`）才可绑定，扫描中/拒绝/过期/已绑定冲突/缺失或非本人分别返回 `AttachmentBindErrorCode`（`Scanning/Rejected/Expired/AlreadyBound/NotFound/Forbidden/InvalidState`），任一不可绑定即整体失败、不绑定子集。`SaveAsync` 与 `BindToMessageAsync` 接入带错误码结果，`DefaultIncomingMessageProcessor` 将稳定错误码透传为客户端错误码。
- 完成标准测试：`VoiceAttachmentBindTests` 11 项集成测试覆盖正常（Available+语音元数据绑定为 Bound 并读回元数据）、legacy Confirmed、重复 attachment id 去重、扫描中/拒绝/过期/已绑定/无效状态/缺失或非本人各自稳定错误码、混合可绑定+扫描整体失败且不绕过安全状态。完整集成套件 112/112 通过，单元套件 332/332 通过。

### P1：`CALL-CTRL-1` 临时通话信令状态机

1. 以 Server 签发的短期 call grant 为授权输入，实现 invite、ringing、accept、reject、cancel、end、timeout 和 reconnect；使用 call id + command id 幂等、单调 revision 和有界 TTL 处理重复、乱序和断线。
2. SDP/ICE 只允许在受预算限制的临时信令路径转发，不进入 PostgreSQL、持久化 Outbox 或 JetStream 历史；审计只保存参与者、状态、时间、失败分类和必要 QoE 汇总。
3. 明确多设备竞态、双方同时挂断、邀请过期、黑名单/权限变化和 Gateway 切换语义；结束或超时后立即清理临时路由状态。
4. 音频媒体由 WebRTC/STUN/TURN/SFU 承载。Realtime 不转发 UDP 音频包，不实现自定义重传、拥塞控制或媒体加密层。

完成标准：状态迁移表与错误语义固定，重复/乱序/超时/重连均可测试且终态唯一；控制面故障不泄漏长期会话，不让媒体回落到数据库或消息队列。

**当前进度（2026-08-13）**：
- 需求 1/2/3：以短期 call grant 为授权输入，实现完整临时通话信令状态机。`CallCommandType`（Invite/Ringing/Accept/Reject/Cancel/End/Reconnect/Timeout）驱动 `CallState`（Idle→Ringing→Active→Ended），终态唯一、终态后任意命令被拒。`DefaultCallControlProcessor` 用 call id + command id 幂等、单调 revision（乱序/陈旧拒绝）与有界 TTL 处理重复、乱序和断线；`DefaultCallControlProcessor` 逻辑超时（Ringing→Missed / Active→TimedOut）与存储 TTL（Ringing/Active/Ended 各带 `CleanupGraceMs`）分离，超时后清理临时路由状态。
- 授权：`ICallGrantVerifier` 校验 grant 归属/参与方/过期/签名，过期与非参与方分别返回 `GrantExpired` / `GrantInvalid`；`NatsCallControlConsumer` 从 NATS 头提取可信身份，`CallControlWorker` 校验 actor 与身份头一致，不匹配 fail-closed 返回 `GrantInvalid`。
- 需求 2（零持久化）：SDP/ICE 仅经 `NatsCallSignalForwarder` 在 Core NATS 的 `chat.call-signals` 非持久化 subject 转发，`MaxSdpBytes` 预算超限 fail-closed 拒绝；命令经 `chat.call-commands` 消费即处理、失败即 NACK，不进入 JetStream/PostgreSQL/Outbox。审计 `InMemoryCallAuditStore` 只存参与者、状态、时间与失败分类。音频媒体不落入任何持久层。
- 实现与接入：`InMemoryCallStateStore`（ConcurrentDictionary + 注入时钟）、`RedisCallStateStore`（Garnet Lua CAS + TTL）、`CallMetrics`（低基数迁移/失败/超时）。`CallControlWorker` 作为宿主托管服务消费 Core NATS 命令并回发结果。
- 测试：`CallStateMachineTests` + `DefaultCallControlProcessorTests` 单测覆盖迁移表、幂等重放、乱序/陈旧 revision、逻辑超时、重连、终态唯一、SDP 不持久化、授权过期、预算。`CallControlLifecycleTests` 5 项集成测试经宿主 DI + Core NATS 驱动完整生命周期（Invite→Accept→End 状态收敛）、重复 command id 幂等且不重复转发、身份头不匹配/过期 grant/非参与方 fail-closed、以及零持久化（JetStream 无 call 流、Postgres 无 call 表）。完整集成套件 117/117 通过，单元套件 375/375 通过。

## 跨仓衔接

1. **Server → Realtime：** Server 提供唯一关系权威、安全策略和短期 call grant；Realtime 只消费投影/命令，不反向写 Server 业务表。
2. **Realtime → Shared：** Realtime 固定 list/catch-up、partial/reset 和通话状态机语义后，向 Shared 提交稳定字段、错误和预算；投影表、租约、digest、JetStream subject 与数据库序号不进入外部 wire。
3. **Shared → Gateway：** Shared 统一外部协议；Gateway 只做显式 mapper、连接鉴权、能力协商和路由，不复制 Realtime DTO 或承载媒体。
4. **Gateway → Client：** Client 整页事务应用关系投影、用显式水位恢复；通话 UI/设备、WebRTC 协商和媒体播放属于 Client，TURN/SFU 是独立媒体面。

## 本阶段非目标

- 不迁移 Realtime 持久化 Outbox/NATS wire 到新的二进制格式；外部二进制评估与内部事件格式分批进行。
- 不恢复旧关系 mutation/list/sync，不让 Redis、JetStream 或客户端缓存成为关系权威。
- 不在 Postgres、Outbox、JetStream 或 TCP Gateway 中转音频媒体。

## 验证顺序

聚焦单测/契约测试 → PostgreSQL 集成测试 → Release 配置构建 → 固定快照的 5–10 分钟 admission/capacity 短 A/B。本阶段以闭环正确性和热点收益为止，不安排长时稳定性测试。
