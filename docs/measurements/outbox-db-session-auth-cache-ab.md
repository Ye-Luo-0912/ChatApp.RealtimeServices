# OUTBOX-DB-1 减少重复读取 A/B（已建会话免重复授权：会话级授权缓存的每消息收益）

> 生成时间：2026-09-02 17:25:33 UTC；语料固定、随机种子 20260813；每配置 inbound 2000 条消息、40 个会话、每会话 50 条（已建会话）。

| 配置 | 热路径 SQL/消息 | SQL 往返/消息 | 总执行时间/消息(ms) | WAL 字节/消息 | WAL 记录/消息 |
|---|---|---|---|---|---|
| A：每条消息完整授权读取 | 1 | 8.0 | 0.32 | 2,503 | 19.0 |
| B：会话级授权缓存（已建会话免重复授权） | 1 | 8.0 | 0.27 | 3,086 | 24.5 |

> 以 pg_stat_statements 语句级 calls/wal_bytes 归因，A/B 同容器顺序运行、语句数与写入行集完全相同、
> 会话分布完全相同（conversationCount 个会话、每会话 msgsPerConv 条消息）。唯一差异是授权读取粒度：
> A 每条消息读取 direct_user_state（AspNetUsers 2 行）+ direct_authorization（T_BlockRecords 1 次 EXISTS +
> T_UserFriendEntry 2 次 EXISTS，共 4 处表读取）；B 仅每会话第一条消息做完整授权读取（建立会话），
> 后续 49 条跳过授权读取（会话级授权缓存命中，模拟「已建会话免重复授权」）。

## 结论

> 本次实测 B 较 A 每消息执行耗时节省 0.045 ms（0.32 → 0.27 ms）；按每会话 
> 50 条消息，理论上限为回收 (msgsPerConv−1)/msgsPerConv ≈ 98% 的授权读取成本
> （单次授权读取成本约 0.08 ms/消息，见 outbox-db-auth-reads-ab.md）。会话级授权缓存方向可行：
> 会话建立后授权事实稳定，落地生产需对「会话建立」与「授权事实变更（好友/黑名单/隐私策略变化）」
> 做 TTL 或显式失效注入，使缓存命中不绕过会话建立后的授权变化；B 为测量专用变体，不直接落生产。

## A：每条消息完整授权读取 热路径语句（近热路径 = 调用数 ≥ 消息数一半）

| 调用 | 调用/消息 | exec/消息(ms) | wal_records/消息 | wal_bytes/调用 | sql 片段 |
|---|---|---|---|---|---|
| 2,000 | 1.0 | 0.19 | 24.4 | 3,294 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     F…` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `RESET ALL` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `CLOSE ALL` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD SEQUENCES` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `UNLISTEN *` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD TEMP` |

## A：每条消息完整授权读取（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 5,006,332 | 2,503 |
| WAL 记录 | 38,080 | 19.0 |
| WAL FPI | 0 | 0.00 |
| WAL write | 1,506 | 0.753 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 844BF37870930544 | 2,000 | 379.1 | 2,000 | 0 | 657 | 48,749 | 0 | 6,589,478 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| 1F1A5B390EB81DF3 | 1 | 203.3 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 13FE8940C25F27BC | 1 | 31.9 | 4,454 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 7FD7FA9679F7127A | 1 | 5.2 | 29 | 0 | 0 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 9DFE0023C220BBE7 | 2,003 | 4.7 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 20928A0A690DF580 | 2,003 | 3.1 | 2,003 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 2,003 | 2.3 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 763D4E8E0DC293DC | 2,003 | 0.5 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| 8464F5314FC1C676 | 2,003 | 0.4 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| F9011CF57CBB30AB | 2,003 | 0.3 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 2,003 | 0.2 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |
| 881865BABB8509BA | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| 844BF37870930544 | 2,000 | 6,589,478 | 48,749 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| 1F1A5B390EB81DF3 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 13FE8940C25F27BC | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 7FD7FA9679F7127A | 1 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 9DFE0023C220BBE7 | 2,003 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 20928A0A690DF580 | 2,003 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 2,003 | 0 | 0 | 0 | `RESET ALL` |
| 763D4E8E0DC293DC | 2,003 | 0 | 0 | 0 | `CLOSE ALL` |
| 8464F5314FC1C676 | 2,003 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| F9011CF57CBB30AB | 2,003 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 2,003 | 0 | 0 | 0 | `DISCARD TEMP` |
| 881865BABB8509BA | 1 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| schema_migrations | 0 | 0 | 0 | 0 | 0 |
| schema_migration_checkpoints | 0 | 0 | 0 | 0 | 0 |
| messages | 2,000 | 0 | 0 | 0 | 0 |
| outbox | 2,000 | 0 | 0 | 0 | 0 |
| conversations | 0 | 2,000 | 0 | 1,961 | 920 |
| conversation_members | 0 | 2,000 | 0 | 1,963 | 913 |
| device_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |
| message_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| message_reactions | 0 | 0 | 0 | 0 | 0 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| user_deletion_tombstones | 0 | 0 | 0 | 0 | 0 |
| command_idempotency_ledger | 2,000 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| conversation_membership_periods | 0 | 0 | 0 | 0 | 0 |
| account_cleanup_jobs | 0 | 0 | 0 | 0 | 0 |
| message_state | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| friendships | 0 | 0 | 0 | 0 | 0 |
| relationship_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_items | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_inbox | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_snapshots | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_rebuild_state | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_history | 0 | 0 | 0 | 0 | 0 |
| outbox_replay_audit | 0 | 0 | 0 | 0 | 0 |

> HOT 命中率（hot_updates / updates）：conversations 1,961/2,000（98%）；conversation_members 1,963/2,000（98%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。


## B：会话级授权缓存（已建会话免重复授权）热路径语句（近热路径 = 调用数 ≥ 消息数一半）

| 调用 | 调用/消息 | exec/消息(ms) | wal_records/消息 | wal_bytes/调用 | sql 片段 |
|---|---|---|---|---|---|
| 1,960 | 1.0 | 0.13 | 23.7 | 3,142 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     F…` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `RESET ALL` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `CLOSE ALL` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD SEQUENCES` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `UNLISTEN *` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD TEMP` |

## B：会话级授权缓存（已建会话免重复授权）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 6,173,216 | 3,087 |
| WAL 记录 | 48,997 | 24.5 |
| WAL FPI | 0 | 0.00 |
| WAL write | 1,941 | 0.971 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 825FD7576B1D99A9 | 1,960 | 266.9 | 1,960 | 0 | 552 | 47,485 | 0 | 6,158,936 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| 1F1A5B390EB81DF3 | 1 | 208.2 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 13FE8940C25F27BC | 1 | 39.4 | 4,454 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 844BF37870930544 | 40 | 9.3 | 40 | 0 | 13 | 1,099 | 0 | 140,061 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| 7FD7FA9679F7127A | 1 | 6.2 | 29 | 0 | 0 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 9DFE0023C220BBE7 | 2,003 | 4.6 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 20928A0A690DF580 | 2,003 | 3.1 | 2,003 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 2,003 | 1.9 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 763D4E8E0DC293DC | 2,003 | 0.5 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| 8464F5314FC1C676 | 2,003 | 0.4 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| F9011CF57CBB30AB | 2,003 | 0.3 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 2,003 | 0.2 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |
| 881865BABB8509BA | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| 825FD7576B1D99A9 | 1,960 | 6,158,936 | 47,485 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| 844BF37870930544 | 40 | 140,061 | 1,099 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| 1F1A5B390EB81DF3 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 13FE8940C25F27BC | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 7FD7FA9679F7127A | 1 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 9DFE0023C220BBE7 | 2,003 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 20928A0A690DF580 | 2,003 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 2,003 | 0 | 0 | 0 | `RESET ALL` |
| 763D4E8E0DC293DC | 2,003 | 0 | 0 | 0 | `CLOSE ALL` |
| 8464F5314FC1C676 | 2,003 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| F9011CF57CBB30AB | 2,003 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 2,003 | 0 | 0 | 0 | `DISCARD TEMP` |
| 881865BABB8509BA | 1 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| schema_migrations | 0 | 0 | 0 | 0 | 0 |
| schema_migration_checkpoints | 0 | 0 | 0 | 0 | 0 |
| messages | 2,000 | 0 | 0 | 0 | 0 |
| outbox | 2,000 | 0 | 0 | 0 | 0 |
| conversations | 0 | 2,000 | 0 | 1,946 | 809 |
| conversation_members | 0 | 2,000 | 0 | 1,962 | 843 |
| device_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |
| message_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| message_reactions | 0 | 0 | 0 | 0 | 0 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| user_deletion_tombstones | 0 | 0 | 0 | 0 | 0 |
| command_idempotency_ledger | 2,000 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| conversation_membership_periods | 0 | 0 | 0 | 0 | 0 |
| account_cleanup_jobs | 0 | 0 | 0 | 0 | 0 |
| message_state | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| friendships | 0 | 0 | 0 | 0 | 0 |
| relationship_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_items | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_inbox | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_snapshots | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_rebuild_state | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_history | 0 | 0 | 0 | 0 | 0 |
| outbox_replay_audit | 0 | 0 | 0 | 0 | 0 |

> HOT 命中率（hot_updates / updates）：conversations 1,946/2,000（97%）；conversation_members 1,962/2,000（98%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。

