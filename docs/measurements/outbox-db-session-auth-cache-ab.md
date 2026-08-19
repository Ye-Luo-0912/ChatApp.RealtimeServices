# OUTBOX-DB-1 减少重复读取 A/B（已建会话免重复授权：会话级授权缓存的每消息收益）

> 生成时间：2026-08-19 06:39:18 UTC；语料固定、随机种子 20260813；每配置 inbound 2000 条消息、40 个会话、每会话 50 条（已建会话）。

| 配置 | 热路径 SQL/消息 | SQL 往返/消息 | 总执行时间/消息(ms) | WAL 字节/消息 | WAL 记录/消息 |
|---|---|---|---|---|---|
| A：每条消息完整授权读取 | 1 | 8.0 | 0.33 | 4,426 | 34.6 |
| B：会话级授权缓存（已建会话免重复授权） | 1 | 8.0 | 0.30 | 1,788 | 14.5 |

> 以 pg_stat_statements 语句级 calls/wal_bytes 归因，A/B 同容器顺序运行、语句数与写入行集完全相同、
> 会话分布完全相同（conversationCount 个会话、每会话 msgsPerConv 条消息）。唯一差异是授权读取粒度：
> A 每条消息读取 direct_user_state（AspNetUsers 2 行）+ direct_authorization（T_BlockRecords 1 次 EXISTS +
> T_UserFriendEntry 2 次 EXISTS，共 4 处表读取）；B 仅每会话第一条消息做完整授权读取（建立会话），
> 后续 49 条跳过授权读取（会话级授权缓存命中，模拟「已建会话免重复授权」）。

## 结论

> 本次实测 B 较 A 每消息执行耗时节省 0.036 ms（0.33 → 0.30 ms）；按每会话 
> 50 条消息，理论上限为回收 (msgsPerConv−1)/msgsPerConv ≈ 98% 的授权读取成本
> （单次授权读取成本约 0.08 ms/消息，见 outbox-db-auth-reads-ab.md）。会话级授权缓存方向可行：
> 会话建立后授权事实稳定，落地生产需对「会话建立」与「授权事实变更（好友/黑名单/隐私策略变化）」
> 做 TTL 或显式失效注入，使缓存命中不绕过会话建立后的授权变化；B 为测量专用变体，不直接落生产。

## A：每条消息完整授权读取 热路径语句（近热路径 = 调用数 ≥ 消息数一半）

| 调用 | 调用/消息 | exec/消息(ms) | wal_records/消息 | wal_bytes/调用 | sql 片段 |
|---|---|---|---|---|---|
| 2,000 | 1.0 | 0.22 | 25.0 | 3,325 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     F…` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `RESET ALL` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD SEQUENCES` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `CLOSE ALL` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `UNLISTEN *` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD TEMP` |

## A：每条消息完整授权读取（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 8,852,416 | 4,426 |
| WAL 记录 | 69,227 | 34.6 |
| WAL FPI | 103 | 0.05 |
| WAL write | 832 | 0.416 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| F441132799D72C49 | 2,000 | 443.2 | 2,000 | 0 | 620 | 49,942 | 0 | 6,650,940 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| E11B52EF572B8535 | 1 | 201.3 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 403F8B358778114D | 2,003 | 9.8 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 878A9E463E585A2C | 2,003 | 5.2 | 2,003 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 4040643821AD66AF | 2,003 | 5.0 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| D28E47E4803A0167 | 2,003 | 1.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 2,003 | 0.7 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 2,003 | 0.6 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| B71A600DFCB91748 | 1 | 0.4 | 235 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 265A210A7402B089 | 1 | 0.3 | 29 | 0 | 0 | 3 | 0 | 181 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 516E8F1759460B47 | 2,003 | 0.2 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| F441132799D72C49 | 2,000 | 6,650,940 | 49,942 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| 265A210A7402B089 | 1 | 181 | 3 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| E11B52EF572B8535 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 403F8B358778114D | 2,003 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 878A9E463E585A2C | 2,003 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 4040643821AD66AF | 2,003 | 0 | 0 | 0 | `RESET ALL` |
| D28E47E4803A0167 | 2,003 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 2,003 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 2,003 | 0 | 0 | 0 | `UNLISTEN *` |
| B71A600DFCB91748 | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 516E8F1759460B47 | 2,003 | 0 | 0 | 0 | `DISCARD TEMP` |
| DA559F39F26D405A | 1 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| relationship_projection_rebuild_state | 0 | 0 | 0 | 0 | 0 |
| device_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| messages | 2,000 | 0 | 0 | 0 | 0 |
| command_idempotency_ledger | 2,000 | 0 | 0 | 0 | 0 |
| conversation_members | 0 | 2,000 | 0 | 2,000 | -16 |
| account_cleanup_jobs | 0 | 0 | 0 | 0 | 0 |
| user_deletion_tombstones | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_items | 0 | 0 | 0 | 0 | 0 |
| friendships | 0 | 0 | 0 | 0 | 0 |
| conversations | 0 | 2,000 | 0 | 2,000 | -4 |
| relationship_projection_history | 0 | 0 | 0 | 0 | 0 |
| outbox_replay_audit | 0 | 0 | 0 | 0 | 0 |
| conversation_membership_periods | 0 | 0 | 0 | 0 | 0 |
| relationship_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| message_state | 0 | 0 | 0 | 0 | 0 |
| message_reactions | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_inbox | 0 | 0 | 0 | 0 | 0 |
| relationship_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| message_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| schema_migration_checkpoints | 0 | 0 | 0 | 0 | 0 |
| schema_migrations | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_snapshots | 0 | 0 | 0 | 0 | 0 |
| outbox | 2,000 | 0 | 0 | 0 | 0 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |

> HOT 命中率（hot_updates / updates）：conversation_members 2,000/2,000（100%）；conversations 2,000/2,000（100%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。


## B：会话级授权缓存（已建会话免重复授权）热路径语句（近热路径 = 调用数 ≥ 消息数一半）

| 调用 | 调用/消息 | exec/消息(ms) | wal_records/消息 | wal_bytes/调用 | sql 片段 |
|---|---|---|---|---|---|
| 1,960 | 1.0 | 0.18 | 24.4 | 3,180 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     F…` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `RESET ALL` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD SEQUENCES` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `UNLISTEN *` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `CLOSE ALL` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD TEMP` |

## B：会话级授权缓存（已建会话免重复授权）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 3,577,517 | 1,789 |
| WAL 记录 | 29,029 | 14.5 |
| WAL FPI | 0 | 0.00 |
| WAL write | 786 | 0.393 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| CDC538CEE21E315E | 1,960 | 366.5 | 1,960 | 0 | 519 | 48,794 | 0 | 6,234,514 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| E11B52EF572B8535 | 1 | 201.4 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| F441132799D72C49 | 40 | 8.9 | 40 | 0 | 11 | 993 | 0 | 124,675 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| 403F8B358778114D | 2,003 | 8.6 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 878A9E463E585A2C | 2,003 | 4.4 | 2,003 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 4040643821AD66AF | 2,003 | 4.1 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| D28E47E4803A0167 | 2,003 | 0.9 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| C0D0E27048376284 | 2,003 | 0.6 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 92049CC8AB443DC6 | 2,003 | 0.6 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| B71A600DFCB91748 | 1 | 0.4 | 239 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 516E8F1759460B47 | 2,003 | 0.2 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |
| 265A210A7402B089 | 1 | 0.2 | 29 | 0 | 0 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| CDC538CEE21E315E | 1,960 | 6,234,514 | 48,794 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| F441132799D72C49 | 40 | 124,675 | 993 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| E11B52EF572B8535 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 403F8B358778114D | 2,003 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 878A9E463E585A2C | 2,003 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 4040643821AD66AF | 2,003 | 0 | 0 | 0 | `RESET ALL` |
| D28E47E4803A0167 | 2,003 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| C0D0E27048376284 | 2,003 | 0 | 0 | 0 | `UNLISTEN *` |
| 92049CC8AB443DC6 | 2,003 | 0 | 0 | 0 | `CLOSE ALL` |
| B71A600DFCB91748 | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 516E8F1759460B47 | 2,003 | 0 | 0 | 0 | `DISCARD TEMP` |
| 265A210A7402B089 | 1 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| DA559F39F26D405A | 1 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| relationship_projection_rebuild_state | 0 | 0 | 0 | 0 | 0 |
| device_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| messages | 2,000 | 0 | 0 | 0 | 0 |
| command_idempotency_ledger | 2,000 | 0 | 0 | 0 | 0 |
| conversation_members | 0 | 2,000 | 0 | 2,000 | 12 |
| account_cleanup_jobs | 0 | 0 | 0 | 0 | 0 |
| user_deletion_tombstones | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_items | 0 | 0 | 0 | 0 | 0 |
| friendships | 0 | 0 | 0 | 0 | 0 |
| conversations | 0 | 2,000 | 0 | 2,000 | -7 |
| relationship_projection_history | 0 | 0 | 0 | 0 | 0 |
| outbox_replay_audit | 0 | 0 | 0 | 0 | 0 |
| conversation_membership_periods | 0 | 0 | 0 | 0 | 0 |
| relationship_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| message_state | 0 | 0 | 0 | 0 | 0 |
| message_reactions | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_inbox | 0 | 0 | 0 | 0 | 0 |
| relationship_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| message_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| schema_migration_checkpoints | 0 | 0 | 0 | 0 | 0 |
| schema_migrations | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_snapshots | 0 | 0 | 0 | 0 | 0 |
| outbox | 2,000 | 0 | 0 | 0 | 0 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |

> HOT 命中率（hot_updates / updates）：conversation_members 2,000/2,000（100%）；conversations 2,000/2,000（100%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。

