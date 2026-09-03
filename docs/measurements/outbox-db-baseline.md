# OUTBOX-DB-1 测量基线（PG 级 A/B）

> 生成时间：2026-09-02 17:26:10 UTC；语料固定、随机种子 20260813；inbound 窗口 2000 条消息。

## Inbound 热路径（SaveAsync：message + conversation/unread + outbox insert）

## Inbound 热路径（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 4,805,406 | 2,403 |
| WAL 记录 | 34,645 | 17.3 |
| WAL FPI | 134 | 0.07 |
| WAL write | 1,539 | 0.769 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 1F1A5B390EB81DF3 | 1 | 205.2 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 45E911E5AB9676D1 | 2,000 | 152.8 | 2,000 | 0 | 534 | 30,212 | 0 | 4,659,398 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_e21b15e5d93e"."messages" (  …` |
| 8999337357638EDC | 2,000 | 108.4 | 2,000 | 0 | 0 | 8,091 | 0 | 705,454 | `WITH upsert_conversation AS (     INSERT INTO "perf_e21b15e5d93e"."conversations" (       …` |
| C4572D1B14825E12 | 2,000 | 43.5 | 4,000 | 0 | 0 | 0 | 0 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM UNNEST($1) AS …` |
| 13FE8940C25F27BC | 1 | 20.1 | 4,427 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 7FD7FA9679F7127A | 1 | 5.3 | 29 | 0 | 0 | 8 | 0 | 458 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 9DFE0023C220BBE7 | 2,003 | 5.0 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| D6C944F452669EEE | 2,000 | 3.5 | 0 | 0 | 0 | 0 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 20928A0A690DF580 | 2,003 | 3.2 | 2,003 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 2,003 | 2.4 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| D8A6A35AB613B78D | 2,000 | 0.7 | 0 | 0 | 0 | 0 | 0 | 0 | `COMMIT` |
| 763D4E8E0DC293DC | 2,003 | 0.5 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| 8464F5314FC1C676 | 2,003 | 0.5 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| F9011CF57CBB30AB | 2,003 | 0.3 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 2,003 | 0.2 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |
| 881865BABB8509BA | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| 45E911E5AB9676D1 | 2,000 | 4,659,398 | 30,212 | 0 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_e21b15e5d93e"."messages" (  …` |
| 8999337357638EDC | 2,000 | 705,454 | 8,091 | 0 | `WITH upsert_conversation AS (     INSERT INTO "perf_e21b15e5d93e"."conversations" (       …` |
| 7FD7FA9679F7127A | 1 | 458 | 8 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 1F1A5B390EB81DF3 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| C4572D1B14825E12 | 2,000 | 0 | 0 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM UNNEST($1) AS …` |
| 13FE8940C25F27BC | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 9DFE0023C220BBE7 | 2,003 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| D6C944F452669EEE | 2,000 | 0 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 20928A0A690DF580 | 2,003 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 2,003 | 0 | 0 | 0 | `RESET ALL` |
| D8A6A35AB613B78D | 2,000 | 0 | 0 | 0 | `COMMIT` |
| 763D4E8E0DC293DC | 2,003 | 0 | 0 | 0 | `CLOSE ALL` |
| 8464F5314FC1C676 | 2,003 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| F9011CF57CBB30AB | 2,003 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 2,003 | 0 | 0 | 0 | `DISCARD TEMP` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| schema_migrations | 0 | 0 | 0 | 0 | 0 |
| schema_migration_checkpoints | 0 | 0 | 0 | 0 | 0 |
| messages | 2,000 | 0 | 0 | 0 | 0 |
| outbox | 2,000 | 0 | 0 | 0 | 0 |
| conversations | 0 | 2,000 | 0 | 2,000 | 11 |
| conversation_members | 0 | 2,000 | 0 | 2,000 | 0 |
| device_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |
| message_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| message_reactions | 0 | 0 | 0 | 0 | 0 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| user_deletion_tombstones | 0 | 0 | 0 | 0 | 0 |
| command_idempotency_ledger | 0 | 0 | 0 | 0 | 0 |
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

> HOT 命中率（hot_updates / updates）：conversations 2,000/2,000（100%）；conversation_members 2,000/2,000（100%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。


## Outbox 排水（claim + complete）

## Outbox 排水（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 0 | 0 |
| WAL 记录 | 0 | 0.0 |
| WAL FPI | 0 | 0.00 |
| WAL write | 1 | 0.001 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 1F1A5B390EB81DF3 | 1 | 211.4 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| F14A6EE303359508 | 40 | 26.9 | 2,000 | 0 | 47 | 5,641 | 0 | 746,550 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_e21b15e5d93e"."o…` |
| 8E00D4765111919B | 40 | 25.4 | 2,000 | 0 | 25 | 10,827 | 0 | 1,195,236 | `UPDATE "perf_e21b15e5d93e"."outbox" AS item SET published_at_ms = $1, status = $4, locked_…` |
| 13FE8940C25F27BC | 1 | 20.2 | 4,427 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 7FD7FA9679F7127A | 1 | 8.2 | 29 | 0 | 0 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 9DFE0023C220BBE7 | 83 | 0.2 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 20928A0A690DF580 | 83 | 0.1 | 83 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 83 | 0.1 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 763D4E8E0DC293DC | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| 8464F5314FC1C676 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 881865BABB8509BA | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| F9011CF57CBB30AB | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| 8E00D4765111919B | 40 | 1,195,236 | 10,827 | 0 | `UPDATE "perf_e21b15e5d93e"."outbox" AS item SET published_at_ms = $1, status = $4, locked_…` |
| F14A6EE303359508 | 40 | 746,550 | 5,641 | 0 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_e21b15e5d93e"."o…` |
| 1F1A5B390EB81DF3 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 13FE8940C25F27BC | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 7FD7FA9679F7127A | 1 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 9DFE0023C220BBE7 | 83 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 20928A0A690DF580 | 83 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 83 | 0 | 0 | 0 | `RESET ALL` |
| 763D4E8E0DC293DC | 83 | 0 | 0 | 0 | `CLOSE ALL` |
| 8464F5314FC1C676 | 83 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 881865BABB8509BA | 1 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| F9011CF57CBB30AB | 83 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 83 | 0 | 0 | 0 | `DISCARD TEMP` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| schema_migrations | 0 | 0 | 0 | 0 | 0 |
| schema_migration_checkpoints | 0 | 0 | 0 | 0 | 0 |
| messages | 0 | 0 | 0 | 0 | 0 |
| outbox | 0 | 4,000 | 0 | 1,749 | 2,251 |
| conversations | 0 | 0 | 0 | 0 | 0 |
| conversation_members | 0 | 0 | 0 | 0 | 0 |
| device_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |
| message_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| message_reactions | 0 | 0 | 0 | 0 | 0 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| user_deletion_tombstones | 0 | 0 | 0 | 0 | 0 |
| command_idempotency_ledger | 0 | 0 | 0 | 0 | 0 |
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

> HOT 命中率（hot_updates / updates）：outbox 1,749/4,000（44%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。


## 说明

- 本报告只记录经读写路径的 SQL/WAL 总量，不含任何消息正文、附件地址或凭据。
- 热路径每消息成本 = 窗口增量 / 消息数；A/B 时以同一语料/种子重跑，对比各条 SQL 的 calls/time/rows/wal_bytes 是否下降。

