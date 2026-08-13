# OUTBOX-DB-1 测量基线（PG 级 A/B）

> 生成时间：2026-08-13 10:42:08 UTC；语料固定、随机种子 20260813；inbound 窗口 2000 条消息。

## Inbound 热路径（SaveAsync：message + conversation/unread + outbox insert）

## Inbound 热路径（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 5,898,970 | 2,949 |
| WAL 记录 | 43,580 | 21.8 |
| WAL FPI | 0 | 0.00 |
| WAL write | 732 | 0.366 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|
| 2E2DDA1559AE03AD | 2,000 | 241.5 | 2,000 | 0 | 466 | 34,242 | 5,237,806 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_1c72952b407a"."messages" (  …` |
| AD398CE96D5C30F8 | 2,000 | 138.0 | 2,000 | 0 | 0 | 8,091 | 705,363 | `WITH upsert_conversation AS (     INSERT INTO "perf_1c72952b407a"."conversations" (       …` |
| 4D103CE8ABD01E23 | 2,000 | 40.7 | 4,000 | 0 | 0 | 0 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM UNNEST($1) AS …` |
| 403F8B358778114D | 2,003 | 9.4 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 2,003 | 7.0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| CF1D5941D5B56432 | 2,000 | 4.6 | 0 | 0 | 0 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 878A9E463E585A2C | 2,003 | 4.4 | 2,003 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 1CA7E40EFC47C423 | 2,000 | 1.1 | 0 | 0 | 0 | 0 | 0 | `COMMIT` |
| D28E47E4803A0167 | 2,003 | 1.1 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 2,003 | 0.6 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| B71A600DFCB91748 | 1 | 0.5 | 224 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| C0D0E27048376284 | 2,003 | 0.4 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 516E8F1759460B47 | 2,003 | 0.2 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |
| 265A210A7402B089 | 1 | 0.2 | 29 | 0 | 0 | 3 | 181 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | sql |
|---|---|---|---|---|
| 2E2DDA1559AE03AD | 2,000 | 5,237,806 | 34,242 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_1c72952b407a"."messages" (  …` |
| AD398CE96D5C30F8 | 2,000 | 705,363 | 8,091 | `WITH upsert_conversation AS (     INSERT INTO "perf_1c72952b407a"."conversations" (       …` |
| 265A210A7402B089 | 1 | 181 | 3 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 4D103CE8ABD01E23 | 2,000 | 0 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM UNNEST($1) AS …` |
| 403F8B358778114D | 2,003 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 2,003 | 0 | 0 | `RESET ALL` |
| CF1D5941D5B56432 | 2,000 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 878A9E463E585A2C | 2,003 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 1CA7E40EFC47C423 | 2,000 | 0 | 0 | `COMMIT` |
| D28E47E4803A0167 | 2,003 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 2,003 | 0 | 0 | `CLOSE ALL` |
| B71A600DFCB91748 | 1 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| C0D0E27048376284 | 2,003 | 0 | 0 | `UNLISTEN *` |
| 516E8F1759460B47 | 2,003 | 0 | 0 | `DISCARD TEMP` |
| DA559F39F26D405A | 1 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| relationship_projection_rebuild_state | 0 | 0 | 0 | 0 | 0 |
| device_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| messages | 1,966 | 0 | 0 | 0 | 0 |
| command_idempotency_ledger | 0 | 0 | 0 | 0 | 0 |
| conversation_members | 0 | 1,966 | 0 | 1,966 | -34 |
| account_cleanup_jobs | 0 | 0 | 0 | 0 | 0 |
| user_deletion_tombstones | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_items | 0 | 0 | 0 | 0 | 0 |
| friendships | 0 | 0 | 0 | 0 | 0 |
| conversations | 0 | 1,966 | 0 | 1,966 | 16 |
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
| outbox | 1,966 | 0 | 0 | 0 | 0 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |


## Outbox 排水（claim + complete）

## Outbox 排水（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 0 | 0 |
| WAL 记录 | 0 | 0.0 |
| WAL FPI | 0 | 0.00 |
| WAL write | 79 | 0.040 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|
| C5FA397E315F9D09 | 40 | 48.0 | 2,000 | 0 | 172 | 13,161 | 2,200,874 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_1c72952b407a"."o…` |
| E8836F9DF6B2FBE6 | 40 | 32.8 | 2,000 | 0 | 140 | 11,806 | 1,905,321 | `UPDATE "perf_1c72952b407a"."outbox" AS item SET published_at_ms = $1, status = $4, locked_…` |
| B71A600DFCB91748 | 1 | 0.6 | 227 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 403F8B358778114D | 83 | 0.3 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 265A210A7402B089 | 1 | 0.2 | 29 | 0 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 4040643821AD66AF | 83 | 0.2 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 83 | 0.2 | 83 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| D28E47E4803A0167 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| C0D0E27048376284 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 92049CC8AB443DC6 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 516E8F1759460B47 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | sql |
|---|---|---|---|---|
| C5FA397E315F9D09 | 40 | 2,200,874 | 13,161 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_1c72952b407a"."o…` |
| E8836F9DF6B2FBE6 | 40 | 1,905,321 | 11,806 | `UPDATE "perf_1c72952b407a"."outbox" AS item SET published_at_ms = $1, status = $4, locked_…` |
| B71A600DFCB91748 | 1 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 403F8B358778114D | 83 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 265A210A7402B089 | 1 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 4040643821AD66AF | 83 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 83 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| D28E47E4803A0167 | 83 | 0 | 0 | `DISCARD SEQUENCES` |
| C0D0E27048376284 | 83 | 0 | 0 | `UNLISTEN *` |
| 92049CC8AB443DC6 | 83 | 0 | 0 | `CLOSE ALL` |
| DA559F39F26D405A | 1 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 516E8F1759460B47 | 83 | 0 | 0 | `DISCARD TEMP` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| relationship_projection_rebuild_state | 0 | 0 | 0 | 0 | 0 |
| device_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| messages | 0 | 0 | 0 | 0 | 0 |
| command_idempotency_ledger | 0 | 0 | 0 | 0 | 0 |
| conversation_members | 0 | 0 | 0 | 0 | 0 |
| account_cleanup_jobs | 0 | 0 | 0 | 0 | 0 |
| user_deletion_tombstones | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_items | 0 | 0 | 0 | 0 | 0 |
| friendships | 0 | 0 | 0 | 0 | 0 |
| conversations | 0 | 0 | 0 | 0 | 0 |
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
| outbox | 0 | 0 | 0 | 0 | 0 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |


## 说明

- 本报告只记录经读写路径的 SQL/WAL 总量，不含任何消息正文、附件地址或凭据。
- 热路径每消息成本 = 窗口增量 / 消息数；A/B 时以同一语料/种子重跑，对比各条 SQL 的 calls/time/rows/wal_bytes 是否下降。

