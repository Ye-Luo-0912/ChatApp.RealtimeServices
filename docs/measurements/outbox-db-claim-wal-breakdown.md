# OUTBOX-DB-1 claim 路径 WAL 分解（FPI 隔离 + HOT 命中率）

> 生成时间：2026-08-19 06:39:50 UTC；语料固定、随机种子 20260813；每配置 inbound 2000 条消息、delete-on-complete 排水 2000 条。

| 配置 | claim WAL/消息 | claim WAL 记录/消息 | claim FPI/消息 | claim FPI 字节估算/消息 |
|---|---|---|---|---|
| A：先 CHECKPOINT 再排水（稳态页） | 359 | 2.7 | 0.00 | 0 |
| B：直接排水（容器冷页） | 359 | 2.7 | 0.00 | 0 |

> 以 pg_stat_statements 语句级 wal_bytes/wal_records/wal_fpi 归因，A/B 同容器顺序运行、语料同种子前缀不同；
> FPI 字节估算按 8KB/页 折算；A 在排水前强制 CHECKPOINT，使排水窗口内页已落盘、近似生产稳态（无 FPI）；
> B 不 CHECKPOINT，直接排水（冷页首次修改可能触发整页镜像）。

## A：先 CHECKPOINT 再排水（稳态页）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 328,382 | 164 |
| WAL 记录 | 2,457 | 1.2 |
| WAL FPI | 0 | 0.00 |
| WAL write | 58 | 0.029 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| E11B52EF572B8535 | 1 | 201.3 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| C5FA397E315F9D09 | 40 | 32.1 | 2,000 | 0 | 357 | 5,319 | 0 | 719,936 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_36c2686bbcdd"."o…` |
| 26CAC6B03022E745 | 40 | 16.8 | 2,000 | 0 | 0 | 2,305 | 0 | 132,165 | `DELETE FROM "perf_36c2686bbcdd"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| B71A600DFCB91748 | 1 | 0.4 | 233 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 403F8B358778114D | 83 | 0.4 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 83 | 0.3 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 83 | 0.2 | 83 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 265A210A7402B089 | 1 | 0.2 | 29 | 0 | 13 | 3 | 0 | 181 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| D28E47E4803A0167 | 83 | 0.1 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| C0D0E27048376284 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 92049CC8AB443DC6 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 516E8F1759460B47 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| C5FA397E315F9D09 | 40 | 719,936 | 5,319 | 0 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_36c2686bbcdd"."o…` |
| 26CAC6B03022E745 | 40 | 132,165 | 2,305 | 0 | `DELETE FROM "perf_36c2686bbcdd"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| 265A210A7402B089 | 1 | 181 | 3 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| E11B52EF572B8535 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| B71A600DFCB91748 | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 403F8B358778114D | 83 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 83 | 0 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 83 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| D28E47E4803A0167 | 83 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| C0D0E27048376284 | 83 | 0 | 0 | 0 | `UNLISTEN *` |
| 92049CC8AB443DC6 | 83 | 0 | 0 | 0 | `CLOSE ALL` |
| DA559F39F26D405A | 1 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 516E8F1759460B47 | 83 | 0 | 0 | 0 | `DISCARD TEMP` |

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
| outbox | 0 | 2,000 | 2,000 | 1,748 | 2,282 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |

> HOT 命中率（hot_updates / updates）：outbox 1,748/2,000（87%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。


## B：直接排水（容器冷页）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 689,418 | 345 |
| WAL 记录 | 4,916 | 2.5 |
| WAL FPI | 0 | 0.00 |
| WAL write | 58 | 0.029 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| E11B52EF572B8535 | 1 | 201.4 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| C5FA397E315F9D09 | 40 | 42.3 | 2,000 | 0 | 44 | 5,314 | 0 | 719,461 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_36c2686bbcdd"."o…` |
| 26CAC6B03022E745 | 40 | 27.1 | 2,000 | 0 | 0 | 2,304 | 0 | 132,106 | `DELETE FROM "perf_36c2686bbcdd"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| 403F8B358778114D | 83 | 0.4 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| B71A600DFCB91748 | 1 | 0.4 | 237 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 4040643821AD66AF | 83 | 0.3 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 83 | 0.3 | 83 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 265A210A7402B089 | 1 | 0.2 | 29 | 0 | 0 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| D28E47E4803A0167 | 83 | 0.1 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 516E8F1759460B47 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| C5FA397E315F9D09 | 40 | 719,461 | 5,314 | 0 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_36c2686bbcdd"."o…` |
| 26CAC6B03022E745 | 40 | 132,106 | 2,304 | 0 | `DELETE FROM "perf_36c2686bbcdd"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| E11B52EF572B8535 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 403F8B358778114D | 83 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| B71A600DFCB91748 | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 4040643821AD66AF | 83 | 0 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 83 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 265A210A7402B089 | 1 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| D28E47E4803A0167 | 83 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 83 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 83 | 0 | 0 | 0 | `UNLISTEN *` |
| DA559F39F26D405A | 1 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 516E8F1759460B47 | 83 | 0 | 0 | 0 | `DISCARD TEMP` |

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
| outbox | 0 | 2,000 | 2,000 | 1,748 | 2,283 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |

> HOT 命中率（hot_updates / updates）：outbox 1,748/2,000（87%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。

