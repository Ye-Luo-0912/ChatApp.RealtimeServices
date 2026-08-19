# OUTBOX-DB-1 排水 HOT 更细归因（claim-only vs claim+delete）

> 生成时间：2026-08-19 06:39:33 UTC；语料固定、随机种子 20260813；两配置均 fillfactor=50、每配置 inbound 2000 条消息；A 只 claim（不 delete）、B claim+delete（delete-on-complete）；两窗口前均 TRUNCATE outbox 从空表起始。

| 配置 | claim 消息数 | claim WAL/消息 | claim WAL 记录/消息 | outbox updates | outbox hot_updates | HOT 命中率 | outbox dead tuples |
|---|---|---|---|---|---|---|---|
| A：claim-only（无 delete） | 2,000 | 381 | 2.8 | 2,000 | 1,723 | 86% | 450 |
| B：claim+delete（生产默认） | 2,000 | 371 | 2.7 | 2,000 | 1,723 | 86% | 2,286 |

> 以 pg_stat_statements 语句级 wal_bytes/wal_records 归因 claim，A/B 同容器顺序运行、仅差是否 delete；
> HOT 命中率 = hot_updates / updates（排水窗口内 outbox 表）；delete-on-complete 排水里唯一的 UPDATE 是 claim（
> locked_by/claim_token/locked_until_ms/attempt_count 全为非索引列，理论上可 HOT），complete 是 DELETE（无 HOT 概念）；
> 若 A 的 claim HOT 命中率显著高于 B 且 B 的 dead tuple 更多，则 residual（HOT<100%）来自 delete 生成的
> dead tuple 挤压 claim 的页内版本链空间（页填充率限制），而非索引列改写。

## A：claim-only（无 delete，隔离 claim HOT 上限）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 845,843 | 423 |
| WAL 记录 | 6,323 | 3.2 |
| WAL FPI | 0 | 0.00 |
| WAL write | 11 | 0.005 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| E11B52EF572B8535 | 1 | 201.4 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| C5FA397E315F9D09 | 10 | 22.8 | 2,000 | 0 | 358 | 5,675 | 0 | 763,341 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_fd7ef7543cf1"."o…` |
| B71A600DFCB91748 | 1 | 0.6 | 234 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 265A210A7402B089 | 1 | 0.4 | 29 | 0 | 9 | 3 | 0 | 181 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 403F8B358778114D | 13 | 0.1 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 13 | 0.1 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 13 | 0.1 | 13 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| D28E47E4803A0167 | 13 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 13 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 13 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 516E8F1759460B47 | 13 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| C5FA397E315F9D09 | 10 | 763,341 | 5,675 | 0 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_fd7ef7543cf1"."o…` |
| 265A210A7402B089 | 1 | 181 | 3 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| E11B52EF572B8535 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| B71A600DFCB91748 | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 403F8B358778114D | 13 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 13 | 0 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 13 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| DA559F39F26D405A | 1 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| D28E47E4803A0167 | 13 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 13 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 13 | 0 | 0 | 0 | `UNLISTEN *` |
| 516E8F1759460B47 | 13 | 0 | 0 | 0 | `DISCARD TEMP` |

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
| outbox | 0 | 2,000 | 0 | 1,723 | 450 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |

> HOT 命中率（hot_updates / updates）：outbox 1,723/2,000（86%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。


## B：claim+delete（delete-on-complete 生产默认形态）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 771,562 | 386 |
| WAL 记录 | 5,540 | 2.8 |
| WAL FPI | 0 | 0.00 |
| WAL write | 20 | 0.010 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| E11B52EF572B8535 | 1 | 201.3 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| C5FA397E315F9D09 | 10 | 19.8 | 2,000 | 0 | 358 | 5,425 | 0 | 742,674 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_fd7ef7543cf1"."o…` |
| 26CAC6B03022E745 | 10 | 7.1 | 2,000 | 0 | 0 | 2,290 | 0 | 131,362 | `DELETE FROM "perf_fd7ef7543cf1"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| B71A600DFCB91748 | 1 | 0.4 | 238 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 403F8B358778114D | 23 | 0.2 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 265A210A7402B089 | 1 | 0.2 | 29 | 0 | 0 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 4040643821AD66AF | 23 | 0.1 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 23 | 0.1 | 23 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| D28E47E4803A0167 | 23 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 92049CC8AB443DC6 | 23 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 23 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 516E8F1759460B47 | 23 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| C5FA397E315F9D09 | 10 | 742,674 | 5,425 | 0 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_fd7ef7543cf1"."o…` |
| 26CAC6B03022E745 | 10 | 131,362 | 2,290 | 0 | `DELETE FROM "perf_fd7ef7543cf1"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| E11B52EF572B8535 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| B71A600DFCB91748 | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 403F8B358778114D | 23 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 265A210A7402B089 | 1 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 4040643821AD66AF | 23 | 0 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 23 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| D28E47E4803A0167 | 23 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| DA559F39F26D405A | 1 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 92049CC8AB443DC6 | 23 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 23 | 0 | 0 | 0 | `UNLISTEN *` |
| 516E8F1759460B47 | 23 | 0 | 0 | 0 | `DISCARD TEMP` |

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
| outbox | 0 | 2,000 | 2,000 | 1,723 | 2,286 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |

> HOT 命中率（hot_updates / updates）：outbox 1,723/2,000（86%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。

