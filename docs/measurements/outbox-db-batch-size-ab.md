# OUTBOX-DB-1 有界批量 claim/complete A/B（批量上限对每消息往返/事务开销的影响）

> 生成时间：2026-08-13 22:30:18 UTC；语料固定、随机种子 20260813；delete-on-complete 排水 2000 条。

| 批量上限 | 排水 SQL 往返/消息 | 排水耗时/消息(ms) | 排水 WAL 字节/消息 | claim 调用 | delete 调用 |
|---|---|---|---|---|---|
| A：1（每条消息独立 claim+delete） | 2.00 | 0.48 | 336 | 2000 | 2000 |
| B：200（有界批量，单事务跨度可控） | 0.01 | 0.02 | 438 | 10 | 10 |

> 以 pg_stat_statements 语句级 calls/wal_bytes 归因，A/B 同容器顺序运行、仅 claim/complete 批量上限不同；
> claim/delete 均为单语句（`FOR UPDATE ... SKIP LOCKED ... LIMIT @batch_size` / `DELETE ... USING UNNEST`），
> 批量上限只改变每条消息的平均往返次数与事务提交次数（autocommit 下每语句一个事务）：批量越大、
> 每消息往返与提交开销越低，而上限本身保证单事务/单 claim 跨度有界，积压时不会形成失控大事务。

## A：批量上限 1 的排水语句

| 语句 | 调用 | 调用/消息 | exec/消息(ms) | wal_bytes/调用 | sql 片段 |
|---|---|---|---|---|---|
| 2,000 | 1.00 | 0.443 | 246 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "per…` |
| 2,000 | 1.00 | 0.040 | 89 | `DELETE FROM "perf_921a9e54d42a"."outbox" AS item USING UNNEST($1, $2) AS…` |

## A：批量上限 1（往返最频繁）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 1,004,558 | 502 |
| WAL 记录 | 12,902 | 6.5 |
| WAL FPI | 0 | 0.00 |
| WAL write | 109 | 0.054 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| C5FA397E315F9D09 | 2,000 | 885.7 | 2,000 | 0 | 0 | 4,285 | 0 | 492,815 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_921a9e54d42a"."o…` |
| E11B52EF572B8535 | 1 | 200.3 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 26CAC6B03022E745 | 2,000 | 80.4 | 2,000 | 0 | 0 | 3,142 | 0 | 178,808 | `DELETE FROM "perf_921a9e54d42a"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| 403F8B358778114D | 4,003 | 31.4 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 4,003 | 17.1 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 4,003 | 15.4 | 4,003 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| D28E47E4803A0167 | 4,003 | 3.3 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 4,003 | 2.3 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 4,003 | 2.0 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 516E8F1759460B47 | 4,003 | 0.5 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |
| B71A600DFCB91748 | 1 | 0.5 | 232 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 265A210A7402B089 | 1 | 0.3 | 29 | 0 | 0 | 3 | 0 | 181 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| C5FA397E315F9D09 | 2,000 | 492,815 | 4,285 | 0 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_921a9e54d42a"."o…` |
| 26CAC6B03022E745 | 2,000 | 178,808 | 3,142 | 0 | `DELETE FROM "perf_921a9e54d42a"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| 265A210A7402B089 | 1 | 181 | 3 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| E11B52EF572B8535 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 403F8B358778114D | 4,003 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 4,003 | 0 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 4,003 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| D28E47E4803A0167 | 4,003 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 4,003 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 4,003 | 0 | 0 | 0 | `UNLISTEN *` |
| 516E8F1759460B47 | 4,003 | 0 | 0 | 0 | `DISCARD TEMP` |
| B71A600DFCB91748 | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| DA559F39F26D405A | 1 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

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
| outbox | 0 | 2,000 | 2,000 | 2,000 | 2,572 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |

> HOT 命中率（hot_updates / updates）：outbox 2,000/2,000（100%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。


## B：批量上限 200 的排水语句

| 语句 | 调用 | 调用/消息 | exec/消息(ms) | wal_bytes/调用 | sql 片段 |
|---|---|---|---|---|---|
| 10 | 0.01 | 0.013 | 74,424 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "per…` |
| 10 | 0.01 | 0.004 | 13,144 | `DELETE FROM "perf_921a9e54d42a"."outbox" AS item USING UNNEST($1, $2) AS…` |

## B：有界批量上限 200（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 372,299 | 186 |
| WAL 记录 | 2,659 | 1.3 |
| WAL FPI | 0 | 0.00 |
| WAL write | 20 | 0.010 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| E11B52EF572B8535 | 1 | 200.3 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| C5FA397E315F9D09 | 10 | 26.6 | 2,000 | 0 | 48 | 5,424 | 0 | 744,240 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_921a9e54d42a"."o…` |
| 26CAC6B03022E745 | 10 | 7.7 | 2,000 | 0 | 0 | 2,291 | 0 | 131,441 | `DELETE FROM "perf_921a9e54d42a"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| B71A600DFCB91748 | 1 | 0.5 | 236 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 265A210A7402B089 | 1 | 0.3 | 29 | 0 | 0 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 403F8B358778114D | 23 | 0.2 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 878A9E463E585A2C | 23 | 0.1 | 23 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 4040643821AD66AF | 23 | 0.1 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| D28E47E4803A0167 | 23 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| C0D0E27048376284 | 23 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 92049CC8AB443DC6 | 23 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| 516E8F1759460B47 | 23 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| C5FA397E315F9D09 | 10 | 744,240 | 5,424 | 0 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_921a9e54d42a"."o…` |
| 26CAC6B03022E745 | 10 | 131,441 | 2,291 | 0 | `DELETE FROM "perf_921a9e54d42a"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| E11B52EF572B8535 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| B71A600DFCB91748 | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 265A210A7402B089 | 1 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 403F8B358778114D | 23 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 878A9E463E585A2C | 23 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 4040643821AD66AF | 23 | 0 | 0 | 0 | `RESET ALL` |
| D28E47E4803A0167 | 23 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| DA559F39F26D405A | 1 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| C0D0E27048376284 | 23 | 0 | 0 | 0 | `UNLISTEN *` |
| 92049CC8AB443DC6 | 23 | 0 | 0 | 0 | `CLOSE ALL` |
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
| outbox | 0 | 2,000 | 2,000 | 1,722 | 2,282 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |

> HOT 命中率（hot_updates / updates）：outbox 1,722/2,000（86%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。

