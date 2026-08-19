# OUTBOX-DB-1 fillfactor A/B（delete-on-complete 排水，HOT 命中率）

> 生成时间：2026-08-19 06:39:01 UTC；语料固定、随机种子 20260813；每配置 inbound 2000 条消息、delete-on-complete 排水 2000 条；两窗口前均 TRUNCATE outbox 从空表起始。

| 配置 | claim WAL/消息 | claim WAL 记录/消息 | complete(删除) WAL/消息 | 排水合计 WAL/消息 | outbox HOT 命中率 |
|---|---|---|---|---|---|
| fillfactor=75（A） | 917 | 5.6 | 62 | 979 | 30% |
| fillfactor=50（B） | 359 | 2.7 | 66 | 425 | 87% |

> 以 pg_stat_statements 语句级 wal_bytes/wal_records 归因，A/B 同容器顺序运行、仅 fillfactor 不同；
> HOT 命中率 = hot_updates / updates（排水窗口内 outbox 表），按页填充率模型 HOT ≈ (100 − fillfactor) / fillfactor；
> fillfactor 只影响改变之后新插入行所在的页，先 ALTER 再 TRUNCATE 再插入，保证 A/B 各窗口页布局符合其 fillfactor。

## A：fillfactor=75（delete-on-complete 排水）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 65,482 | 33 |
| WAL 记录 | 484 | 0.2 |
| WAL FPI | 0 | 0.00 |
| WAL write | 61 | 0.030 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| E11B52EF572B8535 | 1 | 201.3 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| C5FA397E315F9D09 | 40 | 46.6 | 2,000 | 0 | 387 | 11,263 | 0 | 1,834,676 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_35424215d91c"."o…` |
| 26CAC6B03022E745 | 40 | 18.6 | 2,000 | 0 | 0 | 2,200 | 0 | 124,200 | `DELETE FROM "perf_35424215d91c"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| 403F8B358778114D | 83 | 0.5 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| B71A600DFCB91748 | 1 | 0.4 | 234 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 4040643821AD66AF | 83 | 0.3 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 83 | 0.2 | 83 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 265A210A7402B089 | 1 | 0.2 | 29 | 0 | 12 | 3 | 0 | 181 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| D28E47E4803A0167 | 83 | 0.1 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 516E8F1759460B47 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| C5FA397E315F9D09 | 40 | 1,834,676 | 11,263 | 0 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_35424215d91c"."o…` |
| 26CAC6B03022E745 | 40 | 124,200 | 2,200 | 0 | `DELETE FROM "perf_35424215d91c"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| 265A210A7402B089 | 1 | 181 | 3 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| E11B52EF572B8535 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 403F8B358778114D | 83 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| B71A600DFCB91748 | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 4040643821AD66AF | 83 | 0 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 83 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| D28E47E4803A0167 | 83 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 83 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 83 | 0 | 0 | 0 | `UNLISTEN *` |
| 516E8F1759460B47 | 83 | 0 | 0 | 0 | `DISCARD TEMP` |
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
| outbox | 0 | 2,000 | 2,000 | 600 | 3,400 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |

> HOT 命中率（hot_updates / updates）：outbox 600/2,000（30%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。


## B：fillfactor=50（delete-on-complete 排水）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 228,161 | 114 |
| WAL 记录 | 1,630 | 0.8 |
| WAL FPI | 0 | 0.00 |
| WAL write | 57 | 0.029 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| E11B52EF572B8535 | 1 | 201.3 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| C5FA397E315F9D09 | 40 | 33.8 | 2,000 | 0 | 354 | 5,313 | 0 | 718,853 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_35424215d91c"."o…` |
| 26CAC6B03022E745 | 40 | 20.8 | 2,000 | 0 | 0 | 2,304 | 0 | 132,088 | `DELETE FROM "perf_35424215d91c"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| B71A600DFCB91748 | 1 | 0.6 | 238 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 403F8B358778114D | 83 | 0.5 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 83 | 0.3 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 83 | 0.2 | 83 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 265A210A7402B089 | 1 | 0.2 | 29 | 0 | 0 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| D28E47E4803A0167 | 83 | 0.1 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| C0D0E27048376284 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 92049CC8AB443DC6 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 516E8F1759460B47 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| C5FA397E315F9D09 | 40 | 718,853 | 5,313 | 0 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_35424215d91c"."o…` |
| 26CAC6B03022E745 | 40 | 132,088 | 2,304 | 0 | `DELETE FROM "perf_35424215d91c"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| E11B52EF572B8535 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| B71A600DFCB91748 | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 403F8B358778114D | 83 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 83 | 0 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 83 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 265A210A7402B089 | 1 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
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
| outbox | 0 | 2,000 | 2,000 | 1,749 | 2,284 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |

> HOT 命中率（hot_updates / updates）：outbox 1,749/2,000（87%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。

