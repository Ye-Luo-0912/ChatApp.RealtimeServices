# OUTBOX-DB-1 有界批量 claim/complete A/B（批量上限对每消息往返/事务开销的影响）

> 生成时间：2026-09-02 17:26:22 UTC；语料固定、随机种子 20260813；delete-on-complete 排水 2000 条。

| 批量上限 | 排水 SQL 往返/消息 | 排水耗时/消息(ms) | 排水 WAL 字节/消息 | claim 调用 | delete 调用 |
|---|---|---|---|---|---|
| A：1（每条消息独立 claim+delete） | 2.00 | 0.27 | 337 | 2000 | 2000 |
| B：200（有界批量，单事务跨度可控） | 0.01 | 0.01 | 438 | 10 | 10 |

> 以 pg_stat_statements 语句级 calls/wal_bytes 归因，A/B 同容器顺序运行、仅 claim/complete 批量上限不同；
> claim/delete 均为单语句（`FOR UPDATE ... SKIP LOCKED ... LIMIT @batch_size` / `DELETE ... USING UNNEST`），
> 批量上限只改变每条消息的平均往返次数与事务提交次数（autocommit 下每语句一个事务）：批量越大、
> 每消息往返与提交开销越低，而上限本身保证单事务/单 claim 跨度有界，积压时不会形成失控大事务。

## A：批量上限 1 的排水语句

| 语句 | 调用 | 调用/消息 | exec/消息(ms) | wal_bytes/调用 | sql 片段 |
|---|---|---|---|---|---|
| 2,000 | 1.00 | 0.251 | 246 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "per…` |
| 2,000 | 1.00 | 0.015 | 89 | `DELETE FROM "perf_efa9759c1fa4"."outbox" AS item USING UNNEST($1, $2) AS…` |

## A：批量上限 1（往返最频繁）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 1,097,370 | 549 |
| WAL 记录 | 10,834 | 5.4 |
| WAL FPI | 0 | 0.00 |
| WAL write | 2,211 | 1.105 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 3A4E8E22A044D557 | 2,000 | 503.0 | 2,000 | 0 | 0 | 4,285 | 0 | 493,100 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_efa9759c1fa4"."o…` |
| 1F1A5B390EB81DF3 | 1 | 212.3 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| F542118EEE5F9B21 | 2,000 | 30.7 | 2,000 | 0 | 0 | 3,142 | 0 | 179,952 | `DELETE FROM "perf_efa9759c1fa4"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| 13FE8940C25F27BC | 1 | 19.5 | 4,611 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 9DFE0023C220BBE7 | 4,003 | 7.9 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 7FD7FA9679F7127A | 1 | 6.2 | 29 | 0 | 0 | 1 | 0 | 68 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 20928A0A690DF580 | 4,003 | 5.9 | 4,003 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 4,003 | 3.0 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 763D4E8E0DC293DC | 4,003 | 0.9 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| 8464F5314FC1C676 | 4,003 | 0.7 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| F9011CF57CBB30AB | 4,003 | 0.5 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 4,003 | 0.3 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |
| 881865BABB8509BA | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| 3A4E8E22A044D557 | 2,000 | 493,100 | 4,285 | 0 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_efa9759c1fa4"."o…` |
| F542118EEE5F9B21 | 2,000 | 179,952 | 3,142 | 0 | `DELETE FROM "perf_efa9759c1fa4"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| 7FD7FA9679F7127A | 1 | 68 | 1 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 1F1A5B390EB81DF3 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 13FE8940C25F27BC | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 9DFE0023C220BBE7 | 4,003 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 20928A0A690DF580 | 4,003 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 4,003 | 0 | 0 | 0 | `RESET ALL` |
| 763D4E8E0DC293DC | 4,003 | 0 | 0 | 0 | `CLOSE ALL` |
| 8464F5314FC1C676 | 4,003 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| F9011CF57CBB30AB | 4,003 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 4,003 | 0 | 0 | 0 | `DISCARD TEMP` |
| 881865BABB8509BA | 1 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| schema_migrations | 0 | 0 | 0 | 0 | 0 |
| schema_migration_checkpoints | 0 | 0 | 0 | 0 | 0 |
| messages | 0 | 0 | 0 | 0 | 0 |
| outbox | 0 | 2,000 | 2,000 | 2,000 | 2,572 |
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

> HOT 命中率（hot_updates / updates）：outbox 2,000/2,000（100%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。


## B：批量上限 200 的排水语句

| 语句 | 调用 | 调用/消息 | exec/消息(ms) | wal_bytes/调用 | sql 片段 |
|---|---|---|---|---|---|
| 10 | 0.01 | 0.007 | 74,414 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "per…` |
| 10 | 0.01 | 0.002 | 13,171 | `DELETE FROM "perf_efa9759c1fa4"."outbox" AS item USING UNNEST($1, $2) AS…` |

## B：有界批量上限 200（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 2,083,874 | 1,042 |
| WAL 记录 | 14,938 | 7.5 |
| WAL FPI | 0 | 0.00 |
| WAL write | 742 | 0.371 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 1F1A5B390EB81DF3 | 1 | 200.9 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 13FE8940C25F27BC | 1 | 23.2 | 4,611 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 3A4E8E22A044D557 | 10 | 14.5 | 2,000 | 0 | 48 | 5,423 | 0 | 744,144 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_efa9759c1fa4"."o…` |
| 7FD7FA9679F7127A | 1 | 6.4 | 29 | 0 | 0 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| F542118EEE5F9B21 | 10 | 4.5 | 2,000 | 0 | 0 | 2,291 | 0 | 131,712 | `DELETE FROM "perf_efa9759c1fa4"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| 9DFE0023C220BBE7 | 23 | 0.1 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 20928A0A690DF580 | 23 | 0.0 | 23 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 23 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 881865BABB8509BA | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 8464F5314FC1C676 | 23 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 763D4E8E0DC293DC | 23 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| F9011CF57CBB30AB | 23 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 23 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| 3A4E8E22A044D557 | 10 | 744,144 | 5,423 | 0 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_efa9759c1fa4"."o…` |
| F542118EEE5F9B21 | 10 | 131,712 | 2,291 | 0 | `DELETE FROM "perf_efa9759c1fa4"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| 1F1A5B390EB81DF3 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 13FE8940C25F27BC | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 7FD7FA9679F7127A | 1 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 9DFE0023C220BBE7 | 23 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 20928A0A690DF580 | 23 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 23 | 0 | 0 | 0 | `RESET ALL` |
| 881865BABB8509BA | 1 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 8464F5314FC1C676 | 23 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 763D4E8E0DC293DC | 23 | 0 | 0 | 0 | `CLOSE ALL` |
| F9011CF57CBB30AB | 23 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 23 | 0 | 0 | 0 | `DISCARD TEMP` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| schema_migrations | 0 | 0 | 0 | 0 | 0 |
| schema_migration_checkpoints | 0 | 0 | 0 | 0 | 0 |
| messages | 0 | 0 | 0 | 0 | 0 |
| outbox | 0 | 2,000 | 2,000 | 1,722 | 2,282 |
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

> HOT 命中率（hot_updates / updates）：outbox 1,722/2,000（86%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。

