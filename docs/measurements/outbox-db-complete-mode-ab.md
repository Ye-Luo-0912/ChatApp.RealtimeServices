# OUTBOX-DB-1 完成模式 A/B（排水完整生命周期 WAL）

> 生成时间：2026-09-02 17:26:15 UTC；语料固定、随机种子 20260813；每配置 inbound 2000 条消息、排水 2000 条。

| 配置 | claim WAL/消息 | complete WAL/消息 | 后续 cleanup WAL/消息 | 排水生命周期合计 WAL/消息 |
|---|---|---|---|---|
| 保留（A：claim+MarkPublished+cleanup） | 373 | 597 | 55 | 1,025 |
| 即删（B：claim+DeleteClaimed，生产默认） | 360 | 66 | — | 426 |

> 以 pg_stat_statements 语句级 wal_bytes / rows 归因，A/B 同容器顺序运行、仅完成模式不同；
> A 为保留模式完整生命周期（claim + MarkPublished + 全量 cleanup，清理清空 Published 行）；
> B 为生产默认 delete-on-complete（claim + DeleteClaimedPublished），无中间 Published 态、无 cleanup。

## A：保留模式（claim + MarkPublished）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 367,708 | 184 |
| WAL 记录 | 2,738 | 1.4 |
| WAL FPI | 0 | 0.00 |
| WAL write | 136 | 0.068 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 1F1A5B390EB81DF3 | 1 | 207.4 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 22B5E9584E26118C | 40 | 25.8 | 2,000 | 0 | 46 | 5,641 | 0 | 747,539 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_47ae898ffd34"."o…` |
| 52B7BAB5408781B3 | 40 | 25.2 | 2,000 | 0 | 26 | 10,829 | 0 | 1,195,453 | `UPDATE "perf_47ae898ffd34"."outbox" AS item SET published_at_ms = $1, status = $4, locked_…` |
| 13FE8940C25F27BC | 1 | 19.6 | 4,395 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 7FD7FA9679F7127A | 1 | 6.4 | 29 | 0 | 0 | 2 | 0 | 118 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 9DFE0023C220BBE7 | 83 | 0.2 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 20928A0A690DF580 | 83 | 0.1 | 83 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 83 | 0.1 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 763D4E8E0DC293DC | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| 8464F5314FC1C676 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| F9011CF57CBB30AB | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 881865BABB8509BA | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 73FA2B7FF171D0F7 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| 52B7BAB5408781B3 | 40 | 1,195,453 | 10,829 | 0 | `UPDATE "perf_47ae898ffd34"."outbox" AS item SET published_at_ms = $1, status = $4, locked_…` |
| 22B5E9584E26118C | 40 | 747,539 | 5,641 | 0 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_47ae898ffd34"."o…` |
| 7FD7FA9679F7127A | 1 | 118 | 2 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 1F1A5B390EB81DF3 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 13FE8940C25F27BC | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 9DFE0023C220BBE7 | 83 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 20928A0A690DF580 | 83 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 83 | 0 | 0 | 0 | `RESET ALL` |
| 763D4E8E0DC293DC | 83 | 0 | 0 | 0 | `CLOSE ALL` |
| 8464F5314FC1C676 | 83 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| F9011CF57CBB30AB | 83 | 0 | 0 | 0 | `UNLISTEN *` |
| 881865BABB8509BA | 1 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 73FA2B7FF171D0F7 | 83 | 0 | 0 | 0 | `DISCARD TEMP` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| schema_migrations | 0 | 0 | 0 | 0 | 0 |
| schema_migration_checkpoints | 0 | 0 | 0 | 0 | 0 |
| messages | 0 | 0 | 0 | 0 | 0 |
| outbox | 0 | 4,000 | 0 | 1,748 | 2,252 |
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

> HOT 命中率（hot_updates / updates）：outbox 1,748/4,000（44%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。


## A：后续 cleanup（删除 Published 行）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 1,945,830 | 973 |
| WAL 记录 | 16,552 | 8.3 |
| WAL FPI | 0 | 0.00 |
| WAL write | 82 | 0.041 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 1F1A5B390EB81DF3 | 1 | 207.2 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 13FE8940C25F27BC | 1 | 34.3 | 4,396 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 7FD7FA9679F7127A | 1 | 6.3 | 29 | 0 | 0 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| BC2454792391E55A | 2 | 5.8 | 2,000 | 0 | 0 | 2,042 | 0 | 110,934 | `DELETE FROM "perf_47ae898ffd34"."outbox" WHERE ctid IN (     SELECT ctid     FROM "perf_47…` |
| 9DFE0023C220BBE7 | 5 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 881865BABB8509BA | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 20928A0A690DF580 | 5 | 0.0 | 5 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 5 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 8464F5314FC1C676 | 5 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 763D4E8E0DC293DC | 5 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| F9011CF57CBB30AB | 5 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 5 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| BC2454792391E55A | 2 | 110,934 | 2,042 | 0 | `DELETE FROM "perf_47ae898ffd34"."outbox" WHERE ctid IN (     SELECT ctid     FROM "perf_47…` |
| 1F1A5B390EB81DF3 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 13FE8940C25F27BC | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 7FD7FA9679F7127A | 1 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 9DFE0023C220BBE7 | 5 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 881865BABB8509BA | 1 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 20928A0A690DF580 | 5 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 5 | 0 | 0 | 0 | `RESET ALL` |
| 8464F5314FC1C676 | 5 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 763D4E8E0DC293DC | 5 | 0 | 0 | 0 | `CLOSE ALL` |
| F9011CF57CBB30AB | 5 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 5 | 0 | 0 | 0 | `DISCARD TEMP` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| schema_migrations | 0 | 0 | 0 | 0 | 0 |
| schema_migration_checkpoints | 0 | 0 | 0 | 0 | 0 |
| messages | 0 | 0 | 0 | 0 | 0 |
| outbox | 0 | 0 | 2,000 | 0 | 2,000 |
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


## B：delete-on-complete（claim + DeleteClaimedPublished）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 2,038,040 | 1,019 |
| WAL 记录 | 14,535 | 7.3 |
| WAL FPI | 0 | 0.00 |
| WAL write | 721 | 0.360 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 1F1A5B390EB81DF3 | 1 | 205.2 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 13FE8940C25F27BC | 1 | 22.9 | 4,397 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 22B5E9584E26118C | 40 | 21.4 | 2,000 | 0 | 43 | 5,311 | 0 | 720,572 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_47ae898ffd34"."o…` |
| 44236674C8A59426 | 40 | 10.1 | 2,000 | 0 | 0 | 2,304 | 0 | 132,288 | `DELETE FROM "perf_47ae898ffd34"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| 7FD7FA9679F7127A | 1 | 6.7 | 29 | 0 | 0 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
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
| 22B5E9584E26118C | 40 | 720,572 | 5,311 | 0 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_47ae898ffd34"."o…` |
| 44236674C8A59426 | 40 | 132,288 | 2,304 | 0 | `DELETE FROM "perf_47ae898ffd34"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
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
| outbox | 0 | 2,000 | 2,000 | 1,749 | 2,284 |
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

> HOT 命中率（hot_updates / updates）：outbox 1,749/2,000（87%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。

