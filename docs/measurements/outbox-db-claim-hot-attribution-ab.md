# OUTBOX-DB-1 排水 HOT 更细归因（claim-only vs claim+delete）

> 生成时间：2026-09-02 16:06:36 UTC；语料固定、随机种子 20260813；两配置均 fillfactor=50、每配置 inbound 2000 条消息；A 只 claim（不 delete）、B claim+delete（delete-on-complete）；两窗口前均 TRUNCATE outbox 从空表起始。

| 配置 | claim 消息数 | claim WAL/消息 | claim WAL 记录/消息 | outbox updates | outbox hot_updates | HOT 命中率 | outbox dead tuples |
|---|---|---|---|---|---|---|---|
| A：claim-only（无 delete） | 2,000 | 1,043 | 2.8 | 2,000 | 1,723 | 86% | 450 |
| B：claim+delete（生产默认） | 2,000 | 1,033 | 2.7 | 2,000 | 1,723 | 86% | 2,286 |

> 以 pg_stat_statements 语句级 wal_bytes/wal_records 归因 claim，A/B 同容器顺序运行、仅差是否 delete；
> HOT 命中率 = hot_updates / updates（排水窗口内 outbox 表）；delete-on-complete 排水里唯一的 UPDATE 是 claim（
> locked_by/claim_token/locked_until_ms/attempt_count 全为非索引列，理论上可 HOT），complete 是 DELETE（无 HOT 概念）；
> 若 A 的 claim HOT 命中率显著高于 B 且 B 的 dead tuple 更多，则 residual（HOT<100%）来自 delete 生成的
> dead tuple 挤压 claim 的页内版本链空间（页填充率限制），而非索引列改写。

## A：claim-only（无 delete，隔离 claim HOT 上限）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 287,476 | 144 |
| WAL 记录 | 2,156 | 1.1 |
| WAL FPI | 0 | 0.00 |
| WAL write | 108 | 0.054 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 1F1A5B390EB81DF3 | 1 | 213.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 13FE8940C25F27BC | 1 | 51.1 | 3,934 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 23F83703EC45ABDF | 10 | 19.2 | 2,000 | 0 | 358 | 5,677 | 308 | 2,087,192 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_fb342889857c"."o…` |
| 7FD7FA9679F7127A | 1 | 1.3 | 29 | 0 | 20 | 11 | 11 | 86,255 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 9DFE0023C220BBE7 | 13 | 0.1 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| B9A3FC5813DDA531 | 13 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 20928A0A690DF580 | 13 | 0.0 | 13 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 881865BABB8509BA | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 8464F5314FC1C676 | 13 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 763D4E8E0DC293DC | 13 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| F9011CF57CBB30AB | 13 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 13 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| 23F83703EC45ABDF | 10 | 2,087,192 | 5,677 | 308 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_fb342889857c"."o…` |
| 7FD7FA9679F7127A | 1 | 86,255 | 11 | 11 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 1F1A5B390EB81DF3 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 13FE8940C25F27BC | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 9DFE0023C220BBE7 | 13 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| B9A3FC5813DDA531 | 13 | 0 | 0 | 0 | `RESET ALL` |
| 20928A0A690DF580 | 13 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 881865BABB8509BA | 1 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 8464F5314FC1C676 | 13 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 763D4E8E0DC293DC | 13 | 0 | 0 | 0 | `CLOSE ALL` |
| F9011CF57CBB30AB | 13 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 13 | 0 | 0 | 0 | `DISCARD TEMP` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| schema_migrations | 0 | 0 | 0 | 0 | 0 |
| schema_migration_checkpoints | 0 | 0 | 0 | 0 | 0 |
| messages | 0 | 0 | 0 | 0 | 0 |
| outbox | 0 | 2,000 | 0 | 1,723 | 450 |
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

> HOT 命中率（hot_updates / updates）：outbox 1,723/2,000（86%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。


## B：claim+delete（delete-on-complete 生产默认形态）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 359,210 | 180 |
| WAL 记录 | 2,519 | 1.3 |
| WAL FPI | 2 | 0.00 |
| WAL write | 126 | 0.063 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 1F1A5B390EB81DF3 | 1 | 210.2 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 13FE8940C25F27BC | 1 | 45.3 | 3,934 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 23F83703EC45ABDF | 10 | 16.0 | 2,000 | 0 | 358 | 5,422 | 308 | 2,066,784 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_fb342889857c"."o…` |
| 3D7EA8433DD4A480 | 10 | 6.0 | 2,000 | 0 | 0 | 2,290 | 0 | 131,628 | `DELETE FROM "perf_fb342889857c"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| 7FD7FA9679F7127A | 1 | 1.3 | 29 | 0 | 2 | 2 | 2 | 15,926 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 9DFE0023C220BBE7 | 23 | 0.1 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| B9A3FC5813DDA531 | 23 | 0.1 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 20928A0A690DF580 | 23 | 0.1 | 23 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 881865BABB8509BA | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 763D4E8E0DC293DC | 23 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| 8464F5314FC1C676 | 23 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| F9011CF57CBB30AB | 23 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 23 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| 23F83703EC45ABDF | 10 | 2,066,784 | 5,422 | 308 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_fb342889857c"."o…` |
| 3D7EA8433DD4A480 | 10 | 131,628 | 2,290 | 0 | `DELETE FROM "perf_fb342889857c"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| 7FD7FA9679F7127A | 1 | 15,926 | 2 | 2 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 1F1A5B390EB81DF3 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 13FE8940C25F27BC | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 9DFE0023C220BBE7 | 23 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| B9A3FC5813DDA531 | 23 | 0 | 0 | 0 | `RESET ALL` |
| 20928A0A690DF580 | 23 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 881865BABB8509BA | 1 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 763D4E8E0DC293DC | 23 | 0 | 0 | 0 | `CLOSE ALL` |
| 8464F5314FC1C676 | 23 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| F9011CF57CBB30AB | 23 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 23 | 0 | 0 | 0 | `DISCARD TEMP` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| schema_migrations | 0 | 0 | 0 | 0 | 0 |
| schema_migration_checkpoints | 0 | 0 | 0 | 0 | 0 |
| messages | 0 | 0 | 0 | 0 | 0 |
| outbox | 0 | 2,000 | 2,000 | 1,723 | 2,286 |
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

> HOT 命中率（hot_updates / updates）：outbox 1,723/2,000（86%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。

