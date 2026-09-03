# OUTBOX-DB-1 fillfactor A/B（delete-on-complete 排水，HOT 命中率）

> 生成时间：2026-09-02 16:06:19 UTC；语料固定、随机种子 20260813；每配置 inbound 2000 条消息、delete-on-complete 排水 2000 条；两窗口前均 TRUNCATE outbox 从空表起始。

| 配置 | claim WAL/消息 | claim WAL 记录/消息 | complete(删除) WAL/消息 | 排水合计 WAL/消息 | outbox HOT 命中率 |
|---|---|---|---|---|---|
| fillfactor=75（A） | 1,575 | 5.6 | 62 | 1,637 | 30% |
| fillfactor=50（B） | 1,022 | 2.7 | 66 | 1,088 | 87% |

> 以 pg_stat_statements 语句级 wal_bytes/wal_records 归因，A/B 同容器顺序运行、仅 fillfactor 不同；
> HOT 命中率 = hot_updates / updates（排水窗口内 outbox 表），按页填充率模型 HOT ≈ (100 − fillfactor) / fillfactor；
> fillfactor 只影响改变之后新插入行所在的页，先 ALTER 再 TRUNCATE 再插入，保证 A/B 各窗口页布局符合其 fillfactor。

## A：fillfactor=75（delete-on-complete 排水）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 0 | 0 |
| WAL 记录 | 0 | 0.0 |
| WAL FPI | 0 | 0.00 |
| WAL write | 0 | 0.000 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 1F1A5B390EB81DF3 | 1 | 210.4 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| B7BAE5270C67B224 | 40 | 36.7 | 2,000 | 0 | 387 | 11,186 | 222 | 3,151,317 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_acba3a875a53"."o…` |
| 13FE8940C25F27BC | 1 | 26.3 | 4,014 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 20D1B34784BA43F4 | 40 | 11.8 | 2,000 | 0 | 0 | 2,200 | 0 | 124,400 | `DELETE FROM "perf_acba3a875a53"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| 7FD7FA9679F7127A | 1 | 1.8 | 29 | 0 | 5 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 9DFE0023C220BBE7 | 83 | 0.3 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 20928A0A690DF580 | 83 | 0.2 | 83 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 83 | 0.1 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 763D4E8E0DC293DC | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| 8464F5314FC1C676 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| F9011CF57CBB30AB | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 881865BABB8509BA | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| 73FA2B7FF171D0F7 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| B7BAE5270C67B224 | 40 | 3,151,317 | 11,186 | 222 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_acba3a875a53"."o…` |
| 20D1B34784BA43F4 | 40 | 124,400 | 2,200 | 0 | `DELETE FROM "perf_acba3a875a53"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| 1F1A5B390EB81DF3 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 13FE8940C25F27BC | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 7FD7FA9679F7127A | 1 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
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
| outbox | 0 | 2,000 | 2,000 | 600 | 3,400 |
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

> HOT 命中率（hot_updates / updates）：outbox 600/2,000（30%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。


## B：fillfactor=50（delete-on-complete 排水）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 1,304,082 | 652 |
| WAL 记录 | 9,963 | 5.0 |
| WAL FPI | 105 | 0.05 |
| WAL write | 322 | 0.161 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 1F1A5B390EB81DF3 | 1 | 205.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| B7BAE5270C67B224 | 40 | 29.1 | 2,000 | 0 | 354 | 5,313 | 308 | 2,045,457 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_acba3a875a53"."o…` |
| 13FE8940C25F27BC | 1 | 26.7 | 4,014 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 20D1B34784BA43F4 | 40 | 15.7 | 2,000 | 0 | 0 | 2,304 | 0 | 132,288 | `DELETE FROM "perf_acba3a875a53"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
| 7FD7FA9679F7127A | 1 | 1.6 | 29 | 0 | 0 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 9DFE0023C220BBE7 | 83 | 0.3 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 20928A0A690DF580 | 83 | 0.2 | 83 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 83 | 0.2 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 763D4E8E0DC293DC | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| 8464F5314FC1C676 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 881865BABB8509BA | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |
| F9011CF57CBB30AB | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 83 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| B7BAE5270C67B224 | 40 | 2,045,457 | 5,313 | 308 | `WITH candidates AS MATERIALIZED (     SELECT item.event_id     FROM "perf_acba3a875a53"."o…` |
| 20D1B34784BA43F4 | 40 | 132,288 | 2,304 | 0 | `DELETE FROM "perf_acba3a875a53"."outbox" AS item USING UNNEST($1, $2) AS arr(event_id, cla…` |
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

