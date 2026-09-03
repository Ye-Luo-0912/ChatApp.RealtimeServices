# OUTBOX-DB-1 索引 A/B（messages 插入写放大）

> 生成时间：2026-09-02 17:26:07 UTC；语料固定、随机种子 20260813；每配置 inbound 2000 条消息，其中约 1/4 携带 reply_to_*+forwarded_from_* 引用；两窗口前均 TRUNCATE messages 从空表起始。

| 配置 | 全局 WAL 字节/消息 | INSERT messages WAL 字节/消息 | INSERT messages WAL 记录/消息 |
|---|---|---|---|
| 索引存在（A） | 2,362 | 2,332 | 15 |
| 索引剔除（B） | 1,594 | 2,333 | 15 |

> 以 pg_stat_statements 语句级 wal_bytes / wal_records 归因，A/B 同容器顺序运行，
> 仅 `ix_messages_reply_to` / `ix_messages_forwarded_from` 两个部分索引的存在性不同；
> INSERT messages 归因匹配 `INSERT INTO ... "messages"` 语句并按 rows 折算。

## A：索引存在（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 4,725,115 | 2,363 |
| WAL 记录 | 34,139 | 17.1 |
| WAL FPI | 147 | 0.07 |
| WAL write | 1,439 | 0.720 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 1F1A5B390EB81DF3 | 1 | 201.9 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| E67FD6367E19C617 | 2,000 | 160.7 | 2,000 | 7 | 551 | 30,208 | 0 | 4,665,516 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_153cb40459a9"."messages" (  …` |
| 1257F0B7ADC798A0 | 2,000 | 113.4 | 2,000 | 0 | 0 | 8,091 | 0 | 705,454 | `WITH upsert_conversation AS (     INSERT INTO "perf_153cb40459a9"."conversations" (       …` |
| 71340D71A9F8F4F | 2,000 | 45.4 | 4,000 | 0 | 0 | 0 | 0 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM UNNEST($1) AS …` |
| 13FE8940C25F27BC | 1 | 19.6 | 4,461 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 9DFE0023C220BBE7 | 2,003 | 5.5 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 7FD7FA9679F7127A | 1 | 5.5 | 29 | 0 | 0 | 13 | 0 | 756 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| D6C944F452669EEE | 2,000 | 3.5 | 0 | 0 | 0 | 0 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 20928A0A690DF580 | 2,003 | 3.3 | 2,003 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 2,003 | 2.8 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| D8A6A35AB613B78D | 2,000 | 0.8 | 0 | 0 | 0 | 0 | 0 | 0 | `COMMIT` |
| 763D4E8E0DC293DC | 2,003 | 0.5 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| 8464F5314FC1C676 | 2,003 | 0.5 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| F9011CF57CBB30AB | 2,003 | 0.3 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 2,003 | 0.2 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |
| 881865BABB8509BA | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| E67FD6367E19C617 | 2,000 | 4,665,516 | 30,208 | 0 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_153cb40459a9"."messages" (  …` |
| 1257F0B7ADC798A0 | 2,000 | 705,454 | 8,091 | 0 | `WITH upsert_conversation AS (     INSERT INTO "perf_153cb40459a9"."conversations" (       …` |
| 7FD7FA9679F7127A | 1 | 756 | 13 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 1F1A5B390EB81DF3 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 71340D71A9F8F4F | 2,000 | 0 | 0 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM UNNEST($1) AS …` |
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


## B：索引剔除（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 3,188,846 | 1,594 |
| WAL 记录 | 23,692 | 11.8 |
| WAL FPI | 9 | 0.00 |
| WAL write | 1,174 | 0.587 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 1F1A5B390EB81DF3 | 1 | 215.8 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| E67FD6367E19C617 | 2,000 | 162.4 | 2,000 | 7 | 552 | 30,213 | 0 | 4,666,048 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_153cb40459a9"."messages" (  …` |
| 1257F0B7ADC798A0 | 2,000 | 117.5 | 2,000 | 0 | 0 | 8,091 | 0 | 705,454 | `WITH upsert_conversation AS (     INSERT INTO "perf_153cb40459a9"."conversations" (       …` |
| 71340D71A9F8F4F | 2,000 | 47.2 | 4,000 | 0 | 0 | 0 | 0 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM UNNEST($1) AS …` |
| 13FE8940C25F27BC | 1 | 19.9 | 4,463 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 7FD7FA9679F7127A | 1 | 6.3 | 29 | 0 | 0 | 6 | 0 | 346 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 9DFE0023C220BBE7 | 2,003 | 5.7 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| D6C944F452669EEE | 2,000 | 3.9 | 0 | 0 | 0 | 0 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 20928A0A690DF580 | 2,003 | 3.4 | 2,003 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| B9A3FC5813DDA531 | 2,003 | 2.9 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| D8A6A35AB613B78D | 2,000 | 0.8 | 0 | 0 | 0 | 0 | 0 | 0 | `COMMIT` |
| 763D4E8E0DC293DC | 2,003 | 0.6 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| 8464F5314FC1C676 | 2,003 | 0.5 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| F9011CF57CBB30AB | 2,003 | 0.3 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 73FA2B7FF171D0F7 | 2,003 | 0.2 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |
| 881865BABB8509BA | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| E67FD6367E19C617 | 2,000 | 4,666,048 | 30,213 | 0 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_153cb40459a9"."messages" (  …` |
| 1257F0B7ADC798A0 | 2,000 | 705,454 | 8,091 | 0 | `WITH upsert_conversation AS (     INSERT INTO "perf_153cb40459a9"."conversations" (       …` |
| 7FD7FA9679F7127A | 1 | 346 | 6 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 1F1A5B390EB81DF3 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 71340D71A9F8F4F | 2,000 | 0 | 0 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM UNNEST($1) AS …` |
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

