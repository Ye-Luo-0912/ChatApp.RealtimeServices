# OUTBOX-DB-1 索引 A/B（messages 插入写放大）

> 生成时间：2026-08-13 18:57:50 UTC；语料固定、随机种子 20260813；每配置 inbound 2000 条消息，其中约 1/4 携带 reply_to_*+forwarded_from_* 引用；两窗口前均 TRUNCATE messages 从空表起始。

| 配置 | 全局 WAL 字节/消息 | INSERT messages WAL 字节/消息 | INSERT messages WAL 记录/消息 |
|---|---|---|---|
| 索引存在（A） | 2,937 | 2,331 | 15 |
| 索引剔除（B） | 2,721 | 2,332 | 15 |

> 以 pg_stat_statements 语句级 wal_bytes / wal_records 归因，A/B 同容器顺序运行，
> 仅 `ix_messages_reply_to` / `ix_messages_forwarded_from` 两个部分索引的存在性不同；
> INSERT messages 归因匹配 `INSERT INTO ... "messages"` 语句并按 rows 折算。

## A：索引存在（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 5,875,417 | 2,938 |
| WAL 记录 | 43,267 | 21.6 |
| WAL FPI | 9 | 0.00 |
| WAL write | 671 | 0.336 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|
| 2E2DDA1559AE03AD | 2,000 | 302.1 | 2,000 | 7 | 467 | 30,198 | 4,663,860 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_5e370ce515dc"."messages" (  …` |
| AD398CE96D5C30F8 | 2,000 | 190.1 | 2,000 | 0 | 0 | 8,091 | 705,363 | `WITH upsert_conversation AS (     INSERT INTO "perf_5e370ce515dc"."conversations" (       …` |
| 4D103CE8ABD01E23 | 2,000 | 63.9 | 4,000 | 0 | 0 | 0 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM UNNEST($1) AS …` |
| 403F8B358778114D | 2,003 | 17.1 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 2,003 | 10.7 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| CF1D5941D5B56432 | 2,000 | 8.6 | 0 | 0 | 0 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 878A9E463E585A2C | 2,003 | 8.3 | 2,003 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 1CA7E40EFC47C423 | 2,000 | 2.5 | 0 | 0 | 0 | 0 | 0 | `COMMIT` |
| D28E47E4803A0167 | 2,003 | 2.0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 2,003 | 1.1 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 2,003 | 0.8 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| B71A600DFCB91748 | 1 | 0.4 | 232 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 265A210A7402B089 | 1 | 0.3 | 29 | 0 | 0 | 4 | 246 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 516E8F1759460B47 | 2,003 | 0.3 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | sql |
|---|---|---|---|---|
| 2E2DDA1559AE03AD | 2,000 | 4,663,860 | 30,198 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_5e370ce515dc"."messages" (  …` |
| AD398CE96D5C30F8 | 2,000 | 705,363 | 8,091 | `WITH upsert_conversation AS (     INSERT INTO "perf_5e370ce515dc"."conversations" (       …` |
| 265A210A7402B089 | 1 | 246 | 4 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 4D103CE8ABD01E23 | 2,000 | 0 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM UNNEST($1) AS …` |
| 403F8B358778114D | 2,003 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 2,003 | 0 | 0 | `RESET ALL` |
| CF1D5941D5B56432 | 2,000 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 878A9E463E585A2C | 2,003 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 1CA7E40EFC47C423 | 2,000 | 0 | 0 | `COMMIT` |
| D28E47E4803A0167 | 2,003 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 2,003 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 2,003 | 0 | 0 | `UNLISTEN *` |
| B71A600DFCB91748 | 1 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 516E8F1759460B47 | 2,003 | 0 | 0 | `DISCARD TEMP` |
| DA559F39F26D405A | 1 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| relationship_projection_rebuild_state | 0 | 0 | 0 | 0 | 0 |
| device_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| messages | 2,032 | 0 | 0 | 0 | 0 |
| command_idempotency_ledger | 0 | 0 | 0 | 0 | 0 |
| conversation_members | 0 | 2,032 | 0 | 2,032 | 32 |
| account_cleanup_jobs | 0 | 0 | 0 | 0 | 0 |
| user_deletion_tombstones | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_items | 0 | 0 | 0 | 0 | 0 |
| friendships | 0 | 0 | 0 | 0 | 0 |
| conversations | 0 | 2,032 | 0 | 2,032 | 4 |
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
| outbox | 2,032 | 400 | 0 | 60 | 340 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |


## B：索引剔除（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 5,442,475 | 2,721 |
| WAL 记录 | 40,353 | 20.2 |
| WAL FPI | 9 | 0.00 |
| WAL write | 671 | 0.336 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|
| 2E2DDA1559AE03AD | 2,000 | 302.1 | 2,000 | 7 | 467 | 30,195 | 4,664,272 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_5e370ce515dc"."messages" (  …` |
| AD398CE96D5C30F8 | 2,000 | 196.3 | 2,000 | 0 | 0 | 8,091 | 705,363 | `WITH upsert_conversation AS (     INSERT INTO "perf_5e370ce515dc"."conversations" (       …` |
| 4D103CE8ABD01E23 | 2,000 | 64.2 | 4,000 | 0 | 0 | 0 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM UNNEST($1) AS …` |
| 403F8B358778114D | 2,003 | 17.5 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 2,003 | 10.9 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| CF1D5941D5B56432 | 2,000 | 10.8 | 0 | 0 | 0 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 878A9E463E585A2C | 2,003 | 8.5 | 2,003 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 1CA7E40EFC47C423 | 2,000 | 2.7 | 0 | 0 | 0 | 0 | 0 | `COMMIT` |
| D28E47E4803A0167 | 2,003 | 2.0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 2,003 | 1.1 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 2,003 | 0.9 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| B71A600DFCB91748 | 1 | 0.6 | 237 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 516E8F1759460B47 | 2,003 | 0.3 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |
| 265A210A7402B089 | 1 | 0.2 | 29 | 0 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | sql |
|---|---|---|---|---|
| 2E2DDA1559AE03AD | 2,000 | 4,664,272 | 30,195 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_5e370ce515dc"."messages" (  …` |
| AD398CE96D5C30F8 | 2,000 | 705,363 | 8,091 | `WITH upsert_conversation AS (     INSERT INTO "perf_5e370ce515dc"."conversations" (       …` |
| 4D103CE8ABD01E23 | 2,000 | 0 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM UNNEST($1) AS …` |
| 403F8B358778114D | 2,003 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 2,003 | 0 | 0 | `RESET ALL` |
| CF1D5941D5B56432 | 2,000 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 878A9E463E585A2C | 2,003 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 1CA7E40EFC47C423 | 2,000 | 0 | 0 | `COMMIT` |
| D28E47E4803A0167 | 2,003 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 2,003 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 2,003 | 0 | 0 | `UNLISTEN *` |
| B71A600DFCB91748 | 1 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 516E8F1759460B47 | 2,003 | 0 | 0 | `DISCARD TEMP` |
| 265A210A7402B089 | 1 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| DA559F39F26D405A | 1 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| relationship_projection_rebuild_state | 0 | 0 | 0 | 0 | 0 |
| device_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| messages | 1,999 | 0 | 0 | 0 | 0 |
| command_idempotency_ledger | 0 | 0 | 0 | 0 | 0 |
| conversation_members | 0 | 1,999 | 0 | 1,999 | -1 |
| account_cleanup_jobs | 0 | 0 | 0 | 0 | 0 |
| user_deletion_tombstones | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_items | 0 | 0 | 0 | 0 | 0 |
| friendships | 0 | 0 | 0 | 0 | 0 |
| conversations | 0 | 1,999 | 0 | 1,999 | -29 |
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
| outbox | 1,999 | 0 | 0 | 0 | 0 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |

