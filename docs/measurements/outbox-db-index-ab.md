# OUTBOX-DB-1 索引 A/B（messages 插入写放大）

> 生成时间：2026-08-13 20:56:31 UTC；语料固定、随机种子 20260813；每配置 inbound 2000 条消息，其中约 1/4 携带 reply_to_*+forwarded_from_* 引用；两窗口前均 TRUNCATE messages 从空表起始。

| 配置 | 全局 WAL 字节/消息 | INSERT messages WAL 字节/消息 | INSERT messages WAL 记录/消息 |
|---|---|---|---|
| 索引存在（A） | 2,875 | 2,331 | 15 |
| 索引剔除（B） | 2,691 | 2,332 | 15 |

> 以 pg_stat_statements 语句级 wal_bytes / wal_records 归因，A/B 同容器顺序运行，
> 仅 `ix_messages_reply_to` / `ix_messages_forwarded_from` 两个部分索引的存在性不同；
> INSERT messages 归因匹配 `INSERT INTO ... "messages"` 语句并按 rows 折算。

## A：索引存在（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 5,750,036 | 2,875 |
| WAL 记录 | 42,845 | 21.4 |
| WAL FPI | 9 | 0.00 |
| WAL write | 671 | 0.336 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 2E2DDA1559AE03AD | 2,000 | 300.8 | 2,000 | 7 | 552 | 30,194 | 0 | 4,663,708 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_8aba86b62ba6"."messages" (  …` |
| E11B52EF572B8535 | 1 | 201.4 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| AD398CE96D5C30F8 | 2,000 | 186.8 | 2,000 | 0 | 0 | 8,091 | 0 | 705,363 | `WITH upsert_conversation AS (     INSERT INTO "perf_8aba86b62ba6"."conversations" (       …` |
| 4D103CE8ABD01E23 | 2,000 | 63.2 | 4,000 | 0 | 0 | 0 | 0 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM UNNEST($1) AS …` |
| 403F8B358778114D | 2,003 | 15.4 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 2,003 | 10.6 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| CF1D5941D5B56432 | 2,000 | 8.3 | 0 | 0 | 0 | 0 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 878A9E463E585A2C | 2,003 | 7.5 | 2,003 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 1CA7E40EFC47C423 | 2,000 | 2.1 | 0 | 0 | 0 | 0 | 0 | 0 | `COMMIT` |
| D28E47E4803A0167 | 2,003 | 2.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 2,003 | 1.0 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 2,003 | 0.8 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| B71A600DFCB91748 | 1 | 0.4 | 233 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 265A210A7402B089 | 1 | 0.4 | 29 | 0 | 0 | 4 | 0 | 246 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| 516E8F1759460B47 | 2,003 | 0.2 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| 2E2DDA1559AE03AD | 2,000 | 4,663,708 | 30,194 | 0 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_8aba86b62ba6"."messages" (  …` |
| AD398CE96D5C30F8 | 2,000 | 705,363 | 8,091 | 0 | `WITH upsert_conversation AS (     INSERT INTO "perf_8aba86b62ba6"."conversations" (       …` |
| 265A210A7402B089 | 1 | 246 | 4 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| E11B52EF572B8535 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 4D103CE8ABD01E23 | 2,000 | 0 | 0 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM UNNEST($1) AS …` |
| 403F8B358778114D | 2,003 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 2,003 | 0 | 0 | 0 | `RESET ALL` |
| CF1D5941D5B56432 | 2,000 | 0 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 878A9E463E585A2C | 2,003 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 1CA7E40EFC47C423 | 2,000 | 0 | 0 | 0 | `COMMIT` |
| D28E47E4803A0167 | 2,003 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 2,003 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 2,003 | 0 | 0 | 0 | `UNLISTEN *` |
| B71A600DFCB91748 | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 516E8F1759460B47 | 2,003 | 0 | 0 | 0 | `DISCARD TEMP` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| relationship_projection_rebuild_state | 0 | 0 | 0 | 0 | 0 |
| device_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| messages | 2,000 | 0 | 0 | 0 | 0 |
| command_idempotency_ledger | 0 | 0 | 0 | 0 | 0 |
| conversation_members | 0 | 2,000 | 0 | 2,000 | 0 |
| account_cleanup_jobs | 0 | 0 | 0 | 0 | 0 |
| user_deletion_tombstones | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_items | 0 | 0 | 0 | 0 | 0 |
| friendships | 0 | 0 | 0 | 0 | 0 |
| conversations | 0 | 2,000 | 0 | 2,000 | 11 |
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
| outbox | 2,000 | 0 | 0 | 0 | 0 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |

> HOT 命中率（hot_updates / updates）：conversation_members 2,000/2,000（100%）；conversations 2,000/2,000（100%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。


## B：索引剔除（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 5,382,684 | 2,691 |
| WAL 记录 | 39,913 | 20.0 |
| WAL FPI | 9 | 0.00 |
| WAL write | 671 | 0.336 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 2E2DDA1559AE03AD | 2,000 | 293.1 | 2,000 | 7 | 553 | 30,198 | 0 | 4,664,428 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_8aba86b62ba6"."messages" (  …` |
| E11B52EF572B8535 | 1 | 201.3 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| AD398CE96D5C30F8 | 2,000 | 183.9 | 2,000 | 0 | 0 | 8,091 | 0 | 705,363 | `WITH upsert_conversation AS (     INSERT INTO "perf_8aba86b62ba6"."conversations" (       …` |
| 4D103CE8ABD01E23 | 2,000 | 63.1 | 4,000 | 0 | 0 | 0 | 0 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM UNNEST($1) AS …` |
| 403F8B358778114D | 2,003 | 15.3 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 2,003 | 10.7 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| CF1D5941D5B56432 | 2,000 | 8.7 | 0 | 0 | 0 | 0 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 878A9E463E585A2C | 2,003 | 7.1 | 2,003 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 1CA7E40EFC47C423 | 2,000 | 2.2 | 0 | 0 | 0 | 0 | 0 | 0 | `COMMIT` |
| D28E47E4803A0167 | 2,003 | 2.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| C0D0E27048376284 | 2,003 | 1.0 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| 92049CC8AB443DC6 | 2,003 | 1.0 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| B71A600DFCB91748 | 1 | 0.5 | 239 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 516E8F1759460B47 | 2,003 | 0.2 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |
| 265A210A7402B089 | 1 | 0.2 | 29 | 0 | 0 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| 2E2DDA1559AE03AD | 2,000 | 4,664,428 | 30,198 | 0 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_8aba86b62ba6"."messages" (  …` |
| AD398CE96D5C30F8 | 2,000 | 705,363 | 8,091 | 0 | `WITH upsert_conversation AS (     INSERT INTO "perf_8aba86b62ba6"."conversations" (       …` |
| E11B52EF572B8535 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 4D103CE8ABD01E23 | 2,000 | 0 | 0 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM UNNEST($1) AS …` |
| 403F8B358778114D | 2,003 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 2,003 | 0 | 0 | 0 | `RESET ALL` |
| CF1D5941D5B56432 | 2,000 | 0 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 878A9E463E585A2C | 2,003 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 1CA7E40EFC47C423 | 2,000 | 0 | 0 | 0 | `COMMIT` |
| D28E47E4803A0167 | 2,003 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| C0D0E27048376284 | 2,003 | 0 | 0 | 0 | `UNLISTEN *` |
| 92049CC8AB443DC6 | 2,003 | 0 | 0 | 0 | `CLOSE ALL` |
| B71A600DFCB91748 | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 516E8F1759460B47 | 2,003 | 0 | 0 | 0 | `DISCARD TEMP` |
| 265A210A7402B089 | 1 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| relationship_projection_rebuild_state | 0 | 0 | 0 | 0 | 0 |
| device_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| messages | 2,000 | 0 | 0 | 0 | 0 |
| command_idempotency_ledger | 0 | 0 | 0 | 0 | 0 |
| conversation_members | 0 | 2,000 | 0 | 2,000 | 0 |
| account_cleanup_jobs | 0 | 0 | 0 | 0 | 0 |
| user_deletion_tombstones | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_items | 0 | 0 | 0 | 0 | 0 |
| friendships | 0 | 0 | 0 | 0 | 0 |
| conversations | 0 | 2,000 | 0 | 2,000 | 11 |
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
| outbox | 2,000 | 0 | 0 | 0 | 0 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |

> HOT 命中率（hot_updates / updates）：conversation_members 2,000/2,000（100%）；conversations 2,000/2,000（100%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。

