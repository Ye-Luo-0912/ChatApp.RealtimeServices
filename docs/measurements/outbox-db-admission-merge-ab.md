# OUTBOX-DB-1 admission 合并 A/B（减少重复读取/往返）

> 生成时间：2026-08-13 21:38:38 UTC；语料固定、随机种子 20260813；每配置 inbound 2000 条消息。

| 配置 | 热路径 SQL/消息 | SQL 往返/消息 | 总执行时间/消息(ms) | WAL 字节/消息 | WAL 记录/消息 |
|---|---|---|---|---|---|
| A：fallback（lifecycle admission + 序号分配 + bundle） | 3 | 12.0 | 0.44 | 2,910 | 21.7 |
| B：合并（admission+序号分配单条 CTE + bundle） | 2 | 11.0 | 0.51 | 2,992 | 22.2 |

> 以 pg_stat_statements 语句级 calls/wal_bytes 归因，A/B 同容器顺序运行、仅 admission/序号分配路径不同；
> 确定性收益是每次消息的 SQL 往返 −1（热路径 3 条 → 2 条）：B 把「生命周期读取 + canonical 账本读取 +
> 事务内授权 + 会话序号分配」合并为单条 CTE，消除 fallback 的独立 `upsert_conversation` 序号分配往返；
> WAL 列不可直接对比：B 额外承担幂等账本 canonical 插入（`command_idempotency_ledger` 每消息 1 行，A 完全没有），
> 且短窗口下 WAL 字节/消息随容器页分配有 ±10% 级波动，故以语句级 calls 与热路径语句数为权威归因。

## A：fallback 热路径语句（近热路径 = 调用数 ≥ 消息数一半）

| 调用 | 调用/消息 | exec/消息(ms) | wal_records/消息 | wal_bytes/调用 | sql 片段 |
|---|---|---|---|---|---|
| 2,000 | 1.0 | 0.17 | 15.1 | 2,317 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_c035260829…` |
| 2,000 | 1.0 | 0.11 | 4.0 | 352 | `WITH upsert_conversation AS (     INSERT INTO "perf_c03526082967"."conve…` |
| 2,000 | 1.0 | 0.04 | 0.0 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     F…` |
| 2,003 | 1.0 | 0.01 | 0.0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 2,003 | 1.0 | 0.01 | 0.0 | 0 | `RESET ALL` |
| 2,000 | 1.0 | 0.01 | 0.0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 2,000 | 1.0 | 0.00 | 0.0 | 0 | `COMMIT` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD SEQUENCES` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `CLOSE ALL` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `UNLISTEN *` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD TEMP` |

## A：fallback（独立 SQL 更多）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 5,821,896 | 2,911 |
| WAL 记录 | 43,384 | 21.7 |
| WAL FPI | 0 | 0.00 |
| WAL write | 666 | 0.333 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 2E2DDA1559AE03AD | 2,000 | 337.9 | 2,000 | 0 | 528 | 30,190 | 0 | 4,634,210 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_c03526082967"."messages" (  …` |
| AD398CE96D5C30F8 | 2,000 | 212.1 | 2,000 | 0 | 0 | 8,091 | 0 | 705,363 | `WITH upsert_conversation AS (     INSERT INTO "perf_c03526082967"."conversations" (       …` |
| E11B52EF572B8535 | 1 | 201.3 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 4D103CE8ABD01E23 | 2,000 | 76.1 | 4,000 | 0 | 0 | 0 | 0 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM UNNEST($1) AS …` |
| 403F8B358778114D | 2,003 | 18.2 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 2,003 | 11.3 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| CF1D5941D5B56432 | 2,000 | 11.2 | 0 | 0 | 0 | 0 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 878A9E463E585A2C | 2,003 | 9.5 | 2,003 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 1CA7E40EFC47C423 | 2,000 | 2.6 | 0 | 0 | 0 | 0 | 0 | 0 | `COMMIT` |
| D28E47E4803A0167 | 2,003 | 2.1 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 2,003 | 1.1 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 2,003 | 1.0 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| B71A600DFCB91748 | 1 | 0.6 | 236 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 516E8F1759460B47 | 2,003 | 0.3 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |
| 265A210A7402B089 | 1 | 0.2 | 29 | 0 | 0 | 3 | 0 | 181 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| 2E2DDA1559AE03AD | 2,000 | 4,634,210 | 30,190 | 0 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_c03526082967"."messages" (  …` |
| AD398CE96D5C30F8 | 2,000 | 705,363 | 8,091 | 0 | `WITH upsert_conversation AS (     INSERT INTO "perf_c03526082967"."conversations" (       …` |
| 265A210A7402B089 | 1 | 181 | 3 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
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


## B：合并热路径语句（近热路径 = 调用数 ≥ 消息数一半）

| 调用 | 调用/消息 | exec/消息(ms) | wal_records/消息 | wal_bytes/调用 | sql 片段 |
|---|---|---|---|---|---|
| 2,000 | 1.0 | 0.22 | 19.1 | 2,863 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_c035260829…` |
| 2,000 | 1.0 | 0.16 | 4.0 | 352 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     F…` |
| 2,003 | 1.0 | 0.01 | 0.0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 2,003 | 1.0 | 0.01 | 0.0 | 0 | `RESET ALL` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 2,000 | 1.0 | 0.00 | 0.0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 2,000 | 1.0 | 0.00 | 0.0 | 0 | `COMMIT` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD SEQUENCES` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `CLOSE ALL` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `UNLISTEN *` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD TEMP` |

## B：合并（admission+序号分配单条 CTE）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 5,984,074 | 2,992 |
| WAL 记录 | 44,473 | 22.2 |
| WAL FPI | 0 | 0.00 |
| WAL write | 800 | 0.400 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 2E2DDA1559AE03AD | 2,000 | 431.6 | 2,000 | 0 | 629 | 38,245 | 0 | 5,726,354 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_c03526082967"."messages" (  …` |
| 9ADAC1E78D13A1E2 | 2,000 | 326.8 | 2,000 | 0 | 0 | 8,091 | 0 | 705,363 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| E11B52EF572B8535 | 1 | 201.3 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 403F8B358778114D | 2,003 | 18.9 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 2,003 | 11.9 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 2,003 | 10.0 | 2,003 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| CF1D5941D5B56432 | 2,000 | 9.6 | 0 | 0 | 0 | 0 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 1CA7E40EFC47C423 | 2,000 | 2.6 | 0 | 0 | 0 | 0 | 0 | 0 | `COMMIT` |
| D28E47E4803A0167 | 2,003 | 2.2 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 2,003 | 1.2 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 2,003 | 1.1 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| B71A600DFCB91748 | 1 | 0.5 | 240 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 516E8F1759460B47 | 2,003 | 0.3 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |
| 265A210A7402B089 | 1 | 0.2 | 29 | 0 | 0 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| 2E2DDA1559AE03AD | 2,000 | 5,726,354 | 38,245 | 0 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_c03526082967"."messages" (  …` |
| 9ADAC1E78D13A1E2 | 2,000 | 705,363 | 8,091 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| E11B52EF572B8535 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 403F8B358778114D | 2,003 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 2,003 | 0 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 2,003 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| CF1D5941D5B56432 | 2,000 | 0 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 1CA7E40EFC47C423 | 2,000 | 0 | 0 | 0 | `COMMIT` |
| D28E47E4803A0167 | 2,003 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 2,003 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 2,003 | 0 | 0 | 0 | `UNLISTEN *` |
| B71A600DFCB91748 | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 516E8F1759460B47 | 2,003 | 0 | 0 | 0 | `DISCARD TEMP` |
| 265A210A7402B089 | 1 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| DA559F39F26D405A | 1 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| relationship_projection_rebuild_state | 0 | 0 | 0 | 0 | 0 |
| device_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| messages | 2,000 | 0 | 0 | 0 | 0 |
| command_idempotency_ledger | 2,000 | 0 | 0 | 0 | 0 |
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

