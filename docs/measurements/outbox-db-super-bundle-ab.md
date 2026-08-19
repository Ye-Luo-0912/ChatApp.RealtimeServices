# OUTBOX-DB-1 合并同事务写入 A/B（super-bundle 单条 CTE）

> 生成时间：2026-08-19 06:38:24 UTC；语料固定、随机种子 20260813；每配置 inbound 2000 条消息。

| 配置 | 热路径 SQL/消息 | SQL 往返/消息 | 总执行时间/消息(ms) | WAL 字节/消息 | WAL 记录/消息 |
|---|---|---|---|---|---|
| A：合并（admission CTE + bundle，2 条数据语句） | 2 | 11.0 | 0.60 | 3,442 | 26.3 |
| B：super-bundle（合并为单条 CTE） | 1 | 8.0 | 0.48 | 2,170 | 16.4 |

> 以 pg_stat_statements 语句级 calls/wal_bytes 归因，A/B 同容器顺序运行、仅写入合并程度不同；
> 确定性收益是每次消息的数据路径 SQL −1（热路径 2 条 → 1 条）：B 把生产合并路径的 admission CTE
> （生命周期/授权/幂等 + 会话与 member 写入 + 序号分配）与 bundle CTE（message + outbox + ledger）
> 合并为单条语句，热路径数据往返 2 → 1；两路径写入行集完全相同（conversations / members / messages /
> outbox / command_idempotency_ledger 各 1 行），故 WAL 列预期接近，收益集中于往返削减；
> SQL 往返/消息含连接会话管理语句，短窗口下随连接池复用有 ±0.1 级波动，以热路径语句数为权威归因。

## A：合并热路径语句（近热路径 = 调用数 ≥ 消息数一半）

| 调用 | 调用/消息 | exec/消息(ms) | wal_records/消息 | wal_bytes/调用 | sql 片段 |
|---|---|---|---|---|---|
| 2,000 | 1.0 | 0.26 | 19.1 | 2,779 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_55a5426383…` |
| 2,000 | 1.0 | 0.20 | 4.0 | 353 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     F…` |
| 2,003 | 1.0 | 0.01 | 0.0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 2,000 | 1.0 | 0.01 | 0.0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 2,003 | 1.0 | 0.01 | 0.0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 2,003 | 1.0 | 0.01 | 0.0 | 0 | `RESET ALL` |
| 2,000 | 1.0 | 0.00 | 0.0 | 0 | `COMMIT` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD SEQUENCES` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `CLOSE ALL` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `UNLISTEN *` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD TEMP` |

## A：合并（admission CTE + bundle）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 6,884,080 | 3,442 |
| WAL 记录 | 52,510 | 26.3 |
| WAL FPI | 0 | 0.00 |
| WAL write | 779 | 0.390 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 2E2DDA1559AE03AD | 2,000 | 527.4 | 2,000 | 0 | 603 | 38,230 | 0 | 5,559,256 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_55a54263834f"."messages" (  …` |
| 9ADAC1E78D13A1E2 | 2,000 | 407.2 | 2,000 | 0 | 0 | 8,091 | 0 | 706,943 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| E11B52EF572B8535 | 1 | 202.5 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 403F8B358778114D | 2,003 | 22.3 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| CF1D5941D5B56432 | 2,000 | 14.0 | 0 | 0 | 0 | 0 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 878A9E463E585A2C | 2,003 | 12.7 | 2,003 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 4040643821AD66AF | 2,003 | 11.6 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 1CA7E40EFC47C423 | 2,000 | 3.5 | 0 | 0 | 0 | 0 | 0 | 0 | `COMMIT` |
| D28E47E4803A0167 | 2,003 | 2.5 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 2,003 | 1.6 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 2,003 | 1.1 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| B71A600DFCB91748 | 1 | 0.8 | 236 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 516E8F1759460B47 | 2,003 | 0.4 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |
| 265A210A7402B089 | 1 | 0.3 | 29 | 0 | 0 | 3 | 0 | 181 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| 2E2DDA1559AE03AD | 2,000 | 5,559,256 | 38,230 | 0 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_55a54263834f"."messages" (  …` |
| 9ADAC1E78D13A1E2 | 2,000 | 706,943 | 8,091 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| 265A210A7402B089 | 1 | 181 | 3 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| E11B52EF572B8535 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 403F8B358778114D | 2,003 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| CF1D5941D5B56432 | 2,000 | 0 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 878A9E463E585A2C | 2,003 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 4040643821AD66AF | 2,003 | 0 | 0 | 0 | `RESET ALL` |
| 1CA7E40EFC47C423 | 2,000 | 0 | 0 | 0 | `COMMIT` |
| D28E47E4803A0167 | 2,003 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 2,003 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 2,003 | 0 | 0 | 0 | `UNLISTEN *` |
| B71A600DFCB91748 | 1 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 516E8F1759460B47 | 2,003 | 0 | 0 | 0 | `DISCARD TEMP` |
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
| conversations | 0 | 2,000 | 0 | 2,000 | 21 |
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


## B：super-bundle 热路径语句（近热路径 = 调用数 ≥ 消息数一半）

| 调用 | 调用/消息 | exec/消息(ms) | wal_records/消息 | wal_bytes/调用 | sql 片段 |
|---|---|---|---|---|---|
| 2,000 | 1.0 | 0.36 | 24.2 | 3,299 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     F…` |
| 2,003 | 1.0 | 0.01 | 0.0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 2,003 | 1.0 | 0.01 | 0.0 | 0 | `RESET ALL` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD SEQUENCES` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `CLOSE ALL` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `UNLISTEN *` |
| 2,003 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD TEMP` |

## B：super-bundle（合并同事务写入为单条 CTE）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 4,340,579 | 2,170 |
| WAL 记录 | 32,796 | 16.4 |
| WAL FPI | 0 | 0.00 |
| WAL write | 817 | 0.408 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| F441132799D72C49 | 2,000 | 716.3 | 2,000 | 0 | 628 | 48,405 | 0 | 6,598,832 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| E11B52EF572B8535 | 1 | 201.2 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 403F8B358778114D | 2,003 | 18.0 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 2,003 | 10.3 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 2,003 | 9.0 | 2,003 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| D28E47E4803A0167 | 2,003 | 2.0 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD SEQUENCES` |
| 92049CC8AB443DC6 | 2,003 | 1.1 | 0 | 0 | 0 | 0 | 0 | 0 | `CLOSE ALL` |
| C0D0E27048376284 | 2,003 | 1.1 | 0 | 0 | 0 | 0 | 0 | 0 | `UNLISTEN *` |
| B71A600DFCB91748 | 1 | 0.6 | 240 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 516E8F1759460B47 | 2,003 | 0.3 | 0 | 0 | 0 | 0 | 0 | 0 | `DISCARD TEMP` |
| 265A210A7402B089 | 1 | 0.2 | 29 | 0 | 0 | 0 | 0 | 0 | `SELECT relname, COALESCE(n_tup_ins, $2), COALESCE(n_tup_upd, $3), COALESCE(n_tup_del, $4),…` |
| DA559F39F26D405A | 1 | 0.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT COALESCE(wal_records, $1), COALESCE(wal_fpi, $2), COALESCE(wal_bytes, $3),        C…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| F441132799D72C49 | 2,000 | 6,598,832 | 48,405 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| E11B52EF572B8535 | 1 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 403F8B358778114D | 2,003 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 4040643821AD66AF | 2,003 | 0 | 0 | 0 | `RESET ALL` |
| 878A9E463E585A2C | 2,003 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
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
| conversations | 0 | 2,000 | 0 | 2,000 | -28 |
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

